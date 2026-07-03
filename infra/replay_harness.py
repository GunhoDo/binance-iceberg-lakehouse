"""replay_harness.py

통제 리플레이 하니스 (PRD v2 P1 / FR-2). 과거 Binance 덤프(CSV)를 **"정확히 초당 N건"**
고정 rate로 Kafka에 발행한다. lag 벤치마크(P2/P3)의 재현 가능한 부하 소스다.

`csv_to_kafka.py`와의 차이:
    - csv_to_kafka: 메시지 간 naive `--delay` sleep (rate가 부정확·비재현).
    - replay_harness: 목표 rate를 시간 슬라이스 단위로 pacing → 실제 rate가 목표에 수렴.
      각 메시지에 **produce_ts(발행 wall-clock, ms)**를 심어 end-to-end lag의 기준점을 만든다.

lag 정의(벤치):
    리플레이 데이터의 원본 event 시각(`time`, 2024년)은 lag 기준으로 무의미하다. 통제
    리플레이에서 측정하는 것은 **파이프라인 지연** = `iceberg_commit_ts − produce_ts`
    (주입 시점→Iceberg 커밋). produce_ts를 페이로드에 실어 P2에서 소비한다.

재현성:
    같은 파일 · 같은 `--rate` · 같은 `--limit`이면 발행 순서·건수가 결정론적이다.
    이 파라미터를 커밋하면 제3자가 동일 벤치 부하를 재생할 수 있다(NFR 재현성).

레코드 스키마: csv_to_kafka/ws_to_kafka와 동일한 문자열 공통 필드(decisions.md D21) +
    produce_ts(int, ms) + source="replay". produce_ts/source는 processed 파이프라인 스키마
    밖이라 무시되고, P2 lag 잡이 별도 스키마로 소비한다.

사용법:
    # 초당 5000건으로 10만 건 발행 (벤치 1회분)
    python infra/replay_harness.py --file data/raw/BTCUSDT-trades-2024-01-01.csv \\
        --rate 5000 --limit 100000

    # Kafka 없이 pacing/스키마만 확인
    python infra/replay_harness.py --file <csv> --rate 1000 --limit 5000 --dry-run

의존성:
    pip install kafka-python-ng
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from typing import Any, Iterator, Optional

KAFKA_BOOTSTRAP = "localhost:9092"

# csv_to_kafka와 동일 (data.binance.vision trades 포맷, 헤더 없음)
TRADES_COLUMNS = [
    "trade_id",
    "price",
    "qty",
    "quote_qty",
    "time",
    "is_buyer_maker",
    "is_best_match",
]

# rate pacing 시간 슬라이스 (초). 슬라이스마다 rate*slice건 발행 후 슬라이스 경계까지 sleep.
PACING_SLICE_SEC = 0.05


def _now_ms() -> int:
    return int(time.time() * 1000)


# --- 순수 함수 (네트워크 불요 · 단위테스트 대상) --------------------------------


def build_trade_record(row: list[str], symbol: str, produce_ts: int) -> Optional[dict[str, Any]]:
    """trades CSV 한 행 → Kafka 레코드. 컬럼 수가 안 맞으면 None(헤더/깨진 행 스킵)."""
    if len(row) != len(TRADES_COLUMNS):
        return None
    rec: dict[str, Any] = dict(zip(TRADES_COLUMNS, row))  # 전부 문자열 (D21 규약)
    rec["symbol"] = symbol
    rec["produce_ts"] = produce_ts  # 발행 시각 (ms) — P2 lag 기준점
    rec["source"] = "replay"
    return rec


def slice_batch_size(rate: int, slice_sec: float = PACING_SLICE_SEC) -> int:
    """슬라이스당 발행할 메시지 수. 최소 1건 보장."""
    return max(1, round(rate * slice_sec))


class RatePacer:
    """시간 슬라이스 경계에 맞춰 sleep하여 목표 rate에 수렴시킨다.

    perf_counter 기반 절대 스케줄(누적 드리프트 방지). 실제 달성 rate는 호출측이 별도 측정.
    """

    def __init__(self, rate: int, slice_sec: float = PACING_SLICE_SEC) -> None:
        self.rate = rate
        self.slice_sec = slice_sec
        self.batch = slice_batch_size(rate, slice_sec)
        self._next_tick = time.perf_counter()

    def wait(self) -> None:
        """다음 슬라이스 경계까지 대기."""
        self._next_tick += self.slice_sec
        sleep = self._next_tick - time.perf_counter()
        if sleep > 0:
            time.sleep(sleep)
        else:
            # 발행이 슬라이스보다 느림 → 스케줄 재동기화(밀린 시간 흡수)
            self._next_tick = time.perf_counter()


def iter_rows(file_path: str, limit: Optional[int]) -> Iterator[list[str]]:
    with open(file_path, newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if limit is not None and i >= limit:
                break
            yield row


# --- 실행 -------------------------------------------------------------------


class ReplayHarness:
    def __init__(
        self,
        file_path: str,
        topic: str,
        symbol: str,
        rate: int,
        limit: Optional[int],
        bootstrap: str = KAFKA_BOOTSTRAP,
        dry_run: bool = False,
    ) -> None:
        self.file_path = file_path
        self.topic = topic
        self.symbol = symbol
        self.rate = rate
        self.limit = limit
        self.bootstrap = bootstrap
        self.dry_run = dry_run
        self._producer = None

    def _get_producer(self):
        if self.dry_run:
            return None
        if self._producer is None:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                linger_ms=5,
            )
        return self._producer

    def run(self) -> dict[str, Any]:
        pacer = RatePacer(self.rate)
        sent = 0
        skipped = 0
        wall_start = time.perf_counter()

        rows = iter_rows(self.file_path, self.limit)
        exhausted = False
        while not exhausted:
            # 한 슬라이스 분량 발행
            for _ in range(pacer.batch):
                try:
                    row = next(rows)
                except StopIteration:
                    exhausted = True
                    break
                rec = build_trade_record(row, self.symbol, _now_ms())
                if rec is None:
                    skipped += 1
                    continue
                if self.dry_run:
                    if sent < 3:
                        print(f"[dry-run] {self.topic} key={self.symbol} {json.dumps(rec)}")
                else:
                    self._get_producer().send(self.topic, key=self.symbol, value=rec)
                sent += 1
            if not exhausted:
                pacer.wait()

        if self._producer is not None:
            self._producer.flush()
            self._producer.close()

        elapsed = time.perf_counter() - wall_start
        actual_rate = sent / elapsed if elapsed > 0 else 0.0
        summary = {
            "sent": sent,
            "skipped": skipped,
            "target_rate": self.rate,
            "actual_rate": round(actual_rate, 1),
            "elapsed_sec": round(elapsed, 2),
        }
        print(
            f"[replay] done. sent={sent} skipped={skipped} "
            f"target={self.rate}/s actual={summary['actual_rate']}/s "
            f"elapsed={summary['elapsed_sec']}s"
        )
        return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="통제 리플레이 하니스 (P1 / FR-2)")
    parser.add_argument("--file", required=True, help="trades CSV 경로 (헤더 없음)")
    parser.add_argument("--topic", default="trades")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--rate", type=int, default=5000, help="목표 초당 발행 건수")
    parser.add_argument("--limit", type=int, default=None, help="총 발행 건수 상한(벤치 1회분)")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--dry-run", action="store_true", help="Kafka 없이 pacing/스키마만 확인")
    args = parser.parse_args()

    harness = ReplayHarness(
        file_path=args.file,
        topic=args.topic,
        symbol=args.symbol,
        rate=args.rate,
        limit=args.limit,
        bootstrap=args.bootstrap,
        dry_run=args.dry_run,
    )
    harness.run()


if __name__ == "__main__":
    main()
