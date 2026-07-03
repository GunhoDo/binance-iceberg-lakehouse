"""ws_to_kafka.py

Binance 공개 WebSocket → Kafka producer. `csv_to_kafka.py`(통제 리플레이 프로듀서)의
실시간 짝(sibling)이다. 같은 토픽(`trades`/`klines`)·같은 레코드 스키마·같은 key
컨벤션을 사용하므로 downstream Spark 스트림이 리플레이/라이브를 구분 없이 소비한다.

PRD(v2) P0 "실시간化" / FR-1. 헤드라인은 통제 리플레이 벤치이고, 이 WebSocket 경로는
"실시간 수집 + 품질 이상탐지 데모" 용도다 (docs/PRD.md §3.2, docs/deep-interview-scope-v2.md).

설계 근거:
    - **combined stream URL로 구독** → 클라이언트가 subscribe 제어 메시지를 보내지 않으므로
      "5 incoming msg/s" 제한을 구조적으로 회피한다 (수신 트래픽에는 제한 없음).
    - **연결당 1024 stream 제한**: (symbols × streams) 조합이 초과하면 즉시 실패시킨다.
    - **지수 백오프 + jitter 재연결**: Binance는 24h마다 연결을 끊고, 네트워크 단절도 잦다.
    - **exchange_event_time / ingest_time 보존**: P2 end-to-end lag 계측의 입력.
    - **검증 전 성능 미주장 원칙**: 여기서는 어떤 lag 수치도 주장하지 않는다. 수집만 한다.

레코드 스키마(csv_to_kafka 파생 + 라이브 메타):
    trades : trade_id, price, qty, quote_qty, time, is_buyer_maker, is_best_match, symbol,
             event_type, exchange_event_time, ingest_time, source
    klines : open_time, open, high, low, close, volume, close_time, quote_volume,
             number_of_trades, taker_buy_base_volume, taker_buy_quote_volume, ignore,
             symbol, interval, is_closed, event_type, exchange_event_time, ingest_time, source

사용법:
    # 실시간 수집 (BTCUSDT aggTrade + kline_1m → Kafka)
    python infra/ws_to_kafka.py --symbols BTCUSDT

    # 여러 심볼 / 스트림 선택
    python infra/ws_to_kafka.py --symbols BTCUSDT,ETHUSDT --streams aggTrade,kline_1m

    # Kafka 없이 payload 매핑만 확인 (네트워크만 사용)
    python infra/ws_to_kafka.py --symbols BTCUSDT --dry-run

의존성:
    pip install websocket-client kafka-python-ng
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import time
from typing import Any, Callable, Optional

# --- 상수 --------------------------------------------------------------------

KAFKA_BOOTSTRAP = "localhost:9092"

# 공개 combined-stream 엔드포인트 (무인증). 데이터 전용 도메인.
WS_BASE_URL = "wss://data-stream.binance.vision/stream?streams="

# Binance: 연결당 최대 1024 stream.
MAX_STREAMS_PER_CONNECTION = 1024

# 지원 stream 종류 → (Kafka topic, event_type)
SUPPORTED_STREAMS = {
    "aggTrade": ("trades", "aggTrade"),
    "kline_1m": ("klines", "kline"),
}

# 재연결 백오프 (초)
BACKOFF_BASE = 1.0
BACKOFF_MAX = 60.0

# WebSocketApp이 서버 ping에 자동 pong 응답. 우리가 주기적 ping도 보내 좀비 연결을 조기 감지.
PING_INTERVAL = 180
PING_TIMEOUT = 10


# --- payload → record 매핑 (순수 함수, 네트워크 불요 · 단위테스트 대상) ---------


def _now_ms() -> int:
    return int(time.time() * 1000)


def build_stream_url(symbols: list[str], streams: list[str]) -> str:
    """symbols × streams 조합으로 combined-stream URL을 만든다.

    Binance stream 이름은 소문자 심볼이다: `btcusdt@aggTrade`, `btcusdt@kline_1m`.
    """
    if not symbols:
        raise ValueError("최소 1개 이상의 심볼이 필요합니다.")

    bad = [s for s in streams if s not in SUPPORTED_STREAMS]
    if bad:
        raise ValueError(f"지원하지 않는 stream: {bad}. 지원: {sorted(SUPPORTED_STREAMS)}")

    parts = [f"{sym.lower()}@{stream}" for sym in symbols for stream in streams]

    if len(parts) > MAX_STREAMS_PER_CONNECTION:
        raise ValueError(
            f"stream 조합 {len(parts)}개가 연결당 한도 {MAX_STREAMS_PER_CONNECTION}개를 초과합니다."
        )

    return WS_BASE_URL + "/".join(parts)


def agg_trade_to_record(data: dict[str, Any], ingest_time: int) -> dict[str, Any]:
    """aggTrade payload → `trades` 토픽 레코드.

    공통 필드는 **CSV 리플레이(csv_to_kafka)와 동일한 문자열 타입**으로 방출한다.
    downstream(`01_build_processed_trades.py`)이 `from_json`을 전부 StringType으로
    선언하고 bool을 `== "True"` 문자열 비교로 처리하기 때문에, WS의 JSON 원시 타입
    (int/bool)을 그대로 흘리면 `is_buyer_maker`가 조용히 false로 오염된다.
    `str(True) == "True"`라 stringify하면 CSV·WS가 동일 downstream을 통과한다.
    (메타 필드 exchange_event_time/ingest_time은 스키마 밖이라 무시되므로 native 유지.)
    """
    price = data["p"]
    qty = data["q"]
    return {
        "trade_id": str(data["a"]),  # aggregate trade id (CSV와 동일하게 문자열)
        "price": price,
        "qty": qty,
        "quote_qty": _quote_qty(price, qty),  # aggTrade엔 없음 → price*qty 산출
        "time": str(data["T"]),  # trade time (ms)
        "is_buyer_maker": str(data["m"]),  # bool → "True"/"False" (downstream =="True")
        "is_best_match": str(data.get("M", True)),
        "symbol": data["s"],
        "event_type": "aggTrade",
        "exchange_event_time": data["E"],  # event time (ms) — P2 lag 입력 (native)
        "ingest_time": ingest_time,
        "source": "websocket",
    }


def kline_to_record(data: dict[str, Any], ingest_time: int) -> dict[str, Any]:
    """kline payload → `klines` 토픽 레코드.

    trades와 동일 이유로 공통 필드를 CSV와 같은 문자열로 방출한다. downstream
    (`02_build_processed_klines.py`)이 `open_time`/`close_time`/`number_of_trades`를
    StringType 파싱 후 `cast("long")` 하므로 JSON int를 그대로 흘리면 안 된다.
    `is_closed`(WS 전용, 캔들 확정 flag `x`)는 downstream 스키마에 없어 무시된다(additive).
    """
    k = data["k"]
    return {
        "open_time": str(k["t"]),
        "open": k["o"],
        "high": k["h"],
        "low": k["l"],
        "close": k["c"],
        "volume": k["v"],
        "close_time": str(k["T"]),
        "quote_volume": k["q"],
        "number_of_trades": str(k["n"]),
        "taker_buy_base_volume": k["V"],
        "taker_buy_quote_volume": k["Q"],
        "ignore": k.get("B", "0"),
        "symbol": k["s"],
        "interval": k["i"],
        "is_closed": k["x"],  # 캔들 확정 여부 (라이브 전용, downstream 무시)
        "event_type": "kline",
        "exchange_event_time": data["E"],
        "ingest_time": ingest_time,
        "source": "websocket",
    }


def _quote_qty(price: str, qty: str) -> str:
    try:
        return repr(float(price) * float(qty))
    except (TypeError, ValueError):
        return ""


# event_type → (topic, key 생성함수, record 생성함수)
def _trade_key(rec: dict[str, Any]) -> str:
    return rec["symbol"]


def _kline_key(rec: dict[str, Any]) -> str:
    return f"{rec['symbol']}_{rec['interval']}"


_DISPATCH: dict[str, tuple[str, Callable, Callable]] = {
    "aggTrade": ("trades", _trade_key, agg_trade_to_record),
    "kline": ("klines", _kline_key, kline_to_record),
}


def route_payload(
    data: dict[str, Any], ingest_time: Optional[int] = None
) -> Optional[tuple[str, str, dict[str, Any]]]:
    """단일 stream data payload → (topic, key, record). 미지원 이벤트면 None.

    combined-stream 프레임은 {"stream": ..., "data": {...}} 이므로 호출측이 data만 넘긴다.
    """
    event_type = data.get("e")
    entry = _DISPATCH.get(event_type)
    if entry is None:
        return None
    topic, key_fn, record_fn = entry
    if ingest_time is None:
        ingest_time = _now_ms()
    record = record_fn(data, ingest_time)
    return topic, key_fn(record), record


def parse_frame(raw: str) -> Optional[dict[str, Any]]:
    """combined-stream 원문 프레임 → 내부 data dict. 형식 불명이면 None."""
    try:
        msg = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if isinstance(msg, dict) and "data" in msg and isinstance(msg["data"], dict):
        return msg["data"]  # combined-stream 래핑
    if isinstance(msg, dict) and "e" in msg:
        return msg  # 단일 raw stream (fallback)
    return None


# --- 실행 루프 ---------------------------------------------------------------


class BinanceWsIngestor:
    """WebSocket 연결 → Kafka 발행. 지수 백오프로 무한 재연결한다."""

    def __init__(
        self,
        symbols: list[str],
        streams: list[str],
        bootstrap: str = KAFKA_BOOTSTRAP,
        dry_run: bool = False,
    ) -> None:
        self.symbols = symbols
        self.streams = streams
        self.url = build_stream_url(symbols, streams)
        self.bootstrap = bootstrap
        self.dry_run = dry_run

        self._producer = None
        self._app = None
        self._stop = False
        self._retries = 0
        self._count = 0

    # -- Kafka --------------------------------------------------------------

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
                linger_ms=20,
            )
        return self._producer

    def _publish(self, topic: str, key: str, record: dict[str, Any]) -> None:
        self._count += 1
        if self.dry_run:
            if self._count <= 5 or self._count % 1000 == 0:
                print(f"[dry-run] {topic} key={key} {json.dumps(record)}")
            return
        self._get_producer().send(topic=topic, key=key, value=record)
        if self._count % 1000 == 0:
            print(f"[ws] produced {self._count} messages (last topic={topic})")

    # -- WebSocket 콜백 -----------------------------------------------------

    def _on_message(self, _ws, raw: str) -> None:
        data = parse_frame(raw)
        if data is None:
            return
        routed = route_payload(data)
        if routed is None:
            return
        topic, key, record = routed
        self._publish(topic, key, record)

    def _on_open(self, _ws) -> None:
        self._retries = 0  # 정상 연결 → 백오프 리셋
        print(f"[ws] connected: {len(self.symbols)}심볼 × {self.streams}")

    def _on_error(self, _ws, error) -> None:
        print(f"[ws] error: {error}")

    def _on_close(self, _ws, code, msg) -> None:
        print(f"[ws] closed: code={code} msg={msg}")

    # -- 재연결 루프 --------------------------------------------------------

    def _backoff_sleep(self) -> None:
        self._retries += 1
        delay = min(BACKOFF_BASE * (2 ** (self._retries - 1)), BACKOFF_MAX)
        delay += random.uniform(0, delay * 0.25)  # jitter (thundering herd 방지)
        print(f"[ws] reconnect in {delay:.1f}s (attempt {self._retries})")
        # 인터럽트 응답성을 위해 잘게 나눠 잔다.
        slept = 0.0
        while slept < delay and not self._stop:
            time.sleep(min(0.5, delay - slept))
            slept += 0.5

    def stop(self, *_args) -> None:
        self._stop = True
        print("[ws] stop requested — draining producer")
        # run_forever는 블로킹이므로 소켓을 능동적으로 닫아 루프를 깨운다.
        if self._app is not None:
            try:
                self._app.close()
            except Exception:  # noqa: BLE001
                pass

    def run(self) -> None:
        import websocket  # websocket-client

        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        print(f"[ws] url={self.url}")
        while not self._stop:
            app = websocket.WebSocketApp(
                self.url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._app = app
            try:
                app.run_forever(ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT)
            except Exception as exc:  # noqa: BLE001 — 어떤 연결 오류든 재연결로 흡수
                print(f"[ws] run_forever raised: {exc}")
            if self._stop:
                break
            self._backoff_sleep()

        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
        print(f"[ws] shutdown. total {self._count} messages produced.")


# --- CLI ---------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Binance WebSocket → Kafka ingestor (P0 / FR-1)")
    parser.add_argument("--symbols", default="BTCUSDT", help="쉼표 구분 심볼 (예: BTCUSDT,ETHUSDT)")
    parser.add_argument(
        "--streams",
        default="aggTrade,kline_1m",
        help=f"쉼표 구분 stream. 지원: {','.join(sorted(SUPPORTED_STREAMS))}",
    )
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP, help="Kafka bootstrap servers")
    parser.add_argument("--dry-run", action="store_true", help="Kafka 발행 없이 매핑 결과만 출력")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    streams = [s.strip() for s in args.streams.split(",") if s.strip()]

    ingestor = BinanceWsIngestor(
        symbols=symbols,
        streams=streams,
        bootstrap=args.bootstrap,
        dry_run=args.dry_run,
    )
    ingestor.run()


if __name__ == "__main__":
    main()
