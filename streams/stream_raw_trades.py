"""stream_raw_trades.py

Kafka topic `trades` → Iceberg table `raw_trades` (append-only).

PRD §10.1, §14.1 참조.

저장 대상 (PRD §10.1):
    Kafka topic, partition, offset, timestamp, message key, message value, ingest_time

본 스켈레톤은 Spark Structured Streaming 골격만 두며 실제 trigger interval,
checkpoint 위치, target file size는 Phase 1에서 config로 결정한다
(`docs/decisions.md` D9).
"""

from __future__ import annotations

from jobs.common.spark_session import get_spark


def build_stream():
    """Kafka source → raw_trades 적재 stream을 정의한다.

    원칙:
    - Raw Zone은 append-only다 (`docs/decisions.md` D7).
    - Kafka 메타데이터 (topic, partition, offset, timestamp) 를 보존한다.
    - 메시지 value는 binary 그대로 보관할지, 1차 파싱 후 보관할지 Phase 1에서 결정.
      현재는 결정을 미루고 골격만 둔다.
    """
    raise NotImplementedError(
        "Phase 1: source format / value parsing 정책 결정 후 구현"
    )


def run() -> None:
    spark = get_spark("stream_raw_trades")  # noqa: F841 (Phase 1까지 사용 보류)
    raise NotImplementedError


if __name__ == "__main__":
    run()
