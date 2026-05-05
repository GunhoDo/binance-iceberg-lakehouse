"""stream_raw_klines.py

Kafka topic `klines` → Iceberg table `raw_klines` (append-only).

PRD §10.1, §6.2, §14.1 참조.

설계 노트:
- 같은 (symbol, interval, open_time) 의 반복 이벤트도 모두 append한다.
- upsert-like 처리는 raw가 아니라 processed 책임이다 (`docs/decisions.md` D7).
"""

from __future__ import annotations

from jobs.common.spark_session import get_spark


def build_stream():
    """Kafka source → raw_klines 적재 stream을 정의한다."""
    raise NotImplementedError(
        "Phase 1: source format / value parsing 정책 결정 후 구현"
    )


def run() -> None:
    spark = get_spark("stream_raw_klines")  # noqa: F841
    raise NotImplementedError


if __name__ == "__main__":
    run()
