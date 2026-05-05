"""stream_raw_orders.py

Kafka topic `orders` → Iceberg table `raw_orders` (append-only).

PRD §10.2, §14.1 참조.

저장 대상 컬럼은 PRD §10.2 참조. 이 table의 source는 simulator이며 실데이터가
아니라는 사실을 잊지 말 것 (PRD §2, §6.3).
"""

from __future__ import annotations

from jobs.common.spark_session import get_spark


def build_stream():
    """Kafka source → raw_orders 적재 stream을 정의한다."""
    raise NotImplementedError(
        "Phase 1: source format / value parsing 정책 결정 후 구현"
    )


def run() -> None:
    spark = get_spark("stream_raw_orders")  # noqa: F841
    raise NotImplementedError


if __name__ == "__main__":
    run()
