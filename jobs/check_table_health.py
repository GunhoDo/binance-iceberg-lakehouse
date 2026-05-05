"""check_table_health.py

Iceberg metadata 기반 운영 지표를 측정해 table_health_summary 에 append.

지표 (PRD §13.4):
- table name
- snapshot count
- file count
- average file size
- small file count
- total record count
- last commit time
- last compaction time
- compaction needed flag

설계 노트:
- Iceberg metadata table (`<catalog>.<db>.<table>.snapshots`,
  `<catalog>.<db>.<table>.files` 등) 을 조회한다.
- compaction needed flag 결정 기준은 PRD §13.5의 임계값을 사용하되, 그 임계값은
  초기값이며 운영하며 조정한다 (`docs/decisions.md` D9).
"""

from __future__ import annotations

from jobs.common.spark_session import get_spark


# PRD §11에 정의된 본 프로젝트의 Iceberg table 목록.
# 본 job은 이 table들을 순회하며 health 지표를 측정한다.
TARGET_TABLES = [
    "glue.binance_lakehouse.processed_trades",
    "glue.binance_lakehouse.processed_klines",
    "glue.binance_lakehouse.processed_orders",
    "glue.binance_lakehouse.market_hourly_summary",
    "glue.binance_lakehouse.order_execution_summary",
]


def run() -> None:
    spark = get_spark("check_table_health")  # noqa: F841

    # for table in TARGET_TABLES:
    #     metadata 지표 측정 → table_health_summary에 append

    raise NotImplementedError("Phase 3: metadata 조회 SQL 결정 후 구현")


if __name__ == "__main__":
    run()
