"""compact_tables.py

Iceberg compaction 실행. PRD §12.4, §14.3 참조.

설계 노트:
- compaction 대상 table과 임계값은 PRD §13.5 / §14.3을 따른다.
- 정확한 rewrite 옵션 (target file size, where 절) 은 Phase 3에서 결정한다
  (`docs/decisions.md` D9).
- 본 job은 Maintenance DAG에서 호출된다 (PRD §14.3).
"""

from __future__ import annotations

from src.pipelines.common.spark_session import get_spark


# Maintenance DAG가 처리할 대상 (PRD §14.3 의 compact_processed_tables /
# compact_serving_tables).
COMPACTION_TARGETS = {
    "processed": [
        "processed_trades",
        "processed_klines",
        "processed_orders",
    ],
    "serving": [
        "market_hourly_summary",
        "order_execution_summary",
    ],
}


def compact(table_name: str) -> None:
    """Iceberg `rewrite_data_files` 시스템 procedure를 호출한다.

    실제 procedure 호출 SQL은 Phase 3에서 결정. 옵션 (target-file-size-bytes,
    where 등) 도 그 시점에 데이터 분포를 보고 결정한다.
    """
    raise NotImplementedError("Phase 3: rewrite 옵션 결정 후 구현")


def run(scope: str = "processed") -> None:
    """Args:
        scope: "processed" | "serving"
    """
    spark = get_spark(f"compact_tables_{scope}")  # noqa: F841

    # for table in COMPACTION_TARGETS[scope]:
    #     compact(table)

    raise NotImplementedError


if __name__ == "__main__":
    run()
