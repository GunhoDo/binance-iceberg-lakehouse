"""merge_order_status_updates.py

processed_orders에 주문 상태 변화를 MERGE INTO로 반영한다.

PRD §10.5, §12.1 참조.

설계 노트:
- 상태 전이: NEW → PARTIALLY_FILLED → FILLED 또는 NEW → CANCELED.
- 실제 MERGE 문은 `sql/08_merge_order_status_updates.sql` 에 둔다.
- 본 Python 파일은 wrapper 역할만 한다 (merge_kline_updates와 동일 패턴).
"""

from __future__ import annotations

from pathlib import Path

from jobs.common.spark_session import get_spark

SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "08_merge_order_status_updates.sql"


def run() -> None:
    spark = get_spark("merge_order_status_updates")  # noqa: F841

    # sql_text = SQL_PATH.read_text(encoding="utf-8")
    # spark.sql(sql_text)

    raise NotImplementedError("Phase 2: SQL 완성 후 활성화")


if __name__ == "__main__":
    run()
