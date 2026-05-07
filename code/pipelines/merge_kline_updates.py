"""merge_kline_updates.py

processed_klines에 kline update를 MERGE INTO로 반영한다.

PRD §10.4, §12.2 참조.

설계 노트:
- 실제 MERGE 문은 SQL 파일 (`sql/07_merge_kline_updates.sql`) 에 둔다.
- 본 Python 파일은 SQL을 spark.sql 로 실행하는 wrapper 역할만 한다.
- SQL을 분리하는 이유: MERGE 로직은 SQL에서 가장 명확하게 표현되며 PRD/docs에서
  바로 인용 가능한 단일 소스가 되어야 한다.
"""

from __future__ import annotations

from pathlib import Path

from code.pipelines.common.spark_session import get_spark

SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "07_merge_kline_updates.sql"


def run() -> None:
    spark = get_spark("merge_kline_updates")  # noqa: F841

    # sql_text = SQL_PATH.read_text(encoding="utf-8")
    # spark.sql(sql_text)
    #
    # MERGE 전후 snapshot 비교는 별도 job 또는 metadata 조회 SQL에서 수행한다
    # (PRD §12.1, §12.2). 본 job은 MERGE 실행만 책임진다.

    raise NotImplementedError("Phase 2: SQL 완성 후 활성화")


if __name__ == "__main__":
    run()
