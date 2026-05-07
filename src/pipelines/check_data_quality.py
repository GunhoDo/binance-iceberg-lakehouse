"""check_data_quality.py

PRD §13.2의 데이터 품질 지표를 측정해 data_quality_summary 에 append.

지표 (PRD §13.2):
- raw / processed row count (trade, kline, order 별)
- duplicate trade/order count
- null symbol count
- invalid price / quantity count
- freshness lag

설계 노트:
- 지표 정의는 PRD §13.2를 그대로 따른다.
- 임계값 비교 (PRD §13.5) 는 본 job이 아니라 대시보드 또는 후속 alert 단계에서 한다.
  본 job은 측정값을 적재만 한다.
"""

from __future__ import annotations

from src.pipelines.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("check_data_quality")  # noqa: F841

    # 각 지표 SQL 실행 → 결과를 data_quality_summary 에 append
    # SQL 은 sql/09_metadata_checks.sql 또는 본 파일에서 직접 작성.
    # Phase 3 진입 시 결정.

    raise NotImplementedError("Phase 3: 지표별 SQL 작성 후 구현")


if __name__ == "__main__":
    run()
