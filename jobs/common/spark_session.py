"""spark_session.py

Spark + Iceberg session 생성의 단일 진입점.

본 스켈레톤은 SparkSession을 어떻게 만들지 시그니처만 둔다. 정확한 catalog 종류
(Hadoop / Hive / Glue), warehouse 경로, jar 버전은 환경에 따라 다르므로 Phase 1
구현 시점에 채운다.

설계 노트:
- catalog 이름과 warehouse 경로는 config로 분리한다 (PRD §16.3).
- 동일 함수를 streams/ 와 jobs/ 모두에서 공유해 설정 표류를 막는다.
"""

from __future__ import annotations

# from pyspark.sql import SparkSession  # Phase 1에서 활성화


def get_spark(app_name: str):
    """Iceberg catalog가 설정된 SparkSession을 반환한다.

    Args:
        app_name: spark.app.name 으로 사용된다. job/stream 단위로 구분되는 이름.

    Returns:
        SparkSession (Phase 1에서 타입 명시).
    """
    raise NotImplementedError(
        "Phase 1: catalog 종류 (Hadoop / Hive / Glue) 결정 후 구현. "
        "decisions.md D8 참조."
    )
