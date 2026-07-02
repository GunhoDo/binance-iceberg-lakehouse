"""spark_bench.py

벤치마크 전용 SparkSession — **로컬 Iceberg(hadoop catalog)**. Glue/S3·AWS 자격증명이
필요 없다(P2/P3는 로컬에서 재현 가능하게 측정한다, PRD NFR 비용/재현성).

production용 `src/pipelines/common/spark_session.py`(Glue/S3)와 의도적으로 분리한다.
로컬 warehouse에 커밋하므로 S3 네트워크 지연이 lag 측정에 섞이지 않아 레버 효과가 깨끗하게
분리된다(= 벤치에 로컬이 더 적합).

Kafka 커넥터는 이미지에 없으므로 spark-submit 시 --packages로 주입한다:
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5
(Iceberg runtime jar는 Dockerfile.spark에서 이미 classpath에 포함.)
"""

from __future__ import annotations

from pyspark.sql import SparkSession

DEFAULT_WAREHOUSE = "/workspace/.bench_warehouse"
CATALOG = "local"


def get_spark_bench(
    app_name: str,
    warehouse: str = DEFAULT_WAREHOUSE,
    packages: str | None = None,
) -> SparkSession:
    """로컬 Iceberg catalog(`local`)가 설정된 SparkSession.

    packages: spark.jars.packages(ivy 좌표). 스트리밍 잡은 Kafka 커넥터를 여기로 주입해
    평범한 `python` 실행으로도 동작하게 한다(spark-submit --packages 불요).
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", f"file://{warehouse}")
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.ui.showConsoleProgress", "false")
    )
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    return builder.getOrCreate()
