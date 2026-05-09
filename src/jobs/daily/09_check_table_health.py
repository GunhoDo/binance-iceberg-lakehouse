"""09_check_table_health.py

Phase3 Iceberg table health check job.

수집 항목:
- data file count
- position delete file count
- equality delete file count
- delete/data file ratio
- avg file size
- total file size
- record count
- manifest count
- snapshot count
- last committed timestamp

결과:
- glue.binance_lakehouse.table_health_summary append
"""

from __future__ import annotations

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    DoubleType,
)
from src.jobs.common.args import parse_job_args
from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import TABLE_HEALTH_SUMMARY


TABLES = [
    ("processed_trades", "COW_APPEND"),
    ("processed_klines", "MOR"),
    ("processed_orders", "MOR"),
    ("market_hourly_summary", "MOR"),
    ("order_execution_summary", "MOR"),
    ("data_quality_summary", "APPEND_ONLY"),
    ("pipeline_run_summary", "APPEND_ONLY"),
    ("table_health_summary", "APPEND_ONLY"),
]

RESULT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("checked_at", TimestampType(), True),
    StructField("table_name", StringType(), False),
    StructField("table_mode", StringType(), False),

    StructField("data_file_count", LongType(), False),
    StructField("position_delete_file_count", LongType(), False),
    StructField("equality_delete_file_count", LongType(), False),
    StructField("delete_to_data_file_ratio", DoubleType(), False),

    StructField("avg_file_size_mb", DoubleType(), False),
    StructField("total_size_mb", DoubleType(), False),
    StructField("record_count", LongType(), False),

    StructField("manifest_count", LongType(), False),
    StructField("snapshot_count", LongType(), False),
    StructField("last_committed_at", TimestampType(), True),
])

def scalar_or_none(spark, query: str):
    rows = spark.sql(query).collect()
    if not rows:
        return None
    return rows[0][0]


def collect_table_health(spark, run_id: str, table_name: str, table_mode: str) -> Row:
    full_table = f"glue.binance_lakehouse.{table_name}"

    file_stats = spark.sql(f"""
        SELECT
            SUM(CASE WHEN content = 0 THEN 1 ELSE 0 END) AS data_file_count,
            SUM(CASE WHEN content = 1 THEN 1 ELSE 0 END) AS position_delete_file_count,
            SUM(CASE WHEN content = 2 THEN 1 ELSE 0 END) AS equality_delete_file_count,
            AVG(CASE WHEN content = 0 THEN file_size_in_bytes ELSE NULL END) AS avg_file_size_bytes,
            SUM(CASE WHEN content = 0 THEN file_size_in_bytes ELSE 0 END) AS total_size_bytes,
            SUM(CASE WHEN content = 0 THEN record_count ELSE 0 END) AS record_count
        FROM {full_table}.files
    """).collect()[0]

    data_file_count = int(file_stats["data_file_count"] or 0)
    position_delete_file_count = int(file_stats["position_delete_file_count"] or 0)
    equality_delete_file_count = int(file_stats["equality_delete_file_count"] or 0)

    delete_file_count = position_delete_file_count + equality_delete_file_count
    delete_to_data_file_ratio = (
        float(delete_file_count) / float(data_file_count)
        if data_file_count > 0
        else 0.0
    )

    avg_file_size_mb = (
        float(file_stats["avg_file_size_bytes"]) / 1024 / 1024
        if file_stats["avg_file_size_bytes"] is not None
        else 0.0
    )

    total_size_mb = (
        float(file_stats["total_size_bytes"]) / 1024 / 1024
        if file_stats["total_size_bytes"] is not None
        else 0.0
    )

    record_count = int(file_stats["record_count"] or 0)

    manifest_count = scalar_or_none(
        spark,
        f"SELECT COUNT(*) FROM {full_table}.manifests",
    )
    manifest_count = int(manifest_count or 0)

    snapshot_count = scalar_or_none(
        spark,
        f"SELECT COUNT(*) FROM {full_table}.snapshots",
    )
    snapshot_count = int(snapshot_count or 0)

    last_committed_at = scalar_or_none(
        spark,
        f"SELECT MAX(committed_at) FROM {full_table}.snapshots",
    )

    return Row(
        run_id=run_id,
        checked_at=None,
        table_name=table_name,
        table_mode=table_mode,
        data_file_count=data_file_count,
        position_delete_file_count=position_delete_file_count,
        equality_delete_file_count=equality_delete_file_count,
        delete_to_data_file_ratio=delete_to_data_file_ratio,
        avg_file_size_mb=avg_file_size_mb,
        total_size_mb=total_size_mb,
        record_count=record_count,
        manifest_count=manifest_count,
        snapshot_count=snapshot_count,
        last_committed_at=last_committed_at,
    )


def run() -> None:
    args = parse_job_args()
    spark = get_spark("phase3_check_table_health")

    results: list[Row] = []

    for table_name, table_mode in TABLES:
        try:
            results.append(
                collect_table_health(
                    spark=spark,
                    run_id=args.run_id,
                    table_name=table_name,
                    table_mode=table_mode,
                )
            )
            print(f"[table_health] collected: {table_name}")
        except Exception as e:
            # MVP에서는 한 테이블 실패가 전체 health check를 죽이지 않도록 한다.
            # 실패 테이블은 로그로 남기고 다음 table로 진행한다.
            print(f"[table_health] skip {table_name}: {e}")

    if not results:
        print("[phase3_check_table_health] no results")
        spark.stop()
        return

    result_df = spark.createDataFrame(results, schema=RESULT_SCHEMA).withColumn(
        "checked_at",
        F.current_timestamp(),
    )

    result_df = result_df.select(
        "run_id",
        "checked_at",
        "table_name",
        "table_mode",
        "data_file_count",
        "position_delete_file_count",
        "equality_delete_file_count",
        "delete_to_data_file_ratio",
        "avg_file_size_mb",
        "total_size_mb",
        "record_count",
        "manifest_count",
        "snapshot_count",
        "last_committed_at",
    )

    result_df.writeTo(TABLE_HEALTH_SUMMARY).append()

    print(
        "[phase3_check_table_health] complete "
        f"run_id={args.run_id}, tables={len(results)}"
    )

    spark.stop()


if __name__ == "__main__":
    run()
