"""04_build_staging_orders_window.py

raw_orders -> staging_orders window 기반 적재 job.

멱등성 기준:
- source_topic + source_partition + source_offset 기준 MERGE INSERT
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

from src.jobs.common.args import parse_job_args
from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import RAW_ORDERS_PATH, STAGING_ORDERS


MESSAGE_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("side", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("order_price", StringType(), True),
    StructField("order_qty", StringType(), True),
    StructField("filled_qty", StringType(), True),
    StructField("avg_fill_price", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("simulated_parameters", StringType(), True),
])


def run() -> None:
    args = parse_job_args()
    spark = get_spark("phase3_build_staging_orders_window")

    raw_df = spark.read.parquet(RAW_ORDERS_PATH)

    windowed_raw_df = (
        raw_df
        .withColumn("ingest_ts", F.to_timestamp(F.col("ingest_time")))
        .where(
            (F.col("ingest_ts") >= F.to_timestamp(F.lit(args.start_ts)))
            & (F.col("ingest_ts") < F.to_timestamp(F.lit(args.end_ts)))
        )
    )

    parsed_df = windowed_raw_df.withColumn(
        "parsed",
        F.from_json(F.col("message_value"), MESSAGE_SCHEMA),
    )

    staging_df = parsed_df.select(
        F.col("parsed.order_id").alias("order_id"),
        F.col("parsed.client_id").alias("client_id"),
        F.col("parsed.symbol").alias("symbol"),
        F.col("parsed.side").alias("side"),
        F.col("parsed.order_type").alias("order_type"),
        F.col("parsed.order_price").cast(DecimalType(20, 8)).alias("order_price"),
        F.col("parsed.order_qty").cast(DecimalType(20, 8)).alias("order_qty"),
        F.col("parsed.filled_qty").cast(DecimalType(20, 8)).alias("filled_qty"),
        F.col("parsed.avg_fill_price").cast(DecimalType(20, 8)).alias("avg_fill_price"),
        F.col("parsed.event_type").alias("event_type"),
        F.col("parsed.order_status").alias("order_status"),
        F.to_timestamp(
            (F.col("parsed.event_time").cast(LongType()) / 1000).cast("timestamp")
        ).alias("event_time"),

        # 원본 JSON 안의 object를 문자열로 보존
        F.get_json_object(F.col("message_value"), "$.simulated_parameters").alias("simulated_parameters"),

        F.col("kafka_topic").alias("source_topic"),
        F.col("kafka_partition").alias("source_partition"),
        F.col("kafka_offset").alias("source_offset"),
        F.col("ingest_time").alias("ingest_time"),
        F.current_timestamp().alias("updated_at"),
    ).where(
        F.col("order_id").isNotNull()
        & F.col("order_status").isNotNull()
        & F.col("event_time").isNotNull()
    )

    w = Window.partitionBy("source_topic", "source_partition", "source_offset").orderBy(
        F.col("ingest_time").desc_nulls_last()
    )

    deduped_df = (
        staging_df
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .drop("rn")
    )

    deduped_df.createOrReplaceTempView("tmp_staging_orders_batch")

    spark.sql(f"""
        MERGE INTO {STAGING_ORDERS} AS target
        USING tmp_staging_orders_batch AS source
        ON target.source_topic = source.source_topic
           AND target.source_partition = source.source_partition
           AND target.source_offset = source.source_offset

        WHEN NOT MATCHED THEN INSERT *
    """)

    raw_count = windowed_raw_df.count()
    batch_count = deduped_df.count()
    target_total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {STAGING_ORDERS}").collect()[0]["cnt"]

    print(
        "[phase3_build_staging_orders_window] complete "
        f"run_id={args.run_id}, raw_window_rows={raw_count}, "
        f"deduped_batch_rows={batch_count}, target_total_rows={target_total}"
    )

    spark.stop()


if __name__ == "__main__":
    run()
