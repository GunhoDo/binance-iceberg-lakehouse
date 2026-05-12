"""02_build_staging_klines_window.py

raw_klines -> staging_klines window 기반 적재 job.

멱등성 기준:
- source_topic + source_partition + source_offset 기준 MERGE INSERT
- 같은 raw Kafka offset을 다시 처리해도 staging_klines에 중복 적재하지 않음
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from src.jobs.common.args import parse_job_args
from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import RAW_KLINES_PATH, STAGING_KLINES


MESSAGE_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("interval", StringType(), True),
    StructField("open_time", StringType(), True),
    StructField("close_time", StringType(), True),
    StructField("open", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("quote_asset_volume", StringType(), True),
    StructField("number_of_trades", StringType(), True),
    StructField("is_closed", StringType(), True),
])


def run() -> None:
    args = parse_job_args()
    spark = get_spark("phase3_build_staging_klines_window")

    raw_df = spark.read.parquet(RAW_KLINES_PATH)

    start_date = F.date_format(F.to_timestamp(F.lit(args.start_ts)), "yyyy-MM-dd")
    end_date = F.date_format(F.to_timestamp(F.lit(args.end_ts)), "yyyy-MM-dd")
    partitioned_raw_df = raw_df.where(
        (F.col("ingest_date") >= start_date)
        & (F.col("ingest_date") <= end_date)
    )

    windowed_raw_df = (
        partitioned_raw_df
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
    F.col("parsed.symbol").alias("symbol"),
    F.col("parsed.interval").alias("interval"),

    F.to_timestamp(
        (F.col("parsed.open_time").cast(LongType()) / 1000).cast("timestamp")
    ).alias("open_time"),
    F.to_timestamp(
        (F.col("parsed.close_time").cast(LongType()) / 1000).cast("timestamp")
    ).alias("close_time"),

    F.col("parsed.open").cast(DecimalType(20, 8)).alias("open"),
    F.col("parsed.high").cast(DecimalType(20, 8)).alias("high"),
    F.col("parsed.low").cast(DecimalType(20, 8)).alias("low"),
    F.col("parsed.close").cast(DecimalType(20, 8)).alias("close"),

    F.col("parsed.volume").cast(DecimalType(30, 8)).alias("volume"),
    F.col("parsed.quote_asset_volume").cast(DecimalType(30, 8)).alias("quote_volume"),
    F.col("parsed.number_of_trades").cast(LongType()).alias("number_of_trades"),

    (
        (F.col("parsed.is_closed") == "true")
        | (F.col("parsed.is_closed") == "True")
        | (F.col("parsed.is_closed") == "1")
    ).cast(BooleanType()).alias("is_closed"),

    F.col("kafka_topic").alias("source_topic"),
    F.col("kafka_partition").alias("source_partition"),
    F.col("kafka_offset").alias("source_offset"),
    F.col("ingest_time").alias("ingest_time"),
    F.current_timestamp().alias("updated_at"),
    ).where(
        F.col("symbol").isNotNull()
        & F.col("interval").isNotNull()
        & F.col("open_time").isNotNull()
    )

    # 같은 Kafka offset이 batch 안에 중복되면 1건만 선택
    w = Window.partitionBy("source_topic", "source_partition", "source_offset").orderBy(
        F.col("ingest_time").desc_nulls_last()
    )

    deduped_df = (
        staging_df
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .drop("rn")
    )

    deduped_df.createOrReplaceTempView("tmp_staging_klines_batch")

    spark.sql(f"""
        MERGE INTO {STAGING_KLINES} AS target
        USING tmp_staging_klines_batch AS source
        ON target.source_topic = source.source_topic
           AND target.source_partition = source.source_partition
           AND target.source_offset = source.source_offset

        WHEN NOT MATCHED THEN INSERT *
    """)

    raw_count = windowed_raw_df.count()
    batch_count = deduped_df.count()
    target_total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {STAGING_KLINES}").collect()[0]["cnt"]

    print(
        "[phase3_build_staging_klines_window] complete "
        f"run_id={args.run_id}, raw_window_rows={raw_count}, "
        f"deduped_batch_rows={batch_count}, target_total_rows={target_total}"
    )

    spark.stop()


if __name__ == "__main__":
    run()
