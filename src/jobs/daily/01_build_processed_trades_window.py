"""01_build_processed_trades_window.py

Raw trades -> processed_trades ingest-window job.

processed_trades is append-only. The job keeps retry/backfill idempotency by
removing duplicate trade_id values inside the batch, then appending only trade_id
values that are not already present in the target table.
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

from src.jobs.common.args import parse_job_args
from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import RAW_TRADES_PATH, PROCESSED_TRADES


MESSAGE_SCHEMA = StructType([
    StructField("trade_id", StringType(), True),
    StructField("price", StringType(), True),
    StructField("qty", StringType(), True),
    StructField("quote_qty", StringType(), True),
    StructField("time", StringType(), True),
    StructField("is_buyer_maker", StringType(), True),
    StructField("is_best_match", StringType(), True),
    StructField("symbol", StringType(), True),
])


def run() -> None:
    args = parse_job_args()
    spark = get_spark("phase3_build_processed_trades_window")

    raw_df = spark.read.parquet(RAW_TRADES_PATH)

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

    processed_df = parsed_df.select(
        F.col("parsed.trade_id").cast(LongType()).alias("trade_id"),
        F.col("parsed.symbol").alias("symbol"),
        F.col("parsed.price").cast(DecimalType(20, 8)).alias("price"),
        F.col("parsed.qty").cast(DecimalType(20, 8)).alias("qty"),
        F.col("parsed.quote_qty").cast(DecimalType(20, 8)).alias("quote_qty"),
        F.to_timestamp(
            (F.col("parsed.time").cast(LongType()) / 1000).cast("timestamp")
        ).alias("trade_time"),
        (F.col("parsed.is_buyer_maker") == "True").cast(BooleanType()).alias("is_buyer_maker"),
        (F.col("parsed.is_best_match") == "True").cast(BooleanType()).alias("is_best_match"),
        F.col("kafka_topic").alias("source_topic"),
        F.col("kafka_partition").alias("source_partition"),
        F.col("kafka_offset").alias("source_offset"),
        F.col("ingest_time").alias("ingest_time"),
    ).where(
        F.col("trade_id").isNotNull()
        & F.col("symbol").isNotNull()
        & F.col("trade_time").isNotNull()
    )

    deduped_df = processed_df.dropDuplicates(["trade_id"])

    existing_trade_ids_df = spark.table(PROCESSED_TRADES).select("trade_id")
    new_trades_df = deduped_df.join(
        existing_trade_ids_df,
        on="trade_id",
        how="left_anti",
    )

    new_trades_df.writeTo(PROCESSED_TRADES).append()

    print(
        "[phase3_build_processed_trades_window] complete "
        f"run_id={args.run_id}, "
        f"start_ts={args.start_ts}, "
        f"end_ts={args.end_ts}"
    )
    spark.stop()


if __name__ == "__main__":
    run()
