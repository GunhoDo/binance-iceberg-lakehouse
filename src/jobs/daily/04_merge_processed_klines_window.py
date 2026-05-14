"""04_merge_processed_klines_window.py

staging_klines -> processed_klines ingest-window based MERGE job.

The Airflow window means "data ingested in this interval". The processed
business key remains (symbol, interval, open_time), so late/backfilled klines
update the hour they belong to instead of the hour they arrived.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.jobs.common.args import parse_job_args
from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import STAGING_KLINES, PROCESSED_KLINES


def run() -> None:
    args = parse_job_args()
    spark = get_spark("phase3_merge_processed_klines_window")

    staging_df = spark.table(STAGING_KLINES)

    staging_with_ingest_df = staging_df.withColumn(
        "ingest_ts",
        F.to_timestamp(F.col("ingest_time")),
    )

    affected_keys_df = (
        staging_with_ingest_df
        .where(
            (F.col("ingest_ts") >= F.to_timestamp(F.lit(args.start_ts)))
            & (F.col("ingest_ts") < F.to_timestamp(F.lit(args.end_ts)))
            & F.col("symbol").isNotNull()
            & F.col("interval").isNotNull()
            & F.col("open_time").isNotNull()
        )
        .select("symbol", "interval", "open_time")
        .distinct()
    )

    impacted_df = staging_df.alias("staging").join(
        affected_keys_df.alias("keys"),
        on=["symbol", "interval", "open_time"],
        how="inner",
    )

    w = Window.partitionBy("symbol", "interval", "open_time").orderBy(
        F.col("source_offset").desc_nulls_last(),
        F.col("updated_at").desc_nulls_last(),
    )

    latest_df = (
        impacted_df
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .drop("rn")
        .withColumn("updated_at", F.current_timestamp())
    )

    latest_df.createOrReplaceTempView("tmp_processed_klines_source")

    spark.sql(f"""
    MERGE INTO {PROCESSED_KLINES} AS target
    USING tmp_processed_klines_source AS source
    ON target.symbol = source.symbol
       AND target.interval = source.interval
       AND target.open_time = source.open_time

    WHEN MATCHED AND source.source_offset >= target.source_offset THEN UPDATE SET
        target.close_time = source.close_time,
        target.open = source.open,
        target.high = source.high,
        target.low = source.low,
        target.close = source.close,
        target.volume = source.volume,
        target.quote_volume = source.quote_volume,
        target.number_of_trades = source.number_of_trades,
        target.is_closed = source.is_closed,
        target.source_topic = source.source_topic,
        target.source_partition = source.source_partition,
        target.source_offset = source.source_offset,
        target.ingest_time = source.ingest_time,
        target.updated_at = source.updated_at

    WHEN NOT MATCHED THEN INSERT (
        symbol,
        interval,
        open_time,
        close_time,
        open,
        high,
        low,
        close,
        volume,
        quote_volume,
        number_of_trades,
        is_closed,
        source_topic,
        source_partition,
        source_offset,
        ingest_time,
        updated_at
    )
    VALUES (
        source.symbol,
        source.interval,
        source.open_time,
        source.close_time,
        source.open,
        source.high,
        source.low,
        source.close,
        source.volume,
        source.quote_volume,
        source.number_of_trades,
        source.is_closed,
        source.source_topic,
        source.source_partition,
        source.source_offset,
        source.ingest_time,
        source.updated_at
    )
    """)

    affected_key_count = affected_keys_df.count()
    source_count = latest_df.count()
    target_total = spark.sql(f"SELECT COUNT(*) AS cnt FROM {PROCESSED_KLINES}").collect()[0]["cnt"]

    print(
        "[phase3_merge_processed_klines_window] complete "
        f"run_id={args.run_id}, "
        f"affected_keys={affected_key_count}, "
        f"source_rows={source_count}, "
        f"target_total_rows={target_total}"
    )

    spark.stop()


if __name__ == "__main__":
    run()
