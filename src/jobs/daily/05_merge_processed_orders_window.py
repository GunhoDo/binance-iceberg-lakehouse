"""05_merge_processed_orders_window.py

staging_orders -> processed_orders window 기반 MERGE job.

멱등성 기준:
- processed_orders는 order_id 기준 최신 상태만 유지
- 같은 window 재실행 시 같은 order_id는 UPDATE만 수행
- source_df 컬럼을 target schema와 맞춘 뒤 INSERT * 사용
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.jobs.common.args import parse_job_args
from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import STAGING_ORDERS, PROCESSED_ORDERS


def run() -> None:
    args = parse_job_args()
    spark = get_spark("phase3_merge_processed_orders_window")

    staging_df = spark.table(STAGING_ORDERS)

    windowed_df = staging_df.where(
        (F.col("event_time") >= F.to_timestamp(F.lit(args.start_ts)))
        & (F.col("event_time") < F.to_timestamp(F.lit(args.end_ts)))
    )

    ranked_windowed_df = windowed_df.withColumn(
        "status_rank",
        F.when(F.col("order_status") == "NEW", F.lit(1))
        .when(F.col("order_status") == "PARTIALLY_FILLED", F.lit(2))
        .when(F.col("order_status").isin("FILLED", "CANCELED"), F.lit(3))
        .otherwise(F.lit(0)),
    )

    latest_window = Window.partitionBy("order_id").orderBy(
        F.col("event_time").desc_nulls_last(),
        F.col("status_rank").desc_nulls_last(),
        F.col("source_offset").desc_nulls_last(),
    )

    latest_event_df = (
        ranked_windowed_df
        .withColumn("rn", F.row_number().over(latest_window))
        .where(F.col("rn") == 1)
        .drop("rn", "status_rank")
    )

    lifecycle_df = ranked_windowed_df.groupBy("order_id").agg(
        F.min("event_time").alias("created_at"),
        F.max("event_time").alias("last_event_at"),
        F.max(F.when(F.col("order_status") == "FILLED", F.col("event_time"))).alias("filled_at"),
        F.max(F.when(F.col("order_status") == "CANCELED", F.col("event_time"))).alias("canceled_at"),
    )

    # processed_orders target schema와 컬럼명/순서를 맞춤
    source_df = (
        latest_event_df.alias("latest")
        .join(lifecycle_df.alias("life"), on="order_id", how="left")
        .select(
            F.col("latest.order_id").alias("order_id"),
            F.col("latest.client_id").alias("client_id"),
            F.col("latest.symbol").alias("symbol"),
            F.col("latest.side").alias("side"),
            F.col("latest.order_type").alias("order_type"),
            F.col("latest.order_price").alias("order_price"),
            F.col("latest.order_qty").alias("order_qty"),
            F.col("latest.filled_qty").alias("filled_qty"),
            F.col("latest.avg_fill_price").alias("avg_fill_price"),
            F.col("latest.event_type").alias("event_type"),
            F.col("latest.order_status").alias("order_status"),
            F.col("life.created_at").alias("created_at"),
            F.col("life.last_event_at").alias("updated_at"),
            F.col("life.filled_at").alias("filled_at"),
            F.col("life.canceled_at").alias("canceled_at"),
            F.col("latest.simulated_parameters").alias("simulated_parameters"),
            F.col("latest.source_topic").alias("source_topic"),
            F.col("latest.source_partition").alias("source_partition"),
            F.col("latest.source_offset").alias("source_offset"),
        )
    )

    source_df.createOrReplaceTempView("tmp_processed_orders_source")

    spark.sql(f"""
        MERGE INTO {PROCESSED_ORDERS} AS target
        USING tmp_processed_orders_source AS source
        ON target.order_id = source.order_id

        WHEN MATCHED AND source.updated_at >= target.updated_at THEN UPDATE SET
            target.client_id = source.client_id,
            target.symbol = source.symbol,
            target.side = source.side,
            target.order_type = source.order_type,
            target.order_price = source.order_price,
            target.order_qty = source.order_qty,
            target.filled_qty = source.filled_qty,
            target.avg_fill_price = source.avg_fill_price,
            target.event_type = source.event_type,
            target.order_status = source.order_status,
            target.created_at = source.created_at,
            target.updated_at = source.updated_at,
            target.filled_at = source.filled_at,
            target.canceled_at = source.canceled_at,
            target.simulated_parameters = source.simulated_parameters,
            target.source_topic = source.source_topic,
            target.source_partition = source.source_partition,
            target.source_offset = source.source_offset

        WHEN NOT MATCHED THEN INSERT *
    """)

    source_count = source_df.count()
    target_total = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {PROCESSED_ORDERS}"
    ).collect()[0]["cnt"]

    print(
        "[phase3_merge_processed_orders_window] complete "
        f"run_id={args.run_id}, "
        f"source_rows={source_count}, "
        f"target_total_rows={target_total}"
    )

    spark.stop()


if __name__ == "__main__":
    run()
