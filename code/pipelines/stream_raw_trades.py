"""stream_raw_trades.py

Kafka topic `trades` → S3 plain Parquet (append-only).

Raw Zone은 Iceberg가 아니다 (decisions.md D7).

PRD §10.1, §14.1 참조.

실행:
    PYTHONPATH=. spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4 \\
        streams/stream_raw_trades.py
"""

from __future__ import annotations

from pyspark.sql import functions as F

from jobs.common.spark_session import get_spark_streaming

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "trades"
OUTPUT_PATH = "s3a://binance-iceberg-lake/raw/trades/"
CHECKPOINT_PATH = "s3a://binance-iceberg-lake/checkpoints/raw_trades/"


def run() -> None:
    spark = get_spark_streaming("stream_raw_trades")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    raw_df = kafka_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").cast("long").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("message_value"),
        F.date_format(F.current_timestamp(), "yyyy").alias("year"),
        F.date_format(F.current_timestamp(), "MM").alias("month"),
        F.current_timestamp().cast("string").alias("ingest_time"),
    )

    query = (
        raw_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("year", "month")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()