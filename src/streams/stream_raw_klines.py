"""stream_raw_klines.py

Kafka topic `klines` → S3 plain Parquet (append-only).

Raw Zone은 Iceberg가 아니다 (decisions.md D7, stream_raw_trades.py 참조).
같은 (symbol, interval, open_time) 의 반복 이벤트도 모두 append한다.
upsert-like 처리는 processed layer의 책임이다.

PRD §10.1, §6.2, §14.1 참조.

실행:
    PYTHONPATH=. spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4 \\
        src/pipelines/stream_raw_klines.py
"""

from __future__ import annotations

from pyspark.sql import functions as F

from src.pipelines.common.spark_session import get_spark_streaming

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "klines"
OUTPUT_PATH = "s3a://binance-iceberg-lake/raw/klines/"
CHECKPOINT_PATH = "s3a://binance-iceberg-lake/checkpoints/raw_klines/"


def run() -> None:
    spark = get_spark_streaming("stream_raw_klines")

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
        F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("ingest_date"),
        F.current_timestamp().cast("string").alias("ingest_time"),
    )

    query = (
        raw_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("ingest_date")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
