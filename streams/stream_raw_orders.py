"""stream_raw_orders.py

Kafka topic `orders` → Iceberg table `raw_orders` (append-only).

PRD §10.2, §14.1 참조.

저장 대상 컬럼은 PRD §10.2 참조. 이 table의 source는 simulator이며 실데이터가
아니라는 사실을 잊지 말 것 (PRD §2, §6.3).
"""

"""stream_raw_orders.py

Kafka topic `orders` → Iceberg table `glue.raw.raw_orders` (append-only).

PRD §10.2, §14.1 참조.

실행:
    PYTHONPATH=. spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4 \\
        streams/stream_raw_klines.py
"""

from __future__ import annotations

from pyspark.sql import functions as F

from jobs.common.spark_session import get_spark_streaming

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "orders"
OUTPUT_PATH = "s3://binance-iceberg-lake/raw/orders/"
CHECKPOINT_PATH = "s3://binance-iceberg-lake/checkpoints/raw_orders/"


def run() -> None:
    spark = get_spark_streaming("stream_raw_orders")

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