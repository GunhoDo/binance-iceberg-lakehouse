"""batch_ingest_kafka.py — k3d Kafka 토픽 → S3 raw 일회성 배치 적재 (Phase K4)

스트리밍 소비자(stream_raw_*.py) 대신, k3d Kafka 에 이미 쌓인 라이브 데이터를 한 번에
읽어(earliest→latest, 체크포인트 없음) S3 raw 존에 append 한다. X-6 의 orders 배치 적재를
토픽 일반화한 것 — raw 스키마(카프카 봉투 + ingest_date/ingest_time)는 stream_raw_* 와 동일.

멀티심볼 E2E 부트스트랩용: klines/trades 를 적재하면 processed 파이프라인(02/04/06,
01)이 심볼별로 처리한다(잡들은 이미 group by symbol). 파싱은 processed 단계 책임.

실행(Spark on k8s, K2 이미지 — kafka 커넥터 jar 포함):
    spark-submit src/streams/batch_ingest_kafka.py \
      --topic klines --bootstrap kafka-0.kafka.binance-lakehouse.svc.cluster.local:9092
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from src.jobs.common.spark_session import get_spark


TOPIC_OUTPUT = {
    "klines": "s3a://binance-iceberg-lake/raw/klines/",
    "trades": "s3a://binance-iceberg-lake/raw/trades/",
    "orders": "s3a://binance-iceberg-lake/raw/orders/",
}


def run() -> None:
    parser = argparse.ArgumentParser(description="Bounded Kafka topic -> S3 raw batch ingest")
    parser.add_argument("--topic", required=True, choices=sorted(TOPIC_OUTPUT))
    parser.add_argument(
        "--bootstrap",
        default="kafka-0.kafka.binance-lakehouse.svc.cluster.local:9092",
    )
    args = parser.parse_args()

    output_path = TOPIC_OUTPUT[args.topic]
    spark = get_spark(f"phase_k4_batch_ingest_{args.topic}")

    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
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

    total = raw_df.count()
    (
        raw_df.write.format("parquet")
        .mode("append")
        .partitionBy("ingest_date")
        .save(output_path)
    )

    print(
        f"[phase_k4_batch_ingest] complete topic={args.topic} rows={total} -> {output_path}"
    )
    spark.stop()


if __name__ == "__main__":
    run()
