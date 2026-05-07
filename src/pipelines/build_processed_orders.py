"""build_processed_orders.py

raw_orders → processed_orders 변환 job.

PRD §10.5 참조.

역할:
- raw order event를 정제해 MERGE 입력용 staging 형태로 준비
- 실제 processed_orders 반영은 merge_order_status_updates.py 에서 MERGE INTO로 처리

설계 노트:
- build_processed_klines와 동일하게 정제 + staging 까지만 책임을 분리한다.
- micro-batch 안의 같은 order_id 중복 dedup 전략은 Phase 2에서 결정
  (`docs/decisions.md` D6).

raw_orders → staging_orders 정제 job.

본 job은 raw_orders의 message_value(JSON 문자열)를 파싱하고 타입을 변환한 뒤,
MERGE 대상 staging table인 staging_orders에 적재한다.

실제 processed_orders 반영은 08_merge_order_status_updates.sql에서 수행한다.

처리:
- message_value JSON 파싱
- string → DECIMAL / TIMESTAMP 변환
- 주문 상태 이벤트 정규화
- Kafka metadata 보존
- staging_orders에 append

실행:
    PYTHONPATH=. spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.iceberg:iceberg-aws-bundle:1.7.0,org.apache.hadoop:hadoop-aws:3.3.4 \
      src/pipelines/build_processed_orders.py
"""
from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from src.pipelines.common.spark_session import get_spark

RAW_PATH = "s3a://binance-iceberg-lake/raw/orders/"
STAGING_TABLE = "glue.binance_lakehouse.staging_orders"

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
])


def run() -> None:
    spark = get_spark("build_processed_orders")

    raw_df = spark.read.parquet(RAW_PATH)

    parsed_df = raw_df.withColumn(
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

        # Unix ms → TIMESTAMP
        F.to_timestamp(
            (F.col("parsed.event_time").cast("long") / 1000).cast("timestamp")
        ).alias("event_time"),

        # nested object는 schema로 강제 파싱하지 않고 원본 JSON 일부를 문자열로 보존한다.
        F.get_json_object(
            F.col("message_value"),
            "$.simulated_parameters"
        ).alias("simulated_parameters"),

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

    staging_df.writeTo(STAGING_TABLE).append()

    print(f"staging_orders 적재 완료: {staging_df.count()} rows")


if __name__ == "__main__":
    run()