"""build_processed_klines.py

raw_klines → processed_klines 변환 job.

PRD §10.4 참조.

역할:
- raw kline event를 정제된 kline 단위로 변환
- 같은 (symbol, interval, open_time) 의 반복 update를 처리하기 위해 다음 단계인
  merge_kline_updates에 입력될 staging 형태로 준비

설계 노트:
- 본 job은 정제 + staging 까지만 책임진다. 실제 processed_klines로의 반영은
  merge_kline_updates.py 에서 MERGE INTO로 처리한다.
- 이렇게 분리하는 이유는 MERGE 대상 micro-batch 안에서 같은 키가 여러 번 나올 수
  있어, MERGE 직전에 키 단위 dedup이 필요하기 때문이다 (`docs/decisions.md` D6).

실행:
    PYTHONPATH=. spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.iceberg:iceberg-aws-bundle:1.7.0,org.apache.hadoop:hadoop-aws:3.3.4 \
      src/pipelines/build_processed_klines.py
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from src.pipelines.common.spark_session import get_spark

RAW_PATH = "s3a://binance-iceberg-lake/raw/klines/"
STAGING_TABLE = "glue.binance_lakehouse.staging_klines"

# raw_klines.message_value 실제 구조 기준
# 현재 raw 데이터는 historical kline 형태이며 is_closed 또는 WebSocket close flag x가 없다.
MESSAGE_SCHEMA = StructType([
    StructField("open_time", StringType(), True),
    StructField("open", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True),
    StructField("close_time", StringType(), True),
    StructField("quote_volume", StringType(), True),
    StructField("number_of_trades", StringType(), True),
    StructField("taker_buy_base_volume", StringType(), True),
    StructField("taker_buy_quote_volume", StringType(), True),
    StructField("ignore", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("interval", StringType(), True),
])


def run() -> None:
    spark = get_spark("build_processed_klines")

    raw_df = spark.read.parquet(RAW_PATH)

    parsed_df = raw_df.withColumn(
        "parsed",
        F.from_json(F.col("message_value"), MESSAGE_SCHEMA),
    )

    staging_df = parsed_df.select(
        F.col("parsed.symbol").alias("symbol"),
        F.col("parsed.interval").alias("interval"),

        # Unix ms → TIMESTAMP
        F.to_timestamp(
            (F.col("parsed.open_time").cast("long") / 1000).cast("timestamp")
        ).alias("open_time"),
        F.to_timestamp(
            (F.col("parsed.close_time").cast("long") / 1000).cast("timestamp")
        ).alias("close_time"),

        F.col("parsed.open").cast(DecimalType(20, 8)).alias("open"),
        F.col("parsed.high").cast(DecimalType(20, 8)).alias("high"),
        F.col("parsed.low").cast(DecimalType(20, 8)).alias("low"),
        F.col("parsed.close").cast(DecimalType(20, 8)).alias("close"),
        F.col("parsed.volume").cast(DecimalType(20, 8)).alias("volume"),
        F.col("parsed.quote_volume").cast(DecimalType(24, 8)).alias("quote_volume"),
        F.col("parsed.number_of_trades").cast("long").alias("number_of_trades"),

        # Phase 2 raw_klines는 historical closed kline으로 간주한다.
        F.lit(True).alias("is_closed"),

        F.col("kafka_topic").alias("source_topic"),
        F.col("kafka_partition").alias("source_partition"),
        F.col("kafka_offset").alias("source_offset"),
        F.current_timestamp().alias("updated_at"),
    ).where(
        F.col("symbol").isNotNull()
        & F.col("interval").isNotNull()
        & F.col("open_time").isNotNull()
    )

    staging_df.writeTo(STAGING_TABLE).append()

    print(f"staging_klines 적재 완료: {staging_df.count()} rows")


if __name__ == "__main__":
    run()