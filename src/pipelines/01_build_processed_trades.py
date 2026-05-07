"""build_processed_trades.py

raw_trades → processed_trades 정제 job.

PRD §10.3 참조.

처리:
- message_value JSON 파싱
- 타입 변환 (string → 적절한 타입)
- trade_id 기준 중복 제거
- trade_time: Unix ms → TIMESTAMP 변환

실행:
    PYTHONPATH=. spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.iceberg:iceberg-aws-bundle:1.7.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        src/pipelines/build_processed_trades.py
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

from src.pipelines.common.spark_session import get_spark

RAW_PATH = "s3a://binance-iceberg-lake/raw/trades/"
TARGET_TABLE = "glue.binance_lakehouse.processed_trades"

# message_value JSON 스키마
# 실제 데이터 확인 결과 기반 (모두 string으로 들어옴)
MESSAGE_SCHEMA = StructType([
    StructField("trade_id",       StringType(), True),
    StructField("price",          StringType(), True),
    StructField("qty",            StringType(), True),
    StructField("quote_qty",      StringType(), True),
    StructField("time",           StringType(), True),
    StructField("is_buyer_maker", StringType(), True),
    StructField("is_best_match",  StringType(), True),
    StructField("symbol",         StringType(), True),
])


def run() -> None:
    spark = get_spark("build_processed_trades")

    # 1. raw_trades 읽기
    raw_df = spark.read.parquet(RAW_PATH)

    # 2. message_value 파싱
    parsed_df = raw_df.withColumn(
        "parsed", F.from_json(F.col("message_value"), MESSAGE_SCHEMA)
    )

    # 3. 컬럼 추출 + 타입 변환
    processed_df = parsed_df.select(
        F.col("parsed.trade_id").cast(LongType()).alias("trade_id"),
        F.col("parsed.symbol").alias("symbol"),
        F.col("parsed.price").cast(DecimalType(20, 8)).alias("price"),
        F.col("parsed.qty").cast(DecimalType(20, 8)).alias("qty"),
        F.col("parsed.quote_qty").cast(DecimalType(20, 8)).alias("quote_qty"),
        # Unix ms → timestamp
        F.to_timestamp(
            (F.col("parsed.time").cast(LongType()) / 1000).cast("timestamp")
        ).alias("trade_time"),
        # "True"/"False" 문자열 → boolean
        (F.col("parsed.is_buyer_maker") == "True").cast(BooleanType()).alias("is_buyer_maker"),
        (F.col("parsed.is_best_match") == "True").cast(BooleanType()).alias("is_best_match"),
        F.col("kafka_topic").alias("source_topic"),
        F.col("kafka_partition").alias("source_partition"),
        F.col("kafka_offset").alias("source_offset"),
        F.col("ingest_time"),
    )

    # 4. trade_id 기준 중복 제거
    deduped_df = processed_df.dropDuplicates(["trade_id"])

    # 5. processed_trades에 append
    deduped_df.writeTo(TARGET_TABLE).append()

    print(f"processed_trades 적재 완료: {deduped_df.count()} rows")


if __name__ == "__main__":
    run()