-- 00_create_raw_tables.sql
--
-- Raw Zone은 Iceberg가 아니다.
-- Spark Structured Streaming이 S3에 plain Parquet으로 쓰고,
-- Athena/Glue에서 읽을 수 있도록 External Table만 정의한다.
--
-- 이유:
--   - Raw는 재처리 기준이 되는 원본이다. Iceberg의 snapshot/MERGE가 필요 없다.
--   - Iceberg는 processed(Silver) 부터 시작한다.
--   - 스터디 가이드: "브론즈의 스몰파일은 아이스버그 밖이라 건드리지는 못함"
--
-- 모든 테이블은 binance_lakehouse 데이터베이스 안에 둔다.
-- 레이어 구분은 테이블 이름 prefix로 한다 (raw_).
--
-- Athena에서 실행한다.

CREATE EXTERNAL TABLE IF NOT EXISTS binance_lakehouse.raw_trades (
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_timestamp   BIGINT,
    message_key       STRING,
    message_value     STRING,
    ingest_time       STRING
)
PARTITIONED BY (year STRING, month STRING)
STORED AS PARQUET
LOCATION 's3://binance-iceberg-lake/raw/trades/';

CREATE EXTERNAL TABLE IF NOT EXISTS binance_lakehouse.raw_klines (
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_timestamp   BIGINT,
    message_key       STRING,
    message_value     STRING,
    ingest_time       STRING
)
PARTITIONED BY (year STRING, month STRING)
STORED AS PARQUET
LOCATION 's3://binance-iceberg-lake/raw/klines/';

CREATE EXTERNAL TABLE IF NOT EXISTS binance_lakehouse.raw_orders (
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    kafka_timestamp   BIGINT,
    message_key       STRING,
    message_value     STRING,
    ingest_time       STRING
)
PARTITIONED BY (year STRING, month STRING)
STORED AS PARQUET
LOCATION 's3://binance-iceberg-lake/raw/orders/';

-- 파티션 인식 (새 파티션이 생길 때마다 실행)
-- MSCK REPAIR TABLE binance_lakehouse.raw_trades;
-- MSCK REPAIR TABLE binance_lakehouse.raw_klines;
-- MSCK REPAIR TABLE binance_lakehouse.raw_orders;