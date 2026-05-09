RAW_TRADES_PATH = "s3a://binance-iceberg-lake/raw/trades/"
RAW_KLINES_PATH = "s3a://binance-iceberg-lake/raw/klines/"
RAW_ORDERS_PATH = "s3a://binance-iceberg-lake/raw/orders/"

PROCESSED_TRADES = "glue.binance_lakehouse.processed_trades"

STAGING_KLINES = "glue.binance_lakehouse.staging_klines"
PROCESSED_KLINES = "glue.binance_lakehouse.processed_klines"

STAGING_ORDERS = "glue.binance_lakehouse.staging_orders"
PROCESSED_ORDERS = "glue.binance_lakehouse.processed_orders"

MARKET_HOURLY_SUMMARY = "glue.binance_lakehouse.market_hourly_summary"
ORDER_EXECUTION_SUMMARY = "glue.binance_lakehouse.order_execution_summary"

DATA_QUALITY_SUMMARY = "glue.binance_lakehouse.data_quality_summary"
PIPELINE_RUN_SUMMARY = "glue.binance_lakehouse.pipeline_run_summary"
TABLE_HEALTH_SUMMARY = "glue.binance_lakehouse.table_health_summary"