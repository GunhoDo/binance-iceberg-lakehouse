--staging_orders는 정제된 이벤트 로그라서 append 중심이다 따라서 COW/MERGE 옵션은 굳이 안 넣었다.

CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.staging_orders (
    order_id              STRING,
    client_id             STRING,
    symbol                STRING,
    side                  STRING,
    order_type            STRING,
    order_price           DECIMAL(20, 8),
    order_qty             DECIMAL(20, 8),
    filled_qty            DECIMAL(20, 8),
    avg_fill_price        DECIMAL(20, 8),
    event_type            STRING,
    order_status          STRING,
    event_time            TIMESTAMP,
    simulated_parameters  STRING,
    source_topic          STRING,
    source_partition      INT,
    source_offset         BIGINT,
    ingest_time           STRING,
    updated_at            TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version' = '2'
);