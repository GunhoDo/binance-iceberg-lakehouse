-- spark에서 실행

CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.staging_klines (
    symbol            STRING,
    `interval`        STRING,
    open_time         TIMESTAMP,
    close_time        TIMESTAMP,
    open              DECIMAL(20, 8),
    high              DECIMAL(20, 8),
    low               DECIMAL(20, 8),
    close             DECIMAL(20, 8),
    volume            DECIMAL(20, 8),
    quote_volume      DECIMAL(24, 8),
    number_of_trades  BIGINT,
    is_closed         BOOLEAN,
    source_topic      STRING,
    source_partition  INT,
    source_offset     BIGINT,
    updated_at        TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(open_time))
TBLPROPERTIES (
    'format-version' = '2'
);
