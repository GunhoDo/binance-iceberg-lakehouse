-- 02_create_processed_trades.sql
--
-- processed_trades — raw trade event를 정제한 체결 단위 Iceberg table.
-- PRD §10.3, §11 (COW) 참조.
--
-- Write Pattern: Append (PRD §9).
--
-- processed_trades
-- raw_trades의 message_value를 파싱한 Iceberg table.
-- Write Pattern: Append (중복 제거 후 insert)
-- spark에서 실행

CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.processed_trades (
    trade_id          BIGINT,
    symbol            STRING,
    price             DECIMAL(20, 8),
    qty               DECIMAL(20, 8),
    quote_qty         DECIMAL(20, 8),
    trade_time        TIMESTAMP,       -- time(ms) → timestamp 변환
    is_buyer_maker    BOOLEAN,
    is_best_match     BOOLEAN,
    source_topic      STRING,
    source_partition  INT,
    source_offset     BIGINT,
    ingest_time       STRING
)
USING iceberg
PARTITIONED BY (days(trade_time))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write'
);