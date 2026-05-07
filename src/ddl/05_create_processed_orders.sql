-- 04_create_processed_orders.sql
--
-- processed_orders — 주문별 최신 상태를 관리하는 Iceberg table.
-- PRD §10.5, §11 (COW) 참조.
--
-- 상태 전이:
--   NEW → PARTIALLY_FILLED → FILLED
--   NEW → CANCELED
--
-- Write Pattern: Append + MERGE (PRD §9).
-- Key: order_id

CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.processed_orders (
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
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    filled_at             TIMESTAMP,
    canceled_at           TIMESTAMP,
    simulated_parameters  STRING,
    source_topic          STRING,
    source_partition      INT,
    source_offset         BIGINT
)
USING iceberg
PARTITIONED BY (days(updated_at))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode' = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write'
);