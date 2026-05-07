-- 07_merge_kline_updates.sql
--
-- staging_klines의 kline update를 processed_klines에 MERGE INTO로 반영한다.
-- PRD §10.4, §12.2, §11 (COW) 참조.
--
-- Key: (symbol, interval, open_time)
--
-- 결정 사항 (decisions.md D6):
--   1. MERGE source 안에서 같은 key가 여러 번 나타날 수 있으므로,
--      MERGE 직전에 key 단위 dedup을 수행한다.
--   2. dedup 기준:
--        - PARTITION BY symbol, `interval`, open_time
--        - ORDER BY source_offset DESC, updated_at DESC
--   3. late event 방어:
--        - source.source_offset >= target.source_offset 인 경우에만 UPDATE한다.
--
-- 참고:
--   - Phase 2의 raw_klines는 historical kline 기반이므로 is_closed=true로 적재한다.
--   - 향후 WebSocket kline 수집으로 확장할 경우, close flag(x/is_closed)와
--     event_time 기반 late event 방어 조건을 추가 검토한다.

MERGE INTO glue.binance_lakehouse.processed_klines AS target
USING (
    SELECT
        symbol,
        `interval`,
        open_time,
        close_time,
        open,
        high,
        low,
        close,
        volume,
        quote_volume,
        number_of_trades,
        is_closed,
        source_topic,
        source_partition,
        source_offset,
        updated_at
    FROM (
        SELECT
            symbol,
            `interval`,
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            number_of_trades,
            is_closed,
            source_topic,
            source_partition,
            source_offset,
            updated_at,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, `interval`, open_time
                ORDER BY source_offset DESC, updated_at DESC
            ) AS rn
        FROM glue.binance_lakehouse.staging_klines
        WHERE symbol IS NOT NULL
          AND `interval` IS NOT NULL
          AND open_time IS NOT NULL
    ) deduped
    WHERE rn = 1
) AS source
ON target.symbol = source.symbol
   AND target.`interval` = source.`interval`
   AND target.open_time = source.open_time

WHEN MATCHED AND source.source_offset >= target.source_offset THEN UPDATE SET
    target.close_time        = source.close_time,
    target.open              = source.open,
    target.high              = source.high,
    target.low               = source.low,
    target.close             = source.close,
    target.volume            = source.volume,
    target.quote_volume      = source.quote_volume,
    target.number_of_trades  = source.number_of_trades,
    target.is_closed         = source.is_closed,
    target.source_topic      = source.source_topic,
    target.source_partition  = source.source_partition,
    target.source_offset     = source.source_offset,
    target.updated_at        = source.updated_at

WHEN NOT MATCHED THEN INSERT (
    symbol,
    `interval`,
    open_time,
    close_time,
    open,
    high,
    low,
    close,
    volume,
    quote_volume,
    number_of_trades,
    is_closed,
    source_topic,
    source_partition,
    source_offset,
    updated_at
)
VALUES (
    source.symbol,
    source.`interval`,
    source.open_time,
    source.close_time,
    source.open,
    source.high,
    source.low,
    source.close,
    source.volume,
    source.quote_volume,
    source.number_of_trades,
    source.is_closed,
    source.source_topic,
    source.source_partition,
    source.source_offset,
    source.updated_at
);