-- 05_create_serving_tables.sql
--
-- Serving tables — Grafana 조회 대상. PRD §10.6, §11 (COW), §13.1 참조.
--
-- 본 단계의 컬럼은 PRD §10.6 / §13.1의 지표 목록을 기반으로 작성한다.
-- 정확한 타입과 partition은 Phase 2에서 결정 (decisions.md D9).

-- ----------------------------------------------------------------------------
-- market_hourly_summary
-- ----------------------------------------------------------------------------
--
-- source: processed_klines (OHLCV 기준) + processed_trades (보조 지표)
-- key: (symbol, summary_hour)
-- write pattern: MERGE
-- table mode: MOR
-- Phase 2 MVP: full aggregation 후 동일 key는 UPDATE, 신규 key는 INSERT
-- Future: Airflow execution window 기준 incremental aggregation

CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.market_hourly_summary (
    summary_hour             TIMESTAMP,
    symbol                   STRING,

    open_price               DECIMAL(20, 8),
    high_price               DECIMAL(20, 8),
    low_price                DECIMAL(20, 8),
    close_price              DECIMAL(20, 8),

    kline_volume             DECIMAL(30, 8),
    kline_quote_volume       DECIMAL(30, 8),
    kline_number_of_trades   BIGINT,

    trade_count              BIGINT,
    trade_qty                DECIMAL(30, 8),
    trade_quote_qty          DECIMAL(30, 8),
    avg_trade_price          DECIMAL(20, 8),
    vwap                     DECIMAL(20, 8),
    maker_trade_count        BIGINT,
    taker_trade_count        BIGINT,

    updated_at               TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(summary_hour))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read'
);

-- ----------------------------------------------------------------------------
-- order_execution_summary
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.order_execution_summary (
    summary_hour          TIMESTAMP,
    symbol                STRING,
    total_orders          BIGINT,
    filled_orders         BIGINT,
    canceled_orders       BIGINT,
    fill_rate             DOUBLE,
    cancel_rate           DOUBLE,
    avg_fill_delay_sec    DOUBLE,
    avg_order_qty         DECIMAL(20, 8),
    avg_filled_qty        DECIMAL(20, 8),
    total_order_qty       DECIMAL(30, 8),
    total_filled_qty      DECIMAL(30, 8),

    -- Phase X: 방향 분리 슬리피지 (market_hourly_summary.vwap 벤치마크 대비, 양수=유리)
    benchmark_vwap        DECIMAL(20, 8),
    avg_buy_fill_price    DECIMAL(20, 8),
    avg_sell_fill_price   DECIMAL(20, 8),
    buy_slippage_bps      DOUBLE,
    sell_slippage_bps     DOUBLE,
    slippage_cost_quote   DECIMAL(30, 8),

    updated_at            TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(summary_hour))
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read'
);
