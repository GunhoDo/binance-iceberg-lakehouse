-- 11_add_slippage_order_execution_summary.sql
--
-- Phase X (방향 분리 슬리피지) 마이그레이션.
--
-- order_execution_summary 에 벤치마크(VWAP) 대비 방향 분리 슬리피지 컬럼을 추가한다.
--   benchmark_vwap      : market_hourly_summary.vwap 조인값 (Phase G 산출)
--   avg_buy_fill_price  : BUY FILLED 주문의 체결가중 평균 체결가
--   avg_sell_fill_price : SELL FILLED 주문의 체결가중 평균 체결가
--   buy_slippage_bps    : (vwap - buy_fill)/vwap × 10000, 양수=유리(싸게 매수)
--   sell_slippage_bps   : (sell_fill - vwap)/vwap × 10000, 양수=유리(비싸게 매도)
--   slippage_cost_quote : Σ filled_qty × 방향보정 가격차 (양수=유리, quote 통화)
--
-- 신규 설치는 07_create_serving_tables.sql 의 CREATE 정의에 이미 포함돼 있다.
-- 이 ALTER 는 07 을 CREATE IF NOT EXISTS 로 이미 만든 기존 환경을 위한 것이다.
-- Iceberg ADD COLUMN 은 메타데이터 전용이라 재작성이 없다. 최초 1회만 적용한다.
--
-- 컬럼 위치는 baseline CREATE 와 동일하게 total_filled_qty 뒤·updated_at 앞에 둔다
-- (07 job 의 WHEN NOT MATCHED THEN INSERT * 위치 정합).

ALTER TABLE glue.binance_lakehouse.order_execution_summary
    ADD COLUMNS (
        benchmark_vwap        DECIMAL(20, 8)  AFTER total_filled_qty,
        avg_buy_fill_price    DECIMAL(20, 8)  AFTER benchmark_vwap,
        avg_sell_fill_price   DECIMAL(20, 8)  AFTER avg_buy_fill_price,
        buy_slippage_bps      DOUBLE          AFTER avg_sell_fill_price,
        sell_slippage_bps     DOUBLE          AFTER buy_slippage_bps,
        slippage_cost_quote   DECIMAL(30, 8)  AFTER sell_slippage_bps
    );
