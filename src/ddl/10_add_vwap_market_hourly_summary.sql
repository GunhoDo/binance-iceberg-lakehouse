-- 10_add_vwap_market_hourly_summary.sql
--
-- Phase G (VWAP 벤치마크) 마이그레이션.
--
-- market_hourly_summary 에 vwap 컬럼을 추가한다.
--   vwap = Σ(quote_qty) / NULLIF(Σ(qty), 0)  (processed_trades 기준, 시간당 집계)
--
-- 신규 설치는 07_create_serving_tables.sql 의 CREATE 정의에 이미 vwap 이 포함돼 있다.
-- 이 ALTER 는 07 을 CREATE IF NOT EXISTS 로 이미 만든 기존 환경을 위한 것이다.
-- Iceberg 는 ADD COLUMN 을 메타데이터 전용 연산으로 처리하므로 재작성이 없다.
-- 이미 vwap 이 있는 환경에서 재실행하면 실패하므로 최초 1회만 적용한다.

ALTER TABLE glue.binance_lakehouse.market_hourly_summary
    ADD COLUMN vwap DECIMAL(20, 8)
    AFTER avg_trade_price;
