-- execution_vs_market_view.sql (Phase X / X-4)
--
-- 서빙 뷰: "시장이 이렇게 움직인 시간대에, 우리 주문은 이만큼 체결됐고,
-- 벤치마크(VWAP) 대비 이만큼 유·불리했다"를 한 행에서 본다.
--
-- Grafana 는 Athena 데이터소스로 조회하므로 Athena 뷰로 생성한다(Glue 에 뷰로 등록).
-- 적용:  athena 쿼리로 이 파일을 실행 (database = binance_lakehouse).
--
-- 슬리피지 부호 규약: 양수 = 유리. buy_slippage_bps 는 VWAP보다 싸게 매수,
-- sell_slippage_bps 는 VWAP보다 비싸게 매도했을 때 양수.
--
-- 순환 방지: benchmark_vwap 은 market_hourly_summary(시장 체결) 유래,
-- 체결가는 order_execution_summary(주문 앵커=분 close 기반) 유래 — 서로 다른 시계열.

CREATE OR REPLACE VIEW binance_lakehouse.execution_vs_market AS
SELECT
    o.summary_hour,
    o.symbol,
    o.benchmark_vwap        AS market_vwap,
    o.avg_buy_fill_price,
    o.avg_sell_fill_price,
    o.buy_slippage_bps,
    o.sell_slippage_bps,
    o.slippage_cost_quote,
    o.fill_rate,
    o.cancel_rate,
    o.avg_fill_delay_sec,
    m.high_price,
    m.low_price,
    m.kline_quote_volume
FROM binance_lakehouse.order_execution_summary o
LEFT JOIN binance_lakehouse.market_hourly_summary m
       ON m.summary_hour = o.summary_hour
      AND m.symbol = o.symbol;
