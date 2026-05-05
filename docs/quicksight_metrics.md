# QuickSight Metrics

PRD §15에 정의된 4개 대시보드의 지표 목록을 그대로 정리한다. 정확한 차트 형태와
필터는 Phase 4에서 결정한다.

## Market Dashboard

source: `market_hourly_summary`

- symbol/hour 거래량
- symbol/hour 가격 변화
- OHLCV
- number of trades
- volatility proxy

## Order Execution Dashboard

source: `order_execution_summary`

- total orders
- filled orders
- canceled orders
- fill rate
- cancel rate
- average fill delay
- slippage proxy

## Data Quality Dashboard

source: `data_quality_summary`

- raw row count
- processed row count
- row count difference
- duplicate count
- null required column count
- invalid price/quantity count
- freshness lag

## Iceberg Operations Dashboard

source: `table_health_summary`

- processed table file count
- serving table file count
- average file size
- small file count
- snapshot count
- last compaction time
- compaction needed flag

## 보류

- QuickSight 데이터셋 새로고침 주기, 권한, SPICE 사용 여부 — Phase 4 진입 시 결정.
- 차트 종류와 필터 — Phase 4에서 실제 데이터로 결정.
