# Grafana Metrics

현재 `dashboard/grafana/dashboards/*.json`에 구현된 Grafana dashboard 기준의 지표 목록이다. 모든 dashboard는 Athena datasource `athena_iceberg`를 사용하며, datasource 설정은 `dashboard/grafana/provisioning/datasources/athena.yml`에 둔다.

공통 설정:

- datasource: `grafana-athena-datasource`, uid `athena_iceberg`
- catalog: `AwsDataCatalog`
- database: `binance_lakehouse`
- refresh: `30s`
- 기본 dashboard time range: `2024-01-01T00:00:00Z` ~ `2024-03-01T00:00:00Z`
- 대부분의 추세 패널은 각 source table의 최신 시각 기준 최근 30일을 조회한다.

## Binance Market Overview

dashboard file: `dashboard/grafana/dashboards/binance-market-overview.json`  
source: `market_hourly_summary`

| Panel | Type | Metrics | Query 기준 |
|---|---|---|---|
| Close Price Trend | timeseries | `close_price` | `summary_hour >= max(summary_hour) - 30 days` |
| Kline Volume Trend | timeseries | `kline_volume` | `summary_hour >= max(summary_hour) - 30 days` |
| Trade Count Trend | timeseries | `trade_count` | `summary_hour >= max(summary_hour) - 30 days` |
| Average Trade Price Trend | timeseries | `avg_trade_price` | `summary_hour >= max(summary_hour) - 30 days` |
| Maker / Taker Trade Count | timeseries | `maker_trade_count`, `taker_trade_count` | `summary_hour >= max(summary_hour) - 30 days` |
| Recent Market Summary | table | `summary_hour`, `symbol`, OHLC, volume, quote volume, trade counts, quantities, `updated_at` | latest 100 rows by `summary_hour DESC` |

단위:

- `close_price`, `avg_trade_price`: `currencyUSD`

## Order Execution Summary

dashboard file: `dashboard/grafana/dashboards/order-execution.json`  
source: `order_execution_summary`

| Panel | Type | Metrics | Query 기준 |
|---|---|---|---|
| Fill Rate (%) | stat | `AVG(fill_rate) * 100` | `summary_hour >= max(summary_hour) - 30 days` |
| Cancel Rate (%) | stat | `AVG(cancel_rate) * 100` | `summary_hour >= max(summary_hour) - 30 days` |
| Total Orders | stat | `SUM(total_orders)` | `summary_hour >= max(summary_hour) - 30 days` |
| Avg Fill Delay (sec) | stat | `AVG(avg_fill_delay_sec)` | `summary_hour >= max(summary_hour) - 30 days` |
| Fill / Cancel Rate Trend | timeseries | `fill_rate * 100`, `cancel_rate * 100` | ordered by `summary_hour` |
| Hourly Orders | timeseries | `total_orders`, `filled_orders`, `canceled_orders` | ordered by `summary_hour` |
| Order Quantity Trend | timeseries | `total_order_qty`, `total_filled_qty` | ordered by `summary_hour` |
| Recent Order Execution Summary | table | order count, rate, delay, quantity, `updated_at` columns | latest 100 rows by `summary_hour DESC` |

Thresholds:

- Fill Rate: red below 90, yellow from 90, green from 98
- Cancel Rate: green below 5, yellow from 5, red from 10

단위:

- Fill/Cancel Rate: `percent`
- Avg Fill Delay: `s`

## Lakehouse Data Quality

dashboard file: `dashboard/grafana/dashboards/data-quality.json`  
source: `data_quality_summary`

| Panel | Type | Metrics | Query 기준 |
|---|---|---|---|
| Latest Checked Tables | stat | `COUNT(DISTINCT table_name)` | latest `checked_at` |
| Latest Total Rows | stat | `SUM(row_count)` | latest `checked_at` |
| Latest Null Count | stat | `SUM(null_count)` | latest `checked_at` |
| Latest Duplicate Count | stat | `SUM(duplicate_count)` | latest `checked_at` |
| Row Count by Table | timeseries | `table_name`, `row_count` | `checked_at >= max(checked_at) - 30 days` |
| Null / Duplicate Count | timeseries | `SUM(null_count)`, `SUM(duplicate_count)` | grouped by `checked_at` for latest 30 days |
| Latest Quality Check by Table | table | latest row per `(table_name, check_name)` | `ROW_NUMBER()` by latest `checked_at` |

Thresholds:

- Latest Null Count: green at 0, red from 1
- Latest Duplicate Count: green at 0, red from 1

## Iceberg Table Operations

dashboard file: `dashboard/grafana/dashboards/iceberg-operations.json`  
source: `table_health_summary`

| Panel | Type | Metrics | Query 기준 |
|---|---|---|---|
| Latest Data Files | stat | `SUM(data_file_count)` | latest `checked_at` |
| Position Delete Files | stat | `SUM(position_delete_file_count)` | latest `checked_at` |
| Avg Delete/Data Ratio | stat | `AVG(delete_to_data_file_ratio)` | latest `checked_at` |
| Avg File Size MB | stat | `AVG(avg_file_size_mb)` | latest `checked_at` |
| Snapshot Count | stat | `SUM(snapshot_count)` | latest `checked_at` |
| Manifest Count | stat | `SUM(manifest_count)` | latest `checked_at` |
| Total Records | stat | `SUM(record_count)` | latest `checked_at` |
| Total Size MB | stat | `SUM(total_size_mb)` | latest `checked_at` |
| File Count Trend | timeseries | `data_file_count`, `position_delete_file_count`, `equality_delete_file_count` by table | `checked_at >= max(checked_at) - 30 days` |
| Snapshot / Manifest Count Trend | timeseries | `snapshot_count`, `manifest_count` by table | `checked_at >= max(checked_at) - 30 days` |
| Latest Table Health by Table | table | latest file/delete/size/record/metadata counts per table | `ROW_NUMBER()` by latest `checked_at` |

Thresholds:

- Position Delete Files: green below 5, yellow from 5, red from 10
- Avg Delete/Data Ratio: green below 0.3, yellow from 0.3, red from 0.5
- Avg File Size MB: red below 16, yellow from 16, green from 64

## Current Gaps

- `pipeline_run_summary`는 아직 별도 Grafana dashboard에 연결되어 있지 않다.
- Grafana alert rule은 아직 JSON/provisioning으로 정의하지 않았다. 현재는 panel threshold로만 위험 신호를 표시한다.
