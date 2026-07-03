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

## Streaming End-to-End Lag (P5 신규)

dashboard file: `dashboard/grafana/dashboards/streaming-lag.json`
source: `lag_samples` (P2/P3 벤치 센터피스)

| Panel | Type | Metrics | Query 기준 |
|---|---|---|---|
| p50/p95/p99 Lag (latest run) | stat | `approx_percentile(lag_ms, x)/1000` | `run_id = 최신 commit_ts` |
| Throughput (latest run) | stat | `count / (max(commit_ts)-min(produce_ts))` | 최신 run |
| Lag Percentiles over Time | timeseries | p50/p95/p99 | `from_unixtime(commit_ts/1000)` 버킷 |
| Ablation by Config | table | config별 p50/p95/p99·max·throughput | `GROUP BY config_label ORDER BY p95` |

- Thresholds(p95): green < 15s, yellow ≥ 15s, red ≥ 25s. 단위 `s`.
- 기본 time range는 `now-24h`(lag는 벤치 실행 wall-clock 기준).
- **데이터소스 전제(정직)**: `lag_samples`는 벤치가 **로컬 hadoop catalog(file://)** 에 쓴다.
  이 Athena 대시보드가 값을 보려면 lag_samples가 **lakehouse(Glue/S3)에 발행**돼 있어야 한다
  (예: 벤치를 glue catalog로 실행하거나 별도 publish). 대시보드 JSON은 두 경우 동일.

## Data Quality Anomalies (P5 신규)

dashboard file: `dashboard/grafana/dashboards/quality-events.json`
source: `quality_events` (P4 이상탐지 출력)

| Panel | Type | Metrics | Query 기준 |
|---|---|---|---|
| CRITICAL / WARN Anomalies | stat | `COUNT(*)` by severity | 최근 7일 |
| Distinct Checks Firing | stat | `COUNT(DISTINCT check_name)` | 최근 7일 |
| Tables with Anomalies | stat | `COUNT(DISTINCT source_table)` | 최근 7일 |
| Anomaly Count by Check | timeseries | check_name별 건수 | 최근 30일 |
| Anomaly Count by Severity | timeseries | CRITICAL/WARN 건수 | 최근 30일 |
| Recent Anomalies | table | detected_at·severity·check·dimension·detail | latest 100 by `detected_at DESC` |

- Thresholds: CRITICAL green at 0 / red from 1, WARN green at 0 / yellow from 1.

## Pipeline Run Status (P5 신규)

dashboard file: `dashboard/grafana/dashboards/pipeline-runs.json`
source: `pipeline_run_summary`

| Panel | Type | Metrics | Query 기준 |
|---|---|---|---|
| Succeeded / Failed Tasks | stat | `COUNT(*)` by status | 최근 30일 |
| Success Rate (%) | stat | `succeeded / total * 100` | 최근 30일 |
| Avg Task Duration (s) | stat | `AVG(duration_sec)` | 최근 30일 |
| Succeeded/Failed over Time | timeseries | status별 건수 | 최근 30일 |
| Task Duration by Task | timeseries | `duration_sec` by task | 최근 30일 |
| Recent Pipeline Runs | table | status·duration·rows·error | latest 100 by `created_at DESC` |

- `status`는 `UPPER(status) IN ('SUCCESS','SUCCEEDED')` / `('FAILED','FAILURE','ERROR')`로 방어적 비교.
- Success Rate thresholds: red < 90, yellow ≥ 90, green ≥ 99.

## Alerting (P5 신규 / FR-11 알림 연동)

provisioning: `dashboard/grafana/provisioning/alerting/`

| 파일 | 역할 |
|---|---|
| `contact-points.yaml` | Discord contact point (`${DISCORD_WEBHOOK_URL}` 확장, 미설정 시 graceful) |
| `notification-policies.yaml` | CRITICAL은 빠른 그룹핑으로 Discord 라우팅 |
| `alert-rules.yaml` | ① `quality_events` CRITICAL 존재(24h) ② `pipeline_run_summary` 실패(24h) → Discord |

- 각 룰: Athena 쿼리(A) → reduce last(B) → threshold > 0 (C). condition=C.
- webhook은 docker-compose grafana 서비스의 `DISCORD_WEBHOOK_URL`로 주입. P4 `quality_scan`
  Discord 알림과 **같은 채널**을 재사용해 알림 경로를 일원화한다.
- **검증됨(grafana 프로파일 실기동)**: 대시보드 7개(신규 3 포함)·알림 룰 2개·Discord contact
  point가 provisioning으로 정상 등록됨을 Grafana API로 확인. 데이터 렌더링은 Athena 필요.
- **주의(실측)**: 잘못된 alerting provisioning은 격리되지 않고 **Grafana 기동 자체를 실패**
  시킨다. 특히 discord contact point는 빈 webhook url이면 provisioning 전체가 죽는다.
  그래서 compose는 미설정 시 placeholder url을 기본값으로 넣어 Grafana가 항상 기동하게 하고,
  실제 전송만 조용히 실패시킨다(진짜 graceful).

## Current Gaps / 정직성 한계

- **모든 대시보드·알림은 config-only**: 실제 렌더링은 Athena datasource + AWS(Glue/S3)가
  있어야 확인된다(기존 4개 대시보드도 동일). 저장소 검증 범위는 JSON/YAML 유효성 + rawSQL
  컬럼이 DDL과 일치하는지까지.
- **lag 대시보드는 lag_samples의 lakehouse 발행에 의존**(위 §Streaming Lag 전제).
- alert-rules.yaml의 Athena query model은 Grafana 버전별 필드 차이가 있을 수 있어
  첫 로드 시 Alerting > Alert rules에서 상태 확인이 필요하다.
- **alerting provisioning은 격리되지 않는다**: 오류 시 Grafana 기동 자체가 실패하므로,
  이 디렉터리를 수정하면 반드시 grafana를 재기동해 provisioning 에러가 없는지 확인할 것.
