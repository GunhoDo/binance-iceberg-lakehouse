# Operations

본 문서는 PRD §13 (Observability Plan) 과 §14 (Airflow Plan) 을 운영 관점에서
정리한다. 임의의 도구 / 임계값을 추가하지 않는다.

## Observability 분류

본 프로젝트는 ETL 구현 자체에서 끝나지 않고, 데이터와 파이프라인이 운영 가능한
상태인지 확인할 수 있는 관측 지표를 함께 설계한다.

| 분류 | 대표 지표 | 저장소 |
|---|---|---|
| Business | symbol/hour 거래량, fill rate 등 | `market_hourly_summary`, `order_execution_summary` |
| Data Quality | row count, duplicate, null, freshness lag | `data_quality_summary` |
| Pipeline | DAG run status, task duration, retry count | `pipeline_run_summary` |
| Iceberg Operation | snapshot count, file count, avg file size | `table_health_summary` |

## 임계값 (PRD §13.5)

아래 값은 **초기 운영 시작 임계값**이며 데이터 유입량과 commit 빈도를 관찰하면서
조정한다 (`decisions.md` D9 참조).

| Metric | Warning Condition |
|---|---|
| `duplicate_order_count` | `> 0` |
| `null_symbol_count` | `> 0` |
| `invalid_price_count` | `> 0` |
| `invalid_quantity_count` | `> 0` |
| `freshness_lag_minutes` | `> 10` |
| `avg_file_size_mb` | `< 64` |
| `small_file_count` | `> 10` |
| `snapshot_count` | `> 20` |
| `last_successful_run_hours` | `> 24` |

## Airflow

### Streaming ingestion (long-running)

- `trades` collector
- `klines` collector
- `orders` simulator
- `stream_raw_trades`
- `stream_raw_klines`
- `stream_raw_orders`

위는 DAG가 아니라 long-running 프로세스로 운영된다.

### Daily Pipeline DAG

`build_processed_trades → build_processed_klines → build_processed_orders →
merge_order_status_updates → build_market_hourly_summary →
build_order_execution_summary → check_data_quality → check_table_health`

### Maintenance DAG

`check_small_files → compact_processed_tables → compact_serving_tables →
check_after_compaction`

### 두 DAG를 분리하는 이유

데이터 처리 흐름과 Iceberg 유지보수 작업의 실행 목적이 다르기 때문이다.

## 보류 항목

- DAG 실행 주기 / SLA — Phase 3 진입 시 결정.
- 알람 채널 — Phase 3 후반에 결정. 알람 도구는 PRD에 없는 한 추가하지 않는다.
