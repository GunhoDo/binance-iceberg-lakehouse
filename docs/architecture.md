# Architecture

본 문서는 PRD §8을 기반으로 본 MVP의 컴포넌트와 책임 경계를 정리한다.
임의의 컴포넌트는 추가하지 않는다.

## 컴포넌트 책임

| 컴포넌트 | 책임 |
|---|---|
| `collectors/` | Binance public market data 수집 (trades, klines) → Kafka publish |
| `simulators/` | user-level orders 합성 → Kafka publish |
| Kafka | 이벤트별 topic으로 분리된 이벤트 스트림 보관 |
| `streams/` | Kafka → Spark Structured Streaming → Raw Zone (append-only) |
| Raw Zone | Kafka 원본 메타데이터 포함, 재처리 기준 |
| Staging Tables | MERGE 입력을 안정화하기 위한 중간 테이블. Phase 2에서는 `staging_klines`를 사용한다. |
| `jobs/` / `pipelines/` | Raw → Staging → Processed → Serving Spark batch job |
| Iceberg metadata | snapshot / files / partitions 등 운영 가시성 source |
| `dags/` | Phase 3에서 Spark job을 orchestrate |
| QuickSight | Phase 4에서 serving / observability table 시각화 |

## MVP 흐름

```text
Binance Public Market Data
   ├── trades collector
   │      ↓
   │   Kafka topic: trades
   │      ↓
   │   raw_trades
   │      ↓
   │   processed_trades
   │
   └── klines collector
          ↓
       Kafka topic: klines
          ↓
       raw_klines
          ↓
       staging_klines
          ↓
       processed_klines

Order Simulator
   ↓
Kafka topic: orders
   ↓
raw_orders
   ↓
processed_orders

processed_trades + processed_klines + processed_orders
   ↓
market_hourly_summary
order_execution_summary
data_quality_summary
pipeline_run_summary
table_health_summary
   ↓
QuickSight
```

## Airflow 확장 후 흐름

```text
Streaming Collectors (long-running)
   ↓
Raw Zone

Airflow Daily Pipeline DAG
   ↓
build_processed_trades
   ↓
build_processed_klines        # raw_klines → staging_klines
   ↓
merge_kline_updates           # staging_klines → processed_klines
   ↓
build_processed_orders
   ↓
merge_order_status_updates
   ↓
build_serving_summaries
   ↓
check_data_quality
   ↓
check_table_health

Airflow Maintenance DAG (분리)
   ↓
check_small_files
   ↓
compact_tables
   ↓
check_after_compaction
```

Pipeline DAG와 Maintenance DAG를 분리하는 이유는 데이터 처리 흐름과 Iceberg
유지보수 작업의 실행 목적이 다르기 때문이다 (PRD §14.3).

## 책임 경계 — 자주 헷갈릴 만한 두 가지

### Kline upsert는 누구의 책임인가

Raw Zone은 append-only로만 받고, kline의 upsert-like 처리는 processed layer에서
한다. 이유는 `decisions.md` D7 참조.

### staging_klines는 왜 필요한가

`processed_klines`는 (symbol, interval, open_time) 기준으로 MERGE한다.
하지만 MERGE source 안에 같은 key가 여러 번 존재하면 하나의 target row에 여러
source row가 매칭될 수 있다.

따라서 Phase 2에서는 raw_klines를 바로 processed_klines에 반영하지 않고,
먼저 staging_klines에 정제 결과를 적재한다. 이후 `07_merge_kline_updates.sql`에서
MERGE 직전에 key 단위 dedup을 수행한 뒤 processed_klines에 반영한다.

이 staging table은 영구 serving table이 아니라, MERGE source 안정화를 위한 중간 테이블이다.

### trades와 klines는 왜 processed에서 합치지 않는가

`decisions.md` D4 참조. processed_market_events 단일 테이블로 합치면 sparse
union schema가 silver에 그대로 옮겨지기 때문이다.

