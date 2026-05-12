# PRD: Binance Market Data 기반 Apache Iceberg Lakehouse MVP

## 1. Overview

본 프로젝트는 Binance 공개 시장 데이터와 시뮬레이션 주문 이벤트를 이용해 Apache Iceberg 기반 Lakehouse를 구축하는 데이터 엔지니어링 MVP다.

Binance의 `trades`와 `klines`는 공개 시장 데이터로 수집하고, user-level `orders`는 시뮬레이터로 생성한다. 거래소의 사용자별 주문 데이터는 본질적으로 private data이므로, 본 프로젝트에서는 실제 주문 데이터인 것처럼 가장하지 않고 주문 도착률, 가격 분포, 체결/취소율을 설계한 시뮬레이터를 통해 주문 이벤트를 생성한다.

본 프로젝트는 전통적인 Bronze/Silver/Gold 용어 대신 실제 책임이 드러나는 Raw / Processed / Serving 명칭을 사용한다. 여기서 Raw는 Bronze, Processed는 Silver, Serving은 Gold에 해당한다.

이 프로젝트의 핵심은 단순 ETL이 아니라, 다음 문제를 검증하는 것이다.

- 서로 다른 성격의 market/order event를 topic과 raw table로 분리한다.
- Raw Zone을 append-only로 유지하여 복구 가능성을 확보한다.
- trades와 klines는 processed layer에서도 분리하여 sparse union schema를 피한다.
- 주문 상태 변화(`NEW → PARTIALLY_FILLED → FILLED / CANCELED`)를 Iceberg MERGE로 관리한다.
- kline/trade/order 기반 지표를 serving table로 사전 집계한다.
- Iceberg metadata table을 이용해 파일 수, snapshot 수, compaction 필요 여부를 관찰한다.
- Airflow와 Grafana를 통해 파이프라인과 데이터 상태를 운영 지표로 확인한다.
- kline update는 `staging_klines`에서 key 단위 dedup 후 `processed_klines`에 MERGE한다.
- order status event는 `staging_orders`에서 최신 상태를 선별한 뒤 `processed_orders`에 MERGE한다.
- update가 발생할 수 있는 `processed_klines`, `processed_orders`는 확장성을 고려해 MOR(Merge-on-Read) 기반으로 설계한다.
- MOR table에서 발생할 수 있는 delete file과 manifest 증가는 Iceberg metadata table로 관찰하고, maintenance 단계에서 rewrite/compaction 정책을 적용한다.

---

## 2. Background

Binance는 공개 market data stream으로 trade, aggregate trade, kline/candlestick 등 다양한 이벤트를 제공한다. Kline stream은 open, close, high, low, volume, number of trades 같은 캔들 데이터를 제공하며, aggregate trade는 동일 가격과 방향의 체결을 집계한 market trade 정보를 제공한다.

반면 사용자별 주문 이벤트는 public market data가 아니라 user data stream 또는 account/order API 영역에 속한다. 주문 상태나 execution report는 user data stream과 연결되어 있으며, API key 및 signature가 필요한 private account 영역이다.

따라서 본 프로젝트는 다음 원칙을 따른다.

- `trades`, `klines`: Binance public market data 기반 실데이터
- `orders`: user-level private data를 대체하는 시뮬레이션 데이터
- simulated orders는 실제 Binance 주문이라고 주장하지 않는다.
- order simulator는 도메인 가정과 실험 목적을 문서화한다.

이 구성은 market data와 private order data의 경계를 명확히 유지하면서도, Iceberg의 update, snapshot, compaction이 필요한 주문 상태 관리 시나리오를 자연스럽게 만든다.

---

## 3. Domain Problem

거래소 데이터는 이벤트 유형별로 도착 시점, 스키마, 볼륨, 출처 시스템이 다르다.

| Event | Source | Characteristics |
|---|---|---|
| `trades` | Binance public market stream/API | 체결 단위 시장 데이터, 고빈도 append-only |
| `klines` | Binance public kline stream/API | 시간 구간별 OHLCV 데이터, interval 진행 중 반복 update |
| `orders` | simulator | 사용자 주문 상태 이벤트, 상태 변화와 취소/체결 발생 |

이를 하나의 topic 또는 하나의 wide table에 합치면 `price`, `quantity`, `open`, `high`, `low`, `close`, `order_status`, `cancel_reason` 등이 이벤트 타입별로 대부분 null이 되는 sparse union schema가 발생한다.

따라서 이벤트 성격에 따라 topic과 raw table을 분리한다. 또한 processed layer에서도 `trades`와 `klines`를 하나의 `processed_market_events` table로 합치지 않고, `processed_trades`와 `processed_klines`로 분리한다. 공통 market KPI가 필요한 경우에는 serving table인 `market_hourly_summary` 생성 단계에서 `symbol`과 time window 기준으로 조합한다.

---

## 4. Goals

### 4.1 Core Goals

- Binance public data 또는 샘플 수집기를 통해 `trades`, `klines` 이벤트를 수집한다.
- 주문 시뮬레이터를 통해 `orders` 이벤트를 생성한다.
- Kafka topic을 이벤트 성격별로 분리한다.
- Spark Structured Streaming으로 Kafka topic을 읽어 S3 Raw Zone에 append-only 저장한다.
- Raw Zone을 기반으로 Iceberg staging/processed tables를 생성한다.
- 주문 상태 변화 이벤트를 `processed_orders`에 `MERGE INTO`로 반영한다.
- 진행 중인 kline update를 `staging_klines`에 정제한 뒤, MERGE 직전 key 단위 dedup을 거쳐 `processed_klines`에 최신 상태로 반영한다.
- 주문 상태 이벤트를 `staging_orders`에 정제한 뒤, `order_id` 기준 최신 상태를 `processed_orders`에 MERGE한다.
- market/order 지표를 serving tables로 사전 집계한다.
- update가 발생하는 `processed_klines`, `processed_orders`는 확장성을 고려해 MOR 기반 Iceberg table로 설계한다.
- MOR table의 delete file, manifest, snapshot 상태를 Iceberg metadata table로 확인한다.
- Iceberg snapshot과 files metadata를 조회한다.
- Compaction 전후 파일 개수와 평균 파일 크기를 비교한다.

### 4.2 Operation Goals

- 데이터 품질 지표를 monitoring table에 저장한다.
- 파이프라인 실행 결과를 summary table로 추적한다.
- Iceberg table의 file count, average file size, snapshot count를 관찰한다.
- 기준값 초과 시 compaction 필요 여부를 판단한다.

### 4.3 Extension Goals

- Airflow DAG로 Raw → Processed → Serving 흐름을 자동화한다.
- Iceberg maintenance DAG를 분리해 compaction을 자동화한다.
- Grafana에서 market KPI, order KPI, pipeline/table health 지표를 시각화한다.
- 데이터 규모가 커질 경우 S3 + Glue Catalog + Athena + Grafana 구조로 확장한다.

---

## 5. Non-Goals

초기 MVP에서는 다음을 구현하지 않는다.

- 실제 Binance user account 주문 수집
- 실제 주문 제출 또는 자동매매
- 투자 전략 추천
- 수익률 최적화
- 실시간 trading bot
- order book 전체 재구성
- Schema Registry
- Kafka Connect
- DLQ
- MSK 운영
- Exactly-once end-to-end 보장
- 대규모 클라우드 운영 자동화

본 프로젝트는 거래 시스템을 만드는 것이 아니라, market/order event를 데이터 플랫폼 관점에서 수집·정제·갱신·집계·모니터링하는 Lakehouse MVP다.

---

## 6. Data Sources

### 6.1 Binance Trades

`trades` 또는 `aggTrades`는 실제 시장 체결 데이터를 나타낸다.

필드:

- `symbol`
- `trade_id` 또는 `agg_trade_id`
- `price`
- `quantity`
- `trade_time`
- `is_buyer_maker`
- `is_best_match`

### 6.2 Binance Klines

`klines`는 특정 interval의 OHLCV 캔들 데이터다.

필드:

- `symbol`
- `interval`
- `open_time`
- `close_time`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `quote_volume`
- `number_of_trades`
- `is_closed`

Kline event는 interval이 진행되는 동안 같은 `symbol`, `interval`, `open_time` 조합으로 여러 번 도착할 수 있다. Raw Zone에서는 모든 event를 append-only로 보관하고, processed layer에서는 최신 kline 상태 또는 `is_closed=true` 상태를 기준으로 MERGE한다.

### 6.3 Simulated Orders

`orders`는 user-level private order event를 대체하기 위해 생성한다.

시뮬레이터는 다음 가정을 가진다.

- 주문 도착률: 시간 구간별 Poisson 또는 고정 rate 기반
- 주문 방향: BUY / SELL 비율 설정
- 주문 가격: 최근 kline close price 주변 분포에서 샘플링
- 주문 수량: log-normal 또는 fixed range 기반
- 주문 상태: NEW, PARTIALLY_FILLED, FILLED, CANCELED
- 취소율: config 기반
- 부분 체결률: config 기반
- 체결 판단: trade price 또는 kline close price와 주문 가격 비교

시뮬레이터 출력은 실제 Binance user data가 아니라, Iceberg 기반 주문 상태 관리 실험을 위한 synthetic order events다.

---

## 7. Kafka Topic Design

초기 MVP에서는 topic을 이벤트 성격별로 분리한다.

| Topic | Source | Reason | Raw Write Pattern |
|---|---|---|---|
| `trades` | Binance public market data | 체결 단위 고빈도 market event | Append only |
| `klines` | Binance public market data | 시간 구간별 OHLCV event | Append only |
| `orders` | simulator | 사용자 주문 상태 변화 event | Append only |

topic을 분리하는 이유는 다음과 같다.

- 이벤트별 스키마가 다르다.
- 이벤트별 볼륨이 다르다.
- 이벤트별 도착 시점이 다르다.
- public market data와 simulated private order data의 출처가 다르다.
- 하나의 wide topic/table에 합치면 sparse union schema가 발생한다.
- downstream에서는 자연스럽게 `symbol`, `event_time`, `order_id` 기준 join/MERGE로 처리할 수 있다.

Kline의 upsert-like 처리는 Raw Zone이 아니라 processed layer의 책임이다. Raw Zone은 Kafka event를 그대로 append-only로 보관한다.

---

## 8. Architecture

초기 MVP 구조는 다음과 같다.

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
staging_orders
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
Grafana
```
`staging_klines`와 `staging_orders`는 serving 대상 테이블이 아니라, MERGE source를 안정화하기 위한 중간 테이블이다. Raw Zone은 append-only로 원본 이벤트를 보존하고, staging table은 raw JSON을 정제한 이벤트 로그를 보관한다. processed table은 MERGE를 통해 최신 상태를 관리한다.


Airflow 확장 후 구조는 다음과 같다.

```text
Airflow Daily Pipeline DAG

build_processed_trades ───────────────┐
                                      ├── build_market_hourly_summary
build_staging_klines → merge_processed_klines ┘

build_staging_orders → merge_processed_orders → build_order_execution_summary

build_market_hourly_summary
build_order_execution_summary
        ↓
check_data_quality
        ↓
check_table_health
```

Daily Pipeline DAG는 Spark 로직을 직접 포함하지 않는다. Airflow는 task dependency, schedule, retry, logging만 담당하고, 실제 Spark job은 `spark-runner` 컨테이너 안에서 실행한다.

실행 구조는 다음과 같다.

```text
Airflow BashOperator
   ↓ docker exec
spark-runner container
   ↓
orchestration/scripts/run_job_with_log.sh
   ↓
orchestration/scripts/run_job.sh
   ↓
src/jobs/daily/*.py
```

`run_job_with_log.sh`는 각 task의 실행 결과를 `pipeline_run_summary`에 append-only로 기록한다.

Airflow Maintenance DAG는 Daily Pipeline DAG와 분리한다.

```text
check_table_health_before
   ↓
run_iceberg_maintenance
   ↓
check_table_health_after
```

Maintenance job은 table policy에 따라 `rewrite_data_files`, `rewrite_position_delete_files`, `rewrite_manifests`, `expire_snapshots`를 수행한다. `remove_orphan_files`는 위험도가 있으므로 MVP에서는 실행하지 않고 skip한다.

---

## 9. Table Design

| Table | Reason | Write Pattern |
|---|---|---|
| `raw_trades` | Binance trade event 원본 보관. 파싱 오류나 집계 로직 변경 시 재처리 가능하다. | Append only, Streaming |
| `raw_klines` | Binance kline event 원본 보관. interval 진행 중 같은 open_time으로 여러 event가 도착해도 모두 보관한다. | Append only, Streaming |
| `raw_orders` | simulator가 생성한 주문 이벤트 원본 보관. 주문 상태 재처리와 시뮬레이터 검증에 사용한다. | Append only, Streaming |
| `processed_trades` | raw trade event를 정제한 체결 단위 table이다. trade_id, price, quantity, trade_time 기준으로 분석한다. | Append |
| `staging_klines` | raw kline event를 파싱·정규화한 MERGE source table이다. MERGE 직전 `(symbol, interval, open_time)` 기준 dedup에 사용한다. | Append |
| `processed_klines` | raw kline event를 정제한 캔들 단위 table이다. symbol, interval, open_time 기준으로 OHLCV 최신 상태를 관리한다. | Append + MERGE |
| `staging_orders` | raw order event를 파싱·정규화한 MERGE source table이다. `order_id` 기준 최신 상태 선별에 사용한다. | Append |
| `processed_orders` | 주문별 최신 상태를 관리한다. NEW, PARTIALLY_FILLED, FILLED, CANCELED 상태 변화가 발생하므로 MERGE가 필요하다. | MERGE |
| `market_hourly_summary` | symbol/hour 기준 가격·거래량 KPI를 사전 집계한다. | MERGE, Incremental |
| `order_execution_summary` | 주문 체결률, 취소율, 평균 체결 지연 등을 사전 집계한다. | MERGE, Incremental |
| `data_quality_summary` | raw/processed row count, duplicate, null, freshness 지표를 저장한다. | Append only |
| `pipeline_run_summary` | Airflow DAG 또는 수동 실행 결과를 기록한다. | Append only |
| `table_health_summary` | Iceberg metadata 기반 file count, avg file size, snapshot count, compaction 필요 여부를 기록한다. | Append only |

---

## 10. Data Flow

### 10.1 Raw Market Data

`raw_trades`와 `raw_klines`는 Binance public market data를 Kafka를 통해 수집한 원본 저장 계층이다.

저장 대상:

- Kafka topic
- Kafka partition
- Kafka offset
- Kafka timestamp
- message key
- message value
- ingest_time

Raw Zone은 append-only로 유지한다. 수집기 오류, 파싱 오류, 집계 로직 변경이 발생해도 raw event를 기준으로 processed table을 재생성할 수 있어야 한다.

---

### 10.2 Raw Orders

`raw_orders`는 order simulator가 생성한 주문 이벤트를 보관한다.

저장 대상:

- order_id
- client_id
- symbol
- side
- order_type
- order_price
- order_qty
- event_type
- order_status
- event_time
- simulated_parameters
- ingest_time

이 table은 실제 Binance private order data가 아니라, user-level order lifecycle을 재현하기 위한 synthetic event source다.

---

### 10.3 Processed Trades

`processed_trades`는 raw trade event를 정제한 체결 단위 Iceberg table이다.

역할:

- trade id 기준 중복 제거
- symbol, trade_time 기준 정규화
- price, quantity 타입 정리
- source topic/partition/offset 보존

주요 컬럼:

- `symbol`
- `trade_id`
- `price`
- `quantity`
- `trade_time`
- `is_buyer_maker`
- `source_topic`
- `source_partition`
- `source_offset`
- `ingest_time`

---

### 10.4 Processed Klines

`processed_klines`는 kline update를 반영한 캔들 단위 Iceberg table이다.

`trades`와 `klines`는 모두 market data이지만 분석 단위가 다르다. `trades`는 개별 체결 event이고, `klines`는 일정 interval의 OHLCV aggregate event이다. 따라서 processed layer에서도 이를 하나의 wide table로 합치지 않고 `processed_trades`, `processed_klines`로 분리한다.

Kline은 interval이 닫히기 전까지 같은 `symbol`, `interval`, `open_time`에 대해 반복 업데이트될 수 있다. 따라서 Phase 2에서는 raw kline event를 먼저 `staging_klines`에 정제 적재하고, `processed_klines`에 MERGE하기 직전에 `(symbol, interval, open_time)` 기준으로 key 단위 dedup을 수행한다.

MERGE 기준:

- key: `(symbol, interval, open_time)`
- dedup order: `source_offset DESC`, `updated_at DESC`
- late event 방어: `source.source_offset >= target.source_offset`

현재 Phase 2의 raw kline 데이터는 historical kline 형태이며 `is_closed` 또는 Binance WebSocket close flag `x`가 없다. 따라서 Phase 2에서는 모든 kline을 이미 종료된 캔들로 보고 `is_closed = true`로 적재한다. 향후 WebSocket 기반 실시간 kline 수집으로 확장할 경우 raw message에 `x` 또는 `is_closed` 필드를 포함하고, processed layer에서 이를 `BOOLEAN`으로 변환한다.

`processed_klines`는 반복 update 가능성이 있는 table이므로 MOR(Merge-on-Read)로 설계한다. 이는 실시간 kline stream에서 같은 `(symbol, interval, open_time)` 키가 interval 종료 전까지 여러 번 갱신될 수 있기 때문이다.

MOR 적용 후에는 delete file 증가와 read amplification을 Iceberg metadata table로 관찰하고, maintenance 단계에서 compaction/rewrite 정책을 적용한다.

주요 컬럼:

- `symbol`
- `interval`
- `open_time`
- `close_time`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `quote_volume`
- `number_of_trades`
- `is_closed`
- `source_topic`
- `source_partition`
- `source_offset`
- `updated_at`

### 10.5 Processed Orders

`processed_orders`는 주문별 최신 상태를 관리하는 Iceberg table이다.

주문 상태는 다음과 같이 변할 수 있다.

```text
NEW
→ PARTIALLY_FILLED
→ FILLED
```

또는

```text
NEW
→ CANCELED
```

Raw Zone은 모든 주문 상태 이벤트를 append-only로 보관한다. Phase 2에서는 raw order event를 먼저 `staging_orders`에 정제 적재하고, `processed_orders`에 MERGE하기 직전에 `order_id` 기준으로 최신 이벤트를 선택한다.

MERGE 기준:

- key: `order_id`
- dedup order: `event_time DESC`, `status_rank DESC`, `source_offset DESC`
- 상태 우선순위: `NEW=1`, `PARTIALLY_FILLED=2`, `FILLED=3`, `CANCELED=3`
- late event 방어: `source.updated_at >= target.updated_at`

`processed_orders`는 주문별 최신 상태 table이며, 같은 `order_id`에 대해 상태 전이가 발생한다. 따라서 향후 주문 이벤트 규모와 상태 update 빈도 증가를 고려해 MOR(Merge-on-Read)로 설계한다.

MOR 적용 후에는 delete file과 manifest 증가를 Iceberg metadata table로 관찰하고, maintenance 단계에서 rewrite/compaction 정책을 적용한다.

주요 컬럼:

- `order_id`
- `symbol`
- `side`
- `order_type`
- `order_price`
- `order_qty`
- `filled_qty`
- `avg_fill_price`
- `order_status`
- `created_at`
- `updated_at`
- `filled_at`
- `canceled_at`
- `simulated_parameters`
- `source_topic`
- `source_partition`
- `source_offset`

---

### 10.6 Serving Tables

`market_hourly_summary`는 symbol/hour 단위 market KPI를 저장한다.

`processed_klines`는 OHLCV 지표의 기준이 되고, `processed_trades`는 trade count, average trade size, maker/taker proxy와 같은 보조 지표를 제공한다. 두 processed table은 serving 단계에서 symbol과 time window 기준으로 조합한다.

예상 지표:

- open price
- close price
- high price
- low price
- trade volume
- quote volume
- number of trades
- average trade size
- volatility proxy

`order_execution_summary`는 simulator 주문 결과 KPI를 저장한다.

예상 지표:

- total orders
- filled orders
- canceled orders
- partial filled orders
- fill rate
- cancel rate
- average fill delay
- average slippage proxy

Serving table은 BI 조회 성능과 비용을 줄이기 위한 사전 집계 계층이다. 대시보드는 `processed_*` 상세 테이블을 매번 스캔하지 않고, 미리 계산된 `market_hourly_summary`, `order_execution_summary`를 조회한다.

---

## 11. Iceberg Table Mode

본 MVP는 table의 update 특성에 따라 COW(Copy-on-Write)와 MOR(Merge-on-Read)를 구분해 사용한다.

- append-only 성격이 강한 table은 COW 또는 append 중심으로 유지한다.
- row-level update / MERGE 빈도가 증가할 수 있는 table은 MOR를 사용한다.
- dashboard 조회 중심의 serving table은 데이터가 늘어나는 상황을 가정하여 MOR를 사용한다.

| Layer | Table | Mode | Reason |
|---|---|---|---|
| Raw | `raw_trades`, `raw_klines`, `raw_orders` | Plain Parquet append-only | Kafka 원본 이벤트를 재처리 기준으로 보존한다. |
| Staging | `staging_klines`, `staging_orders` | Iceberg append-only | Raw JSON을 파싱한 정제 이벤트 로그이며, MERGE source로 사용한다. |
| Processed | `processed_trades` | COW / Append | 체결 이벤트는 append-only 성격이 강하고 기존 row update가 거의 없다. |
| Processed | `processed_klines` | MOR | 실시간 kline stream에서는 같은 `(symbol, interval, open_time)` 키가 interval 종료 전까지 반복 update될 수 있다. |
| Processed | `processed_orders` | MOR | 같은 `order_id`에 대해 주문 상태 전이가 발생한다. |
| Serving | `market_hourly_summary` | MOR | 같은 `(symbol, summary_hour)` 집계 row가 late event, 재처리, incremental window 갱신으로 반복 MERGE될 수 있다. |
| Serving | `order_execution_summary` | MOR | 같은 `(symbol, summary_hour)` 주문 KPI row가 late order event, 재처리, incremental aggregation으로 반복 MERGE될 수 있다. |
| Observability | `data_quality_summary` | Append only | 실행별 품질 지표를 새 row로 누적한다. 기존 row를 최신 상태로 갱신하지 않는다. |
| Observability | `pipeline_run_summary` | Append only | 파이프라인 실행 이력을 시간순으로 누적한다. |
| Observability | `table_health_summary` | Append only | table health 관측값을 시간순 snapshot으로 누적한다. |

`processed_klines`와 `processed_orders`는 MOR를 사용하므로 delete file과 manifest 증가를 관찰해야 한다. Phase 3 Maintenance 단계에서는 `rewrite_data_files`, `rewrite_manifests`, snapshot expiration 등 Iceberg maintenance 작업을 통해 MOR table의 read amplification과 metadata 증가를 관리한다.

Serving table은 dashboard 조회 대상이지만, 본 프로젝트의 `market_hourly_summary`와 `order_execution_summary`는 immutable feature table이 아니다. 두 table은 같은 `(symbol, summary_hour)` key에 대해 late event, 재처리, Airflow execution window 기반 incremental aggregation으로 값이 반복 갱신될 수 있다. 따라서 장기적인 데이터 증가와 update 비용을 고려해 MOR로 설계한다.

Observability table은 누적형 로그 테이블이다. 각 pipeline run 또는 health check마다 새 row를 추가하며, 같은 key의 최신 상태를 유지하기 위해 기존 row를 반복 update하지 않는다. 따라서 append-only가 기본이다.

향후 최신 상태 조회가 필요해지면 `current_table_health`, `current_pipeline_status` 같은 별도 current-state table을 만들고, 해당 table에 MOR를 적용한다.

실제 Phase 3 구현에서는 `market_hourly_summary`, `order_execution_summary`도 동일 summary key에 대해 window 재실행, late event 반영, backfill 시 반복 MERGE될 수 있으므로 MOR 대상으로 관리한다.

단, Observability table은 current-state table이 아니라 append-only log table이므로 MOR 대상이 아니다. `data_quality_summary`, `pipeline_run_summary`, `table_health_summary`는 실행별 관측 결과를 새 row로 누적한다. 다만 append-only table도 small file이 누적될 수 있으므로 `rewrite_data_files` 대상에는 포함할 수 있다.

---

## 12. Iceberg Experiments

### 12.1 Order Status MERGE Check

`staging_orders`를 `processed_orders`에 MERGE하기 전후로 snapshot을 확인한다.

확인 대상:

- 같은 `order_id`에 대한 상태 이벤트 sequence
- `NEW → FILLED`, `NEW → CANCELED`, `NEW → PARTIALLY_FILLED → FILLED` 상태 전이
- MERGE 후 `processed_orders`에 `order_id`당 하나의 row만 남는지
- `filled_at`, `canceled_at` 반영 여부
- snapshot 변화

### 12.2 Kline Update MERGE Check

`staging_klines`를 `processed_klines`에 MERGE하기 전후로 snapshot을 확인한다.

확인 대상:

- `staging_klines`에 정제된 kline update가 적재되었는지
- 같은 `symbol`, `interval`, `open_time`에 대한 반복 update가 dedup되는지
- MERGE source에서 `ROW_NUMBER()` 기준 최신 row만 선택되는지
- `processed_klines` snapshot 변화
- MERGE 전후 record count와 sample row 변화

Phase 2 기준 dedup 전략:

```text
PARTITION BY symbol, interval, open_time
ORDER BY source_offset DESC, updated_at DESC
```
Phase 2의 historical kline 데이터는 close flag가 없으므로 is_closed=true로 처리한다. 실시간 WebSocket kline으로 확장할 경우 is_closed=false 중간 업데이트와 is_closed=true 최종 캔들 반영을 추가로 검증한다.

### 12.3 Metadata Check

Iceberg metadata table을 조회하여 table 상태를 확인한다.

확인 대상:

- files
- snapshots
- partitions

예시 지표:

- file count
- average file size
- total record count
- snapshot count

### 12.4 Compaction Check

Streaming write로 작은 파일이 여러 개 생성된 상태에서 compaction을 실행하고 전후를 비교한다.

| Metric | Before | After |
|---|---:|---:|
| File count | TBD | TBD |
| Average file size | TBD | TBD |
| Snapshot count | TBD | TBD |
| Query time | TBD | TBD |

### 12.5 MOR Delete File Check

`processed_klines`와 `processed_orders`는 MOR table이므로 MERGE 이후 Iceberg metadata table에서 data file과 delete file 분포를 확인한다.

확인 대상:

- `files.content` 분포
- data file count
- delete file count
- average file size
- manifest count
- snapshot 변화

예시:

```sql
SELECT content, COUNT(*) AS file_count
FROM glue.binance_lakehouse.processed_orders.files
GROUP BY content;
```

MOR table은 write 비용을 줄이는 대신 read amplification과 delete file 누적이 발생할 수 있으므로, Phase 3 maintenance에서 `rewrite_data_files`, `rewrite_manifests`, snapshot expiration을 검증한다.

---

## 13. Observability Plan

본 프로젝트는 ETL 파이프라인 구현 자체에서 끝나지 않고, 데이터와 파이프라인이 운영 가능한 상태인지 확인할 수 있는 관측 지표를 함께 설계한다.

관측 지표는 세 가지로 나눈다.

1. Market / Order Business Metrics
2. Data Quality Metrics
3. Pipeline & Iceberg Operation Metrics

### 13.1 Business Metrics

- symbol/hour 거래량
- symbol/hour 가격 변동률
- kline close 기준 OHLCV
- total orders
- filled orders
- canceled orders
- fill rate
- cancel rate
- average fill delay
- slippage proxy

### 13.2 Data Quality Metrics

- raw trade count
- raw kline count
- raw order count
- processed trade count
- processed kline count
- processed order count
- duplicate trade/order count
- null symbol count
- invalid price count
- invalid quantity count
- freshness lag

### 13.3 Pipeline Metrics

- DAG run status
- task duration
- task success/failure count
- retry count
- last successful run time
- processed row count per run

### 13.4 Iceberg Operation Metrics

- table name
- snapshot count
- file count
- average file size
- small file count
- total record count
- last commit time
- last compaction time
- compaction needed flag
- data file count
- delete file count
- delete/data file ratio
- manifest count

### 13.5 Monitoring Thresholds

아래 기준값은 초기 운영 시작 임계값이며, 실제 데이터 유입량과 commit 빈도를 관찰하면서 조정한다.

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
| `delete_file_count` | `> 10` |
| `delete_to_data_file_ratio` | `> 0.5` |
| `manifest_count` | `> 20` |

---

## 14. Airflow Plan

### 14.1 Streaming Ingestion

Kafka 수집기와 streaming job은 장기 실행 프로세스다.

대상:

- `trades` collector
- `klines` collector
- `orders` simulator
- `stream_raw_trades`
- `stream_raw_klines`
- `stream_raw_orders`

### 14.2 Daily Pipeline DAG

Phase 3에서는 Airflow-ready daily job을 `src/jobs/daily/` 아래에 별도로 작성한다. 기존 `src/pipelines/`는 Phase 2 reference 구현으로 유지한다.

Daily job은 다음 공통 인자를 받는다.

- `--start-ts`
- `--end-ts`
- `--run-id`

DAG 구조는 다음과 같다.

```text
build_processed_trades ───────────────┐
                                      ├── build_market_hourly_summary
build_staging_klines → merge_processed_klines ┘

build_staging_orders → merge_processed_orders → build_order_execution_summary

build_market_hourly_summary
build_order_execution_summary
        ↓
check_data_quality
        ↓
check_table_health
```

각 task는 `run_job_with_log.sh`를 통해 실행되며, task별 실행 결과는 `pipeline_run_summary`에 append-only로 기록한다.

Airflow는 Spark 로직을 직접 실행하지 않고, `docker exec spark-runner` 방식으로 Spark 실행 컨테이너에 job 실행을 위임한다.

### 14.3 Maintenance DAG

Maintenance DAG는 Daily Pipeline DAG와 분리한다. 데이터 처리 목적과 Iceberg table 유지보수 목적이 다르기 때문이다.

```text
check_table_health_before
   ↓
run_iceberg_maintenance
   ↓
check_table_health_after
```

`run_iceberg_maintenance`는 table policy에 따라 다음 작업을 수행한다.

- `rewrite_data_files`
- MOR table 대상 `rewrite_position_delete_files`
- `rewrite_manifests`
- `expire_snapshots`
- `remove_orphan_files`는 MVP에서 skip

Maintenance 전후의 table 상태는 `table_health_summary`에 append-only로 저장한다.


---

## 15. Grafana Dashboard Plan

Phase 4에서는 serving/monitoring tables를 Grafana로 시각화한다.

### 15.1 Market Dashboard

- symbol/hour 거래량
- symbol/hour 가격 변화
- OHLCV
- number of trades
- volatility proxy

### 15.2 Order Execution Dashboard

- total orders
- filled orders
- canceled orders
- fill rate
- cancel rate
- average fill delay
- slippage proxy

### 15.3 Data Quality Dashboard

- raw row count
- processed row count
- row count difference
- duplicate count
- null required column count
- invalid price/quantity count
- freshness lag

### 15.4 Iceberg Operations Dashboard

- processed table file count
- serving table file count
- average file size
- small file count
- snapshot count
- last compaction time
- compaction needed flag

---

## 16. Scalability Plan

초기 MVP는 local Kafka, local Spark, local Raw Zone, local Iceberg warehouse를 기준으로 구현한다. 이후 데이터 규모 증가와 클라우드 전환을 고려해 다음 원칙을 따른다.

### 16.1 Kafka Scale

초기에는 `trades`, `klines`, `orders` topic을 적은 partition 수로 시작한다. 데이터 유입량이 증가하면 topic partition 수를 늘려 producer와 Spark consumer의 병렬성을 확보한다.

확장 방향:

```text
local Kafka
→ Docker Kafka cluster
→ MSK
```

### 16.2 Spark / Iceberg Scale-up

데이터 크기가 local 환경에서 감당 가능한 수준일 경우 다음 항목을 조정한다.

- Spark executor memory
- Spark shuffle partition 수
- streaming trigger interval
- batch size
- Iceberg target file size
- compaction 주기
- MOR table의 delete file 증가율
- read amplification
- rewrite_data_files 주기
- rewrite_manifests 주기

### 16.3 Scale-out

데이터 규모가 local 환경을 넘어서면 다음 방향으로 확장한다.

```text
Local Kafka + Local Spark + S3 + Glue Catalog
→ Docker Kafka/Spark Cluster
→ S3 + Glue Catalog
→ EMR / Glue Spark
→ Athena + Grafana
```

Scale-out을 고려하여 Spark job은 독립 실행 가능한 단위로 작성하고, Airflow task에서 호출할 수 있도록 구성한다. topic명, table명, 입력 경로, 출력 경로는 config로 분리한다.

---

## 17. Roadmap

### Phase 0. Study & Design

- Binance market data 구조 학습
- trade/kline/order event 차이 정리
- order simulator 설계
- topic 설계
- raw / processed / serving table 설계
- MVP 범위 결정

### Phase 1. Kafka + Raw Zone MVP

- `trades` collector 구현
- `klines` collector 구현
- `orders` simulator 구현
- Kafka topic 생성
- Spark Structured Streaming으로 raw event 적재
- Raw Zone 재처리 가능성 확인

### Phase 2. Iceberg Core MVP

- `processed_trades` 구현
- `staging_klines` 구현
- `processed_klines` 구현
- `staging_orders` 구현
- `processed_orders` 구현
- kline update MERGE 구현
- order status MERGE 구현
- `market_hourly_summary` 구현
- `order_execution_summary` 구현
- Snapshot 및 metadata table 확인
- `processed_klines`, `processed_orders` MOR table mode 적용
- MOR table의 data/delete file 분포 확인
- MOR table maintenance 정책 검증
- delete file rewrite / data file compaction / manifest rewrite 실험
- Compaction 전후 비교

### Phase 3. Observability + Airflow

- `data_quality_summary` 생성
- `pipeline_run_summary` 생성
- `table_health_summary` 생성
- window 기반 idempotent daily jobs 구현
- `run_job.sh` 작성
- `run_spark_sql.sh` 작성
- `run_job_with_log.sh` 작성
- Airflow Daily Pipeline DAG 구현
- Airflow Maintenance DAG 구현
- Airflow / Spark runner 컨테이너 분리
- Docker Compose profile 분리
  - `streaming`
  - `airflow`
- Kafka topic 자동 생성
- Spark dependency를 `Dockerfile.spark`에서 preloading
- Derby metastore 충돌 방지를 위한 job별 `derby.system.home` 분리
- Pipeline 실행 결과를 `pipeline_run_summary`에 append-only로 기록
- Data quality 결과를 `data_quality_summary`에 append-only로 기록
- Iceberg table health 결과를 `table_health_summary`에 append-only로 기록

### Phase 4. Grafana Dashboard

- Market metrics 시각화
- Order execution metrics 시각화
- Data quality metrics 시각화
- Iceberg operation metrics 시각화

### Phase 5. Maintenance

- 코드 리팩토링
- 검증 쿼리 추가
- 실행 문서 보완
- 결과 재측정

---

## 18. Success Criteria

초기 MVP는 다음 조건을 만족하면 완료로 본다.

- Binance public trade/kline 데이터를 수집하거나 샘플링할 수 있다.
- Order simulator가 주문 이벤트를 생성할 수 있다.
- Kafka `trades`, `klines`, `orders` topic에 이벤트를 발행할 수 있다.
- Kafka event를 Raw Zone에 append-only 저장할 수 있다.
- Raw Zone에서 `processed_trades`, `staging_klines`, `processed_klines`, `processed_orders` Iceberg table을 생성할 수 있다.
- Raw kline event를 `staging_klines`에 정제 적재할 수 있다.
- `staging_klines`의 kline update를 key 단위 dedup 후 `processed_klines`에 `MERGE INTO`로 반영할 수 있다.
- Raw order event를 `staging_orders`에 정제 적재할 수 있다.
- `staging_orders`의 order status event를 `order_id` 기준 최신 상태로 dedup 후 `processed_orders`에 `MERGE INTO`로 반영할 수 있다.
- Order status update를 `processed_orders`에 `MERGE INTO`로 반영할 수 있다.
- MERGE 전후 snapshot을 확인할 수 있다.
- `market_hourly_summary`와 `order_execution_summary`를 생성할 수 있다.
- Iceberg metadata table에서 파일 상태를 확인할 수 있다.
- Compaction 전후 파일 수 또는 평균 파일 크기 변화를 확인할 수 있다.

확장 단계는 다음 조건을 만족하면 완료로 본다.

- Airflow DAG로 Raw → Processed → Serving 흐름을 실행할 수 있다.
- Data quality summary를 생성할 수 있다.
- Pipeline run summary를 생성할 수 있다.
- Table health summary를 생성할 수 있다.
- Grafana에서 market, order, data quality, operation 지표를 확인할 수 있다.

Phase 3는 다음 조건을 만족하면 완료로 본다.

- Airflow UI에서 `daily_lakehouse_pipeline` DAG를 실행할 수 있다.
- `daily_lakehouse_pipeline`의 주요 task가 SUCCESS 상태로 완료된다.
- `pipeline_run_summary`에 task별 실행 이력이 append-only로 기록된다.
- 실패 후 retry 또는 재실행 이력이 `pipeline_run_summary`에 함께 보존된다.
- `data_quality_summary`에 품질 검사 결과가 저장된다.
- `table_health_summary`에 Iceberg metadata 기반 health snapshot이 저장된다.
- `iceberg_maintenance` DAG가 Airflow에 등록되고, maintenance 전후 table health check 구조를 가진다.

---

## 19. Design Principle

본 프로젝트는 기능을 한 번에 많이 추가하기보다, 작은 MVP를 먼저 완성한 뒤 자동화와 지표화를 점진적으로 확장한다.

우선순위는 다음과 같다.

1. Binance public market data 수집
2. Orders simulator 설계
3. Kafka 기반 raw event ingestion
4. Raw Zone 기반 복구 가능성 확보
5. Iceberg Core 동작 검증
6. 데이터 품질 및 운영 지표 생성
7. 데이터 파이프라인 자동화
8. 지표 시각화
9. 유지보수성 개선

확장성은 다음 원칙을 따른다.

- 이벤트 성격이 다르면 topic과 processed table을 분리한다.
- Public market data와 simulated private order data를 명확히 구분한다.
- Raw Zone은 append-only로 유지한다.
- update가 필요한 kline/order 상태 table은 Iceberg MERGE로 처리한다.
- Serving table은 대시보드 조회를 위해 사전 집계한다.
- Storage와 Compute를 분리한다.
- Spark job은 Airflow에서 독립 task로 실행 가능하게 작성한다.
- 데이터 경로, topic명, table명은 config로 분리한다.
- Iceberg metadata table을 활용해 파일 수와 snapshot 수를 지속적으로 관찰한다.
- 작은 파일이 일정 기준을 넘으면 compaction 대상이 되도록 설계한다.
