# Architecture

본 문서는 PRD §8을 기반으로 본 MVP의 컴포넌트와 책임 경계를 정리한다. 현재 구현된 Phase 3 구조를 반영한다.

## 1. 컴포넌트 책임

| 컴포넌트 | 책임 |
|---|---|
| `collectors/` | Binance public market data 수집 (`trades`, `klines`) → Kafka publish |
| `simulators/` | user-level `orders` 합성 → Kafka publish |
| Kafka | 이벤트별 topic으로 분리된 이벤트 스트림 보관 |
| `streams/` | Kafka → Spark Structured Streaming → Raw Zone append-only 적재 |
| Raw Zone | Kafka 원본 메타데이터 포함, 재처리 기준 |
| Staging Tables | MERGE source 안정화를 위한 중간 테이블. `staging_klines`, `staging_orders`를 사용한다. |
| `src/pipelines/` | Phase 2 batch/reference jobs |
| `src/jobs/daily/` | Phase 3 Airflow-ready, window-based, idempotent Spark jobs |
| `src/jobs/maintenance/` | Iceberg table maintenance Spark jobs |
| Iceberg metadata | snapshots / files / manifests 등 운영 가시성 source |
| `orchestration/dags/` | Airflow DAG definitions |
| `orchestration/scripts/` | Spark job / Spark SQL 실행 wrapper |
| `spark-runner` | 실제 Spark job과 Spark SQL을 실행하는 컨테이너 |
| Airflow | DAG orchestration, dependency, schedule, retry, logging |
| Grafana | Phase 4에서 serving / observability table 시각화 |

---

## 2. Layer 구조

본 프로젝트는 Bronze/Silver/Gold 대신 Raw / Processed / Serving 명칭을 사용한다.

| 본 프로젝트 | 메달리온 대응 | 책임 |
|---|---|---|
| Raw | Bronze | Kafka event를 원본 그대로 append-only 보관 |
| Processed | Silver | raw event 파싱, 타입 정리, 중복 제거, 최신 상태 관리 |
| Serving | Gold | 대시보드/BI 조회를 위한 사전 집계 |

---

## 3. MVP 흐름

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

processed_trades + processed_klines
   ↓
market_hourly_summary

processed_orders
   ↓
order_execution_summary

Daily checks / metadata checks
   ↓
data_quality_summary
pipeline_run_summary
table_health_summary
   ↓
Grafana (Phase 4)
```

---

## 4. Airflow 확장 후 흐름

Phase 3에서는 Airflow가 Spark 로직을 직접 포함하지 않는다. Airflow는 task dependency, schedule, retry, logging만 담당한다. 실제 Spark 실행은 `spark-runner` 컨테이너가 담당한다.

```text
Airflow BashOperator
   ↓ docker exec
spark-runner container
   ↓
/workspace/orchestration/scripts/run_job_with_log.sh
   ↓
/workspace/orchestration/scripts/run_job.sh
   ↓
src/jobs/daily/*.py
```

Daily Pipeline DAG는 다음 구조를 가진다.

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

Maintenance DAG는 Daily Pipeline DAG와 분리한다.

```text
check_table_health_before
   ↓
run_iceberg_maintenance
   ↓
check_table_health_after
```

---

## 5. Docker Compose 구조

현재 `infra/docker-compose.yml`은 profile 기반으로 실행 범위를 나눈다.

| Profile | 서비스 |
|---|---|
| `streaming` | Kafka, Kafka topic init, Kafka UI |
| `airflow` | Airflow webserver, Airflow scheduler, Airflow postgres, spark-runner |

이 구조를 선택한 이유는 다음과 같다.

- Kafka/streaming 실험과 Airflow orchestration 실험을 분리해서 실행할 수 있다.
- Airflow 이미지를 Spark/JDK 의존성으로 무겁게 만들지 않는다.
- Spark 실행 환경은 `Dockerfile.spark`로 별도 관리한다.
- Airflow는 orchestration 전용으로 유지한다.

---

## 6. processed / serving table의 COW/MOR 선택 기준

Table mode는 update 특성에 따라 구분한다.

| Table | Mode | 이유 |
|---|---|---|
| `processed_trades` | COW / Append | trade event는 append-only 성격이 강하고 기존 row update가 거의 없다. |
| `processed_klines` | MOR | 같은 `(symbol, interval, open_time)` 키가 interval 종료 전까지 반복 update될 수 있다. |
| `processed_orders` | MOR | 같은 `order_id`에 대해 주문 상태 전이가 발생한다. |
| `market_hourly_summary` | MOR | 같은 `(symbol, summary_hour)` 집계 row가 late event, 재처리, Airflow window 재실행으로 반복 MERGE될 수 있다. |
| `order_execution_summary` | MOR | 같은 `(symbol, summary_hour)` 주문 KPI row가 late order event, 재처리, incremental aggregation으로 반복 MERGE될 수 있다. |
| Observability tables | Append only | 실행별 관측 결과를 누적하는 log table이므로 기존 row를 update하지 않는다. |

MOR table은 delete file과 manifest 증가를 동반할 수 있다. 따라서 Iceberg metadata table을 통해 상태를 관찰하고 Phase 3 Maintenance DAG에서 compaction/rewrite 작업을 수행한다.

Observability table은 append-only log table이므로 position delete rewrite 대상은 아니다. 다만 small file이 누적될 수 있으므로 data file compaction 대상에는 포함할 수 있다.

---

## 7. 책임 경계

### 7.1 Kline upsert는 누구의 책임인가

Raw Zone은 append-only로만 event를 저장한다. Kline의 upsert-like 처리는 Processed layer에서 수행한다.

이유:

- Raw는 재처리 기준 원본이다.
- Raw에서 event를 덮어쓰면 재처리 가능성이 낮아진다.
- kline update는 `(symbol, interval, open_time)` 기준으로 Processed table에서 MERGE한다.

---

### 7.2 `staging_klines`는 왜 필요한가

`processed_klines`는 `(symbol, interval, open_time)` 기준으로 MERGE한다.

하지만 MERGE source 안에 같은 key가 여러 번 존재하면 하나의 target row에 여러 source row가 매칭될 수 있다.

따라서 Raw를 바로 Processed에 반영하지 않고, 먼저 `staging_klines`에 정제 결과를 적재한다. 이후 MERGE 직전에 key 단위 dedup을 수행한 뒤 `processed_klines`에 반영한다.

Phase 3에서는 `staging_klines`도 window 기반 job으로 적재하며, Kafka source metadata인 `(source_topic, source_partition, source_offset)`을 idempotency key로 사용한다.

---

### 7.3 `staging_orders`는 왜 필요한가

`processed_orders`는 `order_id` 기준으로 주문의 최신 상태를 관리한다.

하지만 raw order event에는 같은 `order_id`에 대해 `NEW`, `PARTIALLY_FILLED`, `FILLED`, `CANCELED` 같은 상태 이벤트가 여러 번 존재한다.

따라서 Raw를 먼저 `staging_orders`에 정제 적재하고, `order_id` 기준 최신 이벤트를 선택한 뒤 `processed_orders`에 MERGE한다.

이 구조는 주문 이벤트 로그 보존과 주문 최신 상태 관리를 분리하기 위한 것이다.

Phase 3에서는 `staging_orders`도 window 기반 job으로 적재하며, Kafka source metadata인 `(source_topic, source_partition, source_offset)`을 idempotency key로 사용한다.

---

### 7.4 trades와 klines는 왜 processed에서 합치지 않는가

`trades`와 `klines`는 모두 market data이지만 분석 단위가 다르다.

- `trades`: 개별 체결 event
- `klines`: 일정 interval의 OHLCV aggregate

이를 `processed_market_events` 같은 단일 wide table로 합치면 sparse union schema가 Processed layer에 그대로 생긴다.

따라서 Processed layer에서는 `processed_trades`, `processed_klines`로 분리하고, 공통 KPI는 Serving layer의 `market_hourly_summary`에서 조합한다.

---

### 7.5 Observability table은 왜 append-only인가

Observability table은 current-state table이 아니라 이력 로그 table이다.

예를 들어 `table_health_summary`는 특정 table의 최신 health 상태 하나만 유지하는 것이 아니라, `checked_at` 시점별 file count, average file size, snapshot count 등을 누적한다.

```text
checked_at=10:00, table=processed_orders, file_count=31
checked_at=11:00, table=processed_orders, file_count=29
checked_at=12:00, table=processed_orders, file_count=20
```

이 구조에서는 기존 row를 갱신하지 않고 새 관측값을 append한다.

향후 최신 상태만 조회하는 dashboard가 필요하면 `current_table_health` 같은 별도 current-state table을 만들고 MERGE 기반으로 관리한다.

---

## 8. Phase 3 실행 구조

### 8.1 Daily jobs

Phase 3 daily jobs는 다음 공통 인자를 받는다.

- `--start-ts`
- `--end-ts`
- `--run-id`

각 job은 execution window만 처리한다.

주요 job:

```text
src/jobs/daily/01_build_processed_trades_window.py
src/jobs/daily/02_build_staging_klines_window.py
src/jobs/daily/03_merge_processed_klines_window.py
src/jobs/daily/04_build_staging_orders_window.py
src/jobs/daily/05_merge_processed_orders_window.py
src/jobs/daily/06_build_market_hourly_summary_window.py
src/jobs/daily/07_build_order_execution_summary_window.py
src/jobs/daily/08_check_data_quality.py
src/jobs/daily/09_check_table_health.py
```

### 8.2 Scripts

| Script | 책임 |
|---|---|
| `run_job.sh` | Spark job 실행 |
| `run_spark_sql.sh` | Spark SQL 실행 |
| `run_job_with_log.sh` | Spark job 실행 후 `pipeline_run_summary` 기록 |

`run_job.sh`와 `run_spark_sql.sh`는 실행마다 별도의 `derby.system.home`을 지정해 로컬 Derby metastore lock 충돌을 방지한다.

### 8.3 Spark dependency

Spark/Iceberg/Hadoop AWS dependency는 `Dockerfile.spark`에서 이미지 빌드 시점에 preloading한다. 따라서 `run_job.sh`, `run_spark_sql.sh`는 런타임 `--packages`를 사용하지 않는다.

---

## 9. Maintenance 구조

Maintenance job:

```text
src/jobs/maintenance/run_iceberg_maintenance.py
```

Maintenance 대상 table policy:

| Table | Policy |
|---|---|
| `processed_trades` | COW_APPEND |
| `processed_klines` | MOR |
| `processed_orders` | MOR |
| `market_hourly_summary` | MOR |
| `order_execution_summary` | MOR |
| `data_quality_summary` | APPEND_ONLY |
| `pipeline_run_summary` | APPEND_ONLY |
| `table_health_summary` | APPEND_ONLY |

Maintenance procedure:

- 모든 대상 table에 `rewrite_data_files` 적용 가능
- MOR table에만 `rewrite_position_delete_files` 적용
- 모든 대상 table에 `rewrite_manifests` 적용 가능
- `expire_snapshots`로 snapshot 보존 정책 적용
- `remove_orphan_files`는 MVP에서 skip

Maintenance 전후에는 `table_health_summary`를 적재해 file count, delete file count, manifest count, snapshot count 변화를 관찰한다.

---

## 10. 향후 확장

Phase 4에서는 Athena view와 Grafana dashboard를 통해 다음 table을 시각화한다.

- `market_hourly_summary`
- `order_execution_summary`
- `data_quality_summary`
- `pipeline_run_summary`
- `table_health_summary`

개발 중 빠른 확인은 Spark SQL 또는 Athena를 사용하고, 발표/모니터링 화면은 Grafana를 사용한다.
