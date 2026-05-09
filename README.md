# Binance Market Data Iceberg Lakehouse

Binance 공개 시장 데이터(`trades`, `klines`)와 시뮬레이션 주문 이벤트(`orders`)를 이용해 Apache Iceberg 기반 Lakehouse를 구축하는 데이터 엔지니어링 MVP입니다.

---

## 1. 프로젝트 요약

### 도메인

암호화폐 거래소 시장 데이터 플랫폼입니다. Binance의 공개 market data는 실데이터로 사용하고, user-level 주문 이벤트는 도메인 가정을 명시한 시뮬레이터로 생성합니다.

거래소의 사용자별 주문 데이터는 본질적으로 private data이므로, 본 프로젝트에서는 실제 주문 데이터인 것처럼 가장하지 않습니다.

| Event | Source | Data type |
|---|---|---|
| `trades` | Binance public market data | 실데이터 |
| `klines` | Binance public market data | 실데이터 |
| `orders` | simulator | 합성 데이터 |

### 핵심 KPI

| KPI | 의미 |
|---|---|
| Fill Rate | 전체 주문 중 체결된 주문 비율 |
| Symbol/Hour 거래량 | 심볼별 시간 단위 체결 거래량 |
| Pipeline Freshness / Run Status | 데이터 플랫폼의 최신성 및 파이프라인 운영 상태 |

---

## 2. 핵심 설계 의도

이 프로젝트의 핵심은 단순 ETL이 아니라, **거래 도메인에서 발생하는 상태 변화와 재처리 가능성을 Lakehouse 구조로 다루는 것**입니다.

### 왜 Iceberg인가

거래 도메인에는 “나중에 바뀌는 데이터”가 있습니다.

- Kline은 interval이 닫히기 전까지 같은 `(symbol, interval, open_time)` 키로 값이 반복 갱신될 수 있습니다.
- Order는 같은 `order_id`에 대해 `NEW → PARTIALLY_FILLED → FILLED / CANCELED` 상태 변화가 발생합니다.

따라서 단순 append-only Parquet만으로는 최신 상태 관리가 어렵고, `MERGE INTO`, snapshot, metadata table을 제공하는 Iceberg가 적합합니다.

### 왜 staging table을 두는가

`staging_klines`, `staging_orders`는 serving 대상이 아닙니다. Raw JSON을 정제한 뒤, processed table에 MERGE하기 전 source를 안정화하기 위한 중간 계층입니다.

- `staging_klines`: `(symbol, interval, open_time)` 기준 dedup 후 `processed_klines`에 MERGE
- `staging_orders`: `order_id` 기준 최신 상태 선별 후 `processed_orders`에 MERGE

이 구조는 Raw 이벤트 로그 보존과 최신 상태 테이블 관리를 분리합니다.

---

## 3. 전체 아키텍처

```text
Binance Public Market Data
   ├── trades collector
   │      ↓
   │   Kafka topic: trades
   │      ↓
   │   Raw Zone: raw/trades
   │      ↓
   │   processed_trades
   │
   └── klines collector
          ↓
       Kafka topic: klines
          ↓
       Raw Zone: raw/klines
          ↓
       staging_klines
          ↓
       processed_klines

Order Simulator
   ↓
Kafka topic: orders
   ↓
Raw Zone: raw/orders
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

Airflow Daily Pipeline
   ↓
data_quality_summary
pipeline_run_summary
table_health_summary

Phase 4
   ↓
Athena / QuickSight
```

---

## 4. 기술 스택

| 역할 | 기술 |
|---|---|
| Message Queue | Apache Kafka, Docker, KRaft |
| Streaming | Spark Structured Streaming 3.5.5 |
| Batch | PySpark 3.5.5 |
| Table Format | Apache Iceberg format-version 2 |
| Catalog | AWS Glue Catalog |
| Storage | AWS S3 |
| Orchestration | Apache Airflow |
| Query / BI | Athena, QuickSight |
| Infra | Docker Compose profiles, Spark runner container |

---

## 5. Layer 설계

본 프로젝트는 Bronze/Silver/Gold 대신 책임이 더 직접적으로 드러나는 **Raw / Processed / Serving** 명칭을 사용합니다.

| 본 프로젝트 | Medallion 대응 | 책임 |
|---|---|---|
| Raw | Bronze | 원본 이벤트 보존, 재처리 기준 |
| Processed | Silver | 파싱, 정제, dedup, MERGE 기반 최신 상태 관리 |
| Serving | Gold | BI/대시보드 조회를 위한 사전 집계 |

### Raw

Raw Zone은 Iceberg가 아니라 plain Parquet으로 S3에 저장합니다.

- Kafka metadata(topic, partition, offset)를 보존합니다.
- `message_value`는 raw JSON 문자열로 저장합니다.
- 파싱 오류나 집계 로직 변경 시 Raw를 기준으로 재처리할 수 있습니다.

### Processed

Processed layer는 Iceberg table입니다.

| Table | Mode | Key / 특징 |
|---|---|---|
| `processed_trades` | COW / Append | `trade_id` 기준 체결 이벤트 |
| `processed_klines` | MOR | `(symbol, interval, open_time)` 기준 MERGE |
| `processed_orders` | MOR | `order_id` 기준 최신 주문 상태 MERGE |

### Serving

Serving table은 대시보드가 매번 processed 상세 테이블 전체를 스캔하지 않도록 사전 집계합니다.

| Table | Mode | 설명 |
|---|---|---|
| `market_hourly_summary` | MOR | symbol/hour 단위 market KPI |
| `order_execution_summary` | MOR | symbol/hour 단위 order execution KPI |

Serving table도 같은 summary key에 대해 late event, backfill, window 재실행으로 값이 반복 갱신될 수 있으므로 MOR 대상으로 관리합니다.

---

## 6. Phase 3: Airflow-ready Idempotent Pipeline

Phase 3에서는 기존 `src/pipelines/` 구현을 그대로 Airflow에 연결하지 않고, Airflow 실행을 전제로 한 별도 job을 `src/jobs/daily/`에 작성했습니다.

각 daily job은 공통 실행 인자를 받습니다.

```text
--start-ts
--end-ts
--run-id
```

이 설계를 통해 retry, re-run, backfill 상황에서도 같은 window를 다시 실행할 수 있습니다.

### Daily DAG 구조

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

### Airflow / Spark 실행 구조

Airflow 컨테이너는 Spark를 직접 실행하지 않습니다. Airflow는 orchestration만 담당하고, 실제 Spark job은 `spark-runner` 컨테이너에서 실행됩니다.

```text
Airflow BashOperator
   ↓ docker exec
spark-runner
   ↓
orchestration/scripts/run_job_with_log.sh
   ↓
orchestration/scripts/run_job.sh
   ↓
src/jobs/daily/*.py
```

`run_job_with_log.sh`는 task 성공/실패, 시작/종료 시각, duration을 `pipeline_run_summary`에 append-only로 기록합니다.

### Observability tables

| Table | Write pattern | 설명 |
|---|---|---|
| `data_quality_summary` | Append only | row count, null count, duplicate count 등 품질 검사 결과 |
| `pipeline_run_summary` | Append only | Airflow task 실행 이력 |
| `table_health_summary` | Append only | Iceberg files/manifests/snapshots 기반 table health snapshot |

`pipeline_run_summary`는 current-state table이 아니라 실행 이력 로그입니다. 따라서 실패한 task와 재시도 후 성공한 task가 모두 row로 남는 것이 의도한 동작입니다.

---

## 7. Iceberg Maintenance

Maintenance DAG는 Daily Pipeline DAG와 분리합니다. 데이터 처리와 Iceberg table 유지보수는 목적이 다르기 때문입니다.

```text
check_table_health_before
   ↓
run_iceberg_maintenance
   ↓
check_table_health_after
```

Maintenance job은 table policy에 따라 다음 작업을 수행합니다.

- `rewrite_data_files`
- MOR table 대상 `rewrite_position_delete_files`
- `rewrite_manifests`
- `expire_snapshots`
- `remove_orphan_files`는 MVP에서 skip

Observability table은 append-only log table이므로 position delete rewrite 대상은 아니지만, small file이 누적될 경우 data file compaction 대상에는 포함할 수 있습니다.

---

## 8. 실행 방법

### 8.1 인프라 실행

Docker Compose는 profile로 실행 범위를 나눕니다.

```bash
# Kafka / Kafka UI / topic init
docker compose -f infra/docker-compose.yml --profile streaming up -d

# Airflow + spark-runner
docker compose -f infra/docker-compose.yml --profile airflow up -d --build

# 전체 실행
docker compose -f infra/docker-compose.yml --profile streaming --profile airflow up -d --build
```

Airflow UI:

```text
http://<EC2_PUBLIC_IP>:8081
```

기본 로그인:

```text
airflow / airflow
```

Kafka UI:

```text
http://<EC2_PUBLIC_IP>:8090
```

### 8.2 Spark job 수동 실행

Spark 관련 실행은 host가 아니라 `spark-runner` 컨테이너 안에서 실행합니다. `Dockerfile.spark`에서 Iceberg/Hadoop AWS dependency를 preloading하기 때문입니다.

```bash
docker exec -it spark-runner \
  /workspace/orchestration/scripts/run_job.sh \
  src/jobs/daily/01_build_processed_trades_window.py \
  2026-05-08T00:00:00 \
  2026-05-09T00:00:00 \
  manual_test_1
```

### 8.3 Spark SQL 실행

```bash
docker exec -it spark-runner \
  /workspace/orchestration/scripts/run_spark_sql.sh -e "
SHOW TABLES IN glue.binance_lakehouse;
"
```

### 8.4 Observability 확인

```bash
docker exec -it spark-runner \
  /workspace/orchestration/scripts/run_spark_sql.sh -e "
SELECT *
FROM glue.binance_lakehouse.pipeline_run_summary
ORDER BY created_at DESC
LIMIT 20;
"
```

```bash
docker exec -it spark-runner \
  /workspace/orchestration/scripts/run_spark_sql.sh -e "
SELECT *
FROM glue.binance_lakehouse.data_quality_summary
ORDER BY checked_at DESC
LIMIT 20;
"
```

```bash
docker exec -it spark-runner \
  /workspace/orchestration/scripts/run_spark_sql.sh -e "
SELECT *
FROM glue.binance_lakehouse.table_health_summary
ORDER BY checked_at DESC
LIMIT 20;
"
```

---

## 9. 디렉토리 구조

현재 주요 디렉토리 구조는 다음과 같습니다. `metastore_db`, `derby.log`, `.venv`, `__pycache__` 등 로컬 실행 부산물은 README 구조에서 제외합니다.

```text
binance-iceberg-lakehouse/
├── README.md
├── requirements.txt
│
├── docs/
│   ├── PRD.md
│   ├── architecture.md
│   ├── decisions.md
│   ├── operations.md
│   ├── quicksight_metrics.md
│   └── simulator_design.md
│
├── infra/
│   ├── docker-compose.yml
│   ├── Dockerfile.airflow
│   ├── Dockerfile.spark
│   ├── bootstrap_ec2.sh
│   ├── download_data.sh
│   └── csv_to_kafka.py
│
├── src/
│   ├── ddl/
│   │   ├── 00_create_raw_tables.sql
│   │   ├── 01_create_namespaces.sql
│   │   ├── 02_create_processed_trades.sql
│   │   ├── 03_create_processed_klines.sql
│   │   ├── 04_create_staging_klines.sql
│   │   ├── 05_create_staging_orders.sql
│   │   ├── 06_create_processed_orders.sql
│   │   ├── 07_create_serving_tables.sql
│   │   ├── 08_create_observability_tables.sql
│   │   └── 09_quicksight_views.sql
│   │
│   ├── streams/
│   │   ├── stream_raw_trades.py
│   │   ├── stream_raw_klines.py
│   │   └── stream_raw_orders.py
│   │
│   ├── pipelines/
│   │   ├── 01_build_processed_trades.py
│   │   ├── 02_build_processed_klines.py
│   │   ├── 03_merge_kline_updates.sql
│   │   ├── 04_build_processed_orders.py
│   │   ├── 05_merge_order_status_updates.sql
│   │   ├── 06_build_market_hourly_summary.py
│   │   └── 07_build_order_execution_summary.py
│   │
│   ├── jobs/
│   │   ├── common/
│   │   │   ├── args.py
│   │   │   ├── spark_session.py
│   │   │   └── tables.py
│   │   ├── daily/
│   │   │   ├── 01_build_processed_trades_window.py
│   │   │   ├── 02_build_staging_klines_window.py
│   │   │   ├── 03_merge_processed_klines_window.py
│   │   │   ├── 04_build_staging_orders_window.py
│   │   │   ├── 05_merge_processed_orders_window.py
│   │   │   ├── 06_build_market_hourly_summary_window.py
│   │   │   ├── 07_build_order_execution_summary_window.py
│   │   │   ├── 08_check_data_quality.py
│   │   │   └── 09_check_table_health.py
│   │   └── maintenance/
│   │       └── run_iceberg_maintenance.py
│   │
│   ├── simulators/
│   │   └── orders_simulator.py
│   │
│   └── health-queries/
│       └── 01_metadata_checks.sql
│
├── orchestration/
│   ├── dags/
│   │   ├── daily_lakehouse_pipeline.py
│   │   └── iceberg_maintenance.py
│   └── scripts/
│       ├── run_job.sh
│       ├── run_job_with_log.sh
│       └── run_spark_sql.sh
│
├── experiments/
│   ├── 01_compaction_file_stats.ipynb
│   ├── 02_mor_position_delete_lab_fixed.ipynb
│   ├── 03_rewrite_manifests_check.ipynb
│   └── results/
│
├── dashboard/
└── tests/
```

---

## 10. Roadmap

### Phase 1. Kafka + Raw Zone MVP

- trades collector 구현
- klines collector 구현
- orders simulator 구현
- Kafka topic 생성
- Spark Structured Streaming으로 Raw Zone 적재

### Phase 2. Iceberg Core MVP

- processed/staging/serving Iceberg table 구현
- kline update MERGE 구현
- order status MERGE 구현
- Iceberg snapshot/files metadata 확인
- MOR table 실험 및 compaction 실험

### Phase 3. Observability + Airflow

- Airflow Daily Pipeline DAG 구현
- Maintenance DAG 구현
- `data_quality_summary`, `pipeline_run_summary`, `table_health_summary` 생성
- Airflow / Spark runner 분리
- Docker Compose profile 분리
- Spark dependency preloading
- DAG 실행 결과와 observability table 적재 확인

### Phase 4. QuickSight Dashboard

- Athena view 정리
- QuickSight dataset 연결
- Market / Order / Data Quality / Iceberg Operations dashboard 구성

---

## 11. 참고 문서

- `docs/PRD.md` — 프로젝트 정의서
- `docs/decisions.md` — 설계 결정 기록
- `docs/architecture.md` — 아키텍처와 책임 경계
- `docs/operations.md` — 운영 지표, Airflow, maintenance 정책
- `docs/simulator_design.md` — 주문 시뮬레이터 설계
- `docs/quicksight_metrics.md` — Phase 4 QuickSight dashboard 지표 초안
```
