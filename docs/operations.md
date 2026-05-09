# Operations

본 문서는 PRD §13(Observability Plan)과 §14(Airflow Plan)을 운영 관점에서 정리한다. 임의의 도구나 임계값을 추가하지 않고, 현재 구현된 Phase 3 구조를 기준으로 작성한다.

## 1. 운영 관점의 목표

본 프로젝트는 ETL 구현 자체에서 끝나지 않고, 데이터와 파이프라인이 운영 가능한 상태인지 확인할 수 있는 관측 지표를 함께 관리한다.

운영 관점의 핵심 질문은 다음과 같다.

- 데이터가 정상적으로 적재되었는가?
- 중복, null, row count 이상이 있는가?
- Airflow task가 성공했는가, 실패했는가?
- 실패 후 retry 또는 재실행 이력이 남았는가?
- Iceberg table의 file, snapshot, manifest 상태가 악화되고 있는가?
- maintenance 전후 table health가 개선되었는가?

---

## 2. Observability 분류

| 분류 | 대표 지표 | 저장소 |
|---|---|---|
| Business | symbol/hour 거래량, fill rate 등 | `market_hourly_summary`, `order_execution_summary` |
| Data Quality | row count, duplicate count, null count | `data_quality_summary` |
| Pipeline | task status, task duration, retry/re-run history | `pipeline_run_summary` |
| Iceberg Operation | snapshot count, file count, avg file size, delete file count, manifest count | `table_health_summary` |

Observability table은 current-state table이 아니라 append-only log table이다.  
따라서 같은 `run_id`, `task_name`, `table_name`에 대해 여러 row가 존재할 수 있다.

이는 의도한 동작이다. 실패, retry, 재실행, 성공 이력을 모두 보존하기 위함이다.

---

## 3. Observability Tables

### 3.1 `data_quality_summary`

데이터 품질 검사 결과를 실행별로 누적한다.

주요 기록 항목:

- `run_id`
- `checked_at`
- `table_name`
- `check_name`
- `check_status`
- `row_count`
- `null_count`
- `duplicate_count`
- `warning_message`

작성 job:

```text
src/jobs/daily/08_check_data_quality.py
```

대표 검사:

- `processed_trades.trade_id` null/duplicate 검사
- `processed_klines.(symbol, interval, open_time)` null/duplicate 검사
- `processed_orders.order_id` null/duplicate 검사
- `market_hourly_summary.(symbol, summary_hour)` null/duplicate 검사
- `order_execution_summary.(symbol, summary_hour)` null/duplicate 검사

---

### 3.2 `pipeline_run_summary`

Airflow task 또는 수동 실행 결과를 append-only로 기록한다.

주요 기록 항목:

- `run_id`
- `pipeline_name`
- `task_name`
- `status`
- `started_at`
- `ended_at`
- `duration_sec`
- `source_table`
- `target_table`
- `processed_rows`
- `error_message`
- `created_at`

작성 스크립트:

```text
orchestration/scripts/run_job_with_log.sh
```

실행 흐름:

```text
Airflow BashOperator
   ↓ docker exec
spark-runner
   ↓
run_job_with_log.sh
   ↓
run_job.sh
   ↓
src/jobs/daily/*.py
   ↓
run_spark_sql.sh
   ↓
pipeline_run_summary append
```

`pipeline_run_summary`는 current-state table이 아니다. 같은 task가 실패 후 retry되어 성공하면 실패 row와 성공 row가 모두 남는다.

---

### 3.3 `table_health_summary`

Iceberg metadata table 기반으로 table health snapshot을 누적한다.

주요 기록 항목:

- `run_id`
- `checked_at`
- `table_name`
- `table_mode`
- `data_file_count`
- `position_delete_file_count`
- `equality_delete_file_count`
- `delete_to_data_file_ratio`
- `avg_file_size_mb`
- `total_size_mb`
- `record_count`
- `manifest_count`
- `snapshot_count`
- `last_committed_at`

작성 job:

```text
src/jobs/daily/09_check_table_health.py
```

사용 metadata table:

- `<table>.files`
- `<table>.manifests`
- `<table>.snapshots`

---

## 4. 임계값

아래 값은 초기 운영 시작 임계값이며, 실제 데이터 유입량과 commit 빈도를 관찰하면서 조정한다.

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

현재 Phase 3 구현에서는 모든 임계값 기반 alerting을 자동화하지 않는다. 우선 `data_quality_summary`, `pipeline_run_summary`, `table_health_summary`에 관측값을 남기고, Phase 4 QuickSight 또는 Athena view에서 시각화/판단하는 구조로 둔다.

---

## 5. Airflow 운영 구조

### 5.1 Streaming ingestion

다음 프로세스는 Airflow DAG가 아니라 long-running 프로세스로 운영한다.

- `trades` collector
- `klines` collector
- `orders` simulator
- `stream_raw_trades`
- `stream_raw_klines`
- `stream_raw_orders`

이들은 Kafka topic과 Raw Zone을 채우는 역할이며, Daily Pipeline DAG는 이미 적재된 Raw Zone을 기준으로 Processed/Serving/Observability를 갱신한다.

---

### 5.2 Daily Pipeline DAG

DAG 파일:

```text
orchestration/dags/daily_lakehouse_pipeline.py
```

DAG 구조:

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

Daily Pipeline DAG의 특징:

- Airflow는 Spark 로직을 직접 포함하지 않는다.
- Airflow는 `docker exec spark-runner ...` 방식으로 Spark 실행 컨테이너에 job 실행을 위임한다.
- 각 job은 `--start-ts`, `--end-ts`, `--run-id`를 받는다.
- 같은 execution window를 다시 실행해도 target table에 중복 row가 누적되지 않도록 job별 idempotency key를 사용한다.
- task 실행 결과는 `pipeline_run_summary`에 append-only로 기록한다.

---

### 5.3 Maintenance DAG

DAG 파일:

```text
orchestration/dags/iceberg_maintenance.py
```

DAG 구조:

```text
check_table_health_before
   ↓
run_iceberg_maintenance
   ↓
check_table_health_after
```

Maintenance job:

```text
src/jobs/maintenance/run_iceberg_maintenance.py
```

Maintenance job은 table policy에 따라 다음 procedure를 실행한다.

- `rewrite_data_files`
- MOR table 대상 `rewrite_position_delete_files`
- `rewrite_manifests`
- `expire_snapshots`
- `remove_orphan_files`는 MVP에서 skip

Daily Pipeline DAG와 Maintenance DAG를 분리하는 이유는 데이터 처리 흐름과 Iceberg 유지보수 작업의 실행 목적이 다르기 때문이다.

---

## 6. Docker Compose Profiles

현재 Docker Compose는 profile 기반으로 분리한다.

| Profile | 역할 |
|---|---|
| `streaming` | Kafka, Kafka topic init, Kafka UI |
| `airflow` | Airflow webserver, scheduler, postgres, spark-runner |

실행 예시:

```bash
docker compose -f infra/docker-compose.yml --profile streaming up -d
```

```bash
docker compose -f infra/docker-compose.yml --profile airflow up -d --build
```

```bash
docker compose -f infra/docker-compose.yml --profile streaming --profile airflow up -d --build
```

---

## 7. Spark Runner 운영 기준

Phase 3에서는 Airflow 컨테이너가 직접 Spark를 실행하지 않는다.

```text
Airflow container = orchestration only
spark-runner container = Spark execution
```

Spark 실행은 항상 `spark-runner` 컨테이너 안에서 수행한다.

수동 실행 예시:

```bash
docker exec -it spark-runner \
  /workspace/orchestration/scripts/run_job.sh \
  src/jobs/daily/01_build_processed_trades_window.py \
  2026-05-08T00:00:00 \
  2026-05-09T00:00:00 \
  docker_manual_test
```

Spark SQL 조회 예시:

```bash
docker exec -it spark-runner \
  /workspace/orchestration/scripts/run_spark_sql.sh -e "
SELECT *
FROM glue.binance_lakehouse.pipeline_run_summary
ORDER BY created_at DESC
LIMIT 20;
"
```

호스트 EC2에서 `./orchestration/scripts/run_spark_sql.sh`를 직접 실행하면 Iceberg JAR가 없어서 실패할 수 있다. Spark/Iceberg 실행은 `spark-runner` 기준으로 통일한다.

---

## 8. Spark Dependency 운영 기준

`Dockerfile.spark`에서 Iceberg/Hadoop AWS 관련 JAR를 이미지 빌드 시점에 preloading한다.

이유:

- 런타임마다 `--packages`로 Maven/Ivy dependency를 다운로드하지 않는다.
- Airflow 병렬 task 실행 중 Ivy cache 충돌을 줄인다.
- Spark 실행 환경을 `spark-runner` 이미지에 고정한다.

`run_job.sh`, `run_spark_sql.sh`에서는 `--packages`를 사용하지 않는다.

---

## 9. Derby Metastore 충돌 방지

로컬 Spark는 기본 Hive Derby metastore를 사용할 수 있다. Airflow에서 여러 Spark task가 병렬로 실행되면 같은 Derby 경로를 공유하면서 lock 충돌이 발생할 수 있다.

이를 피하기 위해 다음 스크립트는 실행마다 고유한 `derby.system.home`을 설정한다.

- `orchestration/scripts/run_job.sh`
- `orchestration/scripts/run_spark_sql.sh`

예시:

```bash
--conf "spark.driver.extraJavaOptions=-Dderby.system.home=${DERBY_HOME}"
```

---

## 10. 운영 확인 쿼리

### 10.1 Pipeline run summary

```bash
docker exec -it spark-runner \
  /workspace/orchestration/scripts/run_spark_sql.sh -e "
SELECT *
FROM glue.binance_lakehouse.pipeline_run_summary
ORDER BY created_at DESC
LIMIT 20;
"
```

### 10.2 Data quality summary

```bash
docker exec -it spark-runner \
  /workspace/orchestration/scripts/run_spark_sql.sh -e "
SELECT *
FROM glue.binance_lakehouse.data_quality_summary
ORDER BY checked_at DESC
LIMIT 20;
"
```

### 10.3 Table health summary

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

## 11. 보류 항목

- 알람 채널은 Phase 3에서 추가하지 않는다.
- QuickSight dashboard는 Phase 4에서 구성한다.
- `remove_orphan_files`는 위험도가 있으므로 MVP에서는 실행하지 않는다.
- `current_table_health`, `current_pipeline_status` 같은 current-state table은 아직 만들지 않는다.
- 임계값 기반 자동 alerting은 Phase 4 이후에 검토한다.
