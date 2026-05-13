# 100x Scale-out Plan

이 문서는 현재 Binance Iceberg Lakehouse MVP를 데이터 유입량 100배 수준으로 확장한다고 가정했을 때 필요한 변경 사항을 정리한다. 목표는 단순히 더 큰 EC2 인스턴스를 쓰는 것이 아니라, 비용 폭주와 재처리 실패를 막으면서 Raw / Silver / Gold / Observability 흐름을 안정적으로 유지하는 것이다.

---

## 1. 요약

현재 프로젝트의 방향은 크게 틀리지 않았다. Raw Zone은 S3 plain Parquet append-only로 유지하고, Silver/Gold/Observability 계층은 Iceberg table로 관리하는 구조가 적절하다.

100배 규모로 커질 때 핵심 문제는 다음이다.

```text
1. Raw S3 small file 증가
2. Spark job 메모리와 shuffle 비용 증가
3. Iceberg MOR table의 delete file / manifest / snapshot 증가
4. Athena / Grafana query scan 비용 증가
5. backfill 시 ingest_time과 event_time window 불일치
6. 환경별 S3 / Glue / Athena 경로 하드코딩 문제
```

따라서 100x 확장의 핵심 방향은 다음과 같다.

```text
Raw는 plain Parquet append-only 유지
Daily DAG 의미는 ingest_time 기준 유지
Gold dashboard는 business/event time 기준 유지
Backfill은 ingest window와 event window를 명확히 분리
Spark job은 job별 profile로 분리
Iceberg maintenance는 table_health_summary 기반으로 판단
Athena/Grafana에는 비용 제한과 query guardrail 적용
```

가장 중요한 설계 원칙은 다음이다.

```text
ingest_time window  -> 새로 Raw에 들어온 데이터를 찾기 위한 기준
event_time window   -> Silver/Gold의 비즈니스 시간 집계 기준
```

---

## 2. 가정과 비목표

## 2.1 가정

- Raw event volume이 현재 대비 약 100배 증가한다.
- trades가 가장 높은 volume을 가진다.
- klines는 volume은 상대적으로 작지만 같은 `(symbol, interval, open_time)` key가 반복 업데이트될 수 있다.
- orders는 simulator 기반 synthetic event지만, 주문 lifecycle 특성상 update-heavy 데이터로 본다.
- Raw Zone은 계속 S3 plain Parquet append-only로 유지한다.
- Raw partitioning은 현재처럼 `ingest_date=YYYY-MM-DD`를 기본으로 한다.
- AWS Glue는 Iceberg catalog로 사용한다.
- Athena + Grafana는 조회와 dashboard layer로 유지한다.
- Airflow는 Spark job을 shell wrapper로 실행하는 orchestration 역할만 한다.
- Daily DAG의 의미는 “해당 interval 동안 ingest된 Raw 데이터를 처리한다”이다.
- Silver layer는 `ingest_time`을 사용해 영향을 받은 Raw row를 찾는다.
- Gold layer는 `ingest_time`으로 영향을 받은 key/hour를 찾을 수는 있지만, 최종 집계 기준은 반드시 business/event time이어야 한다.
- `processed_trades`는 `trade_id` 기준 append-only idempotency를 유지한다.
- `processed_klines`, `processed_orders`는 update/MERGE가 필요한 Iceberg MOR table로 유지한다.
- 현재 small EC2 profile은 MVP 검증용으로 보수적으로 잡은 값이다.

## 2.2 비목표

다음은 이 문서의 목표가 아니다.

- Raw Zone을 Iceberg table로 변경하지 않는다.
- Gold dashboard의 시간 기준을 `ingest_time`으로 바꾸지 않는다.
- 첫 번째 해결책으로 hourly partitioning을 도입하지 않는다.
- 단순히 더 큰 EC2 instance를 쓰는 방식으로만 해결하지 않는다.
- Athena/Grafana 대신 다른 query engine으로 갈아타는 것을 1차 해결책으로 삼지 않는다.
- idempotency를 희생해서 write 속도만 높이지 않는다.
- backfill을 수동 S3 삭제나 table truncate에 의존하게 만들지 않는다.

---

## 3. 현재 상태 평가

## 3.1 현재 구조에서 좋은 점

현재 MVP는 다음 점에서 방향이 좋다.

- Raw가 append-only Parquet이므로 재처리가 가능하다.
- Silver/Gold가 Iceberg table이므로 MERGE, snapshot, maintenance가 가능하다.
- `processed_trades`는 immutable trade event 특성에 맞게 `trade_id` 기준 append/idempotent 구조를 가진다.
- `processed_klines`와 `processed_orders`는 update성 데이터에 맞게 MERGE 구조를 가진다.
- Gold table을 별도로 두어 Athena/Grafana가 Raw를 직접 스캔하지 않게 한다.
- Observability table이 이미 존재한다.

```text
data_quality_summary
pipeline_run_summary
table_health_summary
```

- `09_check_table_health.py`는 lightweight / full mode를 지원한다.
- maintenance DAG가 Iceberg maintenance operation을 이미 포함하고 있다.

```text
rewrite_data_files
rewrite_position_delete_files
rewrite_manifests
expire_snapshots
```

- Raw reader가 `ingest_date` pruning 후 timestamp filter를 적용하는 방향으로 개선되어 있다.
- `processed_trades`에서 cache/count action을 줄여 memory pressure를 낮춘 것은 올바른 변경이다.

## 3.2 현재 구조의 약점

| 영역 | 현재 문제 | 100x에서의 위험 |
|---|---|---|
| Configuration | S3, Glue, Athena, warehouse 경로 일부 하드코딩 | dev/stage/prod 혼선, 잘못된 bucket 접근 |
| Spark wrapper | `run_job.sh`에 warehouse와 작은 EC2 기본값 하드코딩 | job별 tuning 불가 |
| Raw files | streaming 적재 시 small file 증가 가능 | S3 listing, Spark planning 비용 증가 |
| Kafka | topic partition 수가 MVP 수준 | producer/consumer 병목 |
| Trades dedup | duplicate check 비용 증가 가능 | shuffle, memory pressure 증가 |
| Klines/Orders MERGE | MOR delete file 증가 가능 | query 성능 저하 |
| Gold jobs | blind append면 재실행 시 중복 가능 | dashboard 지표 오염 |
| Athena | query guardrail 부족 | scan 비용 폭주 |
| Backfill | ingest_time과 event_time window 분리 부족 | 빈 결과 또는 일부 계층 누락 |
| Observability | `pipeline_run_summary` 전용 dashboard 부족 | 실패 분석이 느림 |

---

## 4. 목표 아키텍처

## 4.1 전체 흐름

```text
CSV / Binance source / simulator
        ↓
Producer / collector
        ↓
Kafka topics
        ↓
Spark Structured Streaming
        ↓
S3 Raw Zone
plain Parquet, append-only, partitioned by ingest_date
        ↓
Spark batch jobs
        ↓
Silver Iceberg tables
processed_trades, processed_klines, processed_orders
        ↓
Gold Iceberg tables
market_hourly_summary, order_execution_summary
        ↓
Athena + Grafana
```

## 4.2 시간 기준 분리

100x 확장에서는 시간 기준을 명확히 분리해야 한다.

| 단계 | window 기준 | 목적 |
|---|---|---|
| Raw read | `ingest_time` | 새로 S3 Raw에 들어온 row 찾기 |
| Trades Silver | source selection은 `ingest_time`, partition/business time은 `trade_time` | immutable trade append |
| Klines staging | `ingest_time` | 해당 run에 들어온 kline update 수집 |
| Klines processed MERGE | affected key 또는 `open_time` | 최신 kline state 유지 |
| Orders staging | `ingest_time` | 해당 run에 들어온 order event 수집 |
| Orders processed MERGE | affected `order_id` 또는 event lifecycle time | 최신 order state 유지 |
| Market Gold | `trade_time` / `open_time` 기반 `summary_hour` | business-time 시장 dashboard |
| Order Gold | order business time 기반 `summary_hour` | business-time execution dashboard |
| Data Quality | Silver/Gold의 event-time window | 결과 품질 검사 |
| Table Health | business window 불필요 | Iceberg metadata 상태 검사 |

## 4.3 Daily와 Backfill의 차이

Daily run에서는 보통 `ingest_time`과 `event_time`이 같은 날짜대에 있다.

```text
오늘 발생한 데이터
→ 오늘 Raw에 적재
→ daily interval에서 처리
```

하지만 backfill에서는 다르다.

```text
2024년 1월 데이터
→ 2026년 5월 12일에 Raw 적재
```

이 경우:

```text
ingest window = 2026-05-12T00:00:00Z ~ 2026-05-13T00:00:00Z
event window  = 2024-01-01T00:00:00Z ~ 2024-02-01T00:00:00Z
```

따라서 backfill은 별도 DAG 또는 명시적 parameter를 통해 두 window를 분리해야 한다.

---

## 5. 영역별 구체 변경 계획

## 5.1 Configuration과 환경 분리

### 문제

현재 일부 S3 bucket, Glue database, warehouse, Athena path가 코드와 script에 하드코딩되어 있다. MVP에서는 괜찮지만 100x와 환경 분리에서는 위험하다.

변경 대상 파일:

```text
.env
.env.example
infra/docker-compose.yml
orchestration/scripts/run_job.sh
orchestration/scripts/run_spark_sql.sh
src/jobs/common/tables.py
src/jobs/common/spark_session.py
src/pipelines/common/spark_session.py
src/streams/stream_raw_*.py
dashboard/grafana/provisioning/datasources/athena.yml
infra/aws_initial_setup.sh
```

### 목표

다음 값들은 env/config로 분리한다.

```text
AWS_REGION
GLUE_CATALOG_NAME
GLUE_DATABASE
GLUE_WAREHOUSE
LAKEHOUSE_BUCKET
RAW_PREFIX
WAREHOUSE_PREFIX
CHECKPOINT_PREFIX
ATHENA_WORKGROUP
ATHENA_RESULT_S3
GRAFANA_ATHENA_DATASOURCE_UID
SPARK_DRIVER_MEMORY
SPARK_EXECUTOR_MEMORY
SPARK_EXECUTOR_CORES
SPARK_SHUFFLE_PARTITIONS
SPARK_DEFAULT_PARALLELISM
```

### 구현 방향

- `run_job.sh`를 `run_spark_sql.sh`처럼 env 기반으로 수정한다.
- Spark session builder에서 warehouse path 하드코딩을 제거한다.
- `.env`에는 secret이 아니라 환경 설정값만 둔다.
- AWS credential은 EC2 IAM Role / Instance Profile을 사용한다.
- `.env.example`을 추가해 필요한 값을 문서화한다.
- `check_env.sh` 같은 환경 검증 script를 추가한다.

예상 script:

```bash
./orchestration/scripts/check_env.sh
```

검증 항목:

```text
Glue database 존재 여부
S3 bucket 접근 가능 여부
warehouse prefix write 가능 여부
Athena result path 유효성
필수 env var 설정 여부
```

### 기준

| 항목 | 목표 |
|---|---:|
| config 외부의 hardcoded `s3://` path | 0개 |
| config 외부의 hardcoded Glue DB name | 0개 |
| `.env`의 장기 AWS access key | 0개 |

---

## 5.2 Raw S3 layout과 partitioning

### 현재

Raw는 다음 형태를 유지한다.

```text
s3://<bucket>/raw/trades/ingest_date=YYYY-MM-DD/
s3://<bucket>/raw/klines/ingest_date=YYYY-MM-DD/
s3://<bucket>/raw/orders/ingest_date=YYYY-MM-DD/
```

Raw는 Iceberg table이 아니라 plain Parquet append-only다.

### 목표

100x에서도 기본 partition은 `ingest_date`로 유지한다.

`ingest_hour` partition은 첫 번째 해결책이 아니다. 다음 개선을 먼저 적용한다.

```text
1. file size 조정
2. streaming trigger interval 조정
3. ingest_date pruning 확인
4. Spark read filter 최적화
5. Iceberg/S3 file compaction 전략 확인
```

### 왜 hourly partition을 바로 쓰지 않는가?

hourly partition은 scan 범위를 줄일 수 있지만 다음 비용이 있다.

```text
partition 수 증가
S3 listing 증가
Glue/Athena planning overhead 증가
운영 복잡도 증가
```

따라서 daily partition이 너무 커졌다는 증거가 있을 때 마지막 선택지로 검토한다.

### file size 목표

| Zone | 목표 평균 파일 크기 |
|---|---:|
| Raw Parquet | 128MB ~ 512MB |
| Iceberg data file | 128MB ~ 512MB |
| small file 경고 | 평균 32MB 미만 |
| topic/day file count 경고 | 5,000개 초과 |

### 구현 대상

```text
src/streams/stream_raw_trades.py
src/streams/stream_raw_klines.py
src/streams/stream_raw_orders.py
src/jobs/daily/01_build_processed_trades_window.py
src/jobs/daily/02_build_staging_klines_window.py
src/jobs/daily/03_build_staging_orders_window.py
```

추가로 Raw S3 file audit job을 만들면 좋다.

수집 지표:

```text
raw_topic
ingest_date
file_count
total_size_mb
avg_file_size_mb
latest_modified_at
```

---

## 5.3 Kafka partitioning과 producer key

### 문제

100x에서는 Kafka partition 수가 producer throughput과 Spark consumer parallelism을 결정한다.

### 목표 partition 전략

| Topic | 초기 권장 partition 수 | key 전략 |
|---|---:|---|
| trades | 12 ~ 48 | `symbol` 또는 `symbol + trade_id hash` |
| klines | 6 ~ 24 | `symbol + interval` |
| orders | 6 ~ 24 | `order_id` |

### tradeoff

- `symbol` key는 symbol locality를 유지하지만 단일 symbol이면 hot partition이 생길 수 있다.
- `symbol + hash(trade_id)`는 분산은 좋지만 symbol locality는 약해진다.
- 현재처럼 BTCUSDT 단일 symbol 중심이면 hash 기반 분산이 100x 테스트에 더 안전하다.

### 구현 대상

```text
infra/docker-compose.yml
infra/kafka/create-topics.sh
src/collectors/*
src/simulators/orders_simulator.py
```

환경 변수 예시:

```text
KAFKA_TRADES_PARTITIONS=24
KAFKA_KLINES_PARTITIONS=12
KAFKA_ORDERS_PARTITIONS=12
```

검증:

```bash
kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic trades
```

### 기준

| 지표 | 목표 |
|---|---:|
| Consumer lag | batch interval 안에 해소 |
| Partition skew | 최대 partition volume < median의 2배 |
| Producer send error | 지속 error 0 |

---

## 5.4 Spark job profile과 tuning

### 문제

모든 Spark job에 같은 설정을 쓰면 100x에서 비효율적이다. trades append, orders MERGE, Gold aggregation, maintenance는 모두 resource pattern이 다르다.

### 목표

job별 Spark profile을 둔다.

```text
small
medium
merge_heavy
gold_agg
maintenance
backfill
```

### 초기 profile 제안

| Profile | 용도 | Driver memory | Executor memory | Shuffle partitions |
|---|---|---:|---:|---:|
| small | DQ, lightweight check | 2g | 2g | 8 ~ 16 |
| medium | trades append | 4g | 4g | 32 ~ 64 |
| merge_heavy | klines/orders MERGE | 4g ~ 8g | 4g ~ 8g | 64 ~ 200 |
| gold_agg | hourly summary | 4g ~ 8g | 4g ~ 8g | 64 ~ 200 |
| maintenance | rewrite/expire | 8g 이상 | 8g 이상 | 100 ~ 400 |
| backfill | 대량 과거 처리 | 8g 이상 | 8g 이상 | 200 ~ 800 |

### 구현 대상

```text
orchestration/scripts/run_job.sh
orchestration/scripts/run_job_with_log.sh
orchestration/config/spark_profiles.env
orchestration/dags/daily_lakehouse_pipeline.py
orchestration/dags/backfill_lakehouse_pipeline.py
```

실행 형식 예시:

```bash
run_job.sh <job_path> <start_ts> <end_ts> <run_id> [spark_profile]
```

예시:

```bash
./orchestration/scripts/run_job.sh \
  src/jobs/daily/05_merge_processed_orders_window.py \
  2026-05-12T00:00:00Z \
  2026-05-13T00:00:00Z \
  daily_20260512 \
  merge_heavy
```

기본 Spark 설정:

```text
spark.driver.memory
spark.executor.memory
spark.executor.cores
spark.sql.shuffle.partitions
spark.default.parallelism
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
```

### 실행 시간 목표

| Job 종류 | 목표 시간 |
|---|---:|
| Daily Raw -> Silver | 30분 이내 |
| Daily MERGE | 45분 이내 |
| Daily Gold summary | 30분 이내 |
| Data quality | 15분 이내 |
| Table health lightweight | 10분 이내 |
| Maintenance | off-peak 기준 2시간 이내 |

---

## 5.5 `processed_trades` idempotency와 duplicate check 비용 절감

### 현재

`processed_trades`는 immutable trade event에 맞게 `trade_id` 기준으로 idempotent해야 한다.

### 문제

100x에서는 duplicate check가 full table scan이 되면 비용이 커진다.

### 목표

full `processed_trades` table을 대상으로 중복 검사를 하지 않는다.

권장 흐름:

```text
1. Raw를 ingest_date pruning으로 읽는다.
2. start_ts/end_ts 기준 ingest_time filter를 적용한다.
3. raw trade를 parsing/type casting한다.
4. batch 내부에서 trade_id 기준 dedup한다.
5. batch의 min/max trade_time을 구한다.
6. 필요한 경우 processed_trades의 해당 event-time 범위만 비교한다.
7. 새로운 trade_id만 append한다.
```

### 운영 규칙

- daily job에서 full processed table duplicate check 금지.
- production path에서 debug `count()` action 최소화.
- 재사용이 명확하지 않은 `cache()` 금지.
- row count는 가능하면 write 결과나 summary metric으로 수집한다.

### 구현 대상

```text
src/jobs/daily/01_build_processed_trades_window.py
```

추가 로그 지표:

```text
raw_rows
batch_dedup_rows
candidate_trade_time_min
candidate_trade_time_max
inserted_rows
skipped_duplicate_rows
```

### 기준

| 지표 | 경고 기준 |
|---|---:|
| daily duplicate ratio | 5% 초과 warning, 20% 초과 critical |
| job duration | 30분 초과 warning |
| executor memory 사용률 | 80% 이상 지속 시 warning |

---

## 5.6 Klines / Orders Silver MERGE 전략

## Klines

`processed_klines`는 다음 key 기준 최신 상태만 유지한다.

```text
(symbol, interval, open_time)
```

권장 흐름:

```text
1. ingest_time window로 raw klines를 읽는다.
2. staging_klines에 적재한다.
3. staging에서 affected key를 찾는다.
4. 같은 key 내 source_offset / updated_at 기준 최신 row를 고른다.
5. processed_klines에 MERGE한다.
```

주의할 점:

Backfill에서는 staging이 ingest window 기준이고, 실제 business time은 과거일 수 있다. 따라서 단순히 DAG의 data_interval을 `open_time` filter에 그대로 쓰면 비어버릴 수 있다.

더 안전한 방식은 다음이다.

```text
staging rows from ingest window
→ affected kline keys
→ merge affected keys into processed_klines
```

## Orders

`processed_orders`는 다음 key 기준 최신 주문 상태를 유지한다.

```text
order_id
```

권장 흐름:

```text
1. ingest_time window로 raw orders를 읽는다.
2. staging_orders에 적재한다.
3. affected order_id를 찾는다.
4. lifecycle ordering 기준 최신 상태를 고른다.
5. processed_orders에 MERGE한다.
```

주문 lifecycle:

```text
NEW
PARTIALLY_FILLED
FILLED
CANCELED
```

### MOR table 관리 기준

| 지표 | Warning | Critical |
|---|---:|---:|
| position delete files | 100 초과 | 500 초과 |
| delete/data file ratio | 0.3 초과 | 0.5 초과 |
| snapshot count | 200 초과 | 500 초과 |
| manifest count | 1,000 초과 | 5,000 초과 |

---

## 5.7 Gold aggregation 전략

### 원칙

Gold table은 반드시 business/event time 기준이어야 한다.

즉 다음은 금지한다.

```text
Gold summary_hour = ingest_time 기준
```

허용되는 방식은 다음이다.

```text
ingest_time으로 새로 들어온 row를 찾는다
→ 그 row가 영향을 준 business hour를 찾는다
→ 해당 business hour를 event_time 기준으로 재집계한다
```

## Market summary

`market_hourly_summary` 권장 흐름:

```text
new raw ingest window
→ affected trades / klines
→ affected summary_hour 계산 by trade_time / open_time
→ 해당 business hour 재집계
→ market_hourly_summary에 MERGE
```

## Order execution summary

`order_execution_summary` 권장 흐름:

```text
new raw ingest window
→ affected order_id
→ affected summary_hour 계산 by order business time
→ 해당 business hour 재집계
→ order_execution_summary에 MERGE
```

### 필수 조건

Gold job은 같은 window를 다시 실행해도 안전해야 한다.

가능한 방식:

```text
1. MERGE INTO by (symbol, summary_hour)
2. affected hour DELETE 후 INSERT
```

금지:

```text
blind INSERT INTO Gold table
```

### 구현 대상

```text
src/jobs/daily/06_build_market_hourly_summary_window.py
src/jobs/daily/07_build_order_execution_summary_window.py
```

추가 지표:

```text
affected_hours_count
source_rows_read
summary_rows_written
summary_min_hour
summary_max_hour
```

### 기준

| 지표 | 목표 |
|---|---:|
| daily recomputed hours | symbol당 보통 48시간 이하 |
| Gold duplicate key | 0 |
| dashboard query time | 10초 이내 권장 |

---

## 5.8 Iceberg maintenance 전략

### 현재

maintenance DAG가 다음 작업을 수행한다.

```text
rewrite_data_files
rewrite_position_delete_files
rewrite_manifests
expire_snapshots
```

### 목표

고정 주기 maintenance에서 `table_health_summary` 기반 threshold-driven maintenance로 발전시킨다.

### 권장 주기

| 작업 | 대상 | 권장 주기 |
|---|---|---|
| `rewrite_data_files` | 모든 Iceberg table | daily 또는 2~3일마다 off-peak |
| `rewrite_position_delete_files` | MOR table | delete ratio 높을 때 daily |
| `rewrite_manifests` | 큰 table | daily 또는 weekly |
| `expire_snapshots` | 모든 table | daily 또는 weekly |
| `remove_orphan_files` | warehouse path | weekly/manual, 보수적 cutoff |

### threshold

| 지표 | 실행 기준 |
|---|---:|
| avg_file_size_mb | 64MB 미만 |
| data_file_count | table당 5,000 초과 |
| position_delete_file_count | 100 초과 warning, 500 초과 critical |
| delete_to_data_file_ratio | 0.3 초과 warning, 0.5 초과 critical |
| manifest_count | 1,000 초과 warning |
| snapshot_count | 200 초과 warning |
| query latency | baseline 대비 2배 이상 |

### snapshot retention

초기 권장값:

```text
dev/MVP: 3 ~ 7일
production-like demo: 7 ~ 30일
min snapshots to keep: 5 ~ 10
orphan file delete cutoff: 최소 24 ~ 72시간
```

Tradeoff:

```text
짧은 retention = S3 비용 절감
긴 retention = rollback/debugging 유리
```

---

## 5.9 Athena / Grafana 비용 guardrail

### 문제

100x에서는 Athena query 하나가 큰 비용을 만들 수 있다. Grafana dashboard도 refresh 주기가 짧으면 반복 scan 비용이 커진다.

### 목표

Athena/Grafana는 기본적으로 Gold와 Observability table만 조회한다.

필수 guardrail:

```text
전용 Athena workgroup 사용
전용 query result S3 path 사용
query bytes scanned cutoff 설정
CloudWatch metrics 활성화
Grafana datasource는 제한된 workgroup만 사용
dashboard query는 기본 time filter 포함
Raw 직접 조회 panel 금지
```

### 권장 기준

| 항목 | 권장값 |
|---|---:|
| Athena query scan cutoff | dev 1GB, demo 5~10GB |
| Grafana 기본 dashboard range | 최근 7~30일 |
| Panel query timeout | 30초 이내 |
| Dashboard refresh | demo는 30s 가능, 일반 운영은 5m 이상 권장 |

### 구현 대상

```text
infra/aws_initial_setup.sh
dashboard/grafana/provisioning/datasources/athena.yml
dashboard/grafana/dashboards/*.json
docs/grafana_metrics.md
```

Dashboard query 규칙:

```text
Raw table 직접 조회 금지
unbounded history scan 금지
MAX(summary_hour) - INTERVAL '30' DAY 형태의 제한 권장
symbol variable은 cardinality가 낮을 때만 사용
```

---

## 5.10 Observability와 Alerting

### 현재

다음 observability table은 존재한다.

```text
data_quality_summary
pipeline_run_summary
table_health_summary
```

하지만 `pipeline_run_summary` 전용 Grafana dashboard는 아직 없다.

### 목표 dashboard

`pipeline_run_summary` 전용 dashboard를 추가한다.

Panel 후보:

| Panel | Source | 목적 |
|---|---|---|
| Latest pipeline status | `pipeline_run_summary` | 최신 task 상태 확인 |
| Failed task count | `pipeline_run_summary` | 실패 감지 |
| Task duration trend | `pipeline_run_summary` | 느려지는 job 감지 |
| Longest task by run | `pipeline_run_summary` | 병목 task 확인 |
| Error messages | `pipeline_run_summary` | 실패 원인 확인 |
| Processed rows trend | `pipeline_run_summary` | 0건 처리 감지 |

### alert 후보

| Alert | 조건 |
|---|---|
| Daily job failed | latest status = FAILED |
| Zero-row run | 예상되는 run에서 processed_rows = 0 |
| Duration regression | duration > 최근 7회 평균의 2배 |
| Data quality failed | latest check_status = FAIL |
| Delete file ratio high | delete_to_data_file_ratio > 0.5 |
| Snapshot count high | snapshot_count > 500 |
| Avg file size too small | avg_file_size_mb < 32 |
| Data freshness lag | latest summary_hour가 너무 오래됨 |

### 구현 대상

```text
dashboard/grafana/dashboards/pipeline-run-summary.json
orchestration/scripts/run_job_with_log.sh
src/jobs/daily/*
```

`run_job_with_log.sh` 개선 항목:

```text
source_table
target_table
processed_rows
error_message
spark_profile
```

---

## 5.11 Backfill 전략

### 문제

Daily DAG는 ingest-time window를 기준으로 한다. 하지만 과거 데이터를 현재 적재하는 backfill에서는 ingest window와 event window가 다르다.

예시:

```text
데이터 실제 시간: 2024-01
Raw 적재 시간: 2026-05-12
```

필요한 window:

```text
ingest_start=2026-05-12T00:00:00Z
ingest_end=2026-05-13T00:00:00Z
event_start=2024-01-01T00:00:00Z
event_end=2024-02-01T00:00:00Z
```

### 목표

daily DAG와 별도의 backfill DAG를 둔다.

```text
daily_lakehouse_pipeline.py       -> 정기 daily ingest pipeline
backfill_lakehouse_pipeline.py    -> 수동 parameter 기반 backfill pipeline
```

또는 daily DAG에 `dag_run.conf`를 추가해 다음 parameter를 받을 수 있게 한다.

```json
{
  "ingest_start": "2026-05-12T00:00:00Z",
  "ingest_end": "2026-05-13T00:00:00Z",
  "event_start": "2024-01-01T00:00:00Z",
  "event_end": "2024-02-01T00:00:00Z",
  "spark_profile": "backfill"
}
```

### Backfill 실행 규칙

```text
01 Raw -> processed_trades        ingest window 사용
02 Raw -> staging_klines          ingest window 사용
03 Raw -> staging_orders          ingest window 사용
04 staging -> processed_klines    affected key 또는 event window 사용
05 staging -> processed_orders    affected order_id 또는 event window 사용
06 market_hourly_summary          event window 사용
07 order_execution_summary        event window 사용
08 data_quality                   event window 사용
09 table_health                   business window 불필요
```

### 안전 규칙

- backfill은 repeatable해야 한다.
- Gold write는 반드시 MERGE 또는 affected window delete-insert여야 한다.
- Gold blind append 금지.
- backfill은 별도 `run_id`를 사용한다.
- 대형 backfill은 월/주/일 단위로 chunking한다.

Chunk 기준:

| 데이터 규모 | 권장 chunk |
|---|---|
| 작은 demo | monthly |
| 중간 규모 | weekly |
| 100x 대형 | daily 또는 3-day chunk |

---

## 6. P0 / P1 / P2 Backlog

| 우선순위 | 영역 | 변경 사항 | 대상 파일/script | 완료 기준 |
|---|---|---|---|---|
| P0 | Config | S3/Glue/Athena 경로 config화 | `tables.py`, `spark_session.py`, `run_job.sh`, stream scripts | 모든 경로가 env/config 기반 |
| P0 | Spark wrapper | job별 Spark profile 추가 | `run_job.sh`, `run_job_with_log.sh` | job별 profile 선택 가능 |
| P0 | DAG timestamp | Airflow timestamp에 `Z` 명시 | `daily_lakehouse_pipeline.py` | 모든 DAG timestamp가 UTC 명시 |
| P0 | Raw read | Raw job이 Glue raw table이 아니라 S3 Parquet path를 읽도록 보장 | `01/02/03` jobs | Raw는 plain Parquet 유지 |
| P0 | Raw pruning | `ingest_date` pruning test 추가 | Raw reader jobs | daily window에서 full raw scan 없음 |
| P0 | Gold idempotency | Gold MERGE 또는 delete-insert 보장 | `06`, `07` jobs | 같은 window 재실행 시 중복 없음 |
| P0 | Athena guardrail | Athena workgroup scan limit 설정 | `infra/aws_initial_setup.sh` | scan cutoff 적용 |
| P0 | Observability | processed_rows 기록 개선 | `run_job_with_log.sh`, jobs | 0건 처리 run 확인 가능 |
| P1 | Kafka | topic partition 수와 producer key 재설계 | Kafka init, producers | hot partition 없음 |
| P1 | Spark tuning | job별 shuffle/memory tuning | Spark profiles | 목표 시간 내 완료 |
| P1 | Trades | duplicate check 비용 절감 | `01_build_processed_trades_window.py` | full processed scan 없음 |
| P1 | MERGE | affected-key MERGE 최적화 | `04`, `05` jobs | 불필요한 MERGE scan 감소 |
| P1 | Maintenance | threshold-driven maintenance | maintenance DAG, table health | 지표 기반 compaction 판단 |
| P1 | Grafana | pipeline run dashboard 추가 | `pipeline-run-summary.json` | pipeline 상태 시각화 |
| P1 | Freshness | freshness metric 추가 | DQ job, Grafana | stale data 확인 가능 |
| P2 | Backfill | 별도 backfill DAG 추가 | `backfill_lakehouse_pipeline.py` | ingest/event window 분리 |
| P2 | Raw audit | Raw S3 file count/size audit 추가 | new raw audit job | raw small file 증가 감지 |
| P2 | Alerts | Grafana alert provisioning | Grafana provisioning | alert rule version control |
| P2 | Ingest hour | 필요 시에만 `ingest_hour` 검토 | stream writers, raw readers | daily partition 한계 확인 후 적용 |

---

## 7. Exit Criteria

## Configuration

- S3 bucket, Glue database, warehouse, Athena result path가 코드에 하드코딩되어 있지 않다.
- `run_job.sh`와 `run_spark_sql.sh`가 같은 핵심 env var를 지원한다.
- AWS credential은 장기 key가 아니라 IAM Role 기반이다.

## Raw

- Raw는 plain Parquet append-only로 유지된다.
- Raw reader는 `ingest_date` pruning 후 timestamp filter를 적용한다.
- Raw 평균 file size는 보통 128MB ~ 512MB 범위다.
- topic/day file count가 관측 가능하다.

## Spark

- job별 Spark profile을 선택할 수 있다.
- daily job이 목표 시간 안에 완료된다.
- 같은 daily window를 재실행해도 안전하다.
- 대형 backfill은 chunking과 backfill profile로 실행 가능하다.

## Silver / Gold

- `processed_trades`는 `trade_id` 기준 idempotent하다.
- `processed_klines`는 `(symbol, interval, open_time)` 기준 중복이 없다.
- `processed_orders`는 `order_id` 기준 중복이 없다.
- Gold table은 같은 window를 재실행해도 중복 row가 생기지 않는다.
- Gold 지표는 business/event time 기준을 유지한다.

## Iceberg

- `table_health_summary`로 file/delete/snapshot/manifest 상태를 볼 수 있다.
- maintenance는 threshold 기반으로 판단할 수 있다.
- MOR delete file 증가가 관측되고 제어된다.

## Athena / Grafana

- Athena workgroup scan cutoff가 적용되어 있다.
- Grafana query는 time filter를 기본으로 가진다.
- dashboard는 Raw가 아니라 Gold/Observability table을 조회한다.
- pipeline run dashboard가 존재한다.

## Backfill

- backfill은 ingest window와 event window를 분리해 받을 수 있다.
- backfill은 manual cleanup 없이 반복 실행 가능하다.
- backfill이 daily pipeline semantics를 깨지 않는다.

---

## 8. Risks and Tradeoffs

| Risk | 영향 | 대응 |
|---|---|---|
| Raw small file 증가 | Spark planning, S3 listing 비용 증가 | trigger interval 조정, file size 모니터링 |
| daily partition 과대화 | raw scan 비용 증가 | ingest_date pruning 유지, file size 개선 후 ingest_hour 검토 |
| Kafka hot partition | consumer lag 증가 | producer key 조정, partition 수 증가 |
| full duplicate check | Spark memory/shuffle 비용 증가 | affected range/key 기반 check |
| MOR delete file 증가 | Athena/Spark query 성능 저하 | rewrite_position_delete_files, delete ratio alert |
| Gold 중복 row | dashboard 지표 오염 | MERGE 또는 delete-insert 적용 |
| Athena 비용 폭주 | AWS 비용 증가 | workgroup scan cutoff, bounded query |
| Backfill window 혼동 | 빈 결과 또는 잘못된 summary | ingest/event window 분리 |
| Spark 과도한 tuning | 운영 복잡도 증가 | profile 기반으로 측정 후 조정 |
| snapshot retention 과소 | rollback 어려움 | 환경별 retention 정책 분리 |

---

## 9. 검증 테스트

## 9.1 Configuration test

목표:

```text
하드코딩된 S3/Glue/Athena 경로 제거 확인
```

예시:

```bash
grep -R "s3://" -n src orchestration infra dashboard
```

기대 결과:

```text
실제 runtime 경로는 config/env에서 주입된다.
문서나 example 외 코드에는 production S3 path가 남아 있지 않다.
```

## 9.2 Raw pruning test

여러 `ingest_date` partition을 만든 뒤 하루 window만 처리한다.

기대 결과:

```text
Spark가 대상 ingest_date partition만 읽는다.
전체 Raw history scan이 발생하지 않는다.
```

## 9.3 Idempotency test

같은 daily window를 두 번 실행한다.

기대 결과:

| Table | 기대 결과 |
|---|---|
| `processed_trades` | `trade_id` 중복 0 |
| `processed_klines` | `(symbol, interval, open_time)` 중복 0 |
| `processed_orders` | `order_id` 중복 0 |
| `market_hourly_summary` | `(symbol, summary_hour)` 중복 0 |
| `order_execution_summary` | `(symbol, summary_hour)` 중복 0 |

## 9.4 Backfill test

다음처럼 ingest window와 event window가 다른 backfill을 실행한다.

```json
{
  "ingest_start": "2026-05-12T00:00:00Z",
  "ingest_end": "2026-05-13T00:00:00Z",
  "event_start": "2024-01-01T00:00:00Z",
  "event_end": "2024-02-01T00:00:00Z"
}
```

기대 결과:

```text
Raw job은 2026년 ingest data를 읽는다.
Gold output은 2024년 business hour로 생성된다.
같은 config 재실행 시 Gold 중복이 생기지 않는다.
```

## 9.5 100x load simulation

하루치 100x 데이터를 생성하거나 replay한다.

측정 항목:

```text
raw file count
avg raw file size
Spark job duration
shuffle spill
Iceberg data file count
Iceberg delete file count
Athena query scanned bytes
Grafana panel latency
```

통과 기준:

| 지표 | 목표 |
|---|---:|
| Daily pipeline 총 실행 시간 | 2시간 이내 |
| 주요 개별 job 실행 시간 | 45분 이내 |
| Iceberg 평균 file size | 최소 64MB 이상, 권장 128~512MB |
| Gold duplicate key | 0 |
| Data quality failure | 예상치 못한 failure 0 |
| Athena dashboard scan | dev 1GB 이하 권장, demo 10GB 이하 |
| Grafana panel latency | 10~30초 이내 |

## 9.6 Maintenance validation

maintenance 전후 `table_health_summary`를 비교한다.

기대 결과:

```text
data_file_count 감소
delete_file_count 감소
manifest_count 감소
query latency 유지 또는 개선
record_count 손실 없음
```

## 9.7 Failure recovery test

pipeline 중간에 강제로 실패를 발생시키고 retry한다.

기대 결과:

```text
pipeline_run_summary에 실패 기록이 남는다.
retry 후 성공한다.
business key 중복이 생기지 않는다.
downstream job이 안전하게 이어서 실행된다.
```

---

## 최종 권고

현재 MVP 아키텍처는 방향이 맞다. 100x 확장을 위해 필요한 것은 lakehouse 구조 자체를 갈아엎는 것이 아니라, 현재 구조를 더 명시적이고, 설정 가능하고, 관측 가능하고, 반복 실행 가능하게 만드는 것이다.

우선순위는 다음 순서가 좋다.

```text
1. hardcoded 환경 경로 제거
2. job별 Spark profile 추가
3. Raw plain Parquet 유지 + file size 관리
4. Gold write idempotency 보장
5. Athena/Grafana 비용 guardrail 적용
6. daily와 backfill의 time window 분리
7. table_health_summary 기반 Iceberg maintenance 운영
```

이 방향이면 AWS S3 + Glue + Athena + Spark + Airflow 조합을 유지하면서도 비용과 안정성을 통제하는 방식으로 100배 규모 확장을 준비할 수 있다.

