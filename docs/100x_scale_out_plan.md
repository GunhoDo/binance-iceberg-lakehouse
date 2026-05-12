# 100x Scale-out Plan

이 문서는 현재 MVP를 데이터 유입량 100배 수준으로 키운다고 가정했을 때 바꿔야 할 항목을 정리한다. 목표는 단순히 더 큰 인스턴스를 쓰는 것이 아니라, 비용 폭주와 재처리 실패를 막으면서 Raw / Processed / Serving 흐름을 유지하는 것이다.

## Assumptions

- Raw event volume이 현재 대비 100배 증가한다.
- Kafka topic, Spark job, S3 object 수, Iceberg snapshot/delete file 수가 함께 증가한다.
- 조회는 Athena/Grafana 중심으로 운영 지표와 serving table을 확인한다.
- Raw Zone은 계속 plain Parquet append-only로 유지한다.
- Staging / Processed / Serving / Observability는 Iceberg table로 유지한다.

## Immediate Changes

### 1. Configuration

현재 S3 bucket과 warehouse 경로가 여러 파일에 하드코딩되어 있다.

- `src/jobs/common/tables.py`
- `src/jobs/common/spark_session.py`
- `src/pipelines/common/spark_session.py`
- `src/streams/stream_raw_*.py`
- `orchestration/scripts/run_job.sh`

100x 상황에서는 dev/stage/prod, region, bucket, Athena result path가 환경별로 달라진다. 따라서 다음 값은 env/config로 빼야 한다.

```text
LAKEHOUSE_BUCKET
RAW_PREFIX
WAREHOUSE_PREFIX
CHECKPOINT_PREFIX
GLUE_DATABASE
GLUE_CATALOG_NAME
AWS_REGION
ATHENA_WORKGROUP
```

### 2. Raw Partitioning

현재 Raw는 `ingest_date=YYYY-MM-DD` 파티션으로 정리했다. 100x에서도 기본값은 유지한다.

다만 하루 파티션이 너무 커지면 다음을 검토한다.

- topic별 S3 prefix 분리 유지: `raw/trades`, `raw/klines`, `raw/orders`
- `ingest_date` 아래 파일 크기 목표 관리
- streaming trigger interval 조정
- Spark output file size 조정

시간 단위 파티션은 마지막 선택지로 둔다. 파티션 수가 과하게 늘면 Athena/Glue partition 관리 비용과 planning 시간이 커진다.

### 3. Kafka

현재 topic partition 수는 MVP 수준이다. 100x에서는 처리량과 consumer parallelism을 기준으로 partition 수를 늘린다.

권장 변경:

- `trades`: 가장 높은 partition 수
- `klines`: 중간 partition 수
- `orders`: simulator rate에 맞춰 별도 조정
- producer key를 `symbol` 또는 business key 기준으로 정리
- Spark consumer의 executor/cores와 Kafka partition 수를 함께 맞춤

### 4. Spark Jobs

현재 daily jobs는 window 기반이라 재실행 안전성은 확보되어 있다. 100x에서는 실행 비용과 shuffle 비용을 줄여야 한다.

권장 변경:

- Spark executor memory/cores를 job별로 분리
- `spark.sql.shuffle.partitions`를 데이터 크기에 맞게 상향
- raw reader는 반드시 `ingest_date` partition pruning 후 timestamp filter 적용
- serving aggregation은 window 단위 MERGE 유지
- 큰 backfill은 daily DAG와 분리된 backfill DAG로 운영

### 5. Iceberg Maintenance

100x에서는 small files, delete files, manifests, snapshots가 빠르게 늘어난다.

권장 변경:

- maintenance DAG 주기 단축
- MOR table의 `rewrite_position_delete_files` 모니터링 강화
- `rewrite_data_files`는 모든 table에 적용하되 peak time 회피
- `expire_snapshots` 보존 기간을 비용/복구 정책에 맞게 조정
- `table_health_summary` 기준으로 compaction threshold 자동화

### 6. Athena / Grafana Cost Guardrails

Athena는 잘못된 쿼리 한 번으로 큰 scan 비용이 날 수 있다. 100x에서는 workgroup 단위 제한을 필수로 둔다.

필수 가드레일:

- 별도 Athena workgroup 사용
- query result location 분리
- query bytes scanned cutoff 설정
- CloudWatch metrics 활성화
- Grafana datasource는 제한된 workgroup만 사용
- dashboard query는 최근 window 필터를 기본값으로 사용

초기 생성 스크립트는 `infra/aws_initial_setup.sh`를 사용한다.

## Backlog

| Area | Change | Priority |
|---|---|---:|
| Config | S3/Glue/Athena 경로 env/config화 | P0 |
| AWS Guardrail | Athena workgroup scan limit | P0 |
| Raw | `ingest_date` partition pruning 테스트 유지 | P0 |
| Kafka | topic partition 수 재산정 | P1 |
| Spark | job별 executor/shuffle tuning | P1 |
| Iceberg | maintenance threshold 자동화 | P1 |
| Observability | freshness, file count, scan cost dashboard | P1 |
| Backfill | daily DAG와 별도 backfill DAG | P2 |

## Exit Criteria

- 모든 S3/Athena/Glue 경로가 환경 변수로 교체된다.
- Athena workgroup에 query scan cutoff가 설정된다.
- Raw reader와 DQ job이 전체 스캔하지 않는 것을 테스트로 확인한다.
- table health 지표로 compaction 필요 여부를 판단할 수 있다.
- dashboard query는 최근 window 조건을 기본으로 가진다.
