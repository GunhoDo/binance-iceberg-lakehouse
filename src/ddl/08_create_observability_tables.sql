-- 08_create_observability_tables.sql
--
-- Phase 3 Observability tables.
--
-- 세 테이블은 current-state table이 아니라 실행별/시점별 관측 로그 table이다.
-- 따라서 append-only로 운영한다.
--
-- - data_quality_summary: 데이터 품질 검사 결과 누적
-- - pipeline_run_summary: Airflow DAG/task 실행 결과 누적
-- - table_health_summary: Iceberg metadata 기반 table health snapshot 누적

-- ----------------------------------------------------------------------------
-- data_quality_summary
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.data_quality_summary (
    run_id              STRING,
    checked_at          TIMESTAMP,
    table_name          STRING,
    check_name          STRING,
    check_status        STRING,
    row_count           BIGINT,
    null_count          BIGINT,
    duplicate_count     BIGINT,
    warning_message     STRING
)
USING iceberg
PARTITIONED BY (days(checked_at))
TBLPROPERTIES (
    'format-version' = '2'
);

-- ----------------------------------------------------------------------------
-- pipeline_run_summary
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.pipeline_run_summary (
    run_id              STRING,
    pipeline_name       STRING,
    task_name           STRING,
    status              STRING,
    started_at          TIMESTAMP,
    ended_at            TIMESTAMP,
    duration_sec        DOUBLE,
    source_table        STRING,
    target_table        STRING,
    processed_rows      BIGINT,
    error_message       STRING,
    created_at          TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(created_at))
TBLPROPERTIES (
    'format-version' = '2'
);

-- ----------------------------------------------------------------------------
-- table_health_summary
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.table_health_summary (
    run_id                      STRING,
    checked_at                  TIMESTAMP,
    table_name                  STRING,
    table_mode                  STRING,

    data_file_count             BIGINT,
    position_delete_file_count  BIGINT,
    equality_delete_file_count  BIGINT,
    delete_to_data_file_ratio   DOUBLE,

    avg_file_size_mb            DOUBLE,
    total_size_mb               DOUBLE,
    record_count                BIGINT,

    manifest_count              BIGINT,
    snapshot_count              BIGINT,
    last_committed_at           TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(checked_at))
TBLPROPERTIES (
    'format-version' = '2'
);
