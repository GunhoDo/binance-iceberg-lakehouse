-- 09_create_quality_events.sql
--
-- Phase 4 데이터 품질 이상탐지 테이블 (PRD v2 FR-8·FR-9).
--
-- src/quality/quality_scan.py가 규칙(freshness/gap/order/null/schema)을 적용해
-- 이상 한 건당 한 row를 append한다. 시점별 관측 로그이므로 append-only.
--
-- 벤치(local hadoop) 경로는 DDL 없이 quality_scan.ensure_quality_table()이
-- 동일 스키마를 CREATE IF NOT EXISTS로 만든다. 이 파일은 glue/S3 프로덕션 패리티용이며
-- 두 테이블 스키마는 일치한다(같은 드라이버가 식별자만 바꿔 적재).

CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.quality_events (
    event_id        STRING,     -- uuid
    detected_at     TIMESTAMP,  -- 검사 시각
    run_id          STRING,     -- 벤치/배치 실행 식별자
    source_table    STRING,     -- 검사 대상 테이블
    check_name      STRING,     -- FRESHNESS_SLA | GAP | ORDER_REVERSAL | NULL_OR_ZERO | SCHEMA_DRIFT
    severity        STRING,     -- CRITICAL | WARN
    dimension       STRING,     -- symbol / topic / field 등 이상이 발생한 축
    observed        DOUBLE,     -- 관측값 (lag_ms, 결측 수, 역전 건수 등)
    threshold       DOUBLE,     -- 위반한 임계값
    detail          STRING      -- 사람이 읽는 설명
)
USING iceberg
PARTITIONED BY (days(detected_at))
TBLPROPERTIES (
    'format-version' = '2'
);
