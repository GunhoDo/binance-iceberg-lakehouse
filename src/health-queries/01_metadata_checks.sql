-- 01_metadata_checks.sql
--
-- Iceberg metadata table 조회 패턴 모음 (수동 조사용).
--
-- 참고: 이 조회들이 재는 지표(파일 수·delete 비율·스냅샷/매니페스트 수 등)는 이미
-- `table_health_summary`(ops) + Grafana "Iceberg Table Operations" 대시보드로 자동
-- 수집·시각화된다. 이 파일은 임시 조사/디버깅 시 손으로 돌려보는 canonical 쿼리 골격이다.
--
-- Iceberg metadata table (Spark, Glue catalog):
--   <catalog>.<db>.<table>.snapshots
--   <catalog>.<db>.<table>.files
--   <catalog>.<db>.<table>.partitions
--   <catalog>.<db>.<table>.history

-- ----------------------------------------------------------------------------
-- 1) snapshot 변화 추적
-- ----------------------------------------------------------------------------
SELECT
    snapshot_id,
    committed_at,
    operation,          -- append / overwrite / delete / replace
    summary             -- added-records, deleted-records 등 map
FROM glue.binance_lakehouse.processed_orders.snapshots
ORDER BY committed_at DESC
LIMIT 20;

-- ----------------------------------------------------------------------------
-- 2) file 분포 / small file 카운트 (16MB 미만을 small로 본다)
-- ----------------------------------------------------------------------------
SELECT
    COUNT(*)                                                         AS file_count,
    AVG(file_size_in_bytes)                                         AS avg_file_size_bytes,
    SUM(CASE WHEN file_size_in_bytes < 16 * 1024 * 1024 THEN 1 ELSE 0 END) AS small_file_count
FROM glue.binance_lakehouse.processed_orders.files;

-- ----------------------------------------------------------------------------
-- 3) compaction 전후 비교
-- ----------------------------------------------------------------------------
-- 위 (1), (2)를 rewrite_data_files/rewrite_position_delete_files 실행 전후로 두 번
-- 측정해 비교한다. 자동화된 before/after는 maintenance DAG의
-- check_table_health_before / after + table_health_summary가 담당한다.
