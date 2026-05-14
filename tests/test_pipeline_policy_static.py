from __future__ import annotations

import ast
import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def assert_valid_python(test_case: unittest.TestCase, relative_path: str) -> str:
    source = read(relative_path)
    ast.parse(source)
    test_case.assertTrue(source.strip())
    return source


class RawPartitionPolicyTests(unittest.TestCase):
    def test_raw_streams_write_plain_parquet_partitioned_by_ingest_date(self) -> None:
        for path in [
            "src/streams/stream_raw_trades.py",
            "src/streams/stream_raw_klines.py",
            "src/streams/stream_raw_orders.py",
        ]:
            with self.subTest(path=path):
                source = assert_valid_python(self, path)
                self.assertIn('.format("parquet")', source)
                self.assertIn('F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("ingest_date")', source)
                self.assertIn('.partitionBy("ingest_date")', source)
                self.assertNotIn('.partitionBy("year", "month")', source)

    def test_raw_ddl_uses_ingest_date_partition_without_iceberg(self) -> None:
        source = read("src/ddl/00_create_raw_tables.sql")

        self.assertEqual(source.count("PARTITIONED BY (ingest_date STRING)"), 3)
        self.assertEqual(source.count("STORED AS PARQUET"), 3)
        self.assertNotIn("USING iceberg", source)
        self.assertNotIn("PARTITIONED BY (year STRING, month STRING)", source)


class DailyWindowPolicyTests(unittest.TestCase):
    def test_raw_reader_jobs_prune_by_ingest_date_before_timestamp_window(self) -> None:
        for path in [
            "src/jobs/daily/01_build_processed_trades_window.py",
            "src/jobs/daily/02_build_staging_klines_window.py",
            "src/jobs/daily/03_build_staging_orders_window.py",
        ]:
            with self.subTest(path=path):
                source = assert_valid_python(self, path)
                self.assertIn('"yyyy-MM-dd"', source)
                self.assertIn('F.col("ingest_date") >= start_date', source)
                self.assertIn('F.col("ingest_date") <= end_date', source)
                self.assertIn('F.col("ingest_ts") >= F.to_timestamp(F.lit(args.start_ts))', source)
                self.assertIn('F.col("ingest_ts") < F.to_timestamp(F.lit(args.end_ts))', source)

    def test_merge_jobs_select_affected_rows_by_ingest_time(self) -> None:
        expectations = {
            "src/jobs/daily/04_merge_processed_klines_window.py": [
                'affected_keys_df = (',
                'F.to_timestamp(F.col("ingest_time"))',
                '.select("symbol", "interval", "open_time")',
                'on=["symbol", "interval", "open_time"]',
            ],
            "src/jobs/daily/05_merge_processed_orders_window.py": [
                'affected_order_ids_df = (',
                'F.to_timestamp(F.col("ingest_time"))',
                '.select("order_id")',
                'on="order_id"',
            ],
        }

        for path, snippets in expectations.items():
            with self.subTest(path=path):
                source = assert_valid_python(self, path)
                self.assertIn('F.col("ingest_ts") >= F.to_timestamp(F.lit(args.start_ts))', source)
                self.assertIn('F.col("ingest_ts") < F.to_timestamp(F.lit(args.end_ts))', source)
                for snippet in snippets:
                    self.assertIn(snippet, source)

    def test_gold_jobs_select_affected_keys_by_ingest_but_group_by_business_time(self) -> None:
        market_source = assert_valid_python(self, "src/jobs/daily/06_build_market_hourly_summary_window.py")
        self.assertIn("WITH affected_summary_keys AS", market_source)
        self.assertIn("to_timestamp(ingest_time) >= TIMESTAMP '{args.start_ts}'", market_source)
        self.assertIn("date_trunc('hour', open_time) AS summary_hour", market_source)
        self.assertIn("date_trunc('hour', trade_time) AS summary_hour", market_source)
        self.assertIn("date_trunc('hour', k.open_time) = keys.summary_hour", market_source)
        self.assertIn("date_trunc('hour', t.trade_time) = keys.summary_hour", market_source)

        order_source = assert_valid_python(self, "src/jobs/daily/07_build_order_execution_summary_window.py")
        self.assertIn("WITH affected_summary_keys AS", order_source)
        self.assertIn("to_timestamp(ingest_time) >= TIMESTAMP '{args.start_ts}'", order_source)
        self.assertIn("date_trunc('hour', created_at) AS summary_hour", order_source)
        self.assertIn("date_trunc('hour', p.created_at) = keys.summary_hour", order_source)

    def test_data_quality_job_uses_windowed_queries(self) -> None:
        source = assert_valid_python(self, "src/jobs/daily/08_check_data_quality.py")

        expected_windows = [
            'count_query(PROCESSED_TRADES, "trade_time"',
            'count_query(PROCESSED_KLINES, "open_time"',
            'count_query(PROCESSED_ORDERS, "updated_at"',
            'count_query(MARKET_HOURLY_SUMMARY, "summary_hour"',
            'count_query(ORDER_EXECUTION_SUMMARY, "summary_hour"',
        ]
        for expected in expected_windows:
            self.assertIn(expected, source)

        self.assertIn(">= to_timestamp('{start_ts}')", source)
        self.assertIn("< to_timestamp('{end_ts}')", source)
        self.assertIn("window=[{args.start_ts}, {args.end_ts})", source)
        self.assertIn("concat_ws(':', symbol, `interval`, cast(open_time AS string))", source)
        self.assertIn("concat_ws(':', symbol, cast(summary_hour AS string))", source)
        self.assertNotIn("concat(symbol, ':'", source)

    def test_data_quality_summary_schema_and_append_write_are_preserved(self) -> None:
        source = assert_valid_python(self, "src/jobs/daily/08_check_data_quality.py")

        for column in [
            '"run_id"',
            '"checked_at"',
            '"table_name"',
            '"check_name"',
            '"check_status"',
            '"row_count"',
            '"null_count"',
            '"duplicate_count"',
            '"warning_message"',
        ]:
            self.assertIn(column, source)

        self.assertIn(".writeTo(DATA_QUALITY_SUMMARY).append()", source)


class IdempotencyAndMergePolicyTests(unittest.TestCase):
    def test_processed_trades_is_idempotent_by_trade_id_append_only(self) -> None:
        source = assert_valid_python(self, "src/jobs/daily/01_build_processed_trades_window.py")

        self.assertIn('dropDuplicates(["trade_id"])', source)
        self.assertIn('spark.table(PROCESSED_TRADES).select("trade_id")', source)
        self.assertIn('how="left_anti"', source)
        self.assertIn('.writeTo(PROCESSED_TRADES).append()', source)
        self.assertNotIn("MERGE INTO", source)
        self.assertNotIn("Window.partitionBy", source)
        self.assertNotIn(f"SELECT COUNT(*) AS cnt FROM {{PROCESSED_TRADES}}", source)
        self.assertNotIn(".cache()", source)
        self.assertNotIn(".count()", source)
        self.assertNotIn(".unpersist()", source)

    def test_staging_jobs_are_idempotent_by_kafka_offset(self) -> None:
        for path in [
            "src/jobs/daily/02_build_staging_klines_window.py",
            "src/jobs/daily/03_build_staging_orders_window.py",
        ]:
            with self.subTest(path=path):
                source = assert_valid_python(self, path)
                self.assertIn('Window.partitionBy("source_topic", "source_partition", "source_offset")', source)
                self.assertIn("target.source_topic = source.source_topic", source)
                self.assertIn("target.source_partition = source.source_partition", source)
                self.assertIn("target.source_offset = source.source_offset", source)
                self.assertIn("WHEN NOT MATCHED THEN INSERT *", source)

    def test_kline_merge_does_not_overwrite_newer_offset_with_late_event(self) -> None:
        source = assert_valid_python(self, "src/jobs/daily/04_merge_processed_klines_window.py")

        self.assertIn(
            "WHEN MATCHED AND source.source_offset >= target.source_offset THEN UPDATE SET",
            source,
        )

    def test_kline_dedup_uses_source_offset_then_updated_at(self) -> None:
        source = assert_valid_python(self, "src/jobs/daily/04_merge_processed_klines_window.py")

        self.assertRegex(
            source,
            re.compile(
                r"orderBy\(\s*"
                r"F\.col\(\"source_offset\"\)\.desc_nulls_last\(\),\s*"
                r"F\.col\(\"updated_at\"\)\.desc_nulls_last\(\),\s*"
                r"\)",
                re.MULTILINE,
            ),
        )

    def test_order_dedup_uses_event_time_status_rank_then_source_offset(self) -> None:
        source = assert_valid_python(self, "src/jobs/daily/05_merge_processed_orders_window.py")

        self.assertIn('"status_rank"', source)
        self.assertIn('F.when(F.col("order_status") == "NEW", F.lit(1))', source)
        self.assertIn('.when(F.col("order_status") == "PARTIALLY_FILLED", F.lit(2))', source)
        self.assertIn('.when(F.col("order_status").isin("FILLED", "CANCELED"), F.lit(3))', source)
        self.assertRegex(
            source,
            re.compile(
                r"orderBy\(\s*"
                r"F\.col\(\"event_time\"\)\.desc_nulls_last\(\),\s*"
                r"F\.col\(\"status_rank\"\)\.desc_nulls_last\(\),\s*"
                r"F\.col\(\"source_offset\"\)\.desc_nulls_last\(\),\s*"
                r"\)",
                re.MULTILINE,
            ),
        )

    def test_order_merge_does_not_overwrite_newer_state_with_late_event(self) -> None:
        source = assert_valid_python(self, "src/jobs/daily/05_merge_processed_orders_window.py")

        self.assertIn(
            "WHEN MATCHED AND source.updated_at >= target.updated_at THEN UPDATE SET",
            source,
        )

    def test_ingest_time_is_preserved_in_processed_tables(self) -> None:
        kline_source = assert_valid_python(self, "src/jobs/daily/04_merge_processed_klines_window.py")
        order_source = assert_valid_python(self, "src/jobs/daily/05_merge_processed_orders_window.py")

        self.assertIn("target.ingest_time = source.ingest_time", kline_source)
        self.assertIn("source.ingest_time", kline_source)
        self.assertIn("target.ingest_time = source.ingest_time", order_source)
        self.assertIn('F.max("ingest_time").alias("ingest_time")', order_source)
        self.assertIn('F.col("life.ingest_time").alias("ingest_time")', order_source)


class DdlPolicyTests(unittest.TestCase):
    def test_ingest_time_columns_exist_for_incremental_downstream_processing(self) -> None:
        for path in [
            "src/ddl/03_create_processed_klines.sql",
            "src/ddl/04_create_staging_klines.sql",
            "src/ddl/06_create_processed_orders.sql",
        ]:
            with self.subTest(path=path):
                source = read(path)
                self.assertIn("ingest_time", source)


class TableHealthPolicyTests(unittest.TestCase):
    def test_table_health_defaults_to_lightweight_core_tables(self) -> None:
        source = assert_valid_python(self, "src/jobs/daily/09_check_table_health.py")

        self.assertIn('os.environ.get("TABLE_HEALTH_MODE", "lightweight")', source)
        self.assertIn("CORE_TABLES = [", source)
        self.assertIn("OBSERVABILITY_TABLES = [", source)
        self.assertIn('if mode == "full":', source)
        self.assertIn("return CORE_TABLES", source)

        core_section = source.split("CORE_TABLES = [", 1)[1].split("]", 1)[0]
        for table in [
            "processed_trades",
            "processed_klines",
            "processed_orders",
            "market_hourly_summary",
            "order_execution_summary",
        ]:
            self.assertIn(table, core_section)

        for table in [
            "data_quality_summary",
            "pipeline_run_summary",
            "table_health_summary",
        ]:
            self.assertNotIn(table, core_section)

    def test_table_health_full_mode_includes_observability_tables(self) -> None:
        source = assert_valid_python(self, "src/jobs/daily/09_check_table_health.py")

        self.assertIn("return CORE_TABLES + OBSERVABILITY_TABLES", source)
        observability_section = source.split("OBSERVABILITY_TABLES = [", 1)[1].split("]", 1)[0]
        for table in [
            "data_quality_summary",
            "pipeline_run_summary",
            "table_health_summary",
        ]:
            self.assertIn(table, observability_section)

    def test_table_health_combines_snapshot_metadata_query(self) -> None:
        source = assert_valid_python(self, "src/jobs/daily/09_check_table_health.py")

        self.assertIn("COUNT(*) AS snapshot_count", source)
        self.assertIn("MAX(committed_at) AS last_committed_at", source)
        self.assertEqual(source.count("FROM {full_table}.snapshots"), 1)
        self.assertIn("[table_health] snapshots query start", source)
        self.assertIn("[table_health] snapshots query done", source)

    def test_maintenance_dag_runs_table_health_in_full_mode(self) -> None:
        source = assert_valid_python(self, "orchestration/dags/iceberg_maintenance.py")

        self.assertIn("-e TABLE_HEALTH_MODE={table_health_mode}", source)
        self.assertEqual(source.count('table_health_mode="full"'), 2)

    def test_run_spark_sql_defaults_match_small_ec2_settings(self) -> None:
        source = read("orchestration/scripts/run_spark_sql.sh")

        self.assertIn('SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"', source)
        self.assertIn('SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-2}"', source)

class ObservabilityAndMaintenancePolicyTests(unittest.TestCase):
    def test_observability_append_only_and_maintenance_mor_policies_are_preserved(self) -> None:
        observability_sql = read("src/ddl/08_create_observability_tables.sql")
        maintenance_source = assert_valid_python(self, "src/jobs/maintenance/run_iceberg_maintenance.py")

        self.assertIn("CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.data_quality_summary", observability_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.pipeline_run_summary", observability_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS glue.binance_lakehouse.table_health_summary", observability_sql)
        self.assertEqual(observability_sql.count("'format-version' = '2'"), 3)
        self.assertNotIn("'write.merge.mode' = 'merge-on-read'", observability_sql)

        for table in [
            '{"table": "processed_klines", "mode": "MOR"}',
            '{"table": "processed_orders", "mode": "MOR"}',
            '{"table": "market_hourly_summary", "mode": "MOR"}',
            '{"table": "order_execution_summary", "mode": "MOR"}',
        ]:
            self.assertIn(table, maintenance_source)

        self.assertIn('if mode == "MOR":', maintenance_source)
        self.assertIn("rewrite_position_delete_files", maintenance_source)
        self.assertIn("remove_orphan_files skipped in MVP", maintenance_source)


if __name__ == "__main__":
    unittest.main()
