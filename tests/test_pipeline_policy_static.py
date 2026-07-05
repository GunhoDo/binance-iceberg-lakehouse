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

    def test_order_execution_summary_has_direction_split_slippage(self) -> None:
        source = assert_valid_python(self, "src/jobs/daily/07_build_order_execution_summary_window.py")

        # 방향 분리 체결가중 평균 체결가 (FILLED 만)
        self.assertIn("avg_buy_fill_price", source)
        self.assertIn("avg_sell_fill_price", source)
        self.assertIn("p.side = 'BUY'", source)
        self.assertIn("p.side = 'SELL'", source)
        self.assertIn("p.order_status = 'FILLED'", source)
        self.assertIn("p.filled_qty * p.avg_fill_price", source)

        # 벤치마크 = market_hourly_summary.vwap 조인 (순환 방지: 다른 시계열)
        self.assertIn("MARKET_HOURLY_SUMMARY", source)
        self.assertIn("m.vwap AS benchmark_vwap", source)
        self.assertIn("LEFT JOIN {MARKET_HOURLY_SUMMARY} m", source)

        # 방향 분리 slippage_bps (양수=유리), 부호 규약이 BUY/SELL 대칭
        self.assertIn(
            "(m.vwap - o.avg_buy_fill_price) / NULLIF(m.vwap, 0) * 10000 AS buy_slippage_bps",
            source,
        )
        self.assertIn(
            "(o.avg_sell_fill_price - m.vwap) / NULLIF(m.vwap, 0) * 10000 AS sell_slippage_bps",
            source,
        )
        self.assertIn("AS slippage_cost_quote", source)

        # MERGE 가 신규 슬리피지 컬럼을 갱신
        for col in [
            "benchmark_vwap",
            "avg_buy_fill_price",
            "avg_sell_fill_price",
            "buy_slippage_bps",
            "sell_slippage_bps",
            "slippage_cost_quote",
        ]:
            self.assertIn(f"target.{col} = source.{col}", source)

    def test_dag_order_summary_runs_after_market_summary(self) -> None:
        source = assert_valid_python(self, "orchestration/dags/daily_lakehouse_pipeline.py")
        # 07 이 vwap 벤치마크를 조인하므로 06 이후 실행돼야 한다 (X-3)
        self.assertIn(
            "build_market_hourly_summary >> build_order_execution_summary", source
        )

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

    def test_staging_jobs_are_idempotent_by_business_key_not_kafka_offset(self) -> None:
        # K5: 스테이징 dedup/MERGE 는 비즈니스 키 기반이어야 한다. Kafka
        # (topic,partition,offset)은 정체성이 아니라 계보/타이브레이크로만 — 오프셋 재사용
        # (k3d 재생성·구 데이터 오프셋 대역 중복)에 새 심볼 행이 드롭되던 버그를 제거(D32).
        klines = assert_valid_python(self, "src/jobs/daily/02_build_staging_klines_window.py")
        self.assertIn('Window.partitionBy("symbol", "interval", "open_time")', klines)
        self.assertIn("target.symbol = source.symbol", klines)
        self.assertIn("target.`interval` = source.`interval`", klines)
        self.assertIn("target.open_time = source.open_time", klines)

        orders = assert_valid_python(self, "src/jobs/daily/03_build_staging_orders_window.py")
        self.assertIn('Window.partitionBy("order_id", "order_status", "event_time")', orders)
        self.assertIn("target.order_id = source.order_id", orders)
        self.assertIn("target.order_status = source.order_status", orders)
        self.assertIn("target.event_time = source.event_time", orders)

        for source in (klines, orders):
            # 비즈니스 키 UPSERT (MATCHED→UPDATE, NOT MATCHED→INSERT)
            self.assertIn("WHEN MATCHED THEN UPDATE SET *", source)
            self.assertIn("WHEN NOT MATCHED THEN INSERT *", source)
            # 오프셋을 스테이징 MERGE 의 정체성 키로 다시 쓰지 않는다
            self.assertNotIn("target.source_offset = source.source_offset", source)

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

        # 유지보수 전/후 두 번의 table health 를 full 모드로 호출한다.
        self.assertEqual(source.count('table_health_mode="full"'), 2)

        # Phase K3: 실행 방식은 KPO 팩토리가 TABLE_HEALTH_MODE 를 파드 env 로 export 한다
        # (Compose 시절 `docker exec -e TABLE_HEALTH_MODE=` 대체).
        factory = assert_valid_python(self, "orchestration/dags/lib/spark_on_k8s.py")
        self.assertIn("export TABLE_HEALTH_MODE={table_health_mode}", factory)

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


class SlippageAlertRuleTests(unittest.TestCase):
    """슬리피지 알람(FR-5) 배선이 발화 조건에 올바로 연결됐는지 정적 검증.

    실 앵커링 슬리피지는 ~0 중심(<±3bps)이라 실데이터로는 50bps 임계를 넘지 않는다
    (= 좋은 데이터에서 안 울리는 게 정상). 발화는 초과 입력(결함 주입)으로만 가능하고,
    Grafana 실 엔진 발화는 별도 시연(decisions D33)으로 확인했다. 이 테스트는 그 시연이
    대표성을 갖도록 **실 룰이 gt-50 임계 + 올바른 SQL 로 배선**돼 있음을 회귀 방지한다.
    """

    def _slippage_rule_block(self) -> str:
        rules = read("dashboard/grafana/provisioning/alerting/alert-rules.yaml")
        self.assertIn("uid: slippage-threshold-breach", rules)
        # execution 그룹의 슬리피지 룰 이하 블록만 잘라 임계·환원을 검사한다.
        start = rules.index("uid: slippage-threshold-breach")
        return rules[start:]

    def test_rule_uses_gt_50_threshold_on_reduce_last(self) -> None:
        block = self._slippage_rule_block()
        self.assertIn("condition: C", block)
        self.assertIn("reducer: last", block)
        self.assertIn("type: threshold", block)
        self.assertIn("type: gt", block)
        self.assertIn("params: [50]", block)

    def test_rule_queries_direction_split_slippage_of_order_execution_summary(self) -> None:
        block = self._slippage_rule_block()
        self.assertIn("order_execution_summary", block)
        self.assertIn("buy_slippage_bps", block)
        self.assertIn("sell_slippage_bps", block)
        # BUY/SELL 중 큰 |슬리피지| 를 임계와 비교 (방향 상쇄 방지)
        self.assertIn("GREATEST(ABS(buy_slippage_bps), ABS(sell_slippage_bps))", block)

    def test_rule_routes_as_warning_severity(self) -> None:
        block = self._slippage_rule_block()
        self.assertIn("severity: warning", block)


if __name__ == "__main__":
    unittest.main()
