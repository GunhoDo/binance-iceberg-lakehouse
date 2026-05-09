"""run_iceberg_maintenance.py

Iceberg maintenance job.

Maintenance steps:
1. rewrite_data_files
2. rewrite_position_delete_files for MOR tables
3. rewrite_manifests
4. expire_snapshots
5. remove_orphan_files skipped in MVP

This job is designed to be called by Airflow Maintenance DAG.
"""

from __future__ import annotations

from src.jobs.common.args import parse_job_args
from src.jobs.common.spark_session import get_spark


TABLE_POLICIES = [
    {"table": "processed_trades", "mode": "COW_APPEND"},
    {"table": "processed_klines", "mode": "MOR"},
    {"table": "processed_orders", "mode": "MOR"},
    {"table": "market_hourly_summary", "mode": "MOR"},
    {"table": "order_execution_summary", "mode": "MOR"},
    {"table": "data_quality_summary", "mode": "APPEND_ONLY"},
    {"table": "pipeline_run_summary", "mode": "APPEND_ONLY"},
    {"table": "table_health_summary", "mode": "APPEND_ONLY"},
]


def call_procedure(spark, sql: str, label: str) -> None:
    print(f"[maintenance] start: {label}")

    try:
        spark.sql(sql).show(truncate=False)
        print(f"[maintenance] complete: {label}")
    except Exception as e:
        print(f"[maintenance] failed: {label}")
        print(f"[maintenance] reason: {e}")


def run() -> None:
    args = parse_job_args()
    spark = get_spark("phase3_iceberg_maintenance")

    print(
        "[maintenance] run "
        f"run_id={args.run_id}, start_ts={args.start_ts}, end_ts={args.end_ts}"
    )

    for policy in TABLE_POLICIES:
        table = policy["table"]
        mode = policy["mode"]
        full_name = f"binance_lakehouse.{table}"

        print(f"[maintenance] table={table}, mode={mode}")

        call_procedure(
            spark,
            f"""
            CALL glue.system.rewrite_data_files(
              table => '{full_name}'
            )
            """,
            f"rewrite_data_files:{table}",
        )

        if mode == "MOR":
            call_procedure(
                spark,
                f"""
                CALL glue.system.rewrite_position_delete_files(
                  table => '{full_name}',
                  options => map('rewrite-all', 'true')
                )
                """,
                f"rewrite_position_delete_files:{table}",
            )
        else:
            print(f"[maintenance] skip rewrite_position_delete_files:{table}, mode={mode}")

        call_procedure(
            spark,
            f"""
            CALL glue.system.rewrite_manifests(
              table => '{full_name}'
            )
            """,
            f"rewrite_manifests:{table}",
        )

        call_procedure(
            spark,
            f"""
            CALL glue.system.expire_snapshots(
              table => '{full_name}',
              older_than => current_timestamp() - INTERVAL 30 DAYS,
              retain_last => 10
            )
            """,
            f"expire_snapshots:{table}",
        )

    print("[maintenance] remove_orphan_files skipped in MVP")
    spark.stop()


if __name__ == "__main__":
    run()