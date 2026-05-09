"""daily_lakehouse_pipeline.py

Phase 3 Airflow DAG.

Airflow is orchestration only.
Actual Spark jobs run inside spark-runner container.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


SPARK_CONTAINER = "spark-runner"
PROJECT_ROOT = "/workspace"

DEFAULT_ARGS = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def spark_job_task(task_id: str, job_path: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
        docker exec {SPARK_CONTAINER} \
          {PROJECT_ROOT}/orchestration/scripts/run_job_with_log.sh \
          {task_id} \
          {job_path} \
          "{{{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S') }}}}" \
          "{{{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S') }}}}" \
          "{{{{ run_id }}}}"
        """,
    )


with DAG(
    dag_id="daily_lakehouse_pipeline",
    default_args=DEFAULT_ARGS,
    description="Window-based idempotent daily lakehouse pipeline",
    start_date=datetime(2026, 5, 6),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "phase3"],
) as dag:

    build_processed_trades = spark_job_task(
        "build_processed_trades",
        "src/jobs/daily/01_build_processed_trades_window.py",
    )

    build_staging_klines = spark_job_task(
        "build_staging_klines",
        "src/jobs/daily/02_build_staging_klines_window.py",
    )

    merge_processed_klines = spark_job_task(
        "merge_processed_klines",
        "src/jobs/daily/03_merge_processed_klines_window.py",
    )

    build_staging_orders = spark_job_task(
        "build_staging_orders",
        "src/jobs/daily/04_build_staging_orders_window.py",
    )

    merge_processed_orders = spark_job_task(
        "merge_processed_orders",
        "src/jobs/daily/05_merge_processed_orders_window.py",
    )

    build_market_hourly_summary = spark_job_task(
        "build_market_hourly_summary",
        "src/jobs/daily/06_build_market_hourly_summary_window.py",
    )

    build_order_execution_summary = spark_job_task(
        "build_order_execution_summary",
        "src/jobs/daily/07_build_order_execution_summary_window.py",
    )

    check_data_quality = spark_job_task(
        "check_data_quality",
        "src/jobs/daily/08_check_data_quality.py",
    )

    check_table_health = spark_job_task(
        "check_table_health",
        "src/jobs/daily/09_check_table_health.py",
    )

    build_processed_trades >> build_market_hourly_summary

    build_staging_klines >> merge_processed_klines >> build_market_hourly_summary

    build_staging_orders >> merge_processed_orders >> build_order_execution_summary

    [build_market_hourly_summary, build_order_execution_summary] >> check_data_quality
    check_data_quality >> check_table_health
