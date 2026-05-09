"""daily_lakehouse_pipeline.py

Phase3 Airflow DAG.

역할:
- DAG는 orchestration만 담당한다.
- 실제 Spark 처리 로직은 src/jobs/daily/*.py에 둔다.
- 각 task는 orchestration/scripts/run_job.sh를 통해 실행한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = "/opt/airflow/project"

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
        cd {PROJECT_ROOT} && \
        ./orchestration/scripts/run_job.sh \
          {job_path} \
          "{{{{ data_interval_start }}}}" \
          "{{{{ data_interval_end }}}}" \
          "{{{{ run_id }}}}"
        """,
    )


with DAG(
    dag_id="daily_lakehouse_pipeline",
    default_args=DEFAULT_ARGS,
    description="Phase3 window-based idempotent lakehouse daily pipeline",
    start_date=datetime(2026, 5, 6),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "phase3"],
) as dag:

    build_processed_trades = spark_job_task(
        task_id="build_processed_trades",
        job_path="src/jobs/daily/01_build_processed_trades_window.py",
    )

    build_staging_klines = spark_job_task(
        task_id="build_staging_klines",
        job_path="src/jobs/daily/02_build_staging_klines_window.py",
    )

    merge_processed_klines = spark_job_task(
        task_id="merge_processed_klines",
        job_path="src/jobs/daily/03_merge_processed_klines_window.py",
    )

    build_staging_orders = spark_job_task(
        task_id="build_staging_orders",
        job_path="src/jobs/daily/04_build_staging_orders_window.py",
    )

    merge_processed_orders = spark_job_task(
        task_id="merge_processed_orders",
        job_path="src/jobs/daily/05_merge_processed_orders_window.py",
    )

    build_market_hourly_summary = spark_job_task(
        task_id="build_market_hourly_summary",
        job_path="src/jobs/daily/06_build_market_hourly_summary_window.py",
    )

    build_order_execution_summary = spark_job_task(
        task_id="build_order_execution_summary",
        job_path="src/jobs/daily/07_build_order_execution_summary_window.py",
    )

    check_data_quality = spark_job_task(
        task_id="check_data_quality",
        job_path="src/jobs/daily/08_check_data_quality.py",
    )

    check_table_health = spark_job_task(
        task_id="check_table_health",
        job_path="src/jobs/daily/09_check_table_health.py",
    )

    build_processed_trades >> build_market_hourly_summary

    build_staging_klines >> merge_processed_klines >> build_market_hourly_summary

    build_staging_orders >> merge_processed_orders >> build_order_execution_summary

    [build_market_hourly_summary, build_order_execution_summary] >> check_data_quality
    check_data_quality >> check_table_health
