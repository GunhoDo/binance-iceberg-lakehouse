"""iceberg_maintenance.py

Iceberg maintenance DAG.

Flow:
1. table health before maintenance
2. Iceberg maintenance procedures
3. table health after maintenance
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
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def run_job_task(
    task_id: str,
    job_path: str,
    run_id_suffix: str = "",
    table_health_mode: str | None = None,
) -> BashOperator:
    env_args = (
        f"-e TABLE_HEALTH_MODE={table_health_mode} "
        if table_health_mode is not None
        else ""
    )

    return BashOperator(
        task_id=task_id,
        bash_command=f"""
        docker exec {env_args}{SPARK_CONTAINER} \
          {PROJECT_ROOT}/orchestration/scripts/run_job_with_log.sh \
          {task_id} \
          {job_path} \
          "{{{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S') }}}}" \
          "{{{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S') }}}}" \
          "{{{{ run_id }}}}{run_id_suffix}"
        """,
    )


with DAG(
    dag_id="iceberg_maintenance",
    default_args=DEFAULT_ARGS,
    description="Iceberg maintenance DAG for compaction, delete rewrite, manifests, snapshots",
    start_date=datetime(2026, 5, 6),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "maintenance"],
) as dag:

    check_table_health_before = run_job_task(
        task_id="check_table_health_before",
        job_path="src/jobs/daily/09_check_table_health.py",
        run_id_suffix="__before",
        table_health_mode="full",
    )

    run_iceberg_maintenance = run_job_task(
        task_id="run_iceberg_maintenance",
        job_path="src/jobs/maintenance/run_iceberg_maintenance.py",
    )

    check_table_health_after = run_job_task(
        task_id="check_table_health_after",
        job_path="src/jobs/daily/09_check_table_health.py",
        run_id_suffix="__after",
        table_health_mode="full",
    )

    check_table_health_before >> run_iceberg_maintenance >> check_table_health_after
