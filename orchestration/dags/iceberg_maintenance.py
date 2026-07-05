"""iceberg_maintenance.py

Iceberg 유지보수 DAG.

흐름:
1. 유지보수 전 테이블 헬스(full)
2. Iceberg 유지보수 프로시저(compaction / delete rewrite / manifests / snapshots)
3. 유지보수 후 테이블 헬스(full)

Phase K3: 실행은 KubernetesPodOperator(Spark on k8s). lib/spark_on_k8s.py 참조.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

from lib.spark_on_k8s import spark_k8s_task


DEFAULT_ARGS = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


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

    check_table_health_before = spark_k8s_task(
        task_id="check_table_health_before",
        job_path="src/jobs/daily/09_check_table_health.py",
        run_id_suffix="__before",
        table_health_mode="full",
    )

    run_iceberg_maintenance = spark_k8s_task(
        task_id="run_iceberg_maintenance",
        job_path="src/jobs/maintenance/run_iceberg_maintenance.py",
    )

    check_table_health_after = spark_k8s_task(
        task_id="check_table_health_after",
        job_path="src/jobs/daily/09_check_table_health.py",
        run_id_suffix="__after",
        table_health_mode="full",
    )

    check_table_health_before >> run_iceberg_maintenance >> check_table_health_after
