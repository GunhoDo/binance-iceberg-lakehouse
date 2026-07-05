"""daily_lakehouse_pipeline.py

일일 레이크하우스 파이프라인 DAG.

Airflow 는 오케스트레이션 전용. 실제 Spark 잡은 KubernetesPodOperator 로 뜬 파드가
spark-submit(client 모드)로 실행하고, 그 드라이버가 executor 파드를 스케줄한다
(Phase K3 — 기존 `docker exec spark-runner` BashOperator 를 대체). 실행 방식 상세는
lib/spark_on_k8s.py 참조.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

from lib.spark_on_k8s import spark_k8s_task


DEFAULT_ARGS = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="daily_lakehouse_pipeline",
    default_args=DEFAULT_ARGS,
    description="Window-based idempotent daily lakehouse pipeline (Spark on k8s)",
    start_date=datetime(2026, 5, 6),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "k8s"],
) as dag:

    build_processed_trades = spark_k8s_task(
        "build_processed_trades",
        "src/jobs/daily/01_build_processed_trades_window.py",
    )

    build_staging_klines = spark_k8s_task(
        "build_staging_klines",
        "src/jobs/daily/02_build_staging_klines_window.py",
    )

    build_staging_orders = spark_k8s_task(
        "build_staging_orders",
        "src/jobs/daily/03_build_staging_orders_window.py",
    )

    merge_processed_klines = spark_k8s_task(
        "merge_processed_klines",
        "src/jobs/daily/04_merge_processed_klines_window.py",
    )

    merge_processed_orders = spark_k8s_task(
        "merge_processed_orders",
        "src/jobs/daily/05_merge_processed_orders_window.py",
    )

    build_market_hourly_summary = spark_k8s_task(
        "build_market_hourly_summary",
        "src/jobs/daily/06_build_market_hourly_summary_window.py",
    )

    build_order_execution_summary = spark_k8s_task(
        "build_order_execution_summary",
        "src/jobs/daily/07_build_order_execution_summary_window.py",
    )

    check_data_quality = spark_k8s_task(
        "check_data_quality",
        "src/jobs/daily/08_check_data_quality.py",
    )

    check_table_health = spark_k8s_task(
        "check_table_health",
        "src/jobs/daily/09_check_table_health.py",
    )

    build_processed_trades >> build_market_hourly_summary

    build_staging_klines >> merge_processed_klines >> build_market_hourly_summary

    build_staging_orders >> merge_processed_orders >> build_order_execution_summary

    # Phase X: 07 이 market_hourly_summary.vwap 을 벤치마크로 조인하므로 06 이후에 실행돼야 한다.
    build_market_hourly_summary >> build_order_execution_summary

    [build_market_hourly_summary, build_order_execution_summary] >> check_data_quality
    check_data_quality >> check_table_health
