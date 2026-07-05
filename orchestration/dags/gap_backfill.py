"""gap_backfill.py — 갭 탐지 → 자동 백필 트리거 DAG (Phase K3)

흐름:
  detect_gaps (Spark on k8s, XCom 로 갭 요약 push)
    └─ decide (분기): gap_count>0 ?
         ├─ trigger_backfill → daily_lakehouse_pipeline 을 earliest_gap_day 로 재실행
         └─ no_gaps (종료)

MVP: 가장 이른 갭 '하루'만 백필 트리거한다(연속 갭은 다음 실행이 이어서 잡는다).
schedule=None(수동/외부 트리거) — 로컬에서 의도치 않은 백필 폭주 방지. 운영에선
주기 스케줄로 승격. 갭 정의·한계는 detect_gaps.py 및 D30 참조.
"""

from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from lib.spark_on_k8s import spark_k8s_task


DEFAULT_ARGS = {"owner": "lakehouse", "depends_on_past": False, "retries": 0}


def _decide(ti) -> str:
    summary = ti.xcom_pull(task_ids="detect_gaps")
    if isinstance(summary, str):
        summary = json.loads(summary)
    summary = summary or {}
    gap_count = int(summary.get("gap_count", 0))
    print(f"[gap_backfill] decide gap_count={gap_count} summary={summary}")
    return "trigger_backfill" if gap_count > 0 else "no_gaps"


with DAG(
    dag_id="gap_backfill",
    default_args=DEFAULT_ARGS,
    description="market_hourly_summary 갭 탐지 후 daily 파이프라인 자동 백필",
    start_date=datetime(2026, 5, 6),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "k8s", "backfill"],
) as dag:

    detect_gaps = spark_k8s_task(
        task_id="detect_gaps",
        job_path="src/jobs/maintenance/detect_gaps.py",
        do_xcom_push=True,
    )

    decide = BranchPythonOperator(
        task_id="decide",
        python_callable=_decide,
    )

    # earliest_gap_day(YYYY-MM-DD)를 logical_date 로 daily 파이프라인 재실행 →
    # @daily 스케줄이라 data_interval = [그날, 다음날) = 그 하루 백필.
    trigger_backfill = TriggerDagRunOperator(
        task_id="trigger_backfill",
        trigger_dag_id="daily_lakehouse_pipeline",
        logical_date="{{ ti.xcom_pull(task_ids='detect_gaps')['earliest_gap_day'] }}T00:00:00+00:00",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    no_gaps = EmptyOperator(task_id="no_gaps")

    detect_gaps >> decide >> [trigger_backfill, no_gaps]
