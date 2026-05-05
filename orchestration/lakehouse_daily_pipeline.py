"""lakehouse_daily_pipeline.py

Daily pipeline DAG. PRD §14.2 의 task 흐름을 따른다.

build_processed_trades
   ↓
build_processed_klines
   ↓
build_processed_orders
   ↓
merge_order_status_updates
   ↓
build_market_hourly_summary
   ↓
build_order_execution_summary
   ↓
check_data_quality
   ↓
check_table_health

설계 노트:
- 본 스켈레톤은 task 이름과 의존성만 정의한다.
- Operator 종류 (BashOperator vs SparkSubmitOperator vs KubernetesPodOperator) 는
  Phase 3 진입 시 환경에 맞게 결정한다 (`docs/decisions.md` D9).
- merge_kline_updates 는 build_processed_klines 의 staging 흐름과 합쳐 일단
  build_processed_klines 다음에 두지 않는다. PRD §14.2 흐름을 그대로 따른다.
  실제 task 분할은 Phase 3 구현 시 다시 본다.
"""

from __future__ import annotations

# from datetime import datetime
# from airflow import DAG
# Phase 3에서 활성화


TASK_ORDER = [
    "build_processed_trades",
    "build_processed_klines",
    "build_processed_orders",
    "merge_order_status_updates",
    "build_market_hourly_summary",
    "build_order_execution_summary",
    "check_data_quality",
    "check_table_health",
]


def build_dag():
    """DAG 객체를 생성한다.

    schedule, start_date, default_args, retries 정책은 Phase 3에서 결정한다.
    PRD에 명시된 값이 없으므로 임의로 적지 않는다.
    """
    raise NotImplementedError("Phase 3: Airflow 환경 결정 후 구현")


# dag = build_dag()
