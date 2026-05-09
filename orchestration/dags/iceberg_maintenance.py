"""iceberg_maintenance.py

Iceberg Maintenance DAG. PRD §14.3 의 task 흐름을 따른다.

check_small_files
   ↓
compact_processed_tables
   ↓
compact_serving_tables
   ↓
check_after_compaction

설계 노트:
- Pipeline DAG와 분리한다. 데이터 처리 흐름과 Iceberg 유지보수 작업의 실행 목적이
  다르기 때문이다 (PRD §14.3).
- DAG schedule, retry 정책은 Phase 3에서 결정.
"""

from __future__ import annotations

# from airflow import DAG
# Phase 3에서 활성화


TASK_ORDER = [
    "check_small_files",
    "compact_processed_tables",
    "compact_serving_tables",
    "check_after_compaction",
]


def build_dag():
    raise NotImplementedError("Phase 3: Airflow 환경 결정 후 구현")


# dag = build_dag()
