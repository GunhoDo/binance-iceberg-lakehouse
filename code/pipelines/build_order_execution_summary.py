"""build_order_execution_summary.py

processed_orders → order_execution_summary 사전 집계.

PRD §10.6 참조.

지표:
- total / filled / canceled / partial filled orders
- fill rate, cancel rate
- average fill delay
- average slippage proxy

Write Pattern: MERGE, Incremental (PRD §9).
"""

from __future__ import annotations

from code.pipelines.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("build_order_execution_summary")  # noqa: F841

    # 1. 직전 처리 시점 이후의 processed_orders 변경분 읽기
    # 2. 시간 / symbol 단위 KPI 집계
    # 3. order_execution_summary 에 incremental MERGE
    #
    # slippage proxy 정의는 Phase 2에서 결정 (어떤 reference price 와 비교할지).

    raise NotImplementedError("Phase 2: slippage proxy 정의 후 구현")


if __name__ == "__main__":
    run()
