"""build_processed_orders.py

raw_orders → processed_orders 변환 job.

PRD §10.5 참조.

역할:
- raw order event를 정제해 MERGE 입력용 staging 형태로 준비
- 실제 processed_orders 반영은 merge_order_status_updates.py 에서 MERGE INTO로 처리

설계 노트:
- build_processed_klines와 동일하게 정제 + staging 까지만 책임을 분리한다.
- micro-batch 안의 같은 order_id 중복 dedup 전략은 Phase 2에서 결정
  (`docs/decisions.md` D6).
"""

from __future__ import annotations

from code.pipelines.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("build_processed_orders")  # noqa: F841

    # 1. raw_orders incremental 읽기
    # 2. 파싱 / 타입 정리
    # 3. MERGE 입력용 staging 준비
    # 4. (실제 MERGE는 merge_order_status_updates.py)

    raise NotImplementedError("Phase 2: staging schema와 dedup 전략 결정 후 구현")


if __name__ == "__main__":
    run()
