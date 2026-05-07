"""build_processed_trades.py

raw_trades → processed_trades 정제 job.

PRD §10.3 참조.

역할:
- trade id 기준 중복 제거
- symbol, trade_time 기준 정규화
- price, quantity 타입 정리
- source topic/partition/offset 보존

Write Pattern: Append (PRD §9).
"""

from __future__ import annotations

from code.pipelines.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("build_processed_trades")  # noqa: F841

    # 1. raw_trades 읽기 (incremental — 직전 처리 시점 이후만)
    # 2. 파싱 / 타입 정리 / 중복 제거
    # 3. processed_trades에 append
    #
    # incremental 처리 기준 (예: source.commit_time, kafka_offset, ingest_time
    # 중 무엇으로 잘라낼지) 은 Phase 2에서 결정한다.

    raise NotImplementedError("Phase 2: incremental 기준 결정 후 구현")


if __name__ == "__main__":
    run()
