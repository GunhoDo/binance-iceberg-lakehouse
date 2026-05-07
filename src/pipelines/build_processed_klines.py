"""build_processed_klines.py

raw_klines → processed_klines 변환 job.

PRD §10.4 참조.

역할:
- raw kline event를 정제된 kline 단위로 변환
- 같은 (symbol, interval, open_time) 의 반복 update를 처리하기 위해 다음 단계인
  merge_kline_updates에 입력될 staging 형태로 준비

설계 노트:
- 본 job은 정제 + staging 까지만 책임진다. 실제 processed_klines로의 반영은
  merge_kline_updates.py 에서 MERGE INTO로 처리한다.
- 이렇게 분리하는 이유는 MERGE 대상 micro-batch 안에서 같은 키가 여러 번 나올 수
  있어, MERGE 직전에 키 단위 dedup이 필요하기 때문이다 (`docs/decisions.md` D6).
"""

from __future__ import annotations

from src.pipelines.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("build_processed_klines")  # noqa: F841

    # 1. raw_klines incremental 읽기
    # 2. 파싱 / 타입 정리
    # 3. MERGE 입력용 staging table 또는 temp view 준비
    # 4. (실제 MERGE는 merge_kline_updates.py)

    raise NotImplementedError("Phase 2: staging schema 결정 후 구현")


if __name__ == "__main__":
    run()
