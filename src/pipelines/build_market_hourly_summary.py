"""build_market_hourly_summary.py

processed_klines + processed_trades → market_hourly_summary 사전 집계.

PRD §10.6 참조.

설계 노트:
- processed_klines 는 OHLCV 지표의 기준이고, processed_trades 는 trade count,
  average trade size 등 보조 지표를 제공한다 (PRD §10.6).
- 두 processed table을 serving 단계에서 symbol과 time window 기준으로 조합한다.
- Write Pattern: MERGE, Incremental (PRD §9).
"""

from __future__ import annotations

from src.pipelines.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("build_market_hourly_summary")  # noqa: F841

    # 1. 직전 처리 시점 이후의 processed_klines incremental 읽기
    # 2. 같은 시간 window의 processed_trades 보조 지표 join 또는 group by
    # 3. market_hourly_summary 에 incremental MERGE
    #
    # 정확한 incremental 기준 (kline closed 여부, time window 정의) 은 Phase 2에서 결정.

    raise NotImplementedError("Phase 2: incremental 기준과 window 정의 후 구현")


if __name__ == "__main__":
    run()
