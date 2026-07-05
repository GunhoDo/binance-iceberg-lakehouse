"""verify_multisymbol.py — 멀티심볼 서빙 카운트 검증 (Phase K4)

market_hourly_summary(요약/VWAP)와 order_execution_summary(슬리피지)를 심볼별로 집계해
3심볼(BTC/ETH/SOL) 실데이터가 서빙 계층까지 흘렀는지 한눈에 보여준다. 읽기 전용.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import MARKET_HOURLY_SUMMARY, ORDER_EXECUTION_SUMMARY


def run() -> None:
    spark = get_spark("phase_k4_verify_multisymbol")

    print("[verify] market_hourly_summary (심볼별 시간수 · VWAP 범위):")
    (
        spark.table(MARKET_HOURLY_SUMMARY)
        .groupBy("symbol")
        .agg(
            F.count("*").alias("hours"),
            F.round(F.min("vwap"), 2).alias("vwap_min"),
            F.round(F.max("vwap"), 2).alias("vwap_max"),
        )
        .orderBy("symbol")
        .show(truncate=False)
    )

    print("[verify] order_execution_summary (심볼별 주문 · 슬리피지 bps):")
    (
        spark.table(ORDER_EXECUTION_SUMMARY)
        .groupBy("symbol")
        .agg(
            F.count("*").alias("rows"),
            F.round(F.avg("buy_slippage_bps"), 2).alias("avg_buy_slip_bps"),
            F.round(F.avg("sell_slippage_bps"), 2).alias("avg_sell_slip_bps"),
        )
        .orderBy("symbol")
        .show(truncate=False)
    )

    print("[phase_k4_verify_multisymbol] complete")
    spark.stop()


if __name__ == "__main__":
    run()
