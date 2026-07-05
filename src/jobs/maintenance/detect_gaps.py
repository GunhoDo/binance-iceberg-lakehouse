"""detect_gaps.py — market_hourly_summary 시간 연속성 갭 탐지 (Phase K3)

gap_backfill DAG 의 첫 태스크. 심볼별로 summary_hour 의 [min, max] 범위에서 기대되는
시간별 버킷을 생성하고, 실제 존재하는 버킷과 left-anti 조인해 **누락된 시간**을 찾는다.
결과 요약을 stdout 과 KubernetesPodOperator XCom(`/airflow/xcom/return.json`)에 쓴다 →
DAG 가 갭 유무로 백필 트리거를 분기한다.

윈도우 인자(start/end)는 팩토리 호환을 위해 받되 사용하지 않는다 — 갭 탐지는 테이블의
실제 데이터 범위 전체를 스캔해야 의미가 있다(벽시계 기준 아님). 자기 인식: 이 잡은
'내부 갭'(범위 안 빠진 시간)만 본다. 범위 양끝 밖(아직 안 온 미래)은 갭이 아니다.
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import functions as F

from src.jobs.common.args import parse_job_args
from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import MARKET_HOURLY_SUMMARY


XCOM_PATH = Path("/airflow/xcom/return.json")


def run() -> None:
    args = parse_job_args()
    spark = get_spark("phase3_detect_gaps")

    hours = (
        spark.table(MARKET_HOURLY_SUMMARY)
        .select("symbol", "summary_hour")
        .where(F.col("summary_hour").isNotNull())
        .distinct()
    )

    bounds = hours.groupBy("symbol").agg(
        F.min("summary_hour").alias("mn"),
        F.max("summary_hour").alias("mx"),
        F.count("*").alias("actual"),
    )

    # 심볼별 기대 시간 그리드(1시간 간격) 전개 후 실제와 left-anti → 누락 시간.
    grid = bounds.select(
        "symbol",
        F.explode(F.expr("sequence(mn, mx, interval 1 hour)")).alias("summary_hour"),
    )
    missing = grid.join(hours, ["symbol", "summary_hour"], "left_anti")

    total_missing = missing.count()

    per_symbol = {
        r["symbol"]: {
            "expected": int(r["expected"]),
            "actual": int(r["actual"]),
            "missing": int(r["expected"]) - int(r["actual"]),
        }
        for r in bounds.withColumn(
            "expected",
            ((F.unix_timestamp("mx") - F.unix_timestamp("mn")) / 3600 + 1).cast("long"),
        ).collect()
    }

    earliest_gap_day = None
    if total_missing > 0:
        earliest = missing.agg(F.min("summary_hour").alias("e")).collect()[0]["e"]
        if earliest is not None:
            earliest_gap_day = earliest.strftime("%Y-%m-%d")

    summary = {
        "gap_count": int(total_missing),
        "per_symbol": per_symbol,
        "earliest_gap_day": earliest_gap_day,
    }

    print(f"[phase3_detect_gaps] complete run_id={args.run_id}, summary={json.dumps(summary)}")

    # KPO XCom: 파드에 /airflow/xcom 이 마운트됐을 때만 기록(do_xcom_push=True).
    if XCOM_PATH.parent.exists():
        XCOM_PATH.write_text(json.dumps(summary), encoding="utf-8")
        print(f"[phase3_detect_gaps] wrote XCom -> {XCOM_PATH}")

    spark.stop()


if __name__ == "__main__":
    run()
