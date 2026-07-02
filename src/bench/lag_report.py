"""lag_report.py

lag 샘플 → p50/p95/p99 + throughput 표 (PRD v2 P2/P3 / FR-7).

`local.bench.lag_samples`를 config_label·run_id로 집계해 레버별 lag 분포와 처리량을 낸다.
P3 ablation의 "레버별 기여도 표"가 여기서 나온다.

실행 (컨테이너 내부):
    spark-submit src/bench/lag_report.py                 # 전체
    spark-submit src/bench/lag_report.py --run-id run001 # 특정 실행만
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from src.bench.spark_bench import get_spark_bench


def run(run_id: str | None) -> None:
    spark = get_spark_bench("lag_report")
    spark.sparkContext.setLogLevel("WARN")
    # aarch64 JVM에서 percentile_approx whole-stage codegen이 SIGSEGV를 내는 사례가 있어
    # 리포트 세션에서는 codegen을 끈다(소량 집계라 성능 영향 무시 가능).
    spark.conf.set("spark.sql.codegen.wholeStage", "false")

    df = spark.table("local.bench.lag_samples")
    if run_id:
        df = df.where(F.col("run_id") == run_id)

    agg = (
        df.groupBy("run_id", "config_label")
        .agg(
            F.count("*").alias("samples"),
            F.expr("percentile_approx(lag_ms, 0.5)").alias("p50_ms"),
            F.expr("percentile_approx(lag_ms, 0.95)").alias("p95_ms"),
            F.expr("percentile_approx(lag_ms, 0.99)").alias("p99_ms"),
            F.max("lag_ms").alias("max_ms"),
            F.min("produce_ts").alias("_first"),
            F.max("commit_ts").alias("_last"),
        )
        .withColumn(
            "throughput_per_s",
            F.round(F.col("samples") / ((F.col("_last") - F.col("_first")) / 1000.0), 1),
        )
        .drop("_first", "_last")
        .orderBy("run_id", "config_label")
    )

    print("\n===== end-to-end lag (commit_ts − produce_ts) =====")
    agg.show(truncate=False)


def main() -> None:
    p = argparse.ArgumentParser(description="lag 리포트 (P2/P3)")
    p.add_argument("--run-id", default=None)
    run(p.parse_args().run_id)


if __name__ == "__main__":
    main()
