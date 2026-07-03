"""quality_scan.py

데이터 품질 이상탐지 **Spark 드라이버** (PRD v2 P4 / FR-8·FR-9·FR-11).

설계 원칙 — **카탈로그 중립**:
    이 드라이버는 `local.bench.*`를 하드코딩하지 않는다. `ScanConfig`(테이블 식별자·
    검사 컬럼·SLA)와 SparkSession을 **주입**받아 규칙(src/quality/rules.py)을 적용하고
    이상을 `quality_events` 테이블에 적재한 뒤 알림(src/quality/alert.py)을 쏜다.
    → local hadoop 벤치 CLI는 `local.bench.*`를, 프로덕션 엔트리포인트는 `get_spark()` +
      `glue.binance_lakehouse.*`를 넘기면 **같은 코드가 그대로 돈다**. (README §재사용성)

벤치 실행 (컨테이너 내부):
    python src/quality/quality_scan.py --run-id run010 --sla-ms 15000
"""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.quality import rules
from src.quality.alert import send_alerts

# ---------------------------------------------------------------------------
# 설정 (주입) — 여기만 바꾸면 벤치↔프로덕션 전환
# ---------------------------------------------------------------------------

# 벤치 trades 테이블의 기대 스키마 (schema-drift 기준). simpleString 타입 문자열.
BENCH_TRADES_SCHEMA = {
    "produce_ts": "bigint",
    "trade_id": "string",
    "price": "string",
    "qty": "string",
    "symbol": "string",
    "run_id": "string",
}


@dataclass
class ScanConfig:
    trades_table: str            # gap/reversal/null 대상 (bench: local.bench.trades)
    lag_table: str | None        # freshness 입력 (bench: local.bench.lag_samples)
    quality_table: str           # 이상 적재 (bench: local.bench.quality_events)
    quality_namespace: str       # CREATE NAMESPACE IF NOT EXISTS 대상
    id_col: str = "trade_id"     # gap/reversal 컬럼
    gap_step: int = 1            # trades=1, klines=interval_ms
    order_col: str = "produce_ts"  # 도착 순서
    dimension_col: str = "symbol"
    numeric_fields: tuple[str, ...] = ("price", "qty")
    sla_ms: float = 15000.0
    max_missing: int = 0
    expected_schema: dict = field(default_factory=lambda: dict(BENCH_TRADES_SCHEMA))
    # freshness 입력원 (FR-8): 벤치는 lag_samples 재사용, 프로덕션은 이벤트시각 나이(now-max).
    freshness_source: str = "lag"   # "lag" | "event_ts"
    event_ts_col: str | None = None  # freshness_source=="event_ts"일 때 TIMESTAMP 컬럼


def bench_config() -> ScanConfig:
    """local hadoop 벤치용 기본 설정 (P0~P3와 같은 `local.bench` 경로)."""
    return ScanConfig(
        trades_table="local.bench.trades",
        lag_table="local.bench.lag_samples",
        quality_table="local.bench.quality_events",
        quality_namespace="local.bench",
    )


# quality_events 스키마 (bench catalog는 DDL 파일 없이 여기서 생성; prod는 09_*.sql)
QUALITY_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("detected_at", TimestampType(), False),
    StructField("run_id", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("check_name", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("dimension", StringType(), True),
    StructField("observed", DoubleType(), True),
    StructField("threshold", DoubleType(), True),
    StructField("detail", StringType(), True),
])


def ensure_quality_table(spark: SparkSession, cfg: ScanConfig) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.quality_namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {cfg.quality_table} (
            event_id STRING, detected_at TIMESTAMP, run_id STRING, source_table STRING,
            check_name STRING, severity STRING, dimension STRING,
            observed DOUBLE, threshold DOUBLE, detail STRING
        ) USING iceberg
        """
    )


# ---------------------------------------------------------------------------
# 검사 수집 — Spark에서 집계/수집한 값을 순수 규칙 함수에 넘긴다
# ---------------------------------------------------------------------------

def collect_freshness(
    spark: SparkSession, cfg: ScanConfig, trades: DataFrame, run_id: str | None
) -> list[dict]:
    """freshness 검사 (FR-8). 입력원만 다르고 판정(rules.check_freshness)은 동일하다.

    - "lag"      (벤치): lag_samples를 config_label별 p95/max로 집계해 그대로 재사용.
    - "event_ts" (프로덕션): dimension별 이벤트시각 나이(now − max(event_ts))를 SLA와 비교.
      (lag_samples 산출물이 없는 실 파이프라인에서 freshness를 재는 표준 방식.)
    """
    if cfg.freshness_source == "lag":
        if not cfg.lag_table:
            return []
        # aarch64 codegen SIGSEGV 회피 (lag_report.py와 동일 사유)
        spark.conf.set("spark.sql.codegen.wholeStage", "false")
        df = spark.table(cfg.lag_table)
        if run_id:
            df = df.where(F.col("run_id") == run_id)
        agg = (
            df.groupBy("config_label")
            .agg(
                F.expr("percentile_approx(lag_ms, 0.95)").alias("p95_ms"),
                F.max("lag_ms").alias("max_ms"),
            )
            .collect()
        )
        rows = [
            {"dimension": r["config_label"], "p95_ms": float(r["p95_ms"]),
             "max_ms": float(r["max_ms"])}
            for r in agg
        ]
        return rules.check_freshness_all(rows, cfg.sla_ms)

    if cfg.freshness_source == "event_ts":
        if not cfg.event_ts_col:
            return []
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        agg = (
            trades.groupBy(cfg.dimension_col)
            .agg((F.max(F.col(cfg.event_ts_col).cast("double")) * 1000).alias("_last_ms"))
            .collect()
        )
        rows = [
            {"dimension": r[cfg.dimension_col], "p95_ms": now_ms - float(r["_last_ms"]),
             "max_ms": now_ms - float(r["_last_ms"])}
            for r in agg if r["_last_ms"] is not None
        ]
        return rules.check_freshness_all(rows, cfg.sla_ms)

    return []


def collect_sequence_checks(trades: DataFrame, cfg: ScanConfig) -> list[dict]:
    """gap + 순서역전. dimension별 id 시퀀스를 수집해 순수 규칙에 넘긴다.

    수집량은 벤치 규모(수만 건)에 맞춘 값이다. 프로덕션 대용량에서는 같은 규칙을
    Spark window(lag/lead)로 push-down해 후보 행만 collect하도록 바꾸면 된다.
    """
    picked = trades.select(
        F.col(cfg.dimension_col).alias("_dim"),
        F.col(cfg.id_col).cast("long").alias("_id"),
        F.col(cfg.order_col).cast("long").alias("_ord"),
    ).where(F.col("_id").isNotNull()).collect()

    by_dim: dict[str, list[tuple[int, int]]] = {}
    for r in picked:
        by_dim.setdefault(r["_dim"], []).append((r["_ord"], r["_id"]))

    out: list[dict] = []
    for dim, pairs in by_dim.items():
        ids = [i for _, i in pairs]
        out.extend(rules.check_gap(dim, ids, cfg.gap_step, cfg.max_missing))
        # 도착 순서 = (produce_ts, id). 리플레이 한 슬라이스는 produce_ts가 같아 collect
        # 순서가 비결정적이므로 id를 2차 정렬키로 둔다 → 슬라이스 내부는 오름차순으로 고정,
        # 진짜 '나중 슬라이스가 이전보다 작은 id' 인 cross-slice 역전만 잡는다(허위양성 제거).
        seq = [i for _, i in sorted(pairs, key=lambda p: (p[0], p[1]))]
        out.extend(rules.check_order_reversal(dim, seq))
    return out


def collect_null_or_zero(trades: DataFrame, cfg: ScanConfig) -> list[dict]:
    """필수 수치 필드의 NULL/0 집계 → 규칙 적용 (테이블 단위)."""
    exprs = [F.count(F.lit(1)).alias("total")]
    for f in cfg.numeric_fields:
        col = F.col(f)
        exprs.append(F.count(F.when(col.isNull(), 1)).alias(f"{f}__nulls"))
        exprs.append(
            F.count(F.when(col.cast("double").isNull() | (col.cast("double") == 0.0), 1))
            .alias(f"{f}__zeros")
        )
    row = trades.agg(*exprs).collect()[0]
    total = int(row["total"])
    field_stats = {
        f: {
            "nulls": int(row[f"{f}__nulls"]),
            # cast-실패(파싱불가)도 zero 버킷에 포함되나, null은 위에서 이미 카운트되어
            # 중복될 수 있으므로 zeros에서 null 수를 뺀다.
            "zeros": max(int(row[f"{f}__zeros"]) - int(row[f"{f}__nulls"]), 0),
            "total": total,
        }
        for f in cfg.numeric_fields
    }
    return rules.check_null_or_zero(cfg.trades_table.split(".")[-1], field_stats)


def collect_schema_drift(trades: DataFrame, cfg: ScanConfig) -> list[dict]:
    actual = {fld.name: fld.dataType.simpleString() for fld in trades.schema.fields}
    return rules.check_schema_drift(cfg.trades_table.split(".")[-1], actual, cfg.expected_schema)


# ---------------------------------------------------------------------------
# 오케스트레이션
# ---------------------------------------------------------------------------

def scan(spark: SparkSession, cfg: ScanConfig, run_id: str | None) -> list[dict]:
    """모든 검사를 실행하고 컨텍스트를 채운 이상 레코드 목록을 돌려준다(적재 전)."""
    ensure_quality_table(spark, cfg)

    trades = spark.table(cfg.trades_table)
    if run_id and "run_id" in trades.columns:
        trades = trades.where(F.col("run_id") == run_id)

    anomalies: list[dict] = []
    anomalies += collect_freshness(spark, cfg, trades, run_id)
    anomalies += collect_schema_drift(trades, cfg)
    anomalies += collect_sequence_checks(trades, cfg)
    anomalies += collect_null_or_zero(trades, cfg)

    detected_at = datetime.now(timezone.utc)
    for a in anomalies:
        a["event_id"] = str(uuid.uuid4())
        a["detected_at"] = detected_at
        a["run_id"] = run_id
        a["source_table"] = cfg.trades_table
    return anomalies


def persist(spark: SparkSession, cfg: ScanConfig, anomalies: list[dict]) -> None:
    if not anomalies:
        print("[quality] 이상 없음 — 적재 스킵")
        return
    rows = [
        (
            a["event_id"], a["detected_at"], a["run_id"], a["source_table"],
            a["check_name"], a["severity"], a["dimension"],
            a["observed"], a["threshold"], a["detail"],
        )
        for a in anomalies
    ]
    df = spark.createDataFrame(rows, schema=QUALITY_EVENTS_SCHEMA)
    df.writeTo(cfg.quality_table).append()
    print(f"[quality] {len(anomalies)}건 적재 → {cfg.quality_table}")


def run(args: argparse.Namespace) -> None:
    from src.bench.spark_bench import get_spark_bench

    spark = get_spark_bench("quality_scan")
    spark.sparkContext.setLogLevel("WARN")
    # aarch64 openjdk-17에서 whole-stage codegen이 집계 시 SIGSEGV를 내는 사례가 있어
    # 벤치 세션 전체에서 끈다(소량 집계라 성능 영향 무시 가능; lag_report.py와 동일 조치).
    spark.conf.set("spark.sql.codegen.wholeStage", "false")

    cfg = bench_config()
    cfg.sla_ms = args.sla_ms
    cfg.max_missing = args.max_missing

    anomalies = scan(spark, cfg, args.run_id)
    persist(spark, cfg, anomalies)

    result = send_alerts(anomalies, webhook_url=args.webhook)
    print(f"\n===== 품질 이상 스캔 (run_id={args.run_id}) =====")
    print(result["summary"])
    print(f"[alert] sent={result['sent']} reason={result['reason']}")
    spark.stop()


def main() -> None:
    p = argparse.ArgumentParser(description="데이터 품질 이상탐지 (P4/FR-8·FR-9)")
    p.add_argument("--run-id", default=None, help="특정 벤치 실행만 검사")
    p.add_argument("--sla-ms", type=float, default=15000.0, help="freshness SLA (ms)")
    p.add_argument("--max-missing", type=int, default=0, help="허용 gap 결측 수")
    p.add_argument("--webhook", default=None, help="Discord webhook URL (미설정 시 graceful)")
    run(p.parse_args())


if __name__ == "__main__":
    main()
