"""quality_scan_prod.py

**프로덕션 엔트리포인트** — glue/S3 processed_trades에 대한 품질 이상탐지 (PRD v2 P4).

핵심: 이 파일은 `ScanConfig`(테이블 식별자·컬럼·freshness 입력원)만 바꿔 넘길 뿐,
탐지·적재 로직은 **벤치와 동일한** src/quality/quality_scan.py의 `scan()`/`persist()`를
그대로 재사용한다. 규칙(src/quality/rules.py)은 카탈로그를 모른다.

벤치와의 차이는 딱 세 군데(모두 설정):
  1. 세션        : get_spark()  (Glue catalog / S3FileIO)   ← spark_bench 대신
  2. 테이블      : glue.binance_lakehouse.*                  ← local.bench.* 대신
  3. freshness   : event_ts(trade_time) 나이               ← lag_samples 대신

주의: 이 경로는 AWS 자격증명·Glue·S3가 필요해 로컬에서 실측 검증하지 않는다(코드 패리티만).
벤치 경로(quality_scan.py)가 같은 scan()/persist()를 docker에서 실측한다.

실행 (AWS 자격증명 있는 환경):
    PYTHONPATH=. spark-submit \\
        --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.iceberg:iceberg-aws-bundle:1.7.0,org.apache.hadoop:hadoop-aws:3.3.4 \\
        src/quality/quality_scan_prod.py --sla-ms 60000
"""

from __future__ import annotations

import argparse

from src.pipelines.common.spark_session import get_spark
from src.quality.alert import send_alerts
from src.quality.quality_scan import ScanConfig, persist, scan

# processed_trades 기대 스키마 (schema-drift 기준). simpleString 타입 문자열.
PROCESSED_TRADES_SCHEMA = {
    "trade_id": "bigint",
    "symbol": "string",
    "price": "decimal(20,8)",
    "qty": "decimal(20,8)",
    "quote_qty": "decimal(20,8)",
    "trade_time": "timestamp",
    "is_buyer_maker": "boolean",
    "is_best_match": "boolean",
    "source_topic": "string",
    "source_partition": "int",
    "source_offset": "bigint",
    "ingest_time": "string",
}


def prod_config(sla_ms: float, max_missing: int) -> ScanConfig:
    """glue/S3 processed_trades용 설정. quality_events는 09_create_quality_events.sql이 생성."""
    return ScanConfig(
        trades_table="glue.binance_lakehouse.processed_trades",
        lag_table=None,
        quality_table="glue.binance_lakehouse.quality_events",
        quality_namespace="glue.binance_lakehouse",
        id_col="trade_id",
        gap_step=1,
        order_col="trade_time",       # 이벤트 시각(도착 순서 프록시)
        dimension_col="symbol",
        numeric_fields=("price", "qty"),
        sla_ms=sla_ms,
        max_missing=max_missing,
        expected_schema=dict(PROCESSED_TRADES_SCHEMA),
        freshness_source="event_ts",  # lag_samples가 없으므로 이벤트시각 나이로 freshness
        event_ts_col="trade_time",
    )


def run(args: argparse.Namespace) -> None:
    spark = get_spark("quality_scan_prod")
    spark.sparkContext.setLogLevel("WARN")

    cfg = prod_config(args.sla_ms, args.max_missing)
    # 프로덕션 processed_trades에는 run_id 컬럼이 없어 전체를 검사한다(run_id=None).
    anomalies = scan(spark, cfg, run_id=None)
    persist(spark, cfg, anomalies)

    result = send_alerts(anomalies, webhook_url=args.webhook)
    print("\n===== 품질 이상 스캔 (prod / processed_trades) =====")
    print(result["summary"])
    print(f"[alert] sent={result['sent']} reason={result['reason']}")
    spark.stop()


def main() -> None:
    p = argparse.ArgumentParser(description="데이터 품질 이상탐지 — 프로덕션 (P4)")
    p.add_argument("--sla-ms", type=float, default=60000.0, help="freshness SLA (ms)")
    p.add_argument("--max-missing", type=int, default=0, help="허용 gap 결측 수")
    p.add_argument("--webhook", default=None, help="Discord webhook URL (미설정 시 graceful)")
    run(p.parse_args())


if __name__ == "__main__":
    main()
