"""export_anchor_klines.py (Phase A / A-1)

processed_klines 를 orders_simulator 의 앵커 fixture(CSV)로 내보낸다.

출력 CSV 헤더: open_time_ms,close,volume
  - open_time_ms : 그 분(minute)의 시작 epoch milliseconds
  - close        : 그 분의 종가 → 주문 가격 앵커 기준점(의사결정 시점 가격)
  - volume       : 그 분의 거래량 → 도착 분포의 가중치

순환 방지: 여기서 내보내는 close 는 "분 단위" 종가다. 슬리피지 벤치마크로 쓰는
interval VWAP(시간당 집계)과는 다른 시계열이므로, 이 fixture 로 앵커링해도
슬리피지가 0 으로 퇴화하지 않는다. (docs/gold_serving_improvement_plan.md §4.1.1)

klines 는 분당 1행으로 양이 적어 드라이버로 collect 후 단일 CSV 로 쓴다
(Spark CSV 멀티파트 출력·S3 쓰기 회피 — 순수 로컬 fixture).

실행 (러닝 스택 + AWS 자격 필요):
    python src/simulators/export_anchor_klines.py \
      --symbol BTCUSDT \
      --interval 1m \
      --start-ts 2024-01-01T00:00:00Z \
      --end-ts 2024-02-01T00:00:00Z \
      --out fixtures/anchor_btcusdt_2024-01.csv
"""
from __future__ import annotations

import argparse
import csv
import io
from pathlib import Path

from src.jobs.common.spark_session import get_spark
from src.jobs.common.tables import PROCESSED_KLINES


def build_query(
    symbol: str,
    interval: str,
    start_ts: str | None,
    end_ts: str | None,
) -> str:
    where = [
        f"symbol = '{symbol}'",
        f"`interval` = '{interval}'",
        "open_time IS NOT NULL",
        "close IS NOT NULL",
        "volume IS NOT NULL",
    ]
    if start_ts:
        where.append(f"open_time >= TIMESTAMP '{start_ts}'")
    if end_ts:
        where.append(f"open_time < TIMESTAMP '{end_ts}'")

    where_clause = "\n              AND ".join(where)
    return f"""
        SELECT
            CAST(unix_timestamp(open_time) AS BIGINT) * 1000 AS open_time_ms,
            close,
            volume
        FROM {PROCESSED_KLINES}
        WHERE {where_clause}
        ORDER BY open_time
    """


def run() -> None:
    parser = argparse.ArgumentParser(description="Export processed_klines as anchor fixture CSV")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--start-ts")
    parser.add_argument("--end-ts")
    parser.add_argument("--out", required=True, help="출력 CSV 경로")
    args = parser.parse_args()

    spark = get_spark("phaseA_export_anchor_klines")
    rows = spark.sql(build_query(args.symbol, args.interval, args.start_ts, args.end_ts)).collect()
    spark.stop()

    if not rows:
        raise SystemExit(
            f"no klines for symbol={args.symbol} interval={args.interval} "
            f"window=[{args.start_ts}, {args.end_ts})"
        )

    # CSV 본문 생성(로컬/S3 공용)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["open_time_ms", "close", "volume"])
    for row in rows:
        writer.writerow([row["open_time_ms"], row["close"], row["volume"]])
    body = buf.getvalue()

    # --out 이 s3://... 면 boto3 로 업로드(K4: k8s 잡이 픽스처를 S3 로 공유), 아니면 로컬.
    if args.out.startswith("s3://"):
        import boto3

        bucket, _, key = args.out[len("s3://"):].partition("/")
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    else:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(body, encoding="utf-8")

    print(
        f"anchor fixture 완료: {len(rows)} buckets → {args.out} "
        f"(symbol={args.symbol}, interval={args.interval})"
    )


if __name__ == "__main__":
    run()
