"""lag_stream.py

end-to-end lag 계측 스트리밍 잡 (PRD v2 P2 / FR-5).

Kafka `trades`를 구독 → 각 배치를 로컬 Iceberg에 커밋 → **커밋 직후 commit_ts를 찍어**
`lag_ms = commit_ts − produce_ts`를 `local.bench.lag_samples`에 적재한다.

lag 기준점:
    produce_ts는 리플레이 하니스가 발행 시각(wall-clock, ms)으로 심은 값이다. 이 잡과
    리플레이를 **같은 컨테이너(같은 시계)**에서 실행하면 host↔container 시계 왜곡이 없다.

레버(P3 ablation 대상)는 CLI로 주입한다:
    --trigger        : processingTime (예: "0 seconds"=가능한 빨리, "5 seconds")
    --max-offsets    : maxOffsetsPerTrigger (배치 크기 상한; None=무제한)
    --shuffle-parts  : spark.sql.shuffle.partitions (병렬성)
    --label          : 이 설정 조합의 이름 (예: "baseline", "+trigger", "+parallelism")

실행 (컨테이너 내부):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \\
        src/bench/lag_stream.py --label baseline --trigger "10 seconds" \\
        --max-offsets 2000 --duration 60 --run-id run001
"""

from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from src.bench.spark_bench import get_spark_bench

KAFKA_BOOTSTRAP = "kafka:29092"  # 컨테이너 네트워크 내부 리스너
TOPIC = "trades"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"

# replay/ws 레코드에서 lag 계측에 필요한 최소 스키마.
MESSAGE_SCHEMA = StructType([
    StructField("produce_ts", LongType(), True),
    StructField("trade_id", StringType(), True),
    StructField("price", StringType(), True),
    StructField("qty", StringType(), True),
    StructField("symbol", StringType(), True),
])


def _now_ms() -> int:
    return int(time.time() * 1000)


def ensure_tables(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bench")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.bench.trades (
            produce_ts BIGINT, trade_id STRING, price STRING, qty STRING, symbol STRING,
            run_id STRING
        ) USING iceberg
        """
    )
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.bench.lag_samples (
            produce_ts BIGINT, commit_ts BIGINT, lag_ms BIGINT,
            config_label STRING, run_id STRING, batch_id BIGINT
        ) USING iceberg
        """
    )


def make_batch_processor(label: str, run_id: str):
    """foreachBatch 콜백 생성. 배치를 Iceberg에 커밋한 뒤 lag 샘플을 적재한다."""

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        # foreachBatch 배치는 persist 후 재사용한다. (.rdd.isEmpty() 등으로 Kafka 소스를
        # 재스캔하면 오프셋이 소비된 뒤라 빈 결과가 나올 수 있으므로 먼저 materialize.)
        payload = batch_df.withColumn("run_id", F.lit(run_id)).persist()
        try:
            if payload.count() == 0:
                return
            # 1) 실제 커밋 대상 — 이 write가 끝나면 배치가 Iceberg에 durable.
            payload.select(
                "produce_ts", "trade_id", "price", "qty", "symbol", "run_id"
            ).writeTo("local.bench.trades").append()

            commit_ts = _now_ms()  # ← 커밋 직후 시각 = iceberg_commit_ts 근사

            # 2) lag 샘플 적재: lag_ms = commit_ts − produce_ts
            (
                payload.select("produce_ts")
                .withColumn("commit_ts", F.lit(commit_ts))
                .withColumn("lag_ms", F.lit(commit_ts) - F.col("produce_ts"))
                .withColumn("config_label", F.lit(label))
                .withColumn("run_id", F.lit(run_id))
                .withColumn("batch_id", F.lit(batch_id))
                .writeTo("local.bench.lag_samples")
                .append()
            )
            print(f"[lag] batch={batch_id} rows={payload.count()} commit_ts={commit_ts}")
        finally:
            payload.unpersist()

    return process_batch


def run(args: argparse.Namespace) -> None:
    spark = get_spark_bench(f"lag_stream_{args.label}", packages=KAFKA_PACKAGE)
    if args.shuffle_parts:
        spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_parts))
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", TOPIC)
        .option("startingOffsets", args.starting_offsets)
    )
    if args.max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", str(args.max_offsets))
    if args.min_partitions:
        # 단일 Kafka 파티션의 오프셋 범위를 N개 Spark 태스크로 쪼개 읽기 병렬성 확보
        # (토픽 파티션·replay 키 변경 없이). 소비가 병목일 때 드레인이 빨라진다.
        reader = reader.option("minPartitions", str(args.min_partitions))

    parsed = (
        reader.load()
        .select(F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("d"))
        .select("d.*")
        .where(F.col("produce_ts").isNotNull())
    )

    writer = (
        parsed.writeStream
        .foreachBatch(make_batch_processor(args.label, args.run_id))
        .option("checkpointLocation", f"{args.checkpoint}/{args.run_id}")
        .trigger(processingTime=args.trigger)
    )

    query = writer.start()
    print(f"[lag] streaming started label={args.label} run_id={args.run_id} "
          f"trigger='{args.trigger}' max_offsets={args.max_offsets}")
    query.awaitTermination(args.duration)
    query.stop()
    print(f"[lag] stopped after {args.duration}s")
    spark.stop()


def main() -> None:
    p = argparse.ArgumentParser(description="end-to-end lag 계측 스트리밍 (P2/FR-5)")
    p.add_argument("--label", required=True, help="설정 조합 이름 (baseline/+trigger/...)")
    p.add_argument("--run-id", required=True, help="벤치 실행 식별자")
    p.add_argument("--trigger", default="0 seconds", help="processingTime 트리거")
    p.add_argument("--max-offsets", type=int, default=None, help="maxOffsetsPerTrigger")
    p.add_argument("--shuffle-parts", type=int, default=None, help="shuffle.partitions(병렬성)")
    p.add_argument("--min-partitions", type=int, default=None, help="Kafka 읽기 병렬성(minPartitions)")
    p.add_argument("--duration", type=int, default=60, help="측정 시간(초)")
    p.add_argument("--starting-offsets", default="latest", help="earliest|latest")
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    p.add_argument("--checkpoint", default="/workspace/.bench_warehouse/_checkpoints")
    run(p.parse_args())


if __name__ == "__main__":
    main()
