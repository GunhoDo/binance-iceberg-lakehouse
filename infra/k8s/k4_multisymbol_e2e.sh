#!/usr/bin/env bash
# k4_multisymbol_e2e.sh — 멀티심볼 실데이터 E2E (Phase K4)
#
# k3d Kafka 에 K1 이 쌓은 라이브 klines/trades(BTC/ETH/SOL)를 S3 로 배치 적재 →
# 심볼별 파이프라인(요약/VWAP) → 심볼별 앵커 픽스처(S3) → 심볼별 주문 시뮬 →
# 주문 적재 → 슬리피지까지 3심볼 실데이터로 흐르게 한다. 처리 계층이 멱등(trade_id/
# (symbol,open_time)/offset dedup)이라 재실행 안전.
#
# 전제: k3d 실행 + spark-k8s 이미지(kafka 커넥터 jar 포함) import + 30-spark-rbac 적용.
# 실행: infra/k8s/k4_multisymbol_e2e.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NS="binance-lakehouse"
K3D_KAFKA="kafka-0.kafka.binance-lakehouse.svc.cluster.local:9092"
# 라이브 데이터는 오늘(적재 시각/시장 시각 모두 오늘) → 넉넉한 하루 창.
WIN_START="$(date -u +%Y-%m-%d)T00:00:00"
WIN_END="$(date -u -v+1d +%Y-%m-%d 2>/dev/null || date -u -d '+1 day' +%Y-%m-%d)T00:00:00"
# symbols.yaml 의 top-level `symbols:` 블록만 추출(로컬 pyyaml 불요 — awk). shards 블록은
# 들여쓰기가 깊어 제외된다.
SYMBOLS="$(awk '/^symbols:/{f=1;next} /^[a-zA-Z]/{f=0} f&&/^[[:space:]]*-[[:space:]]/{gsub(/[-[:space:]]/,"");print}' "$ROOT/config/symbols.yaml" | tr '\n' ' ')"

# 스파크 confs(client 모드, k8s) — 배치 적재/앵커 export 공용. driver.host=$POD_IP.
# 단일 줄 문자열 반환(끝에 공백 1). \$POD_IP·\$\$ 는 런타임(파드) 확장용 리터럴, $NS 는
# 지금 확장. 호출부는 `{ spark_prefix; echo " <job>.py <args>"; }` 로 잡 파일을 이어붙인다.
spark_prefix() {
  printf '%s ' "exec /opt/spark/bin/spark-submit \
--master k8s://https://kubernetes.default.svc:443 --deploy-mode client \
--conf spark.kubernetes.namespace=$NS \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.container.image=binance-spark-k8s:latest \
--conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
--conf spark.executor.instances=1 \
--conf spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID=aws-creds:AWS_ACCESS_KEY_ID \
--conf spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=aws-creds:AWS_SECRET_ACCESS_KEY \
--conf spark.kubernetes.executor.secretKeyRef.AWS_REGION=aws-creds:AWS_REGION \
--conf spark.kubernetes.executor.secretKeyRef.AWS_DEFAULT_REGION=aws-creds:AWS_DEFAULT_REGION \
--conf spark.driver.host=\$POD_IP --conf spark.driver.bindAddress=0.0.0.0 \
--conf spark.driver.port=7078 --conf spark.blockManager.port=7079 \
--conf spark.driver.memory=1g --conf spark.executor.memory=1g \
--conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby-\$\$ \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
--conf spark.sql.catalog.glue.warehouse=s3://binance-iceberg-lake/warehouse \
--conf spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
--conf spark.sql.parquet.enableVectorizedReader=false \
--conf spark.sql.iceberg.vectorization.enabled=false \
--conf spark.sql.shuffle.partitions=4"
}

echo "==> 창 window=[$WIN_START, $WIN_END)  symbols=[$SYMBOLS]"
# K5 부터 스테이징 dedup 이 비즈니스 키 기반이라(참고 D32) 오프셋 충돌 방지용 스테이징
# 리셋 단계가 필요 없다. 멱등 UPSERT 라 반복 실행 안전.

echo ""; echo "### 1) k3d Kafka klines/trades → S3 raw 배치 적재"
for topic in klines trades; do
  { spark_prefix; echo "  /workspace/src/streams/batch_ingest_kafka.py --topic $topic --bootstrap $K3D_KAFKA"; } \
    | "$ROOT/infra/k8s/k4_run.sh" "ingest-$topic"
done

echo ""; echo "### 2) 시세 파이프라인 (01 trades, 02→04 klines, 06 요약/VWAP) — 심볼별"
for job in 01_build_processed_trades_window 02_build_staging_klines_window \
           04_merge_processed_klines_window 06_build_market_hourly_summary_window; do
  "$ROOT/infra/k8s/spark_submit_k8s.sh" "src/jobs/daily/${job}.py" "$WIN_START" "$WIN_END" "k4-${job%%_*}"
done

echo ""; echo "### 3) 심볼별 앵커 픽스처 → S3"
for sym in $SYMBOLS; do
  low="$(echo "$sym" | tr '[:upper:]' '[:lower:]')"
  { spark_prefix; echo "  /workspace/src/simulators/export_anchor_klines.py --symbol $sym --interval 1m --start-ts $WIN_START --end-ts $WIN_END --out s3://binance-iceberg-lake/fixtures/anchor_${low}.csv"; } \
    | "$ROOT/infra/k8s/k4_run.sh" "anchor-$low"
done

echo ""; echo "### 4) 심볼별 주문 시뮬 → k3d Kafka orders (앵커 모드)"
for sym in $SYMBOLS; do
  low="$(echo "$sym" | tr '[:upper:]' '[:lower:]')"
  { echo "set -e"; echo "python3 /workspace/src/simulators/orders_simulator.py --bootstrap $K3D_KAFKA --topic orders --symbol $sym --num-orders 500 --anchor-klines s3://binance-iceberg-lake/fixtures/anchor_${low}.csv --order-id-prefix ${low}-k4- --seed 42 --max-sleep-ms 0"; } \
    | "$ROOT/infra/k8s/k4_run.sh" "sim-$low"
done

echo ""; echo "### 5) 주문 → S3 raw 배치 적재"
{ spark_prefix; echo "  /workspace/src/streams/batch_ingest_kafka.py --topic orders --bootstrap $K3D_KAFKA"; } \
  | "$ROOT/infra/k8s/k4_run.sh" "ingest-orders"

echo ""; echo "### 6) 주문 파이프라인 (03 staging, 05 merge, 07 슬리피지) — 심볼별"
for job in 03_build_staging_orders_window 05_merge_processed_orders_window \
           07_build_order_execution_summary_window; do
  "$ROOT/infra/k8s/spark_submit_k8s.sh" "src/jobs/daily/${job}.py" "$WIN_START" "$WIN_END" "k4-${job%%_*}"
done

echo ""; echo "### 7) 검증: 심볼별 요약/슬리피지 카운트"
{ spark_prefix; echo "  /workspace/src/jobs/maintenance/verify_multisymbol.py"; } \
  | "$ROOT/infra/k8s/k4_run.sh" "verify"

echo ""; echo "==> K4 멀티심볼 E2E 완료"
