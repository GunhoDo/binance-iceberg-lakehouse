#!/usr/bin/env bash
# build_spark_image.sh — spark-on-k8s 이미지 빌드 + k3d 임포트 (Phase K2)
#
# ingestor 와 동일 패턴: 로컬 빌드 후 k3d image import 로 노드에 적재
# (imagePullPolicy=IfNotPresent). 레지스트리 push 불필요.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER="binance-lakehouse"

echo "==> spark-on-k8s 이미지 빌드"
docker build -f "$ROOT/infra/Dockerfile.spark-k8s" -t binance-spark-k8s:latest "$ROOT"

echo "==> k3d image import (-c $CLUSTER)"
k3d image import binance-spark-k8s:latest -c "$CLUSTER"

echo "==> 완료"
