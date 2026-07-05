#!/usr/bin/env bash
# deploy.sh — Phase K1 배포 (k3d 클러스터 위 멀티심볼 수집)
#
# 전제: k3d 클러스터가 이미 생성돼 있어야 한다.
#   k3d cluster create --config infra/k8s/k3d-cluster.yaml
#
# 실행: infra/k8s/deploy.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER="binance-lakehouse"
NS="binance-lakehouse"

echo "==> ingestor 이미지 빌드 + k3d import"
docker build -f "$ROOT/infra/Dockerfile.ingestor" -t binance-ingestor:latest "$ROOT"
k3d image import binance-ingestor:latest -c "$CLUSTER"

echo "==> namespace + Kafka + ingestor 적용"
kubectl apply -k "$ROOT/infra/k8s"

echo "==> symbols-config ConfigMap 생성 (config/symbols.yaml 단일 소스)"
kubectl create configmap symbols-config \
  --from-file=symbols.yaml="$ROOT/config/symbols.yaml" \
  -n "$NS" --dry-run=client -o yaml | kubectl apply -f -

echo "==> Kafka 준비 대기"
kubectl -n "$NS" rollout status statefulset/kafka --timeout=180s

echo "==> ingestor 재시작(ConfigMap 반영 보장)"
kubectl -n "$NS" rollout restart deployment/ingestor-shard-0 deployment/ingestor-shard-1
kubectl -n "$NS" rollout status deployment/ingestor-shard-0 --timeout=120s
kubectl -n "$NS" rollout status deployment/ingestor-shard-1 --timeout=120s

echo "==> 완료. 토픽 오프셋 확인:"
kubectl -n "$NS" exec kafka-0 -- \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic trades
