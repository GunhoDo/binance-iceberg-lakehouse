#!/usr/bin/env bash
# deploy_airflow.sh — Airflow(KubernetesExecutor)를 k3d 에 helm 배포 (Phase K3)
#
# 전제: k3d 클러스터 `binance-lakehouse` 기동 + K2 자산 적용
#   (binance-spark-k8s 이미지 import, 30-spark-rbac 적용, aws-creds Secret).
#   aws-creds Secret 은 spark_submit_k8s.sh 가 만들거나 아래에서 보장한다.
#
# 실행: infra/k8s/deploy_airflow.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER="binance-lakehouse"
NS="binance-lakehouse"
CHART_VERSION="1.16.0"   # appVersion 2.10.5 (마지막 Airflow-2 chart)

echo "==> helm repo 확인"
helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
helm repo update apache-airflow >/dev/null

echo "==> Airflow 이미지 빌드 + k3d import"
docker build -f "$ROOT/infra/Dockerfile.airflow-k8s" -t binance-airflow:latest "$ROOT"
k3d image import binance-airflow:latest -c "$CLUSTER"

echo "==> aws-creds Secret 보장(로컬 aws configure)"
AWS_KEY="$(aws configure get aws_access_key_id)"
AWS_SECRET="$(aws configure get aws_secret_access_key)"
AWS_REGION="$(aws configure get region || echo ap-northeast-2)"
kubectl -n "$NS" create secret generic aws-creds \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_KEY" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET" \
  --from-literal=AWS_REGION="$AWS_REGION" \
  --from-literal=AWS_DEFAULT_REGION="$AWS_REGION" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

echo "==> spark RBAC 적용(K2)"
kubectl apply -f "$ROOT/infra/k8s/30-spark-rbac.yaml" >/dev/null

echo "==> helm upgrade --install airflow (chart $CHART_VERSION)"
helm upgrade --install airflow apache-airflow/airflow \
  --version "$CHART_VERSION" \
  --namespace "$NS" \
  -f "$ROOT/infra/k8s/airflow-values.yaml" \
  --timeout 15m \
  --wait

echo ""
echo "==> 완료. 웹서버 포트포워드:"
echo "    kubectl -n $NS port-forward svc/airflow-webserver 8080:8080"
echo "    http://localhost:8080  (admin / admin)"
