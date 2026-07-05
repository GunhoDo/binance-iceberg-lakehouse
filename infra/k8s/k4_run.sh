#!/usr/bin/env bash
# k4_run.sh — 임의 스크립트를 spark-k8s 이미지 파드에서 실행 (Phase K4)
#
# 사용: echo "<bash script>" | infra/k8s/k4_run.sh <job-name>
#   또는  infra/k8s/k4_run.sh <job-name> <<'EOF' ... EOF
#
# 45-spark-generic-job 템플릿을 envsubst 로 렌더링(스크립트는 base64 단일 줄) → apply →
# 드라이버 파드 로그 스트리밍 → 완료 판정 → 정리. batch_ingest / anchor-export /
# simulator 등 비표준 인자 잡에 쓴다. aws-creds Secret 은 있어야 한다(K2/K3 에서 생성).
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage: echo <script> | $0 <job-name>" >&2
  exit 1
fi

RAW_NAME="$1"
NS="binance-lakehouse"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export JOB_NAME="k4-$(echo "$RAW_NAME" | tr '[:upper:]_' '[:lower:]-' | tr -cd 'a-z0-9-')"

SCRIPT="$(cat)"
export SCRIPT_B64="$(printf '%s' "$SCRIPT" | base64 | tr -d '\n')"

kubectl -n "$NS" delete job "$JOB_NAME" --ignore-not-found >/dev/null 2>&1 || true
echo "==> Job apply: $JOB_NAME"
envsubst '${JOB_NAME} ${SCRIPT_B64}' \
  < "$ROOT/infra/k8s/45-spark-generic-job.template.yaml" | kubectl apply -f -

echo "==> 드라이버 파드 대기..."
for _ in $(seq 1 60); do
  POD="$(kubectl -n "$NS" get pods -l "job-name=$JOB_NAME" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  [ -n "$POD" ] && break
  sleep 2
done
[ -z "${POD:-}" ] && { echo "ERROR: 파드가 뜨지 않음" >&2; exit 1; }
kubectl -n "$NS" wait --for=condition=Ready "pod/$POD" --timeout=120s || true
kubectl -n "$NS" logs -f "$POD" || true

if kubectl -n "$NS" wait --for=condition=complete "job/$JOB_NAME" --timeout=1800s 2>/dev/null; then
  echo "==> [OK] $JOB_NAME"
  exit 0
else
  echo "==> [FAIL] $JOB_NAME" >&2
  kubectl -n "$NS" get job "$JOB_NAME" -o wide >&2 || true
  exit 1
fi
