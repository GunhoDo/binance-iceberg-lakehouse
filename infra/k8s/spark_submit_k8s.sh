#!/usr/bin/env bash
# spark_submit_k8s.sh — Spark 배치 잡 1건을 k3d 위에서 실행 (Phase K2)
#
# 사용:
#   infra/k8s/spark_submit_k8s.sh <job_path> <start_ts> <end_ts> <run_id>
# 예:
#   infra/k8s/spark_submit_k8s.sh src/jobs/daily/01_build_processed_trades_window.py \
#     2024-01-01T00:00:00 2024-02-01T00:00:00 k2-smoke-01
#
# 동작: aws-creds Secret(로컬 aws configure 자격)을 idempotent 생성 → 40-spark-job
# 템플릿을 envsubst 로 채워 Job apply → 완료 대기 → 드라이버 로그 스트리밍 →
# 종료코드 전파 → Job 정리. 드라이버(Job 파드)가 client 모드로 executor 파드 2개를
# 직접 생성한다.
set -euo pipefail

if [ "$#" -lt 4 ]; then
  echo "Usage: $0 <job_path> <start_ts> <end_ts> <run_id>" >&2
  exit 1
fi

export JOB_PATH="$1"
export START_TS="$2"
export END_TS="$3"
export RUN_ID="$4"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
NS="binance-lakehouse"
# Job 이름은 DNS-1123 준수: 소문자/숫자/'-' 만.
export JOB_NAME="spark-$(echo "$RUN_ID" | tr '[:upper:]_' '[:lower:]-' | tr -cd 'a-z0-9-')"

# --- aws-creds Secret (로컬 aws configure 자격에서 생성, idempotent) ---
AWS_KEY="$(aws configure get aws_access_key_id)"
AWS_SECRET="$(aws configure get aws_secret_access_key)"
AWS_REGION="$(aws configure get region || echo ap-northeast-2)"
if [ -z "${AWS_KEY}" ] || [ -z "${AWS_SECRET}" ]; then
  echo "ERROR: 로컬 aws configure 자격을 찾지 못했습니다 (aws configure 필요)" >&2
  exit 1
fi
kubectl -n "$NS" create secret generic aws-creds \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_KEY" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET" \
  --from-literal=AWS_REGION="$AWS_REGION" \
  --from-literal=AWS_DEFAULT_REGION="$AWS_REGION" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "==> aws-creds Secret 준비 (region=$AWS_REGION)"

# --- 이전 동명 Job 정리 후 apply ---
kubectl -n "$NS" delete job "$JOB_NAME" --ignore-not-found >/dev/null 2>&1 || true
echo "==> Job apply: $JOB_NAME  (job=$JOB_PATH  window=$START_TS..$END_TS)"
envsubst '${JOB_NAME} ${JOB_PATH} ${START_TS} ${END_TS} ${RUN_ID}' \
  < "$ROOT/infra/k8s/40-spark-job.template.yaml" | kubectl apply -f -

# --- 드라이버 파드 대기 + 로그 스트리밍 ---
echo "==> 드라이버 파드 대기..."
for _ in $(seq 1 60); do
  POD="$(kubectl -n "$NS" get pods -l "job-name=$JOB_NAME" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  [ -n "$POD" ] && break
  sleep 2
done
if [ -z "${POD:-}" ]; then echo "ERROR: 드라이버 파드가 뜨지 않음" >&2; exit 1; fi
kubectl -n "$NS" wait --for=condition=Ready "pod/$POD" --timeout=120s || true
echo "==> 드라이버 로그 ($POD):"
kubectl -n "$NS" logs -f "$POD" || true

# --- 완료 판정 ---
echo "==> Job 완료 대기..."
if kubectl -n "$NS" wait --for=condition=complete "job/$JOB_NAME" --timeout=1800s 2>/dev/null; then
  echo "==> [OK] $JOB_NAME 완료"
  RC=0
else
  echo "==> [FAIL] $JOB_NAME 실패 — 상태:" >&2
  kubectl -n "$NS" get job "$JOB_NAME" -o wide >&2 || true
  RC=1
fi

exit $RC
