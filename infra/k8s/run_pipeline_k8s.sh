#!/usr/bin/env bash
# run_pipeline_k8s.sh — 일일 파이프라인 01~09 를 k3d 위에서 순차 실행 (Phase K2)
#
# 사용: infra/k8s/run_pipeline_k8s.sh <start_ts> <end_ts> <run_id_prefix>
# 예:   infra/k8s/run_pipeline_k8s.sh 2024-01-01T00:00:00 2024-02-01T00:00:00 k2
#
# 숫자 순서가 daily_lakehouse_pipeline DAG 의존성(01→06, 02→04→06, 03→05→07,
# 06→07, [06,07]→08→09)을 위상적으로 만족한다. 각 잡은 spark_submit_k8s.sh 로
# 별도 Job(드라이버+executor 파드)으로 실행되며, 하나라도 실패하면 중단한다.
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <start_ts> <end_ts> <run_id_prefix>" >&2
  exit 1
fi
START_TS="$1"; END_TS="$2"; PREFIX="$3"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

JOBS=(
  "01_build_processed_trades_window.py"
  "02_build_staging_klines_window.py"
  "03_build_staging_orders_window.py"
  "04_merge_processed_klines_window.py"
  "05_merge_processed_orders_window.py"
  "06_build_market_hourly_summary_window.py"
  "07_build_order_execution_summary_window.py"
  "08_check_data_quality.py"
  "09_check_table_health.py"
)

for j in "${JOBS[@]}"; do
  step="${j%%_*}"                       # 01, 02, ...
  run_id="${PREFIX}-${step}"
  echo ""
  echo "############################################################"
  echo "# [$step] $j  (run-id=$run_id)"
  echo "############################################################"
  "$ROOT/infra/k8s/spark_submit_k8s.sh" \
    "src/jobs/daily/$j" "$START_TS" "$END_TS" "$run_id"
done

echo ""
echo "==> 파이프라인 01~09 전부 완료"
