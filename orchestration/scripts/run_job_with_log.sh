#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 5 ]; then
  echo "Usage:"
  echo "  $0 <task_name> <job_path> <start_ts> <end_ts> <run_id>"
  exit 1
fi

TASK_NAME="$1"
JOB_PATH="$2"
START_TS="$3"
END_TS="$4"
RUN_ID="$5"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

PIPELINE_NAME="${PIPELINE_NAME:-daily_lakehouse_pipeline}"
STARTED_AT="$(date -u '+%Y-%m-%d %H:%M:%S')"

STATUS="SUCCESS"
ERROR_MESSAGE=""

set +e
./orchestration/scripts/run_job.sh "$JOB_PATH" "$START_TS" "$END_TS" "$RUN_ID"
EXIT_CODE=$?
set -e

ENDED_AT="$(date -u '+%Y-%m-%d %H:%M:%S')"

if [ "$EXIT_CODE" -ne 0 ]; then
  STATUS="FAILED"
  ERROR_MESSAGE="job failed with exit code ${EXIT_CODE}"
fi

# single quote escape
ERROR_MESSAGE_ESCAPED="$(printf "%s" "$ERROR_MESSAGE" | sed "s/'/''/g")"

./orchestration/scripts/run_spark_sql.sh -e "
INSERT INTO glue.binance_lakehouse.pipeline_run_summary
SELECT
  '${RUN_ID}' AS run_id,
  '${PIPELINE_NAME}' AS pipeline_name,
  '${TASK_NAME}' AS task_name,
  '${STATUS}' AS status,
  TIMESTAMP '${STARTED_AT}' AS started_at,
  TIMESTAMP '${ENDED_AT}' AS ended_at,
  CAST(unix_timestamp(TIMESTAMP '${ENDED_AT}') - unix_timestamp(TIMESTAMP '${STARTED_AT}') AS DOUBLE) AS duration_sec,
  NULL AS source_table,
  NULL AS target_table,
  NULL AS processed_rows,
  '${ERROR_MESSAGE_ESCAPED}' AS error_message,
  current_timestamp() AS created_at
"

exit "$EXIT_CODE"
