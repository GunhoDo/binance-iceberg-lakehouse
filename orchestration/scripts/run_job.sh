#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 4 ]; then
  echo "Usage:"
  echo "  $0 <job_path> <start_ts> <end_ts> <run_id>"
  exit 1
fi

JOB_PATH="$1"
START_TS="$2"
END_TS="$3"
RUN_ID="$4"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

if [ ! -f "$JOB_PATH" ]; then
  echo "ERROR: job file not found: $JOB_PATH"
  echo "PROJECT_ROOT=$PROJECT_ROOT"
  echo "PWD=$(pwd)"
  exit 1
fi

SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.iceberg:iceberg-aws-bundle:1.7.0,org.apache.hadoop:hadoop-aws:3.3.4}"

export PYTHONPATH="${PYTHONPATH:-.}"

if [ -x "$PROJECT_ROOT/.venv/bin/python" ]; then
  export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
  export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
fi

echo "==> Run Phase3 Spark job"
echo "project  : $PROJECT_ROOT"
echo "job      : $JOB_PATH"
echo "start_ts : $START_TS"
echo "end_ts   : $END_TS"
echo "run_id   : $RUN_ID"

spark-submit \
  --packages "$SPARK_PACKAGES" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog" \
  --conf "spark.sql.catalog.glue.warehouse=s3://binance-iceberg-lake/warehouse" \
  --conf "spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  --conf "spark.sql.parquet.enableVectorizedReader=false" \
  --conf "spark.sql.iceberg.vectorization.enabled=false" \
  --conf "spark.sql.shuffle.partitions=8" \
  --conf "spark.driver.memory=3g" \
  "$JOB_PATH" \
  --start-ts "$START_TS" \
  --end-ts "$END_TS" \
  --run-id "$RUN_ID"
