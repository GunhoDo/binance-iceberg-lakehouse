#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage:"
  echo "  $0 <sql_file_path>"
  echo "  $0 -e \"<sql_query>\""
  echo ""
  echo "Examples:"
  echo "  $0 src/ddl/07_create_serving_tables.sql"
  echo "  $0 src/pipelines/03_merge_kline_updates.sql"
  echo "  $0 -e \"SHOW TABLES IN glue.binance_lakehouse;\""
  exit 1
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.iceberg:iceberg-aws-bundle:1.7.0,org.apache.hadoop:hadoop-aws:3.3.4}"

GLUE_CATALOG_NAME="${GLUE_CATALOG_NAME:-glue}"
GLUE_WAREHOUSE="${GLUE_WAREHOUSE:-s3://binance-iceberg-lake/warehouse}"

SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-3g}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-8}"

export PYTHONPATH="${PYTHONPATH:-.}"

if [ -x "$PROJECT_ROOT/.venv/bin/python" ]; then
  export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
  export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
fi

COMMON_ARGS=(
  --packages "$SPARK_PACKAGES"
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  --conf "spark.sql.catalog.${GLUE_CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog"
  --conf "spark.sql.catalog.${GLUE_CATALOG_NAME}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
  --conf "spark.sql.catalog.${GLUE_CATALOG_NAME}.warehouse=${GLUE_WAREHOUSE}"
  --conf "spark.sql.catalog.${GLUE_CATALOG_NAME}.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
  --conf "spark.sql.parquet.enableVectorizedReader=false"
  --conf "spark.sql.iceberg.vectorization.enabled=false"
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS}"
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}"
)

echo "==> Running Spark SQL"
echo "    warehouse: $GLUE_WAREHOUSE"
echo "    catalog: $GLUE_CATALOG_NAME"

if [ "$1" = "-e" ]; then
  if [ "$#" -lt 2 ]; then
    echo "ERROR: missing SQL query after -e"
    exit 1
  fi

  spark-sql "${COMMON_ARGS[@]}" -e "$2"
else
  SQL_FILE="$1"

  if [ ! -f "$SQL_FILE" ]; then
    echo "ERROR: sql file not found: $SQL_FILE"
    exit 1
  fi

  echo "    sql file: $SQL_FILE"
  spark-sql "${COMMON_ARGS[@]}" -f "$SQL_FILE"
fi
