#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage:"
  echo "  $0 <sql_file_path>"
  echo "  $0 -e \"<sql_query>\""
  echo ""
  echo "Examples:"
  echo "  $0 src/ddl/07_create_serving_tables.sql"
  echo "  $0 -e \"SHOW TABLES IN glue.binance_lakehouse;\""
  exit 1
fi

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

if [ -f "$PROJECT_ROOT/.env" ]; then
  set -a
  source "$PROJECT_ROOT/.env"
  set +a
fi

DERBY_HOME="/tmp/derby-sql-$(date +%s)-$$"
mkdir -p "$DERBY_HOME"

GLUE_CATALOG_NAME="${GLUE_CATALOG_NAME:-glue}"
GLUE_WAREHOUSE="${GLUE_WAREHOUSE:-s3://binance-iceberg-lake/warehouse}"

SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-2}"

export PYTHONPATH="${PYTHONPATH:-.}"

if [ -x "$PROJECT_ROOT/.venv/bin/python" ]; then
  export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
  export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
fi

COMMON_ARGS=(
  --conf "spark.driver.extraJavaOptions=-Dderby.system.home=${DERBY_HOME}"
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  --conf "spark.sql.catalog.${GLUE_CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog"
  --conf "spark.sql.catalog.${GLUE_CATALOG_NAME}.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog"
  --conf "spark.sql.catalog.${GLUE_CATALOG_NAME}.warehouse=${GLUE_WAREHOUSE}"
  --conf "spark.sql.catalog.${GLUE_CATALOG_NAME}.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider"
  --conf "spark.sql.parquet.enableVectorizedReader=false"
  --conf "spark.sql.iceberg.vectorization.enabled=false"
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS}"
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}"
)

echo "==> Running Spark SQL"
echo "    project  : $PROJECT_ROOT"
echo "    warehouse: $GLUE_WAREHOUSE"
echo "    catalog  : $GLUE_CATALOG_NAME"
echo "    derby    : $DERBY_HOME"

if [ "$1" = "-e" ]; then
  if [ "$#" -lt 2 ]; then
    echo "ERROR: missing SQL query after -e"
    exit 1
  fi

  SQL_QUERY="$2"
  spark-sql "${COMMON_ARGS[@]}" -e "$SQL_QUERY"
else
  SQL_FILE="$1"

  if [ ! -f "$SQL_FILE" ]; then
    echo "ERROR: sql file not found: $SQL_FILE"
    echo "PROJECT_ROOT=$PROJECT_ROOT"
    echo "PWD=$(pwd)"
    exit 1
  fi

  echo "    sql file: $SQL_FILE"
  spark-sql "${COMMON_ARGS[@]}" -f "$SQL_FILE"
fi
