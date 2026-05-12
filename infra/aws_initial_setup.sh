#!/usr/bin/env bash
set -euo pipefail

# AWS initial setup for the lakehouse MVP.
#
# Creates:
# - S3 bucket and standard prefixes
# - Glue database
# - Athena workgroup with per-query scan limit
#
# Required IAM permissions include:
# - s3:CreateBucket, s3:PutBucketVersioning, s3:PutBucketEncryption, s3:PutPublicAccessBlock, s3:PutObject
# - glue:CreateDatabase, glue:GetDatabase
# - athena:CreateWorkGroup, athena:GetWorkGroup, athena:UpdateWorkGroup

AWS_PROFILE="${AWS_PROFILE:-default}"
AWS_REGION="${AWS_REGION:-ap-northeast-2}"

LAKEHOUSE_BUCKET="${LAKEHOUSE_BUCKET:-binance-iceberg-lake}"
GLUE_DATABASE="${GLUE_DATABASE:-binance_lakehouse}"

ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-binance_lakehouse_guarded}"
ATHENA_RESULT_PREFIX="${ATHENA_RESULT_PREFIX:-athena-results/}"

# Default: 10 GiB per query. Override for stricter/looser environments.
ATHENA_BYTES_SCANNED_CUTOFF="${ATHENA_BYTES_SCANNED_CUTOFF:-10737418240}"

S3_URI="s3://${LAKEHOUSE_BUCKET}"
ATHENA_OUTPUT_LOCATION="${ATHENA_OUTPUT_LOCATION:-${S3_URI}/${ATHENA_RESULT_PREFIX}}"

aws_cli() {
  aws --profile "${AWS_PROFILE}" --region "${AWS_REGION}" "$@"
}

bucket_exists() {
  aws_cli s3api head-bucket --bucket "${LAKEHOUSE_BUCKET}" >/dev/null 2>&1
}

create_bucket() {
  if bucket_exists; then
    echo "[aws-init] bucket exists: ${LAKEHOUSE_BUCKET}"
    return
  fi

  echo "[aws-init] creating bucket: ${LAKEHOUSE_BUCKET}"
  if [[ "${AWS_REGION}" == "us-east-1" ]]; then
    aws_cli s3api create-bucket --bucket "${LAKEHOUSE_BUCKET}"
  else
    aws_cli s3api create-bucket \
      --bucket "${LAKEHOUSE_BUCKET}" \
      --create-bucket-configuration "LocationConstraint=${AWS_REGION}"
  fi
}

configure_bucket() {
  echo "[aws-init] configuring bucket guardrails"

  aws_cli s3api put-public-access-block \
    --bucket "${LAKEHOUSE_BUCKET}" \
    --public-access-block-configuration \
      BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

  aws_cli s3api put-bucket-versioning \
    --bucket "${LAKEHOUSE_BUCKET}" \
    --versioning-configuration Status=Enabled

  aws_cli s3api put-bucket-encryption \
    --bucket "${LAKEHOUSE_BUCKET}" \
    --server-side-encryption-configuration \
      '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
}

create_prefixes() {
  echo "[aws-init] creating standard prefixes"

  for prefix in \
    raw/trades/ \
    raw/klines/ \
    raw/orders/ \
    warehouse/ \
    checkpoints/raw_trades/ \
    checkpoints/raw_klines/ \
    checkpoints/raw_orders/ \
    "${ATHENA_RESULT_PREFIX}"; do
    aws_cli s3api put-object --bucket "${LAKEHOUSE_BUCKET}" --key "${prefix}" >/dev/null
  done
}

create_glue_database() {
  if aws_cli glue get-database --name "${GLUE_DATABASE}" >/dev/null 2>&1; then
    echo "[aws-init] glue database exists: ${GLUE_DATABASE}"
    return
  fi

  echo "[aws-init] creating glue database: ${GLUE_DATABASE}"
  aws_cli glue create-database \
    --database-input "Name=${GLUE_DATABASE},Description=Binance Iceberg Lakehouse MVP"
}

create_or_update_athena_workgroup() {
  local create_configuration
  local update_configuration

  create_configuration="$(
    cat <<JSON
{
  "ResultConfiguration": {
    "OutputLocation": "${ATHENA_OUTPUT_LOCATION}"
  },
  "EnforceWorkGroupConfiguration": true,
  "PublishCloudWatchMetricsEnabled": true,
  "BytesScannedCutoffPerQuery": ${ATHENA_BYTES_SCANNED_CUTOFF}
}
JSON
  )"

  update_configuration="$(
    cat <<JSON
{
  "ResultConfigurationUpdates": {
    "OutputLocation": "${ATHENA_OUTPUT_LOCATION}"
  },
  "EnforceWorkGroupConfiguration": true,
  "PublishCloudWatchMetricsEnabled": true,
  "BytesScannedCutoffPerQuery": ${ATHENA_BYTES_SCANNED_CUTOFF}
}
JSON
  )"

  if aws_cli athena get-work-group --work-group "${ATHENA_WORKGROUP}" >/dev/null 2>&1; then
    echo "[aws-init] updating athena workgroup: ${ATHENA_WORKGROUP}"
    aws_cli athena update-work-group \
      --work-group "${ATHENA_WORKGROUP}" \
      --configuration-updates "${update_configuration}"
    return
  fi

  echo "[aws-init] creating athena workgroup: ${ATHENA_WORKGROUP}"
  aws_cli athena create-work-group \
    --name "${ATHENA_WORKGROUP}" \
    --configuration "${create_configuration}" \
    --description "Guarded workgroup for Binance Iceberg Lakehouse"
}

main() {
  echo "[aws-init] profile=${AWS_PROFILE}, region=${AWS_REGION}"
  echo "[aws-init] bucket=${LAKEHOUSE_BUCKET}"
  echo "[aws-init] glue_database=${GLUE_DATABASE}"
  echo "[aws-init] athena_workgroup=${ATHENA_WORKGROUP}"
  echo "[aws-init] athena_bytes_scanned_cutoff=${ATHENA_BYTES_SCANNED_CUTOFF}"

  create_bucket
  configure_bucket
  create_prefixes
  create_glue_database
  create_or_update_athena_workgroup

  cat <<EOF
[aws-init] complete

Use these settings in jobs/dashboards:
  LAKEHOUSE_BUCKET=${LAKEHOUSE_BUCKET}
  GLUE_DATABASE=${GLUE_DATABASE}
  ATHENA_WORKGROUP=${ATHENA_WORKGROUP}
  ATHENA_OUTPUT_LOCATION=${ATHENA_OUTPUT_LOCATION}
EOF
}

main "$@"
