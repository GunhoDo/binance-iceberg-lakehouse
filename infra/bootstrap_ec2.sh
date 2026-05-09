#!/usr/bin/env bash
set -euo pipefail

echo "==> Bootstrap EC2 environment for binance-iceberg-lakehouse"

# -----------------------------------------------------------------------------
# 0. Project root
# -----------------------------------------------------------------------------
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==> Project root: $PROJECT_ROOT"

# -----------------------------------------------------------------------------
# 1. System packages
# -----------------------------------------------------------------------------
echo "==> Updating apt packages"

sudo apt-get update -y

echo "==> Installing system dependencies"

sudo apt-get install -y \
  openjdk-17-jdk \
  unzip \
  curl \
  python3 \
  python3-venv \
  python3-pip

# -----------------------------------------------------------------------------
# 2. AWS CLI v2
# -----------------------------------------------------------------------------
echo "==> Installing or updating AWS CLI v2"

cd ..

rm -rf aws awscliv2.zip

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -o awscliv2.zip

if command -v aws >/dev/null 2>&1; then
  echo "==> AWS CLI already installed. Updating..."
  sudo ./aws/install --update
else
  echo "==> AWS CLI not found. Installing..."
  sudo ./aws/install
fi

aws --version

cd "$PROJECT_ROOT"
# -----------------------------------------------------------------------------
# 3. Java environment
# -----------------------------------------------------------------------------
echo "==> Configuring JAVA_HOME"

JAVA_HOME_PATH="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"

export JAVA_HOME="$JAVA_HOME_PATH"
export PATH="$JAVA_HOME/bin:$PATH"

echo "JAVA_HOME=$JAVA_HOME"

java -version

# -----------------------------------------------------------------------------
# 4. Python virtual environment
# -----------------------------------------------------------------------------
echo "==> Creating Python virtual environment"

if [ ! -d "$PROJECT_ROOT/.venv" ]; then
  python3 -m venv "$PROJECT_ROOT/.venv"
fi

source "$PROJECT_ROOT/.venv/bin/activate"

echo "==> Upgrading pip"

python -m pip install --upgrade pip setuptools wheel

# -----------------------------------------------------------------------------
# 5. Python dependencies
# -----------------------------------------------------------------------------
echo "==> Installing Python dependencies"

if [ ! -f "$PROJECT_ROOT/requirements.txt" ]; then
  echo "ERROR: requirements.txt not found in $PROJECT_ROOT"
  exit 1
fi

pip install -r "$PROJECT_ROOT/requirements.txt"

# -----------------------------------------------------------------------------
# 6. Spark / PySpark environment variables
# -----------------------------------------------------------------------------
echo "==> Configuring Spark / PySpark environment"

PYSPARK_PYTHON_PATH="$PROJECT_ROOT/.venv/bin/python"
PYSPARK_DRIVER_PYTHON_PATH="$PROJECT_ROOT/.venv/bin/python"

if [ ! -x "$PYSPARK_PYTHON_PATH" ]; then
  echo "ERROR: Python executable not found: $PYSPARK_PYTHON_PATH"
  exit 1
fi

SPARK_HOME_PATH="$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"

if [ ! -d "$SPARK_HOME_PATH" ]; then
  echo "ERROR: SPARK_HOME path not found: $SPARK_HOME_PATH"
  exit 1
fi

export PYSPARK_PYTHON="$PYSPARK_PYTHON_PATH"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_DRIVER_PYTHON_PATH"
export SPARK_HOME="$SPARK_HOME_PATH"
export PATH="$SPARK_HOME/bin:$PATH"

echo "PYSPARK_PYTHON=$PYSPARK_PYTHON"
echo "PYSPARK_DRIVER_PYTHON=$PYSPARK_DRIVER_PYTHON"
echo "SPARK_HOME=$SPARK_HOME"

echo "==> Checking Spark commands"

if ! command -v spark-submit >/dev/null 2>&1; then
  echo "ERROR: spark-submit not found"
  exit 1
fi

if ! command -v spark-sql >/dev/null 2>&1; then
  echo "ERROR: spark-sql not found"
  exit 1
fi

spark-submit --version

# -----------------------------------------------------------------------------
# 7. Persist environment variables to ~/.bashrc
# -----------------------------------------------------------------------------
echo "==> Persisting environment variables to ~/.bashrc"

BASHRC_MARKER_BEGIN="# >>> binance-iceberg-lakehouse >>>"
BASHRC_MARKER_END="# <<< binance-iceberg-lakehouse <<<"

# 기존 블록 제거 후 새로 추가
if grep -q "$BASHRC_MARKER_BEGIN" ~/.bashrc; then
  sed -i "/$BASHRC_MARKER_BEGIN/,/$BASHRC_MARKER_END/d" ~/.bashrc
fi

{
  echo ""
  echo "$BASHRC_MARKER_BEGIN"
  echo "export JAVA_HOME=$JAVA_HOME_PATH"
  echo 'export PATH=$JAVA_HOME/bin:$PATH'
  echo "export PYSPARK_PYTHON=$PYSPARK_PYTHON_PATH"
  echo "export PYSPARK_DRIVER_PYTHON=$PYSPARK_DRIVER_PYTHON_PATH"
  echo "export SPARK_HOME=$SPARK_HOME_PATH"
  echo 'export PATH=$SPARK_HOME/bin:$PATH'
  echo "$BASHRC_MARKER_END"
} >> ~/.bashrc

# -----------------------------------------------------------------------------
# 8. AWS CLI configuration from .env
# -----------------------------------------------------------------------------
echo "==> Configuring AWS CLI from .env"

ENV_FILE="${1:-$PROJECT_ROOT/.env}"

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found."
  echo "Create it from .env.example:"
  echo "  cp .env.example .env"
  exit 1
fi

set -a
source "$ENV_FILE"
set +a

: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"
: "${AWS_DEFAULT_REGION:=ap-northeast-2}"
: "${AWS_DEFAULT_OUTPUT:=json}"
: "${AWS_PROFILE:=default}"

echo "==> Configuring AWS CLI profile: $AWS_PROFILE"

aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID" --profile "$AWS_PROFILE"
aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY" --profile "$AWS_PROFILE"
aws configure set region "$AWS_DEFAULT_REGION" --profile "$AWS_PROFILE"
aws configure set output "$AWS_DEFAULT_OUTPUT" --profile "$AWS_PROFILE"

echo "==> Checking AWS identity"

aws sts get-caller-identity --profile "$AWS_PROFILE"

echo ""
echo "==> Done."
echo ""
echo "Apply environment variables now with:"
echo "  source ~/.bashrc"
echo ""
echo "Use this AWS profile with:"
echo "  export AWS_PROFILE=$AWS_PROFILE"
echo ""
echo "Check Spark with:"
echo "  which spark-sql"
echo "  which spark-submit"
echo "  echo \$PYSPARK_PYTHON"
echo "  echo \$SPARK_HOME"