#!/bin/bash
# download_data.sh
# BTCUSDT 2024년 1~12월 trades + klines 다운로드 및 압축 해제

set -e  # 에러 나면 즉시 중단

mkdir -p data/raw

SYMBOL="BTCUSDT"
YEAR="2024"
BASE_URL="https://data.binance.vision/data/spot/monthly"

for m in 01 02 03; do

    # trades
    TRADES_FILE="${SYMBOL}-trades-${YEAR}-${m}.zip"
    echo "[trades] downloading ${TRADES_FILE}..."
    wget -q -P data/raw/ "${BASE_URL}/trades/${SYMBOL}/${TRADES_FILE}"
    unzip -q data/raw/${TRADES_FILE} -d data/raw/
    rm data/raw/${TRADES_FILE}
    echo "[trades] done: ${TRADES_FILE%.zip}.csv"

    # klines (1m)
    KLINES_FILE="${SYMBOL}-1m-${YEAR}-${m}.zip"
    echo "[klines] downloading ${KLINES_FILE}..."
    wget -q -P data/raw/ "${BASE_URL}/klines/${SYMBOL}/1m/${KLINES_FILE}"
    unzip -q data/raw/${KLINES_FILE} -d data/raw/
    rm data/raw/${KLINES_FILE}
    echo "[klines] done: ${KLINES_FILE%.zip}.csv"

done

echo ""
echo "완료"
du -sh data/raw/
ls data/raw/