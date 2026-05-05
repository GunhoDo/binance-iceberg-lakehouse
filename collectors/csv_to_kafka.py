"""csv_to_kafka.py

다운로드한 Binance CSV 파일을 Kafka topic으로 produce하는 로컬 개발용 스크립트.

PRD §6.1, §6.2 의 CSV 컬럼 구조를 기반으로 한다.

사용법:
    # 단일 파일
    python collectors/csv_to_kafka.py --topic trades --file data/raw/BTCUSDT-trades-2024-01.csv

    # 2024년 전체 (1~3월 자동 순회)
    python collectors/csv_to_kafka.py --topic trades --symbol BTCUSDT --all-months
    python collectors/csv_to_kafka.py --topic klines --symbol BTCUSDT --interval 1m --all-months

의존성:
    pip install kafka-python
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
DATA_DIR = Path("data/raw")
YEAR = "2024"

# Binance CSV는 헤더가 없다. 컬럼 순서를 직접 정의한다.
# 출처: data.binance.vision 공식 포맷 (PRD §6.1, §6.2)
TRADES_COLUMNS = [
    "trade_id",
    "price",
    "qty",
    "quote_qty",
    "time",
    "is_buyer_maker",
    "is_best_match",
]

KLINES_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "number_of_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]

MONTHS = [f"{m:02d}" for m in range(1, 4)]  # ["01", "02", "3"]


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def produce_trades(file_path: str, symbol: str, delay: float = 0.0) -> None:
    """trades CSV 한 파일 → Kafka topic `trades` produce."""
    producer = make_producer()
    count = 0

    with open(file_path, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) != len(TRADES_COLUMNS):
                continue

            record = dict(zip(TRADES_COLUMNS, row))
            record["symbol"] = symbol

            producer.send(topic="trades", key=symbol, value=record)
            count += 1

            if delay > 0:
                time.sleep(delay)

            if count % 10_000 == 0:
                print(f"[trades] produced {count} records")

    producer.flush()
    print(f"[trades] done. total {count} records from {file_path}")


def produce_klines(file_path: str, symbol: str, interval: str, delay: float = 0.0) -> None:
    """klines CSV 한 파일 → Kafka topic `klines` produce."""
    producer = make_producer()
    count = 0

    with open(file_path, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) != len(KLINES_COLUMNS):
                continue

            record = dict(zip(KLINES_COLUMNS, row))
            record["symbol"] = symbol
            record["interval"] = interval

            producer.send(topic="klines", key=f"{symbol}_{interval}", value=record)
            count += 1

            if delay > 0:
                time.sleep(delay)

            if count % 1_000 == 0:
                print(f"[klines] produced {count} records")

    producer.flush()
    print(f"[klines] done. total {count} records from {file_path}")


def produce_trades_all_months(symbol: str, delay: float = 0.0) -> None:
    """2024년 1~3월 trades CSV를 순서대로 produce한다."""
    for month in MONTHS:
        file_path = DATA_DIR / f"{symbol}-trades-{YEAR}-{month}.csv"
        if not file_path.exists():
            print(f"[trades] skip (not found): {file_path}")
            continue
        print(f"[trades] start: {file_path}")
        produce_trades(str(file_path), symbol, delay)


def produce_klines_all_months(symbol: str, interval: str, delay: float = 0.0) -> None:
    """2024년 1~3월 klines CSV를 순서대로 produce한다."""
    for month in MONTHS:
        file_path = DATA_DIR / f"{symbol}-{interval}-{YEAR}-{month}.csv"
        if not file_path.exists():
            print(f"[klines] skip (not found): {file_path}")
            continue
        print(f"[klines] start: {file_path}")
        produce_klines(str(file_path), symbol, interval, delay)


def main() -> None:
    parser = argparse.ArgumentParser(description="Binance CSV → Kafka producer")
    parser.add_argument("--topic", required=True, choices=["trades", "klines"])
    parser.add_argument("--file", help="단일 CSV 파일 경로 (--all-months와 택일)")
    parser.add_argument("--all-months", action="store_true", help="2024년 1~3월 전체 순회")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1m", help="klines 전용")
    parser.add_argument("--delay", type=float, default=0.0, help="메시지 간 대기(초)")
    args = parser.parse_args()

    if not args.file and not args.all_months:
        parser.error("--file 또는 --all-months 중 하나를 지정해야 합니다.")

    if args.topic == "trades":
        if args.all_months:
            produce_trades_all_months(args.symbol, args.delay)
        else:
            produce_trades(args.file, args.symbol, args.delay)

    elif args.topic == "klines":
        if args.all_months:
            produce_klines_all_months(args.symbol, args.interval, args.delay)
        else:
            produce_klines(args.file, args.symbol, args.interval, args.delay)


if __name__ == "__main__":
    main()

