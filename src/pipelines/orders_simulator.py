"""orders_simulator.py

User-level private order data를 대체하는 합성 주문 이벤트 생성기.

이 시뮬레이터의 출력은 실제 Binance 주문이 아니다. (PRD §2, §6.3).
Iceberg MERGE 기반 주문 상태 관리 실험을 위해 synthetic order events를 생성한다.

도메인 가정:
- 주문 도착 간격: 고정 sleep 또는 exponential sampling
- 주문 방향: BUY / SELL
- 주문 가격: reference close 주변에서 샘플링
- 주문 수량: fixed range 기반 샘플링
- 주문 상태 전이:
    NEW → PARTIALLY_FILLED → FILLED
    NEW → CANCELED
    NEW → FILLED

실행:
    python simulators/order_simulator.py \
      --bootstrap localhost:9092 \
      --topic orders \
      --num-orders 1000 \
      --reference-close 43000
"""
from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer


# -----------------------------------------------------------------------------
# 기본 config
# -----------------------------------------------------------------------------

DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_CLIENT_COUNT = 50

PARTIAL_FILL_RATE = 0.45
CANCEL_RATE = 0.25

MIN_QTY = 0.001
MAX_QTY = 0.08

PRICE_DEVIATION_RATE = 0.003
# reference_close 기준 ±0.3% 범위에서 주문 가격 샘플링


def now_ms() -> int:
    """현재 UTC epoch milliseconds."""
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def decimal_str(value: float, scale: int = 8) -> str:
    """Binance 스타일 decimal string."""
    return f"{value:.{scale}f}"


# -----------------------------------------------------------------------------
# 도메인 가정
# -----------------------------------------------------------------------------

def sample_arrival_interval(max_sleep_ms: int = 10) -> float:
    """다음 주문이 도착할 때까지의 시간 간격을 샘플링한다.

    Phase 2 MVP에서는 빠른 테스트를 위해 0~max_sleep_ms 사이의 짧은 간격을 사용한다.
    향후 config 기반 Poisson/exponential 분포로 확장한다.

    Returns:
        sleep seconds
    """
    if max_sleep_ms <= 0:
        return 0.0
    return random.uniform(0, max_sleep_ms) / 1000


def sample_side() -> str:
    """주문 방향을 샘플링한다."""
    return random.choice(["BUY", "SELL"])


def sample_order_price(reference_close: float) -> float:
    """최근 kline close 주변에서 주문 가격을 샘플링한다."""
    low = reference_close * (1 - PRICE_DEVIATION_RATE)
    high = reference_close * (1 + PRICE_DEVIATION_RATE)
    return random.uniform(low, high)


def sample_order_qty() -> float:
    """주문 수량을 샘플링한다."""
    return random.uniform(MIN_QTY, MAX_QTY)


def make_base_order(order_index: int, reference_close: float, symbol: str) -> dict[str, Any]:
    """단일 주문의 기본 정보를 생성한다."""
    order_price = sample_order_price(reference_close)
    order_qty = sample_order_qty()

    return {
        "order_id": f"O{order_index:08d}",
        "client_id": f"C{random.randint(1, DEFAULT_CLIENT_COUNT):04d}",
        "symbol": symbol,
        "side": sample_side(),
        "order_type": "LIMIT",
        "order_price": decimal_str(order_price),
        "order_qty": decimal_str(order_qty),
    }


# -----------------------------------------------------------------------------
# 상태 전이
# -----------------------------------------------------------------------------

def create_order_event(
    order: dict[str, Any],
    event_type: str,
    order_status: str,
    filled_qty: float,
    avg_fill_price: float,
    event_time: int,
    simulated_parameters: dict[str, Any],
) -> dict[str, Any]:
    """raw_orders에 저장될 주문 이벤트 dict를 생성한다."""
    return {
        "order_id": order["order_id"],
        "client_id": order["client_id"],
        "symbol": order["symbol"],
        "side": order["side"],
        "order_type": order["order_type"],
        "order_price": order["order_price"],
        "order_qty": order["order_qty"],
        "filled_qty": decimal_str(filled_qty),
        "avg_fill_price": decimal_str(avg_fill_price),
        "event_type": event_type,
        "order_status": order_status,
        "event_time": str(event_time),
        "simulated_parameters": simulated_parameters,
    }


def decide_lifecycle(order: dict[str, Any], market_context: dict[str, Any]) -> list[dict[str, Any]]:
    """단일 주문의 생애 주기를 event sequence로 풀어낸다.

    상태 전이:
    - NEW → PARTIALLY_FILLED → FILLED
    - NEW → PARTIALLY_FILLED → CANCELED
    - NEW → FILLED
    - NEW → CANCELED

    Returns:
        raw_orders message_value로 publish할 event dict list
    """
    order_qty = float(order["order_qty"])
    order_price = float(order["order_price"])

    simulated_parameters = {
        "partial_fill_rate": PARTIAL_FILL_RATE,
        "cancel_rate": CANCEL_RATE,
        "price_deviation_rate": PRICE_DEVIATION_RATE,
        "reference_close": market_context["reference_close"],
        "qty_range": [MIN_QTY, MAX_QTY],
        "note": "synthetic order event, not real Binance private data",
    }

    events: list[dict[str, Any]] = []

    base_time = now_ms()

    # 1. NEW
    events.append(
        create_order_event(
            order=order,
            event_type="ORDER_NEW",
            order_status="NEW",
            filled_qty=0.0,
            avg_fill_price=0.0,
            event_time=base_time,
            simulated_parameters=simulated_parameters,
        )
    )

    has_partial = random.random() < PARTIAL_FILL_RATE
    is_canceled = random.random() < CANCEL_RATE

    # 2. Optional PARTIALLY_FILLED
    partial_filled_qty = 0.0

    if has_partial:
        partial_filled_qty = order_qty * random.uniform(0.2, 0.8)

        events.append(
            create_order_event(
                order=order,
                event_type="ORDER_PARTIALLY_FILLED",
                order_status="PARTIALLY_FILLED",
                filled_qty=partial_filled_qty,
                avg_fill_price=order_price,
                event_time=base_time + random.randint(100, 3000),
                simulated_parameters=simulated_parameters,
            )
        )

    # 3. Final state
    final_time = base_time + random.randint(3000, 15000)

    if is_canceled:
        events.append(
            create_order_event(
                order=order,
                event_type="ORDER_CANCELED",
                order_status="CANCELED",
                filled_qty=partial_filled_qty,
                avg_fill_price=order_price if partial_filled_qty > 0 else 0.0,
                event_time=final_time,
                simulated_parameters=simulated_parameters,
            )
        )
    else:
        events.append(
            create_order_event(
                order=order,
                event_type="ORDER_FILLED",
                order_status="FILLED",
                filled_qty=order_qty,
                avg_fill_price=order_price,
                event_time=final_time,
                simulated_parameters=simulated_parameters,
            )
        )

    return events


# -----------------------------------------------------------------------------
# Kafka publish
# -----------------------------------------------------------------------------

def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )


def publish_to_kafka(
    producer: KafkaProducer,
    events: list[dict[str, Any]],
    topic: str = "orders",
) -> int:
    """생성한 주문 이벤트를 Kafka topic `orders`에 publish한다."""
    sent_count = 0

    for event in events:
        producer.send(
            topic,
            key=event["order_id"],
            value=event,
        )
        sent_count += 1

    return sent_count


def run() -> None:
    parser = argparse.ArgumentParser(description="Synthetic orders simulator")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--num-orders", type=int, default=1000)
    parser.add_argument("--reference-close", type=float, default=43000.0)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--max-sleep-ms", type=int, default=10)
    args = parser.parse_args()

    producer = build_producer(args.bootstrap)

    market_context = {
        "reference_close": args.reference_close,
    }

    total_events = 0

    try:
        for order_index in range(1, args.num_orders + 1):
            order = make_base_order(
                order_index=order_index,
                reference_close=args.reference_close,
                symbol=args.symbol,
            )

            events = decide_lifecycle(order, market_context)
            total_events += publish_to_kafka(producer, events, args.topic)

            sleep_seconds = sample_arrival_interval(args.max_sleep_ms)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        producer.flush()

    finally:
        producer.close()

    print(
        f"orders simulator 완료: "
        f"{args.num_orders} orders, {total_events} events sent to topic={args.topic}"
    )


if __name__ == "__main__":
    run()