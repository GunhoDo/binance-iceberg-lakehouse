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
    python src/simulators/orders_simulator.py \
      --bootstrap localhost:9092 \
      --topic orders \
      --num-orders 10000 \
      --reference-close 43000

    python src/simulators/orders_simulator.py \
      --bootstrap localhost:9092 \
      --topic orders \
      --num-orders 10000 \
      --reference-close 43000 \
      --symbol BTCUSDT \
        --start-ts 2024-01-01T00:00:00Z \
        --end-ts 2024-02-01T00:00:00Z \
      --order-id-prefix 202401 \
      --seed 42
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
MAX_LIFECYCLE_OFFSET_MS = 15_000
# reference_close 기준 ±0.3% 범위에서 주문 가격 샘플링


def now_ms() -> int:
    """현재 UTC epoch milliseconds."""
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def parse_timestamp_ms(value: str) -> int:
    """Parse an ISO-8601 timestamp to UTC epoch milliseconds."""
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return int(parsed.astimezone(timezone.utc).timestamp() * 1000)


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


def make_base_order(
    order_index: int,
    reference_close: float,
    symbol: str,
    order_id_prefix: str,
) -> dict[str, Any]:
    """단일 주문의 기본 정보를 생성한다."""
    order_price = sample_order_price(reference_close)
    order_qty = sample_order_qty()

    return {
        "order_id": f"O{order_id_prefix}{order_index:08d}",
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


def decide_lifecycle(
    order: dict[str, Any],
    market_context: dict[str, Any],
    base_time_ms: int,
) -> list[dict[str, Any]]:
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
        "simulation_start_ts": market_context.get("simulation_start_ts"),
        "simulation_end_ts": market_context.get("simulation_end_ts"),
        "order_id_prefix": market_context["order_id_prefix"],
        "max_lifecycle_offset_ms": MAX_LIFECYCLE_OFFSET_MS,
        "note": "synthetic order event, not real Binance private data",
    }

    events: list[dict[str, Any]] = []

    base_time = base_time_ms

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
    final_time = base_time + random.randint(3000, MAX_LIFECYCLE_OFFSET_MS)

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


def sample_base_time_ms(start_ms: int | None, end_ms: int | None) -> int:
    """Sample the order NEW event time inside the configured simulation window."""
    if start_ms is None and end_ms is None:
        return now_ms()
    if start_ms is None or end_ms is None:
        raise ValueError("--start-ts and --end-ts must be provided together")

    latest_start_ms = end_ms - MAX_LIFECYCLE_OFFSET_MS
    if latest_start_ms <= start_ms:
        raise ValueError(
            "--end-ts must be more than "
            f"{MAX_LIFECYCLE_OFFSET_MS}ms after --start-ts"
        )

    return random.randrange(start_ms, latest_start_ms)


def run() -> None:
    parser = argparse.ArgumentParser(description="Synthetic orders simulator")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--num-orders", type=int, default=1000)
    parser.add_argument("--reference-close", type=float, default=43000.0)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--max-sleep-ms", type=int, default=10)
    parser.add_argument("--start-ts")
    parser.add_argument("--end-ts")
    parser.add_argument("--order-id-prefix", default="")
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()

    if args.num_orders < 1:
        parser.error("--num-orders must be >= 1")
    if (args.start_ts is None) != (args.end_ts is None):
        parser.error("--start-ts and --end-ts must be provided together")
    if args.seed is not None:
        random.seed(args.seed)

    try:
        start_ms = parse_timestamp_ms(args.start_ts) if args.start_ts else None
        end_ms = parse_timestamp_ms(args.end_ts) if args.end_ts else None
    except ValueError as exc:
        parser.error(f"invalid timestamp: {exc}")

    if start_ms is not None and end_ms is not None:
        latest_start_ms = end_ms - MAX_LIFECYCLE_OFFSET_MS
        if latest_start_ms <= start_ms:
            parser.error(
                "--end-ts must be more than "
                f"{MAX_LIFECYCLE_OFFSET_MS}ms after --start-ts"
            )

    producer = build_producer(args.bootstrap)

    market_context = {
        "reference_close": args.reference_close,
        "simulation_start_ts": args.start_ts,
        "simulation_end_ts": args.end_ts,
        "order_id_prefix": args.order_id_prefix,
    }

    total_events = 0

    try:
        for order_index in range(1, args.num_orders + 1):
            order = make_base_order(
                order_index=order_index,
                reference_close=args.reference_close,
                symbol=args.symbol,
                order_id_prefix=args.order_id_prefix,
            )

            base_time_ms = sample_base_time_ms(start_ms, end_ms)
            events = decide_lifecycle(order, market_context, base_time_ms)
            total_events += publish_to_kafka(producer, events, args.topic)

            sleep_seconds = sample_arrival_interval(args.max_sleep_ms)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        producer.flush()

    finally:
        producer.close()

    print(
        f"orders simulator 완료: "
        f"{args.num_orders} orders, {total_events} events sent to topic={args.topic}, "
        f"order_id_prefix={args.order_id_prefix}"
    )


if __name__ == "__main__":
    run()
