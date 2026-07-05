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

앵커 모드 (Phase A) — 실시장 분봉에 주문 가격을 앵커링:
    python src/simulators/orders_simulator.py \
      --bootstrap localhost:9092 \
      --topic orders \
      --num-orders 10000 \
      --symbol BTCUSDT \
      --anchor-klines fixtures/anchor_btcusdt_2024-01.csv \
      --order-id-prefix 202401 \
      --seed 42
  앵커 fixture 는 src/simulators/export_anchor_klines.py 로 생성한다.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
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
# reference_close(또는 앵커 분 close) 기준 ±0.3% 범위에서 주문 가격 샘플링

MINUTE_MS = 60_000

ANCHOR_MODE_KLINE = "kline_anchored"
ANCHOR_MODE_FIXED = "fixed_reference"


# -----------------------------------------------------------------------------
# 실시장 앵커 (Phase A)
# -----------------------------------------------------------------------------
#
# 순환 방지 불변식 (docs/gold_serving_improvement_plan.md §4.1.1):
#   앵커 기준점(주문 가격이 붙는 기준) = 주문 시각이 속한 "분(minute)의 close" = 의사결정 시점 가격.
#   벤치마크(슬리피지 측정 기준) = 다운스트림에서 시간당 집계하는 interval VWAP.
#   둘은 반드시 다른 시계열이어야 하며, 그래야 슬리피지 분포가 0에 퇴화하지 않는다.


@dataclass(frozen=True)
class AnchorBucket:
    """앵커 CSV 한 행 = 1분봉. open_time_ms 는 그 분의 시작 epoch ms."""

    open_time_ms: int
    close: float
    volume: float


@dataclass(frozen=True)
class AnchorFrame:
    """앵커 klines fixture 전체. provenance(경로/sha256) 를 함께 보관한다."""

    path: str
    sha256: str
    buckets: tuple[AnchorBucket, ...]

    @property
    def volumes(self) -> list[float]:
        return [b.volume for b in self.buckets]

    @property
    def total_volume(self) -> float:
        return sum(b.volume for b in self.buckets)


def load_anchor_frame(
    path: str,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> AnchorFrame:
    """앵커 klines CSV(header: open_time_ms,close,volume)를 읽어 AnchorFrame 으로.

    - sha256 은 파일 원문 바이트 기준으로 계산해 provenance 로 기록한다(재현성 감사용).
    - start_ms/end_ms 가 주어지면 [start_ms, end_ms) 창으로 버킷을 필터링한다.
    - volume<=0 또는 close<=0 인 행은 앵커·가중치로 쓸 수 없어 제외한다.
    """
    raw = Path(path).read_bytes()
    sha256 = hashlib.sha256(raw).hexdigest()

    buckets: list[AnchorBucket] = []
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))
    required = {"open_time_ms", "close", "volume"}
    if reader.fieldnames is None or not required.issubset(reader.fieldnames):
        raise ValueError(
            f"anchor frame {path} must have header columns {sorted(required)}, "
            f"got {reader.fieldnames}"
        )

    for row in reader:
        open_time_ms = int(row["open_time_ms"])
        close = float(row["close"])
        volume = float(row["volume"])

        if start_ms is not None and open_time_ms < start_ms:
            continue
        if end_ms is not None and open_time_ms >= end_ms:
            continue
        if close <= 0 or volume <= 0:
            continue

        buckets.append(AnchorBucket(open_time_ms, close, volume))

    if not buckets:
        raise ValueError(
            f"anchor frame {path} has no usable buckets in window "
            f"[{start_ms}, {end_ms}) (need close>0 and volume>0)"
        )

    buckets.sort(key=lambda b: b.open_time_ms)
    return AnchorFrame(path=path, sha256=sha256, buckets=tuple(buckets))


def sample_anchor_bucket(frame: AnchorFrame) -> AnchorBucket:
    """거래량 가중으로 분봉 하나를 뽑는다.

    거래가 몰린 분일수록 주문이 더 많이 도착하도록 volume 을 가중치로 쓴다.
    global random 상태를 사용하므로 seed 고정 시 재현된다.
    """
    return random.choices(frame.buckets, weights=frame.volumes, k=1)[0]


def sample_anchored_base_time_ms(bucket: AnchorBucket) -> int:
    """앵커 분 내부 [open_time_ms, open_time_ms+60s) 에서 NEW 이벤트 시각을 샘플링."""
    return bucket.open_time_ms + random.randrange(0, MINUTE_MS)


def effective_rates(
    bucket: AnchorBucket,
    frame: AnchorFrame,
    vol_linked: bool,
) -> tuple[float, float]:
    """(partial_fill_rate, cancel_rate) 를 돌려준다.

    vol_linked=False 면 상수 그대로. True 면 그 분의 상대 거래량에 따라 조정한다:
    거래량이 평균보다 많은 분일수록 체결이 잘 되고(부분체결↑) 취소가 줄도록(취소↓)
    유동성-체결 상관을 흉내낸다. 값은 [0,1] 로 클램프한다. (선택적, 기본 off)
    """
    if not vol_linked:
        return PARTIAL_FILL_RATE, CANCEL_RATE

    mean_volume = frame.total_volume / len(frame.buckets)
    ratio = bucket.volume / mean_volume if mean_volume > 0 else 1.0

    partial = min(1.0, max(0.0, PARTIAL_FILL_RATE * ratio))
    cancel = min(1.0, max(0.0, CANCEL_RATE / ratio)) if ratio > 0 else CANCEL_RATE
    return partial, cancel


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
    partial_fill_rate: float = PARTIAL_FILL_RATE,
    cancel_rate: float = CANCEL_RATE,
) -> list[dict[str, Any]]:
    """단일 주문의 생애 주기를 event sequence로 풀어낸다.

    상태 전이:
    - NEW → PARTIALLY_FILLED → FILLED
    - NEW → PARTIALLY_FILLED → CANCELED
    - NEW → FILLED
    - NEW → CANCELED

    partial_fill_rate / cancel_rate 는 앵커 분의 상대 거래량에 따라 조정될 수 있어
    (effective_rates) 인자로 받는다. 기본값은 모듈 상수.

    Returns:
        raw_orders message_value로 publish할 event dict list
    """
    order_qty = float(order["order_qty"])
    order_price = float(order["order_price"])

    simulated_parameters = {
        "partial_fill_rate": partial_fill_rate,
        "cancel_rate": cancel_rate,
        "price_deviation_rate": PRICE_DEVIATION_RATE,
        "reference_close": market_context["reference_close"],
        "qty_range": [MIN_QTY, MAX_QTY],
        "simulation_start_ts": market_context.get("simulation_start_ts"),
        "simulation_end_ts": market_context.get("simulation_end_ts"),
        "order_id_prefix": market_context["order_id_prefix"],
        "max_lifecycle_offset_ms": MAX_LIFECYCLE_OFFSET_MS,
        # 앵커 provenance (Phase A) — 재현성·순환부재 감사용
        "anchor_mode": market_context.get("anchor_mode", ANCHOR_MODE_FIXED),
        "anchor_file": market_context.get("anchor_file"),
        "anchor_sha256": market_context.get("anchor_sha256"),
        "vol_linked_rates": market_context.get("vol_linked_rates", False),
        "note": "synthetic order event, not real Binance private data",
    }

    if market_context.get("anchor_mode") == ANCHOR_MODE_KLINE:
        simulated_parameters["anchor_open_time_ms"] = market_context.get("anchor_open_time_ms")
        simulated_parameters["anchor_close"] = market_context.get("anchor_close")
        simulated_parameters["anchor_volume"] = market_context.get("anchor_volume")

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

    has_partial = random.random() < partial_fill_rate
    is_canceled = random.random() < cancel_rate

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

def build_producer(bootstrap: str) -> "KafkaProducer":
    from kafka import KafkaProducer  # 지연 import: 테스트/생성 로직은 kafka 불요

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


def iter_order_events(
    *,
    num_orders: int,
    symbol: str,
    order_id_prefix: str,
    reference_close: float,
    start_ms: int | None,
    end_ms: int | None,
    simulation_start_ts: str | None,
    simulation_end_ts: str | None,
    anchor_frame: AnchorFrame | None = None,
    vol_linked_rates: bool = False,
) -> Iterator[list[dict[str, Any]]]:
    """주문별 이벤트 리스트를 순서대로 생성한다 (Kafka 불요, 순수 함수).

    앵커 모드(anchor_frame 제공)에서는:
      - 거래량 가중으로 분봉을 뽑고(도착 분포),
      - 그 분의 close 를 기준점으로 주문 가격을 앵커링(±0.3%),
      - NEW 시각을 그 분 내부에서 샘플링한다.
    비앵커 모드에서는 기존 동작(고정 reference_close + 창 균등 시각)을 유지한다.

    seed 를 미리 고정하면(run 에서 random.seed) 동일 입력·동일 seed 에 대해 재현된다.
    """
    for order_index in range(1, num_orders + 1):
        if anchor_frame is not None:
            bucket = sample_anchor_bucket(anchor_frame)
            order_reference_close = bucket.close
            base_time_ms = sample_anchored_base_time_ms(bucket)
            partial_rate, cancel_rate = effective_rates(
                bucket, anchor_frame, vol_linked_rates
            )
            context = {
                "reference_close": order_reference_close,
                "simulation_start_ts": simulation_start_ts,
                "simulation_end_ts": simulation_end_ts,
                "order_id_prefix": order_id_prefix,
                "anchor_mode": ANCHOR_MODE_KLINE,
                "anchor_file": anchor_frame.path,
                "anchor_sha256": anchor_frame.sha256,
                "vol_linked_rates": vol_linked_rates,
                "anchor_open_time_ms": bucket.open_time_ms,
                "anchor_close": bucket.close,
                "anchor_volume": bucket.volume,
            }
        else:
            order_reference_close = reference_close
            base_time_ms = sample_base_time_ms(start_ms, end_ms)
            partial_rate, cancel_rate = PARTIAL_FILL_RATE, CANCEL_RATE
            context = {
                "reference_close": order_reference_close,
                "simulation_start_ts": simulation_start_ts,
                "simulation_end_ts": simulation_end_ts,
                "order_id_prefix": order_id_prefix,
                "anchor_mode": ANCHOR_MODE_FIXED,
                "anchor_file": None,
                "anchor_sha256": None,
                "vol_linked_rates": False,
            }

        order = make_base_order(
            order_index=order_index,
            reference_close=order_reference_close,
            symbol=symbol,
            order_id_prefix=order_id_prefix,
        )
        yield decide_lifecycle(
            order, context, base_time_ms, partial_rate, cancel_rate
        )


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
    parser.add_argument(
        "--anchor-klines",
        help="앵커 klines CSV(header: open_time_ms,close,volume). "
        "지정하면 주문 가격을 각 분의 close 에 앵커링하고 도착을 거래량 가중 샘플링한다.",
    )
    parser.add_argument(
        "--vol-linked-rates",
        action="store_true",
        help="(앵커 모드) 분의 상대 거래량에 따라 부분체결/취소율을 조정한다. 기본 off.",
    )
    args = parser.parse_args()

    if args.num_orders < 1:
        parser.error("--num-orders must be >= 1")
    if (args.start_ts is None) != (args.end_ts is None):
        parser.error("--start-ts and --end-ts must be provided together")
    if args.vol_linked_rates and not args.anchor_klines:
        parser.error("--vol-linked-rates requires --anchor-klines")
    if args.seed is not None:
        random.seed(args.seed)

    try:
        start_ms = parse_timestamp_ms(args.start_ts) if args.start_ts else None
        end_ms = parse_timestamp_ms(args.end_ts) if args.end_ts else None
    except ValueError as exc:
        parser.error(f"invalid timestamp: {exc}")

    anchor_frame: AnchorFrame | None = None
    if args.anchor_klines:
        anchor_path = args.anchor_klines
        # K4: s3://... 픽스처면 로컬 /tmp 로 내려받아 쓴다(k8s 잡이 S3 로 공유).
        if anchor_path.startswith("s3://"):
            import boto3

            bucket, _, key = anchor_path[len("s3://"):].partition("/")
            local = f"/tmp/{key.rsplit('/', 1)[-1]}"
            boto3.client("s3").download_file(bucket, key, local)
            anchor_path = local
        try:
            anchor_frame = load_anchor_frame(anchor_path, start_ms, end_ms)
        except (OSError, ValueError) as exc:
            parser.error(f"failed to load --anchor-klines: {exc}")
    elif start_ms is not None and end_ms is not None:
        # 비앵커 모드에서만 창 폭 검증 (앵커 모드는 분봉 시각을 그대로 쓴다)
        latest_start_ms = end_ms - MAX_LIFECYCLE_OFFSET_MS
        if latest_start_ms <= start_ms:
            parser.error(
                "--end-ts must be more than "
                f"{MAX_LIFECYCLE_OFFSET_MS}ms after --start-ts"
            )

    producer = build_producer(args.bootstrap)

    total_events = 0

    try:
        for events in iter_order_events(
            num_orders=args.num_orders,
            symbol=args.symbol,
            order_id_prefix=args.order_id_prefix,
            reference_close=args.reference_close,
            start_ms=start_ms,
            end_ms=end_ms,
            simulation_start_ts=args.start_ts,
            simulation_end_ts=args.end_ts,
            anchor_frame=anchor_frame,
            vol_linked_rates=args.vol_linked_rates,
        ):
            total_events += publish_to_kafka(producer, events, args.topic)

            sleep_seconds = sample_arrival_interval(args.max_sleep_ms)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        producer.flush()

    finally:
        producer.close()

    anchor_note = (
        f"anchored on {anchor_frame.path} (sha256={anchor_frame.sha256[:12]}…, "
        f"{len(anchor_frame.buckets)} buckets)"
        if anchor_frame is not None
        else f"fixed reference_close={args.reference_close}"
    )
    print(
        f"orders simulator 완료: "
        f"{args.num_orders} orders, {total_events} events sent to topic={args.topic}, "
        f"order_id_prefix={args.order_id_prefix}, {anchor_note}"
    )


if __name__ == "__main__":
    run()
