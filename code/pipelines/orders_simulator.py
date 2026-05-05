"""orders_simulator.py

User-level private order data를 대체하는 합성 주문 이벤트 생성기.

이 시뮬레이터의 출력은 **실제 Binance 주문이 아니다** (PRD §2, §6.3).
도메인 가정과 실험 목적은 `docs/simulator_design.md` 참조.

본 스켈레톤은 시뮬레이터의 책임을 함수 단위로 분할만 해둔다. 정확한 분포 파라미터는
Phase 1에서 config로 분리해 결정한다 (`docs/decisions.md` D9).
"""

from __future__ import annotations


# -----------------------------------------------------------------------------
# 도메인 가정 (PRD §6.3) — 각 함수는 가정을 한 가지씩 책임진다.
# -----------------------------------------------------------------------------

def sample_arrival_interval() -> float:
    """다음 주문이 도착할 때까지의 시간 간격을 샘플링.

    PRD §6.3: 시간 구간별 Poisson 또는 고정 rate.
    파라미터는 config 분리.
    """
    raise NotImplementedError


def sample_side() -> str:
    """주문 방향을 샘플링한다. BUY 또는 SELL.

    PRD §6.3: BUY/SELL 비율 config.
    """
    raise NotImplementedError


def sample_order_price(reference_close: float) -> float:
    """주문 가격을 샘플링한다.

    PRD §6.3: 최근 kline close price 주변 분포에서 샘플링.

    Args:
        reference_close: 직전 kline close (가격 anchor).
    """
    raise NotImplementedError


def sample_order_qty() -> float:
    """주문 수량을 샘플링한다.

    PRD §6.3: log-normal 또는 fixed range.
    """
    raise NotImplementedError


# -----------------------------------------------------------------------------
# 상태 전이 (PRD §10.5)
# -----------------------------------------------------------------------------
#   NEW → PARTIALLY_FILLED → FILLED
#   NEW → CANCELED
#
# 두 경로의 비율과 부분 체결 step 수는 config 기반.
# -----------------------------------------------------------------------------

def decide_lifecycle(order: dict, market_context: dict) -> list[dict]:
    """단일 주문의 생애 주기를 결정해 event sequence로 풀어낸다.

    Returns:
        list of event dict. 각 event는 PRD §10.2의 raw_orders 컬럼을 따른다.
            order_id, client_id, symbol, side, order_type, order_price,
            order_qty, event_type, order_status, event_time,
            simulated_parameters, ingest_time
    """
    raise NotImplementedError(
        "Phase 1: 체결 판단 로직 (trade price/kline close vs order price 비교) 결정 후 구현"
    )


# -----------------------------------------------------------------------------
# 실행 진입점
# -----------------------------------------------------------------------------

def publish_to_kafka(events: list[dict], topic: str = "orders") -> None:
    """생성한 주문 이벤트를 Kafka topic `orders`에 publish."""
    raise NotImplementedError


def run() -> None:
    """시뮬레이터 진입점. long-running 프로세스로 운영된다 (PRD §14.1)."""
    raise NotImplementedError


if __name__ == "__main__":
    run()
