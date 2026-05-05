"""trades_collector.py

Binance public market data에서 trade event를 수집해 Kafka topic `trades`로 publish.

PRD §6.1, §7 참조.

본 스켈레톤은 함수 시그니처와 책임 경계만 정의한다.
실제 수집 방식 (REST polling vs WebSocket, batch size, reconnection 정책 등) 은
Phase 1에서 결정한다. `docs/decisions.md` 참조.
"""

from __future__ import annotations


def fetch_trades(symbol: str, **kwargs) -> list[dict]:
    """Binance public endpoint에서 trade event 한 batch를 가져온다.

    Returns:
        list of dict — 각 dict는 PRD §6.1의 trade 필드를 포함한다.
            symbol, trade_id (or agg_trade_id), price, quantity, trade_time,
            is_buyer_maker
    """
    raise NotImplementedError("Phase 1: Binance trades 수집 방식 결정 후 구현")


def publish_to_kafka(events: list[dict], topic: str = "trades") -> None:
    """수집한 trade event를 Kafka topic `trades`에 publish.

    Args:
        events: fetch_trades 출력
        topic: 기본값은 "trades" (PRD §7)
    """
    raise NotImplementedError("Phase 1: Kafka producer 설정 후 구현")


def run() -> None:
    """수집기 진입점. long-running 프로세스로 운영된다 (PRD §14.1).

    설계 노트:
    - 수집 주기, batch size는 config로 분리한다 (PRD §16.3).
    - reconnection, backoff 정책은 Phase 1에서 결정한다.
    """
    raise NotImplementedError


if __name__ == "__main__":
    run()
