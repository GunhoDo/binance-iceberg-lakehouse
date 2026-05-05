"""klines_collector.py

Binance public market data에서 kline event를 수집해 Kafka topic `klines`로 publish.

PRD §6.2, §7 참조.

Kline은 interval이 진행되는 동안 같은 (symbol, interval, open_time) 으로 여러 번
도착할 수 있다 (PRD §6.2). 본 collector는 그 반복 이벤트를 그대로 모두 publish하고,
upsert-like 처리는 processed layer에서 한다 (`docs/decisions.md` D7).
"""

from __future__ import annotations


def fetch_klines(symbol: str, interval: str, **kwargs) -> list[dict]:
    """Binance public endpoint에서 kline event 한 batch를 가져온다.

    Returns:
        list of dict — 각 dict는 PRD §6.2의 kline 필드를 포함한다.
            symbol, interval, open_time, close_time, open, high, low, close,
            volume, quote_volume, number_of_trades, is_closed
    """
    raise NotImplementedError("Phase 1: Binance klines 수집 방식 결정 후 구현")


def publish_to_kafka(events: list[dict], topic: str = "klines") -> None:
    """수집한 kline event를 Kafka topic `klines`에 publish.

    같은 (symbol, interval, open_time) 의 반복 이벤트도 모두 publish한다.
    """
    raise NotImplementedError("Phase 1: Kafka producer 설정 후 구현")


def run() -> None:
    """수집기 진입점. long-running 프로세스로 운영된다 (PRD §14.1)."""
    raise NotImplementedError


if __name__ == "__main__":
    run()
