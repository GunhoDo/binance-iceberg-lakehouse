"""infra/ws_to_kafka.py (P0 / FR-1) 단위 테스트.

네트워크·Kafka 불요: payload → record 매핑과 URL/제한 규칙만 순수 함수로 검증한다.
기존 tests 스타일(정적/의존성 없는 unittest)을 따른다.
"""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_module():
    """websocket-client/kafka 미설치 환경에서도 import 되도록 파일 직접 로드.

    (무거운 의존성은 실행 경로에서만 지연 import 하므로 모듈 로드 자체는 안전하다.)
    """
    path = REPO_ROOT / "infra" / "ws_to_kafka.py"
    spec = importlib.util.spec_from_file_location("ws_to_kafka", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ws = _load_module()


# 실제 Binance combined-stream 프레임 형태 (문서 기준 축약).
AGG_TRADE_FRAME = {
    "stream": "btcusdt@aggTrade",
    "data": {
        "e": "aggTrade",
        "E": 1672515782136,
        "s": "BTCUSDT",
        "a": 12345,
        "p": "20000.00",
        "q": "0.005",
        "f": 100,
        "l": 105,
        "T": 1672515782130,
        "m": True,
        "M": True,
    },
}

KLINE_FRAME = {
    "stream": "btcusdt@kline_1m",
    "data": {
        "e": "kline",
        "E": 1672515782136,
        "s": "BTCUSDT",
        "k": {
            "t": 1672515780000,
            "T": 1672515839999,
            "s": "BTCUSDT",
            "i": "1m",
            "f": 100,
            "L": 200,
            "o": "20000.0",
            "c": "20010.0",
            "h": "20020.0",
            "l": "19990.0",
            "v": "5.0",
            "n": 100,
            "x": False,
            "q": "100000.0",
            "V": "2.5",
            "Q": "50000.0",
            "B": "0",
        },
    },
}


class StreamUrlTests(unittest.TestCase):
    def test_url_lowercases_symbols_and_joins_combos(self) -> None:
        url = ws.build_stream_url(["BTCUSDT", "ETHUSDT"], ["aggTrade", "kline_1m"])
        self.assertTrue(url.startswith("wss://"))
        self.assertIn("btcusdt@aggTrade", url)
        self.assertIn("ethusdt@kline_1m", url)
        # combined-stream은 subscribe 제어 메시지가 없어 5 msg/s 제한을 회피한다.
        self.assertIn("/stream?streams=", url)

    def test_rejects_unsupported_stream(self) -> None:
        with self.assertRaises(ValueError):
            ws.build_stream_url(["BTCUSDT"], ["depth"])

    def test_rejects_empty_symbols(self) -> None:
        with self.assertRaises(ValueError):
            ws.build_stream_url([], ["aggTrade"])

    def test_enforces_1024_stream_limit(self) -> None:
        many = [f"SYM{i}USDT" for i in range(600)]  # 600 × 2 = 1200 > 1024
        with self.assertRaises(ValueError):
            ws.build_stream_url(many, ["aggTrade", "kline_1m"])


class AggTradeMappingTests(unittest.TestCase):
    def test_maps_to_trades_schema(self) -> None:
        data = AGG_TRADE_FRAME["data"]
        rec = ws.agg_trade_to_record(data, ingest_time=1672515782200)
        # 공통 필드는 CSV와 동일하게 문자열로 방출된다.
        self.assertEqual(rec["trade_id"], "12345")
        self.assertEqual(rec["price"], "20000.00")
        self.assertEqual(rec["qty"], "0.005")
        self.assertEqual(rec["time"], "1672515782130")
        self.assertEqual(rec["is_buyer_maker"], "True")
        self.assertEqual(rec["symbol"], "BTCUSDT")
        self.assertEqual(rec["source"], "websocket")
        # P2 lag 계측 입력(메타)은 native 타입으로 보존.
        self.assertEqual(rec["exchange_event_time"], 1672515782136)
        self.assertEqual(rec["ingest_time"], 1672515782200)
        # quote_qty = price*qty 산출
        self.assertAlmostEqual(float(rec["quote_qty"]), 100.0)

    def test_common_fields_are_str_typed_like_csv(self) -> None:
        """downstream(from_json StringType)이 CSV·WS를 동일 처리하려면 공통 필드가 str이어야 한다."""
        rec = ws.agg_trade_to_record(AGG_TRADE_FRAME["data"], ingest_time=1)
        for field in ["trade_id", "price", "qty", "quote_qty", "time",
                      "is_buyer_maker", "is_best_match", "symbol"]:
            self.assertIsInstance(rec[field], str, f"{field}가 문자열이 아님")

    def test_bool_token_matches_downstream_comparison(self) -> None:
        """downstream은 `parsed.is_buyer_maker == "True"`로 비교한다 → 토큰이 정확히 맞아야 함."""
        maker = ws.agg_trade_to_record({**AGG_TRADE_FRAME["data"], "m": True}, ingest_time=1)
        taker = ws.agg_trade_to_record({**AGG_TRADE_FRAME["data"], "m": False}, ingest_time=1)
        self.assertEqual(maker["is_buyer_maker"], "True")
        self.assertEqual(taker["is_buyer_maker"], "False")

    def test_csv_parity_fields_present(self) -> None:
        """리플레이(csv_to_kafka) trades 스키마 필드를 모두 포함해야 downstream이 통합 소비."""
        rec = ws.agg_trade_to_record(AGG_TRADE_FRAME["data"], ingest_time=1)
        for field in [
            "trade_id",
            "price",
            "qty",
            "quote_qty",
            "time",
            "is_buyer_maker",
            "is_best_match",
            "symbol",
        ]:
            self.assertIn(field, rec)


class KlineMappingTests(unittest.TestCase):
    def test_maps_to_klines_schema(self) -> None:
        rec = ws.kline_to_record(KLINE_FRAME["data"], ingest_time=1672515782200)
        # cast("long") 대상 필드는 문자열로 방출된다.
        self.assertEqual(rec["open_time"], "1672515780000")
        self.assertEqual(rec["close_time"], "1672515839999")
        self.assertEqual(rec["number_of_trades"], "100")
        self.assertEqual(rec["open"], "20000.0")
        self.assertEqual(rec["close"], "20010.0")
        self.assertEqual(rec["interval"], "1m")
        self.assertEqual(rec["symbol"], "BTCUSDT")
        self.assertEqual(rec["is_closed"], False)  # WS 전용 flag, downstream 무시
        self.assertEqual(rec["exchange_event_time"], 1672515782136)
        self.assertEqual(rec["source"], "websocket")

    def test_common_fields_are_str_typed_like_csv(self) -> None:
        rec = ws.kline_to_record(KLINE_FRAME["data"], ingest_time=1)
        for field in ["open_time", "close_time", "number_of_trades", "open", "high",
                      "low", "close", "volume", "quote_volume", "symbol", "interval"]:
            self.assertIsInstance(rec[field], str, f"{field}가 문자열이 아님")

    def test_csv_parity_fields_present(self) -> None:
        rec = ws.kline_to_record(KLINE_FRAME["data"], ingest_time=1)
        for field in [
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
            "symbol",
            "interval",
        ]:
            self.assertIn(field, rec)


class RoutingTests(unittest.TestCase):
    def test_routes_agg_trade_to_trades_topic_keyed_by_symbol(self) -> None:
        topic, key, rec = ws.route_payload(AGG_TRADE_FRAME["data"], ingest_time=1)
        self.assertEqual(topic, "trades")
        self.assertEqual(key, "BTCUSDT")  # csv_to_kafka와 동일 key 컨벤션
        self.assertEqual(rec["event_type"], "aggTrade")

    def test_routes_kline_to_klines_topic_keyed_by_symbol_interval(self) -> None:
        topic, key, rec = ws.route_payload(KLINE_FRAME["data"], ingest_time=1)
        self.assertEqual(topic, "klines")
        self.assertEqual(key, "BTCUSDT_1m")  # csv_to_kafka와 동일 key 컨벤션

    def test_unknown_event_returns_none(self) -> None:
        self.assertIsNone(ws.route_payload({"e": "depthUpdate"}, ingest_time=1))


class FrameParsingTests(unittest.TestCase):
    def test_parses_combined_stream_wrapper(self) -> None:
        import json

        data = ws.parse_frame(json.dumps(AGG_TRADE_FRAME))
        self.assertIsNotNone(data)
        self.assertEqual(data["e"], "aggTrade")

    def test_parses_single_raw_stream(self) -> None:
        import json

        data = ws.parse_frame(json.dumps(AGG_TRADE_FRAME["data"]))
        self.assertEqual(data["e"], "aggTrade")

    def test_rejects_garbage(self) -> None:
        self.assertIsNone(ws.parse_frame("not json"))
        self.assertIsNone(ws.parse_frame("{}"))
        self.assertIsNone(ws.parse_frame('{"result": null, "id": 1}'))  # 제어 응답


if __name__ == "__main__":
    unittest.main()
