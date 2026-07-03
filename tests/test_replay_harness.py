"""infra/replay_harness.py (P1 / FR-2) 단위 테스트.

네트워크·Kafka 불요: 레코드 빌드/스키마 파리티 + rate pacing 수학만 순수 함수로 검증한다.
"""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_module():
    path = REPO_ROOT / "infra" / "replay_harness.py"
    spec = importlib.util.spec_from_file_location("replay_harness", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rh = _load_module()

# 실제 Binance trades CSV 한 행 (헤더 없음, 7컬럼).
TRADE_ROW = [
    "3344774379",
    "42283.58000000",
    "0.00069000",
    "29.17567020",
    "1704067200000",
    "True",
    "True",
]


class RecordBuildTests(unittest.TestCase):
    def test_builds_string_typed_record_with_produce_ts(self) -> None:
        rec = rh.build_trade_record(TRADE_ROW, "BTCUSDT", produce_ts=1782996840395)
        self.assertEqual(rec["trade_id"], "3344774379")
        self.assertEqual(rec["price"], "42283.58000000")
        self.assertEqual(rec["is_buyer_maker"], "True")
        self.assertEqual(rec["symbol"], "BTCUSDT")
        self.assertEqual(rec["source"], "replay")
        # P2 lag 기준점이 실려야 한다.
        self.assertEqual(rec["produce_ts"], 1782996840395)

    def test_common_fields_str_typed_like_csv_and_ws(self) -> None:
        """decisions.md D21: 공통 필드는 문자열이어야 downstream이 CSV/WS/replay를 동일 처리."""
        rec = rh.build_trade_record(TRADE_ROW, "BTCUSDT", produce_ts=1)
        for field in TRADE_ROW_FIELDS:
            self.assertIsInstance(rec[field], str, f"{field}가 문자열이 아님")

    def test_rejects_wrong_column_count(self) -> None:
        self.assertIsNone(rh.build_trade_record(["a", "b", "c"], "BTCUSDT", produce_ts=1))
        # 헤더 행("id,price,...")도 컬럼 수가 같으면 통과하지만, 깨진 행은 스킵된다.
        self.assertIsNone(rh.build_trade_record([], "BTCUSDT", produce_ts=1))


TRADE_ROW_FIELDS = ["trade_id", "price", "qty", "quote_qty", "time", "is_buyer_maker", "is_best_match"]


class PacingMathTests(unittest.TestCase):
    def test_slice_batch_size_scales_with_rate(self) -> None:
        # 5000/s, 50ms 슬라이스 → 슬라이스당 250건
        self.assertEqual(rh.slice_batch_size(5000, 0.05), 250)
        self.assertEqual(rh.slice_batch_size(1000, 0.05), 50)

    def test_slice_batch_size_floor_is_one(self) -> None:
        # 아주 낮은 rate라도 최소 1건은 보장 (0건 슬라이스로 멈추지 않게)
        self.assertEqual(rh.slice_batch_size(1, 0.05), 1)

    def test_pacer_batch_matches_slice_size(self) -> None:
        pacer = rh.RatePacer(2000, slice_sec=0.05)
        self.assertEqual(pacer.batch, 100)


class IterRowsTests(unittest.TestCase):
    def test_limit_caps_row_count(self) -> None:
        import csv
        import tempfile

        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="") as f:
            writer = csv.writer(f)
            for i in range(10):
                writer.writerow([str(i)] + TRADE_ROW[1:])
            path = f.name

        rows = list(rh.iter_rows(path, limit=4))
        self.assertEqual(len(rows), 4)
        rows_all = list(rh.iter_rows(path, limit=None))
        self.assertEqual(len(rows_all), 10)


if __name__ == "__main__":
    unittest.main()
