"""src/quality/rules.py (P4 / FR-8·FR-9) 단위 테스트.

Spark/네트워크 불요: 순수 규칙 함수만 검증한다. 이 규칙들이 곧 local 벤치와
glue/S3 프로덕션에서 재사용되는 탐지 로직의 전부다(드라이버는 값만 넘김).
"""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_rules():
    path = REPO_ROOT / "src" / "quality" / "rules.py"
    spec = importlib.util.spec_from_file_location("quality_rules", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rules = _load_rules()


class FreshnessTests(unittest.TestCase):
    def test_p95_over_sla_is_critical(self) -> None:
        a = rules.check_freshness("baseline", p95_ms=26000, sla_ms=15000, max_ms=27000)
        self.assertIsNotNone(a)
        self.assertEqual(a["check_name"], rules.FRESHNESS_SLA)
        self.assertEqual(a["severity"], rules.CRITICAL)

    def test_only_tail_over_sla_is_warn(self) -> None:
        a = rules.check_freshness("opt", p95_ms=12000, sla_ms=15000, max_ms=16000)
        self.assertIsNotNone(a)
        self.assertEqual(a["severity"], rules.WARN)

    def test_within_sla_is_none(self) -> None:
        self.assertIsNone(rules.check_freshness("opt", p95_ms=9000, sla_ms=15000, max_ms=13000))

    def test_all_filters_healthy(self) -> None:
        rows = [
            {"dimension": "baseline", "p95_ms": 25600, "max_ms": 26100},
            {"dimension": "opt", "p95_ms": 9000, "max_ms": 13000},
        ]
        out = rules.check_freshness_all(rows, sla_ms=15000)
        self.assertEqual([a["dimension"] for a in out], ["baseline"])


class GapTests(unittest.TestCase):
    def test_find_gaps_trade_id_step_one(self) -> None:
        gaps = rules.find_gaps([1, 2, 3, 7, 8], step=1)
        self.assertEqual(gaps, [{"prev": 3, "next": 7, "missing": 3}])

    def test_find_gaps_klines_open_time_step_interval(self) -> None:
        # 1분봉(60000ms) open_time에서 한 캔들 결측
        gaps = rules.find_gaps([0, 60000, 180000], step=60000)
        self.assertEqual(gaps, [{"prev": 60000, "next": 180000, "missing": 1}])

    def test_no_gap_when_contiguous(self) -> None:
        self.assertEqual(rules.find_gaps([5, 6, 7, 8], step=1), [])

    def test_dedup_and_unsorted_input(self) -> None:
        self.assertEqual(rules.find_gaps([3, 1, 2, 3], step=1), [])

    def test_check_gap_respects_max_missing(self) -> None:
        # 결측 3건짜리 gap은 max_missing=3이면 통과, 2면 이상
        self.assertEqual(rules.check_gap("BTCUSDT", [1, 2, 6], step=1, max_missing=3), [])
        out = rules.check_gap("BTCUSDT", [1, 2, 6], step=1, max_missing=2)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["check_name"], rules.GAP)

    def test_zero_step_rejected(self) -> None:
        with self.assertRaises(ValueError):
            rules.find_gaps([1, 2], step=0)


class OrderReversalTests(unittest.TestCase):
    def test_detects_backward_jump(self) -> None:
        revs = rules.find_reversals([1, 2, 5, 3, 6])
        self.assertEqual(revs, [(3, 5, 3)])

    def test_monotonic_has_no_reversal(self) -> None:
        self.assertEqual(rules.find_reversals([1, 2, 2, 3]), [])

    def test_check_summarizes_count(self) -> None:
        out = rules.check_order_reversal("BTCUSDT", [3, 2, 1])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["observed"], 2.0)
        self.assertEqual(out[0]["severity"], rules.WARN)


class NullOrZeroTests(unittest.TestCase):
    def test_flags_nulls_and_zeros(self) -> None:
        stats = {
            "price": {"nulls": 2, "zeros": 0, "total": 100},
            "qty": {"nulls": 0, "zeros": 3, "total": 100},
        }
        out = rules.check_null_or_zero("trades", stats)
        self.assertEqual(len(out), 2)
        self.assertTrue(all(a["check_name"] == rules.NULL_OR_ZERO for a in out))
        self.assertTrue(all(a["severity"] == rules.CRITICAL for a in out))

    def test_clean_fields_produce_nothing(self) -> None:
        stats = {"price": {"nulls": 0, "zeros": 0, "total": 100}}
        self.assertEqual(rules.check_null_or_zero("trades", stats), [])


class SchemaDriftTests(unittest.TestCase):
    EXPECTED = {"trade_id": "string", "price": "string", "produce_ts": "bigint"}

    def test_missing_field_is_critical(self) -> None:
        actual = {"trade_id": "string", "produce_ts": "bigint"}
        out = rules.check_schema_drift("trades", actual, self.EXPECTED)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["severity"], rules.CRITICAL)
        self.assertIn("price", out[0]["dimension"])

    def test_type_mismatch_is_critical(self) -> None:
        actual = {"trade_id": "string", "price": "double", "produce_ts": "bigint"}
        out = rules.check_schema_drift("trades", actual, self.EXPECTED)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["check_name"], rules.SCHEMA_DRIFT)

    def test_extra_field_is_warn(self) -> None:
        actual = {**self.EXPECTED, "new_col": "string"}
        out = rules.check_schema_drift("trades", actual, self.EXPECTED)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["severity"], rules.WARN)

    def test_identical_schema_is_clean(self) -> None:
        self.assertEqual(rules.check_schema_drift("trades", dict(self.EXPECTED), self.EXPECTED), [])


if __name__ == "__main__":
    unittest.main()
