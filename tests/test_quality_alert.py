"""src/quality/alert.py (P4 / FR-11) 단위 테스트 — graceful degrade 검증.

webhook 미설정·이상 없음에서 파이프라인을 멈추지 않고 조용히 no-op하는지 본다.
네트워크는 타지 않는다(URL 미주입).
"""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_alert():
    path = REPO_ROOT / "src" / "quality" / "alert.py"
    spec = importlib.util.spec_from_file_location("quality_alert", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


alert = _load_alert()

SAMPLE = [
    {"severity": "CRITICAL", "check_name": "GAP", "dimension": "BTCUSDT", "detail": "결측 3건"},
    {"severity": "WARN", "check_name": "ORDER_REVERSAL", "dimension": "BTCUSDT", "detail": "역전 1건"},
]


class FormatSummaryTests(unittest.TestCase):
    def test_empty_is_healthy_line(self) -> None:
        self.assertIn("이상 없음", alert.format_summary([]))

    def test_counts_critical(self) -> None:
        s = alert.format_summary(SAMPLE)
        self.assertIn("품질 이상 2건", s)
        self.assertIn("CRITICAL 1", s)

    def test_truncates_with_overflow_note(self) -> None:
        many = SAMPLE * 8  # 16건
        s = alert.format_summary(many, max_lines=10)
        self.assertIn("외 6건", s)


class SendAlertsGracefulTests(unittest.TestCase):
    def test_no_anomalies_no_send(self) -> None:
        r = alert.send_alerts([], webhook_url="https://example.invalid/hook")
        self.assertFalse(r["sent"])
        self.assertEqual(r["reason"], "no_anomalies")

    def test_no_webhook_graceful(self) -> None:
        # URL 미주입 + 환경변수 없음 → 조용히 미전송(예외 없음)
        r = alert.send_alerts(SAMPLE, webhook_url=None)
        self.assertFalse(r["sent"])
        self.assertEqual(r["reason"], "no_webhook_configured")
        self.assertIn("품질 이상", r["summary"])


if __name__ == "__main__":
    unittest.main()
