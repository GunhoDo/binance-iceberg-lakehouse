"""src/simulators/orders_simulator.py 앵커링(Phase A) 단위 테스트.

Kafka·Spark·네트워크 불요: 순수 생성 함수(iter_order_events)와 앵커 프레임
로딩/샘플링만 검증한다.

검증 축:
- A-7a seed 재현성: 동일 seed → 동일 이벤트, 다른 seed → 다른 이벤트
- A-7b 가격 앵커링: 앵커 주문 가격이 그 분 close 의 ±PRICE_DEVIATION_RATE 안
- A-7c 도착 분포: 거래량이 큰 분이 더 자주 뽑힌다(가중 상관)
- A-7d provenance: anchor_mode/anchor_file/anchor_sha256/anchor_close 기록
- A-7e 프레임 로딩: 창 필터·불량행 제외·sha256 안정
- 순환부재: 앵커(close) 와 벤치마크(VWAP)는 다른 시계열임을 표현하는 anchor_close 존재
"""
from __future__ import annotations

import importlib.util
import json
import random
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_module():
    path = REPO_ROOT / "src" / "simulators" / "orders_simulator.py"
    spec = importlib.util.spec_from_file_location("orders_simulator", path)
    module = importlib.util.module_from_spec(spec)
    # dataclass + `from __future__ import annotations` 는 sys.modules 등록을 요구한다
    # (Python 3.14 dataclass 가 문자열 어노테이션을 모듈 네임스페이스로 해석).
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sim = _load_module()


ANCHOR_CSV = (
    "open_time_ms,close,volume\n"
    "1704067200000,42000.00000000,10.00000000\n"
    "1704067260000,42100.00000000,5.00000000\n"
    "1704067320000,41900.00000000,20.00000000\n"
)


def _write_csv(text: str) -> str:
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    )
    tmp.write(text)
    tmp.close()
    return tmp.name


def _generate(frame, *, num_orders=200, seed=7, vol_linked=False):
    random.seed(seed)
    return list(
        sim.iter_order_events(
            num_orders=num_orders,
            symbol="BTCUSDT",
            order_id_prefix="T",
            reference_close=43000.0,
            start_ms=None,
            end_ms=None,
            simulation_start_ts=None,
            simulation_end_ts=None,
            anchor_frame=frame,
            vol_linked_rates=vol_linked,
        )
    )


class AnchorFrameLoadTests(unittest.TestCase):
    def test_loads_header_and_computes_stable_sha256(self) -> None:
        path = _write_csv(ANCHOR_CSV)
        frame_a = sim.load_anchor_frame(path)
        frame_b = sim.load_anchor_frame(path)

        self.assertEqual(len(frame_a.buckets), 3)
        self.assertEqual(frame_a.sha256, frame_b.sha256)
        self.assertEqual(len(frame_a.sha256), 64)
        # 버킷은 open_time_ms 오름차순
        times = [b.open_time_ms for b in frame_a.buckets]
        self.assertEqual(times, sorted(times))

    def test_window_filter_and_bad_rows_excluded(self) -> None:
        text = ANCHOR_CSV + (
            "1704067380000,0.00000000,3.00000000\n"      # close<=0 → 제외
            "1704067440000,42200.00000000,0.00000000\n"  # volume<=0 → 제외
        )
        path = _write_csv(text)
        # [1704067260000, 1704067380000) 창 → 두 번째·세 번째 버킷만
        frame = sim.load_anchor_frame(path, start_ms=1704067260000, end_ms=1704067380000)
        times = [b.open_time_ms for b in frame.buckets]
        self.assertEqual(times, [1704067260000, 1704067320000])

    def test_empty_after_filter_raises(self) -> None:
        path = _write_csv(ANCHOR_CSV)
        with self.assertRaises(ValueError):
            sim.load_anchor_frame(path, start_ms=1, end_ms=2)

    def test_missing_header_raises(self) -> None:
        path = _write_csv("a,b,c\n1,2,3\n")
        with self.assertRaises(ValueError):
            sim.load_anchor_frame(path)


class SeedReproducibilityTests(unittest.TestCase):
    def test_same_seed_same_events(self) -> None:
        frame = sim.load_anchor_frame(_write_csv(ANCHOR_CSV))
        a = _generate(frame, seed=123)
        b = _generate(frame, seed=123)
        self.assertEqual(
            json.dumps(a, sort_keys=True), json.dumps(b, sort_keys=True)
        )

    def test_different_seed_different_events(self) -> None:
        frame = sim.load_anchor_frame(_write_csv(ANCHOR_CSV))
        a = _generate(frame, seed=1)
        b = _generate(frame, seed=2)
        self.assertNotEqual(
            json.dumps(a, sort_keys=True), json.dumps(b, sort_keys=True)
        )


class PriceAnchoringTests(unittest.TestCase):
    def test_new_price_within_deviation_of_minute_close(self) -> None:
        frame = sim.load_anchor_frame(_write_csv(ANCHOR_CSV))
        events_per_order = _generate(frame, num_orders=500, seed=99)

        dev = sim.PRICE_DEVIATION_RATE
        for events in events_per_order:
            new_event = events[0]
            self.assertEqual(new_event["order_status"], "NEW")
            params = new_event["simulated_parameters"]
            anchor_close = params["anchor_close"]
            price = float(new_event["order_price"])
            self.assertGreaterEqual(price, anchor_close * (1 - dev) - 1e-6)
            self.assertLessEqual(price, anchor_close * (1 + dev) + 1e-6)

    def test_new_event_time_falls_inside_anchor_minute(self) -> None:
        frame = sim.load_anchor_frame(_write_csv(ANCHOR_CSV))
        for events in _generate(frame, num_orders=300, seed=5):
            new_event = events[0]
            open_ms = new_event["simulated_parameters"]["anchor_open_time_ms"]
            event_ms = int(new_event["event_time"])
            self.assertGreaterEqual(event_ms, open_ms)
            self.assertLess(event_ms, open_ms + sim.MINUTE_MS)


class ArrivalDistributionTests(unittest.TestCase):
    def test_higher_volume_minute_is_sampled_more_often(self) -> None:
        # volume 1000 vs 1 → 고거래량 분이 압도적으로 자주 뽑혀야
        text = (
            "open_time_ms,close,volume\n"
            "1000000000000,100.0,1000.0\n"
            "1000000060000,200.0,1.0\n"
        )
        frame = sim.load_anchor_frame(_write_csv(text))
        random.seed(42)
        counts = {1000000000000: 0, 1000000060000: 0}
        for _ in range(2000):
            bucket = sim.sample_anchor_bucket(frame)
            counts[bucket.open_time_ms] += 1
        self.assertGreater(counts[1000000000000], counts[1000000060000] * 20)

    def test_hourly_order_count_correlates_with_volume_above_0_7(self) -> None:
        """PRD/ROADMAP A-6 지표를 **정량**으로 못박는다.

        지표는 "시간대별(hourly) 주문 수와 실 volume 의 상관 > 0.7". 도착 분포는
        volume 가중이라 기댓값상 분당 주문수 ∝ volume 이지만, 실 num_orders 는
        분(minute) 대비 희소해(예: 9000주문/44640분) 분 단위 상관은 표본잡음으로
        희석된다(≈0.6). PRD 가 요구한 **시간 단위**로 집계하면 잡음이 평균화돼
        임계를 여유롭게 넘는다는 것을 self-contained 합성 픽스처로 검증한다.
        """
        hour_ms = 3_600_000
        minute_ms = 60_000
        # 24시간 × 60분. 시간마다 volume 레벨을 크게 다르게(결정적 패턴) 준다.
        rows = ["open_time_ms,close,volume"]
        base_ms = 1_704_067_200_000  # 2024-01-01T00:00:00Z
        for hour in range(24):
            # 시간별 기준 거래량: 1~24 를 뒤섞어 단조증가가 아닌 굴곡을 만든다.
            hour_level = ((hour * 7) % 24) + 1
            for m in range(60):
                open_ms = base_ms + hour * hour_ms + m * minute_ms
                # 분 내 소폭 변동(결정적) — 시간 총량이 hour_level 을 따르도록.
                vol = hour_level * (1.0 + 0.02 * (m % 5))
                rows.append(f"{open_ms},{100.0 + hour:.8f},{vol:.8f}")
        frame = sim.load_anchor_frame(_write_csv("\n".join(rows) + "\n"))

        random.seed(42)
        cnt_by_hour: dict[int, int] = {}
        vol_by_hour: dict[int, float] = {}
        for b in frame.buckets:
            hr = (b.open_time_ms // hour_ms) * hour_ms
            vol_by_hour[hr] = vol_by_hour.get(hr, 0.0) + b.volume
        for events in sim.iter_order_events(
            num_orders=6000,
            symbol="BTCUSDT",
            order_id_prefix="H",
            reference_close=43000.0,
            start_ms=None,
            end_ms=None,
            simulation_start_ts=None,
            simulation_end_ts=None,
            anchor_frame=frame,
        ):
            open_ms = events[0]["simulated_parameters"]["anchor_open_time_ms"]
            hr = (open_ms // hour_ms) * hour_ms
            cnt_by_hour[hr] = cnt_by_hour.get(hr, 0) + 1

        hours = sorted(vol_by_hour)
        counts = [cnt_by_hour.get(h, 0) for h in hours]
        vols = [vol_by_hour[h] for h in hours]

        n = len(hours)
        mc = sum(counts) / n
        mv = sum(vols) / n
        cov = sum((c - mc) * (v - mv) for c, v in zip(counts, vols))
        vc = sum((c - mc) ** 2 for c in counts)
        vv = sum((v - mv) ** 2 for v in vols)
        r = cov / (vc**0.5 * vv**0.5)

        self.assertGreater(
            r, 0.7, f"hourly count↔volume corr {r:.3f} must exceed PRD A-6 threshold 0.7"
        )


class ProvenanceTests(unittest.TestCase):
    def test_anchor_provenance_recorded(self) -> None:
        path = _write_csv(ANCHOR_CSV)
        frame = sim.load_anchor_frame(path)
        events = _generate(frame, num_orders=10, seed=3)[0]
        params = events[0]["simulated_parameters"]
        self.assertEqual(params["anchor_mode"], sim.ANCHOR_MODE_KLINE)
        self.assertEqual(params["anchor_file"], path)
        self.assertEqual(params["anchor_sha256"], frame.sha256)
        self.assertIn("anchor_close", params)
        self.assertIn("anchor_volume", params)

    def test_non_anchor_mode_marks_fixed_reference(self) -> None:
        random.seed(11)
        events = list(
            sim.iter_order_events(
                num_orders=5,
                symbol="BTCUSDT",
                order_id_prefix="F",
                reference_close=43000.0,
                start_ms=None,
                end_ms=None,
                simulation_start_ts=None,
                simulation_end_ts=None,
                anchor_frame=None,
            )
        )
        params = events[0][0]["simulated_parameters"]
        self.assertEqual(params["anchor_mode"], sim.ANCHOR_MODE_FIXED)
        self.assertIsNone(params["anchor_file"])
        self.assertNotIn("anchor_close", params)


class VolLinkedRatesTests(unittest.TestCase):
    def test_rates_stay_within_unit_interval(self) -> None:
        frame = sim.load_anchor_frame(_write_csv(ANCHOR_CSV))
        for bucket in frame.buckets:
            partial, cancel = sim.effective_rates(bucket, frame, vol_linked=True)
            self.assertGreaterEqual(partial, 0.0)
            self.assertLessEqual(partial, 1.0)
            self.assertGreaterEqual(cancel, 0.0)
            self.assertLessEqual(cancel, 1.0)

    def test_off_returns_constants(self) -> None:
        frame = sim.load_anchor_frame(_write_csv(ANCHOR_CSV))
        partial, cancel = sim.effective_rates(frame.buckets[0], frame, vol_linked=False)
        self.assertEqual(partial, sim.PARTIAL_FILL_RATE)
        self.assertEqual(cancel, sim.CANCEL_RATE)


if __name__ == "__main__":
    unittest.main()
