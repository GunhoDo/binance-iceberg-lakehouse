"""rules.py

데이터 품질 이상탐지 **순수 규칙 함수** (PRD v2 P4 / FR-8·FR-9).

이 모듈은 Spark도 카탈로그도 모른다. 파이썬 값(집계 결과·시퀀스)만 받아
이상(anomaly) 레코드 dict를 돌려준다. 그래서:
  - Spark/Iceberg 런타임 없이 단위테스트 가능(tests/test_quality_rules.py).
  - local hadoop 벤치든 glue/S3 프로덕션이든 **탐지 로직이 100% 동일하게 재사용**된다.
    (카탈로그 차이는 드라이버 src/quality/quality_scan.py의 세션·테이블 식별자에만 존재)

이상 레코드의 최종 컬럼(run_id/detected_at/source_table)은 드라이버가 채운다.
여기서는 검사 고유 필드만 만든다: check_name·severity·dimension·observed·threshold·detail.
"""

from __future__ import annotations

from typing import Iterable, Sequence

CRITICAL = "CRITICAL"
WARN = "WARN"

# check_name 상수 (PRD FR-8·FR-9)
FRESHNESS_SLA = "FRESHNESS_SLA"
GAP = "GAP"
ORDER_REVERSAL = "ORDER_REVERSAL"
NULL_OR_ZERO = "NULL_OR_ZERO"
SCHEMA_DRIFT = "SCHEMA_DRIFT"


def anomaly(
    check_name: str,
    severity: str,
    dimension: str,
    observed: float,
    threshold: float,
    detail: str,
) -> dict:
    """이상 레코드 한 건(검사 고유 필드만). 드라이버가 컨텍스트 필드를 덧붙인다."""
    return {
        "check_name": check_name,
        "severity": severity,
        "dimension": dimension,
        "observed": float(observed),
        "threshold": float(threshold),
        "detail": detail,
    }


# ---------------------------------------------------------------------------
# FR-8: 프레시니스 SLA (lag 지표 재사용)
# ---------------------------------------------------------------------------

def check_freshness(
    dimension: str,
    p95_ms: float,
    sla_ms: float,
    max_ms: float | None = None,
) -> dict | None:
    """토픽/스트림별 지연이 SLA를 넘으면 이상.

    - p95 > SLA          → CRITICAL (분포의 몸통이 SLA 위반)
    - p95 ≤ SLA < max    → WARN     (tail만 튐)
    - 그 외              → None
    lag 지표(produce_ts→commit_ts)를 그대로 재사용한다(FR-8). 프로덕션에서는
    같은 함수에 Iceberg 스냅샷 커밋 지연 등 다른 freshness 입력을 넣으면 된다.
    """
    if p95_ms > sla_ms:
        return anomaly(
            FRESHNESS_SLA, CRITICAL, dimension, p95_ms, sla_ms,
            f"p95 freshness {p95_ms:.0f}ms > SLA {sla_ms:.0f}ms",
        )
    if max_ms is not None and max_ms > sla_ms:
        return anomaly(
            FRESHNESS_SLA, WARN, dimension, max_ms, sla_ms,
            f"tail freshness {max_ms:.0f}ms > SLA {sla_ms:.0f}ms (p95 {p95_ms:.0f}ms 정상)",
        )
    return None


def check_freshness_all(rows: Iterable[dict], sla_ms: float) -> list[dict]:
    """rows: [{"dimension","p95_ms","max_ms"?}, ...] → 이상 목록."""
    out = []
    for r in rows:
        a = check_freshness(r["dimension"], r["p95_ms"], sla_ms, r.get("max_ms"))
        if a is not None:
            out.append(a)
    return out


# ---------------------------------------------------------------------------
# FR-9: gap (결측 open_time / trade_id 불연속)
# ---------------------------------------------------------------------------

def find_gaps(values: Iterable[int], step: int) -> list[dict]:
    """정렬·중복제거한 정수 시퀀스에서 step 간격이 아닌 불연속을 찾는다.

    - trades  : trade_id 연속성(step=1)  → missing = 사라진 체결 수
    - klines  : open_time 연속성(step=interval_ms) → missing = 결측 캔들 수
    같은 함수를 컬럼·step만 바꿔 두 도메인에 재사용한다.
    """
    if step <= 0:
        raise ValueError("step must be positive")
    vs = sorted(set(int(v) for v in values))
    gaps = []
    for a, b in zip(vs, vs[1:]):
        missing = (b - a) // step - 1
        if missing > 0:
            gaps.append({"prev": a, "next": b, "missing": int(missing)})
    return gaps


def check_gap(
    dimension: str,
    values: Iterable[int],
    step: int,
    max_missing: int = 0,
) -> list[dict]:
    """gap 세그먼트마다 이상 한 건. missing > max_missing 인 경우만."""
    out = []
    for g in find_gaps(values, step):
        if g["missing"] > max_missing:
            out.append(anomaly(
                GAP, CRITICAL, dimension, g["missing"], max_missing,
                f"gap {g['prev']}..{g['next']} → 결측 {g['missing']}건",
            ))
    return out


# ---------------------------------------------------------------------------
# FR-9: 순서역전 (도착 순서 대비 id/시각 역행)
# ---------------------------------------------------------------------------

def find_reversals(seq: Sequence[int]) -> list[tuple[int, int, int]]:
    """도착 순서 seq에서 값이 감소하는 지점 목록: (index, prev, curr) with curr < prev.

    단조 증가해야 하는 id/이벤트시각이 뒤로 튀는 경우를 잡는다(동값=중복은 제외).
    """
    return [
        (i, int(seq[i - 1]), int(seq[i]))
        for i in range(1, len(seq))
        if int(seq[i]) < int(seq[i - 1])
    ]


def check_order_reversal(dimension: str, seq: Sequence[int]) -> list[dict]:
    """역전이 있으면 이상 한 건으로 요약(건수 + 첫 사례). 없으면 빈 목록."""
    revs = find_reversals(seq)
    if not revs:
        return []
    i, prev, curr = revs[0]
    return [anomaly(
        ORDER_REVERSAL, WARN, dimension, len(revs), 0,
        f"순서역전 {len(revs)}건 (첫 사례 idx={i}: {prev} → {curr})",
    )]


# ---------------------------------------------------------------------------
# FR-9: NULL / 0 (필수 수치 필드)
# ---------------------------------------------------------------------------

def check_null_or_zero(dimension: str, field_stats: dict) -> list[dict]:
    """field_stats: {field: {"nulls":n,"zeros":z,"total":t}} → 필드별 이상.

    가격·수량 같은 필수 수치 필드의 NULL 또는 0은 손상 데이터로 본다(CRITICAL).
    """
    out = []
    for field, s in field_stats.items():
        bad = int(s.get("nulls", 0)) + int(s.get("zeros", 0))
        if bad > 0:
            out.append(anomaly(
                NULL_OR_ZERO, CRITICAL, f"{dimension}.{field}", bad, 0,
                f"{field}: null {s.get('nulls', 0)}건 · zero {s.get('zeros', 0)}건 "
                f"/ 총 {s.get('total', 0)}건",
            ))
    return out


# ---------------------------------------------------------------------------
# FR-9: 스키마 드리프트 (기대 스키마 대비)
# ---------------------------------------------------------------------------

def check_schema_drift(dimension: str, actual: dict, expected: dict) -> list[dict]:
    """actual/expected: {field: type_str}.

    - 기대 필드 결측       → CRITICAL (downstream 파싱 붕괴)
    - 타입 불일치          → CRITICAL
    - 예상 밖 필드 추가     → WARN     (신규 필드, 관측만)
    """
    out = []
    for field, exp_type in expected.items():
        if field not in actual:
            out.append(anomaly(
                SCHEMA_DRIFT, CRITICAL, f"{dimension}.{field}", 1, 0,
                f"필수 필드 결측: {field} ({exp_type} 기대)",
            ))
        elif actual[field] != exp_type:
            out.append(anomaly(
                SCHEMA_DRIFT, CRITICAL, f"{dimension}.{field}", 1, 0,
                f"타입 불일치: {field} 기대 {exp_type} → 실제 {actual[field]}",
            ))
    for field in actual:
        if field not in expected:
            out.append(anomaly(
                SCHEMA_DRIFT, WARN, f"{dimension}.{field}", 1, 0,
                f"예상 밖 신규 필드: {field} ({actual[field]})",
            ))
    return out
