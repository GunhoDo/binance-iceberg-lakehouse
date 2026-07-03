"""alert.py

품질 이상 알림 — Discord webhook + **graceful degrade** (PRD v2 FR-11).

webhook URL이 없거나(`DISCORD_WEBHOOK_URL` 미설정) `requests`가 없으면
조용히 no-op하고 요약 문자열만 돌려준다. 알림 채널 미설정이 파이프라인을
멈추지 않게 하는 것이 요구사항이다.
"""

from __future__ import annotations

import os
from typing import Sequence

ENV_WEBHOOK = "DISCORD_WEBHOOK_URL"


def format_summary(anomalies: Sequence[dict], max_lines: int = 10) -> str:
    """이상 목록을 사람이 읽을 요약으로. 알림 본문·로그 공용."""
    if not anomalies:
        return "품질 이상 없음 ✅"
    crit = sum(1 for a in anomalies if a.get("severity") == "CRITICAL")
    head = f"⚠️ 품질 이상 {len(anomalies)}건 (CRITICAL {crit})"
    lines = [
        f"- [{a.get('severity')}] {a.get('check_name')} · {a.get('dimension')}: {a.get('detail')}"
        for a in anomalies[:max_lines]
    ]
    if len(anomalies) > max_lines:
        lines.append(f"- … 외 {len(anomalies) - max_lines}건")
    return "\n".join([head, *lines])


def send_alerts(
    anomalies: Sequence[dict],
    webhook_url: str | None = None,
    timeout: float = 5.0,
) -> dict:
    """이상을 Discord로 전송. 항상 결과 dict를 돌려준다(graceful).

    returns: {"sent": bool, "reason": str, "summary": str}
    """
    summary = format_summary(anomalies)
    url = webhook_url or os.environ.get(ENV_WEBHOOK)

    if not anomalies:
        return {"sent": False, "reason": "no_anomalies", "summary": summary}
    if not url:
        return {"sent": False, "reason": "no_webhook_configured", "summary": summary}

    try:
        import requests  # 선택 의존성 — 없으면 graceful degrade
    except ImportError:
        return {"sent": False, "reason": "requests_not_installed", "summary": summary}

    try:
        resp = requests.post(url, json={"content": summary}, timeout=timeout)
        ok = 200 <= resp.status_code < 300
        return {
            "sent": ok,
            "reason": "ok" if ok else f"http_{resp.status_code}",
            "summary": summary,
        }
    except Exception as exc:  # 네트워크 실패도 파이프라인을 멈추지 않는다
        return {"sent": False, "reason": f"post_failed:{exc}", "summary": summary}
