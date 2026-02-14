from __future__ import annotations

from datetime import date
from typing import Any

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from jpswing.db.models import FundRuleSuggestion, IntelRuleSuggestion
from jpswing.fund_intel_orchestrator import FundIntelOrchestrator
from jpswing.notify.discord_router import DiscordRouter, Topic, chunk_embeds, split_discord_content


def _ok_response(url: str) -> httpx.Response:
    req = httpx.Request("POST", url)
    return httpx.Response(status_code=204, request=req)


def test_router_routes_to_topic_webhooks(monkeypatch: Any) -> None:
    called_urls: list[str] = []

    def fake_post(url: str, json: dict[str, Any], timeout: int) -> httpx.Response:  # noqa: ARG001
        called_urls.append(url)
        return _ok_response(url)

    monkeypatch.setattr(httpx, "post", fake_post)
    router = DiscordRouter(
        webhooks={
            Topic.TECH.value: "https://discord.example/tech",
            Topic.FUND_INTEL.value: "https://discord.example/fund",
            Topic.PROPOSALS.value: "https://discord.example/proposals",
        }
    )

    assert router.send(Topic.TECH, {"content": "a"})[0]
    assert router.send(Topic.FUND_INTEL, {"content": "b"})[0]
    assert router.send(Topic.PROPOSALS, {"content": "c"})[0]

    assert called_urls[0].startswith("https://discord.example/tech")
    assert called_urls[1].startswith("https://discord.example/fund")
    assert called_urls[2].startswith("https://discord.example/proposals")


def test_split_discord_content_keeps_chunks_under_2000_and_balances_code_fences() -> None:
    payload = "header\n```python\n" + ("print('x')\n" * 500) + "```\nfooter"
    parts = split_discord_content(payload, max_chars=2000)
    assert len(parts) > 1
    assert all(len(p) <= 2000 for p in parts)
    assert all(p.count("```") % 2 == 0 for p in parts)


def test_chunk_embeds_caps_to_ten_per_message() -> None:
    embeds = [{"title": f"t{i}", "description": "d"} for i in range(23)]
    batches = chunk_embeds(embeds, max_embeds=10, max_text_chars=6000)
    assert len(batches) == 3
    assert [len(batch) for batch in batches] == [10, 10, 3]


def test_router_retries_on_429_with_retry_after(monkeypatch: Any) -> None:
    calls = {"count": 0}
    slept: list[float] = []

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    def fake_post(url: str, json: dict[str, Any], timeout: int) -> httpx.Response:  # noqa: ARG001
        calls["count"] += 1
        req = httpx.Request("POST", url)
        if calls["count"] == 1:
            return httpx.Response(status_code=429, headers={"Retry-After": "0.01"}, request=req)
        return httpx.Response(status_code=204, request=req)

    monkeypatch.setattr(httpx, "post", fake_post)
    router = DiscordRouter(webhooks={Topic.TECH.value: "https://discord.example/tech"}, sleep_fn=fake_sleep)
    ok, err = router.send(Topic.TECH, {"content": "retry me"})

    assert ok
    assert err is None
    assert calls["count"] == 2
    assert slept and slept[0] >= 0.01


def test_noop_proposal_notification_when_no_rows() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    FundRuleSuggestion.__table__.create(engine)
    IntelRuleSuggestion.__table__.create(engine)

    orch = object.__new__(FundIntelOrchestrator)
    with Session(engine) as session:
        messages = FundIntelOrchestrator._build_proposal_notifications(orch, session, date(2026, 2, 14))
    assert messages == []


def test_noop_fund_intel_notification_when_no_signal() -> None:
    orch = object.__new__(FundIntelOrchestrator)
    messages = FundIntelOrchestrator._build_fund_intel_notifications(
        orch,
        session_name="morning",
        business_date=date(2026, 2, 14),
        intel_result={"signals": []},
        fund_state_changed=[],
    )
    assert messages == []

