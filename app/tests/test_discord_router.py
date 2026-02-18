from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from typing import Any

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from jpswing.db.models import FundRuleSuggestion, IntelRuleSuggestion, RuleSuggestion
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
            Topic.THEME.value: "https://discord.example/theme",
            Topic.FUND_INTEL.value: "https://discord.example/fund-legacy",
            Topic.FUND_INTEL_FLASH.value: "https://discord.example/fund-flash",
            Topic.FUND_INTEL_DETAIL.value: "https://discord.example/fund-detail",
            Topic.PROPOSALS.value: "https://discord.example/proposals",
        }
    )

    assert router.send(Topic.TECH, {"content": "a"})[0]
    assert router.send(Topic.THEME, {"content": "b"})[0]
    assert router.send(Topic.FUND_INTEL_FLASH, {"content": "c"})[0]
    assert router.send(Topic.FUND_INTEL_DETAIL, {"content": "d"})[0]
    assert router.send(Topic.PROPOSALS, {"content": "e"})[0]

    assert called_urls[0].startswith("https://discord.example/tech")
    assert called_urls[1].startswith("https://discord.example/theme")
    assert called_urls[2].startswith("https://discord.example/fund-flash")
    assert called_urls[3].startswith("https://discord.example/fund-detail")
    assert called_urls[4].startswith("https://discord.example/proposals")


def test_router_from_config_fund_intel_flash_detail_fallback_to_legacy() -> None:
    discord_cfg = SimpleNamespace(
        webhook_url="",
        webhooks=SimpleNamespace(
            tech="https://discord.example/tech",
            theme="",
            fund_intel="https://discord.example/fund-legacy",
            fund_intel_flash="",
            fund_intel_detail="",
            proposals="https://discord.example/proposals",
        ),
        threads=SimpleNamespace(
            tech=None,
            theme=None,
            fund_intel="111",
            fund_intel_flash=None,
            fund_intel_detail=None,
            proposals=None,
        ),
    )
    router = DiscordRouter.from_config(discord_cfg)
    assert router.webhooks[Topic.THEME.value] == "https://discord.example/fund-legacy"
    assert router.webhooks[Topic.FUND_INTEL_FLASH.value] == "https://discord.example/fund-legacy"
    assert router.webhooks[Topic.FUND_INTEL_DETAIL.value] == "https://discord.example/fund-legacy"
    assert router.threads[Topic.THEME.value] == "111"
    assert router.threads[Topic.FUND_INTEL_FLASH.value] == "111"
    assert router.threads[Topic.FUND_INTEL_DETAIL.value] == "111"


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
    RuleSuggestion.__table__.create(engine)
    FundRuleSuggestion.__table__.create(engine)
    IntelRuleSuggestion.__table__.create(engine)

    orch = object.__new__(FundIntelOrchestrator)
    with Session(engine) as session:
        messages = FundIntelOrchestrator._build_proposal_notifications(orch, session, date(2026, 2, 14))
    assert messages == []


def test_proposal_notification_includes_tech_rows() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    RuleSuggestion.__table__.create(engine)
    FundRuleSuggestion.__table__.create(engine)
    IntelRuleSuggestion.__table__.create(engine)

    with Session(engine) as session:
        session.add(
            RuleSuggestion(
                report_date=date(2026, 2, 14),
                code="72030",
                suggestion_text="RSI閾値を見直す提案",
                source_llm_run_id=None,
                status="pending",
                raw_json={"rule_suggestion": "RSI>80の減点を緩和"},
                created_at=datetime(2026, 2, 14, 12, 0, 0),
            )
        )
        session.commit()

    orch = object.__new__(FundIntelOrchestrator)
    with Session(engine) as session:
        messages = FundIntelOrchestrator._build_proposal_notifications(orch, session, date(2026, 2, 14))
    assert len(messages) == 1
    body = messages[0]
    assert "- [tech] id=" in body
    assert "code=72030" in body
    assert "RSI閾値を見直す提案" in body


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



def test_fund_intel_notification_contains_assessment_and_facts() -> None:
    orch = object.__new__(FundIntelOrchestrator)
    orch.settings = SimpleNamespace(tag_policy={})
    messages = FundIntelOrchestrator._build_fund_intel_notifications(
        orch,
        session_name="close",
        business_date=date(2026, 2, 14),
        intel_result={
            "signals": [
                {
                    "code": "36790",
                    "critical_risk": False,
                    "high_signal_tags": ["share_buyback"],
                    "hard_risks": [],
                    "fund_state_changed": True,
                    "headline": "自己株券買付状況報告書",
                    "summary": "自己株買いの実施が確認された。",
                    "source_url": "https://example.com/doc",
                    "source_type": "edinet",
                    "published_at": "2026-02-13",
                    "facts": ["自己株買いを実施", "取得株数が開示された"],
                    "data_gaps": [],
                    "llm_valid": True,
                }
            ]
        },
        fund_state_changed=[],
        code_name_map={"36790": "じげん"},
    )
    assert len(messages) == 1
    body = messages[0]
    assert "FUND/Intel速報" in body
    assert "判定: ポジティブ（注目タグ）" in body


def test_fund_intel_detail_notification_per_symbol() -> None:
    orch = object.__new__(FundIntelOrchestrator)
    orch.settings = SimpleNamespace(tag_policy={})
    signal = {
        "code": "36790",
        "critical_risk": False,
        "high_signal_tags": ["share_buyback"],
        "hard_risks": [],
        "fund_state_changed": True,
        "headline": "自己株券買付状況報告書",
        "summary": "自己株買いの実施が確認された。",
        "source_url": "https://example.com/doc",
        "source_type": "edinet",
        "published_at": "2026-02-13",
        "facts": ["自己株買いを実施", "取得株数が開示された"],
        "data_gaps": [],
        "llm_valid": True,
    }
    body = FundIntelOrchestrator._build_fund_intel_detail_notification(
        orch,
        session_name="close",
        business_date=date(2026, 2, 14),
        signal=signal,
        code_name_map={"36790": "じげん"},
    )
    assert "FUND/Intel深掘り" in body
    assert "要点: 自己株買いを実施 / 取得株数が開示された" in body
    assert "根拠: https://example.com/doc" in body
    assert body.endswith("＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝")
