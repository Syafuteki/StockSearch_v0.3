from typing import Any

import httpx

from jpswing.intel.llm_client import IntelLlmClient


def test_extract_content_handles_gpt_oss_control_prefix() -> None:
    response = {
        "choices": [
            {
                "message": {
                    "content": '<|channel|>final <|constrain|>json<|message|>{"headline":"h","published_at":null}'
                }
            }
        ]
    }
    content = IntelLlmClient._extract_content(response)
    assert content.startswith('{"headline":"h"')


def test_extract_content_handles_lmstudio_chat_output() -> None:
    response = {
        "output": [
            {"type": "reasoning", "text": "thinking"},
            {
                "type": "message",
                "content": [{"type": "text", "text": '```json\n{"headline":"h2","published_at":null}\n```'}],
            },
        ]
    }
    content = IntelLlmClient._extract_content(response)
    assert content == '{"headline":"h2","published_at":null}'


def test_resolve_mcp_chat_endpoint_from_base_url() -> None:
    client = IntelLlmClient(base_url="http://host.docker.internal:1234/v1", model="openai/gpt-oss-20b")
    assert client._resolve_mcp_chat_endpoint() == "http://host.docker.internal:1234/api/v1/chat"


def test_summarize_symbol_intel_repairs_once_and_merges_source_meta(monkeypatch: Any) -> None:
    calls = {"n": 0}

    def fake_post(url: str, headers: dict[str, Any], json: dict[str, Any], timeout: int) -> httpx.Response:  # noqa: ARG001
        calls["n"] += 1
        req = httpx.Request("POST", url)
        if calls["n"] == 1:
            return httpx.Response(
                status_code=200,
                request=req,
                json={"choices": [{"message": {"content": '{"headline":"h","facts":[]}'}}]},
            )
        return httpx.Response(
            status_code=200,
            request=req,
            json={
                "choices": [
                    {
                        "message": {
                            "content": '{"headline":"h2","summary":"修復済み要約","facts":["f1"],"tags":[],"risk_flags":[],"critical_risk":false,"evidence_refs":[],"data_gaps":[]}'
                        }
                    }
                ]
            },
        )

    monkeypatch.setattr(httpx, "post", fake_post)
    client = IntelLlmClient(base_url="http://host.docker.internal:1234/v1", model="openai/gpt-oss-20b", retries=0)
    payload, valid, err = client.summarize_symbol_intel(
        code="36790",
        source_payload=[
            {
                "source_url": "https://example.com/doc",
                "source_type": "edinet",
                "published_at": "2026-02-13",
                "headline": "自己株券買付状況報告書",
                "full_text": "自己株式の取得を実施しました。取得株数は1,000株です。",
                "snippet": "",
                "xbrl_facts": [],
                "evidence_refs": [],
            }
        ],
        existing_tags=[],
    )
    assert valid is True
    assert err is None
    assert payload["source_url"] == "https://example.com/doc"
    assert payload["source_type"] == "edinet"
    assert payload["published_at"] == "2026-02-13"
    assert payload["summary"] == "修復済み要約"
    assert calls["n"] == 2


def test_summarize_symbol_intel_fallback_uses_source_text(monkeypatch: Any) -> None:
    calls = {"n": 0}

    def fake_post(url: str, headers: dict[str, Any], json: dict[str, Any], timeout: int) -> httpx.Response:  # noqa: ARG001
        calls["n"] += 1
        req = httpx.Request("POST", url)
        if calls["n"] == 1:
            return httpx.Response(status_code=200, request=req, json={"choices": [{"message": {"content": "INVALID_JSON"}}]})
        return httpx.Response(status_code=200, request=req, json={"choices": [{"message": {"content": '{"headline":"x"}'}}]})

    monkeypatch.setattr(httpx, "post", fake_post)
    client = IntelLlmClient(base_url="http://host.docker.internal:1234/v1", model="openai/gpt-oss-20b", retries=0)
    payload, valid, err = client.summarize_symbol_intel(
        code="36790",
        source_payload=[
            {
                "source_url": "https://example.com/doc",
                "source_type": "edinet",
                "published_at": "2026-02-13",
                "headline": "自己株券買付状況報告書",
                "full_text": "自己株式の取得を実施しました。取得株数は1,000株です。",
                "snippet": "自己株式の取得を実施しました。",
                "xbrl_facts": ["取得株数 1,000株"],
                "evidence_refs": [],
            }
        ],
        existing_tags=[],
    )
    assert valid is False
    assert err is not None
    assert payload["source_url"] == "https://example.com/doc"
    assert payload["source_type"] == "edinet"
    assert payload["summary"] != ""
    assert payload["facts"]
