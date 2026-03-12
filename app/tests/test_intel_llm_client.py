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
        company_name="Sample Co",
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
        company_name="Sample Co",
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


def test_summarize_symbol_intel_gap_research_uses_mcp_and_reduces_gaps(monkeypatch: Any) -> None:
    calls: list[str] = []
    mcp_inputs: list[dict[str, Any]] = []

    def fake_post(url: str, headers: dict[str, Any], json: dict[str, Any], timeout: int) -> httpx.Response:  # noqa: ARG001
        calls.append(url)
        req = httpx.Request("POST", url)
        if url.endswith("/api/v1/chat"):
            mcp_inputs.append(__import__("json").loads(json["input"]))
            if len(mcp_inputs) == 1:
                payload = {
                    "output": [
                        {
                            "type": "message",
                            "content": [
                                {
                                    "type": "text",
                                    "text": '{"headline":"初回MCP結果","summary":"初回MCP要約","facts":["提出あり"],"tags":[],"risk_flags":[],"critical_risk":false,"evidence_refs":[],"data_gaps":["報告書本文の取得または抽出が不十分"]}',
                                }
                            ],
                        }
                    ]
                }
                return httpx.Response(status_code=200, request=req, json=payload)
            payload = {
                "output": [
                    {
                        "type": "message",
                        "content": [
                            {
                                "type": "text",
                                "text": '{"headline":"追加調査結果","summary":"MCPで欠損を補完した要約","facts":["IR資料で確認"],"tags":[],"risk_flags":[],"critical_risk":false,"evidence_refs":["https://example.com/ir"],"data_gaps":[]}',
                            }
                        ],
                    }
                ]
            }
            return httpx.Response(status_code=200, request=req, json=payload)
        payload = {
            "choices": [
                {
                    "message": {
                        "content": '{"headline":"初回結果","summary":"初回要約","facts":["提出あり"],"tags":[],"risk_flags":[],"critical_risk":false,"evidence_refs":[],"data_gaps":["報告書本文の取得または抽出が不十分"]}'
                    }
                }
            ]
        }
        return httpx.Response(status_code=200, request=req, json=payload)

    monkeypatch.setattr(httpx, "post", fake_post)
    client = IntelLlmClient(
        base_url="http://host.docker.internal:1234/v1",
        model="openai/gpt-oss-20b",
        retries=0,
        use_mcp=True,
        mcp_integrations=["mcp/playwright"],
    )
    payload, valid, err = client.summarize_symbol_intel(
        code="36790",
        company_name="Sample Co",
        source_payload=[
            {
                "source_url": "https://example.com/doc",
                "source_type": "edinet",
                "published_at": "2026-02-13",
                "headline": "サンプル開示",
                "full_text": "提出メタデータのみ",
                "snippet": "",
                "xbrl_facts": [],
                "evidence_refs": [],
            }
        ],
        existing_tags=[],
    )
    assert valid is True
    assert err is None
    assert payload["summary"] == "MCPで欠損を補完した要約"
    assert payload["facts"] == ["IR資料で確認"]
    assert payload["data_gaps"] == []
    assert "https://example.com/ir" in payload["evidence_refs"]
    assert sum(1 for url in calls if url.endswith("/api/v1/chat")) == 2
    assert len(mcp_inputs) == 2
    assert mcp_inputs[1]["task"] == "resolve_data_gaps_with_mcp"
    assert mcp_inputs[1]["company_name"] == "Sample Co"
    assert "mcp_research_hints" in mcp_inputs[1]
    assert "gap_resolution_targets" in mcp_inputs[1]
    assert "Sample Co IR" in mcp_inputs[1]["mcp_research_hints"]["search_queries"]
    assert "API" in mcp_inputs[1]["mcp_research_hints"]["source_navigation_hints"][0]["browser_access_note"]
    assert "full_text" not in mcp_inputs[1]["sources"][0]


def test_summarize_symbol_intel_mcp_result_does_not_readd_source_gap(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    def fake_post(url: str, headers: dict[str, Any], json: dict[str, Any], timeout: int) -> httpx.Response:  # noqa: ARG001
        req = httpx.Request("POST", url)
        if url.endswith("/api/v1/chat"):
            calls.append(__import__("json").loads(json["input"]))
            return httpx.Response(
                status_code=200,
                request=req,
                json={
                    "output": [
                        {
                            "type": "message",
                            "content": [
                                {
                                    "type": "text",
                                    "text": '{"headline":"Resolved","summary":"resolved via MCP","facts":["official IR confirmed"],"tags":[],"risk_flags":[],"critical_risk":false,"evidence_refs":["https://example.com/ir"],"data_gaps":[]}',
                                }
                            ],
                        }
                    ]
                },
            )
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(httpx, "post", fake_post)
    client = IntelLlmClient(
        base_url="http://host.docker.internal:1234/v1",
        model="openai/gpt-oss-20b",
        retries=0,
        use_mcp=True,
        mcp_integrations=["mcp/playwright"],
    )
    payload, valid, err = client.summarize_symbol_intel(
        code="36790",
        company_name="Sample Co",
        source_payload=[
            {
                "source_url": "https://api.edinet-fsa.go.jp/api/v2/documents/S100TEST?type=1",
                "source_type": "edinet",
                "published_at": "2026-02-13",
                "headline": "Sample filing",
                "full_text": "short",
                "snippet": "",
                "xbrl_facts": [],
                "evidence_refs": [],
            }
        ],
        existing_tags=[],
    )
    assert valid is True
    assert err is None
    assert payload["data_gaps"] == []
    assert payload["summary"] == "resolved via MCP"
    assert len(calls) == 1
    assert calls[0]["company_name"] == "Sample Co"
    assert "Sample Co IR" in calls[0]["mcp_research_hints"]["search_queries"]
    assert calls[0]["mcp_research_hints"]["source_navigation_hints"][0]["doc_id"] == "S100TEST"


def test_build_gap_resolution_targets_adds_mna_and_bond_specific_guidance() -> None:
    targets = IntelLlmClient._build_gap_resolution_targets(
        code="88020",
        company_name="三菱地所",
        unresolved_gaps=[
            "子会社化による収益への具体的影響は未開示",
            "社債発行条件（利率・償還期間）等の詳細情報が不足",
        ],
        source_payload=[
            {
                "source_url": "https://api.edinet-fsa.go.jp/api/v2/documents/S100XOJ4?type=1",
                "source_type": "edinet",
                "headline": "臨時報告書",
                "published_at": "2026-03-04 15:41",
            },
            {
                "source_url": "https://api.edinet-fsa.go.jp/api/v2/documents/S100XOJ5?type=1",
                "source_type": "edinet",
                "headline": "訂正発行登録書",
                "published_at": "2026-03-04 15:41",
            },
        ],
    )

    assert len(targets) == 2

    impact_target = targets[0]
    assert impact_target["gap"] == "子会社化による収益への具体的影響は未開示"
    assert "子会社化による収益への具体的影響" in impact_target["gap_components"]
    assert "impact" in impact_target["inferred_categories"]
    assert "impact_or_outlook" in impact_target["target_fact_types"]
    assert "JPX timely disclosure" in impact_target["likely_sources"]
    assert any("子会社化 業績影響" in q for q in impact_target["suggested_queries"])
    assert any("timely disclosure" in hint for hint in impact_target["document_hints"])
    assert any("earnings impact" in q for q in impact_target["resolution_questions"])

    financing_target = targets[1]
    assert financing_target["gap"] == "社債発行条件（利率・償還期間）等の詳細情報が不足"
    assert "利率" in financing_target["gap_components"]
    assert "償還期間" in financing_target["gap_components"]
    assert "financing_terms" in financing_target["inferred_categories"]
    assert "terms_or_conditions" in financing_target["target_fact_types"]
    assert "EDINET shelf registration supplement / prospectus" in financing_target["likely_sources"]
    assert any("発行登録追補書類" in q for q in financing_target["suggested_queries"])
    assert any("社債 利率 償還" in q for q in financing_target["suggested_queries"])
    assert any("condition-determination" in hint or "supplement" in hint for hint in financing_target["document_hints"])
    assert any("coupon" in q or "maturity" in q for q in financing_target["resolution_questions"])


def test_build_gap_resolution_targets_has_generic_fallback_for_unknown_gap() -> None:
    targets = IntelLlmClient._build_gap_resolution_targets(
        code="12340",
        company_name="Sample Co",
        unresolved_gaps=["開示資料だけでは詳細な前提が読み取れない"],
        source_payload=[
            {
                "source_url": "https://example.com/doc",
                "source_type": "edinet",
                "headline": "お知らせ",
                "published_at": "2026-03-04",
            }
        ],
    )

    assert len(targets) == 1
    target = targets[0]
    assert target["gap"] == "開示資料だけでは詳細な前提が読み取れない"
    assert "generic" in target["inferred_categories"]
    assert "unspecified_detail" in target["target_fact_types"] or "terms_or_conditions" in target["target_fact_types"]
    assert "company IR release" in target["likely_sources"]
    assert any("Sample Co 開示資料だけでは詳細な前提が読み取れない" in q for q in target["suggested_queries"])
    assert target["resolution_questions"]
