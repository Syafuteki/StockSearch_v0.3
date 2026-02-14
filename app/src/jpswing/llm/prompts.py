from __future__ import annotations

import json
from datetime import date
from typing import Any


def build_top10_messages(
    *,
    report_date: date,
    run_type: str,
    candidates_payload: list[dict[str, Any]],
    rules_payload: dict[str, Any],
) -> list[dict[str, str]]:
    system_prompt = (
        "あなたは日本株スイング候補の評価アシスタントです。"
        "与えられたJSON以外の事実は作らず、未取得情報は data_gaps に明示してください。"
        "必ず厳格なJSONのみを返し、説明文やMarkdownを付けないでください。"
    )
    user_prompt = {
        "task": "Top30候補をTop10にランキングし、売買目安を短く作成する",
        "report_date": report_date.isoformat(),
        "run_type": run_type,
        "output_schema": {
            "top10": [
                {
                    "code": "string",
                    "top10_rank": "1..10",
                    "thesis_bull": ["string"],
                    "thesis_bear": ["string"],
                    "key_levels": {
                        "entry_idea": "string",
                        "stop_idea": "string",
                        "takeprofit_idea": "string",
                    },
                    "event_risks": ["string"],
                    "confidence_0_100": "0..100",
                    "data_gaps": ["string"],
                    "rule_suggestion": "string|null",
                }
            ]
        },
        "selection_rules": rules_payload.get("step3", {}),
        "candidates": candidates_payload,
    }
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)},
    ]
