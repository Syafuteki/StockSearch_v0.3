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
        "You are a Japan equity swing-trade analysis assistant.\n"
        "Return ONLY a JSON object matching the requested schema.\n"
        "Write natural-language fields in Japanese.\n"
        "Do not fabricate facts. If data is missing, put it in data_gaps.\n"
        "Do not output empty thesis arrays.\n"
        "Do not use placeholders such as N/A, unknown, or none for key levels."
    )

    user_prompt = {
        "task": "Rank Top30 candidates into Top10 for JP swing trading and provide concise rationale.",
        "report_date": report_date.isoformat(),
        "run_type": run_type,
        "rules": [
            "Use only provided candidate/event/market data.",
            "thesis_bull and thesis_bear must each contain at least one short bullet.",
            "key_levels.entry_idea/stop_idea/takeprofit_idea must be concrete text.",
        ],
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


def build_single_candidate_messages(
    *,
    report_date: date,
    run_type: str,
    candidate_payload: dict[str, Any],
    rules_payload: dict[str, Any],
) -> list[dict[str, str]]:
    system_prompt = (
        "You are a Japan equity swing-trade analysis assistant.\n"
        "Return ONLY a JSON object matching the requested schema.\n"
        "Write natural-language fields in Japanese.\n"
        "Do not fabricate facts. If data is missing, put it in data_gaps.\n"
        "Do not output empty thesis arrays.\n"
        "Do not use placeholders such as N/A, unknown, or none for key levels."
    )

    user_prompt = {
        "task": "Analyze one JP swing candidate and return one structured evaluation item.",
        "report_date": report_date.isoformat(),
        "run_type": run_type,
        "rules": [
            "Use only provided candidate/event/market data.",
            "Return exactly one JSON object (not top10 array).",
            "Do not include code or rank in the output.",
            "thesis_bull and thesis_bear must each contain at least one short bullet.",
            "key_levels.entry_idea/stop_idea/takeprofit_idea must be concrete text.",
        ],
        "output_schema": {
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
        },
        "selection_rules": rules_payload.get("step3", {}),
        "candidate": candidate_payload,
    }
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)},
    ]


def build_single_candidate_repair_messages(
    *,
    report_date: date,
    run_type: str,
    candidate_payload: dict[str, Any],
    rules_payload: dict[str, Any],
    previous_output: str,
    validation_error: str,
) -> list[dict[str, str]]:
    system_prompt = (
        "You are a JSON formatter for JP equity swing analysis.\n"
        "Return ONLY one JSON object matching the requested schema.\n"
        "Do not add markdown, code fences, or explanations."
    )
    user_prompt = {
        "task": "Repair previous output into valid JSON schema for one candidate.",
        "report_date": report_date.isoformat(),
        "run_type": run_type,
        "validation_error": validation_error,
        "rules": [
            "Use only the information in candidate and previous_output.",
            "Return exactly one JSON object.",
            "Do not include code or rank in the output.",
        ],
        "output_schema": {
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
        },
        "selection_rules": rules_payload.get("step3", {}),
        "candidate": candidate_payload,
        "previous_output": previous_output,
    }
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(user_prompt, ensure_ascii=False)},
    ]
