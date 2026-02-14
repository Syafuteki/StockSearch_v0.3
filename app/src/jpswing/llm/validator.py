from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from jpswing.llm.schema import LlmTop10Response


def extract_json_text(content: str) -> str:
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3 and lines[0].startswith("```") and lines[-1].startswith("```"):
            text = "\n".join(lines[1:-1]).strip()
    return text


def validate_llm_output(content: str) -> tuple[LlmTop10Response | None, str | None, dict[str, Any] | None]:
    if not content.strip():
        return None, "empty_output", None
    json_text = extract_json_text(content)
    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError as exc:
        return None, f"invalid_json:{exc}", None
    try:
        model = LlmTop10Response.model_validate(payload)
    except ValidationError as exc:
        return None, f"schema_error:{exc}", payload if isinstance(payload, dict) else None
    return model, None, payload

