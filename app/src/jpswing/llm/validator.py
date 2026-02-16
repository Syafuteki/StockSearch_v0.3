from __future__ import annotations

import json
from typing import Any

from pydantic import ValidationError

from jpswing.llm.schema import LlmSingleCandidateResponse, LlmTop10Response


def _extract_first_json_block(text: str) -> str:
    decoder = json.JSONDecoder()
    for idx, ch in enumerate(text):
        if ch not in "{[":
            continue
        try:
            _, end = decoder.raw_decode(text[idx:])
        except json.JSONDecodeError:
            continue
        return text[idx : idx + end]
    return text


def extract_json_text(content: str) -> str:
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3 and lines[0].startswith("```") and lines[-1].startswith("```"):
            text = "\n".join(lines[1:-1]).strip()
    marker = "<|message|>"
    marker_pos = text.find(marker)
    if marker_pos >= 0:
        text = text[marker_pos + len(marker) :].strip()
    text = _extract_first_json_block(text)
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


def _extract_single_candidate_payload(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    top10 = payload.get("top10")
    if isinstance(top10, list) and top10:
        first = top10[0]
        if isinstance(first, dict):
            return first
    candidate_result = payload.get("candidate_result")
    if isinstance(candidate_result, dict):
        return candidate_result
    return payload


def validate_single_candidate_output(content: str) -> tuple[dict[str, Any] | None, str | None, dict[str, Any] | None]:
    if not content.strip():
        return None, "empty_output", None
    json_text = extract_json_text(content)
    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError as exc:
        return None, f"invalid_json:{exc}", None

    candidate_payload = _extract_single_candidate_payload(payload)
    if not isinstance(candidate_payload, dict):
        return None, "schema_error:single_candidate_payload_missing", payload if isinstance(payload, dict) else None

    try:
        model = LlmSingleCandidateResponse.model_validate(candidate_payload)
    except ValidationError as exc:
        return None, f"schema_error:{exc}", payload if isinstance(payload, dict) else None
    return model.model_dump(), None, payload if isinstance(payload, dict) else None
