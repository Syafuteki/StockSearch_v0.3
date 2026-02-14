from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from jsonschema import ValidationError, validate


INTEL_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "headline",
        "published_at",
        "source_url",
        "source_type",
        "facts",
        "tags",
        "risk_flags",
        "critical_risk",
        "evidence_refs",
        "data_gaps",
    ],
    "properties": {
        "headline": {"type": "string", "minLength": 1},
        "published_at": {"type": ["string", "null"]},
        "source_url": {"type": "string", "minLength": 1},
        "source_type": {"type": "string", "minLength": 1},
        "summary": {"type": "string"},
        "facts": {"type": "array", "items": {"type": "string"}},
        "tags": {"type": "array", "items": {"type": "string"}},
        "risk_flags": {"type": "array", "items": {"type": "string"}},
        "critical_risk": {"type": "boolean"},
        "evidence_refs": {"type": "array", "items": {"type": "string"}},
        "data_gaps": {"type": "array", "items": {"type": "string"}},
    },
}


@dataclass(slots=True)
class IntelValidationResult:
    payload: dict[str, Any] | None
    valid: bool
    error: str | None


def validate_intel_payload(payload: dict[str, Any]) -> IntelValidationResult:
    try:
        validate(payload, INTEL_ITEM_SCHEMA)
    except ValidationError as exc:
        return IntelValidationResult(payload=None, valid=False, error=str(exc))
    return IntelValidationResult(payload=payload, valid=True, error=None)


def parse_published_at(value: str | None) -> datetime | None:
    if not value:
        return None
    txt = value.strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(txt, fmt)
        except ValueError:
            continue
    return None

