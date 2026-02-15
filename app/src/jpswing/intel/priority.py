from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class PriorityInput:
    code: str
    fund_state: str
    fund_score: float
    has_new_edinet: bool
    theme_strength: float
    theme_strength_delta: float
    has_high_signal_tag: bool


STATE_WEIGHT = {"IN": 1.0, "WATCH": 0.6, "OUT": 0.2}


def calculate_priority(item: PriorityInput) -> float:
    state_w = STATE_WEIGHT.get(item.fund_state.upper(), 0.2)
    score = (
        # Prefer symbols with new EDINET docs so Intel deep-dive has concrete sources.
        (state_w * 0.25)
        + (max(0.0, min(1.0, item.fund_score)) * 0.20)
        + (0.45 if item.has_new_edinet else 0.0)
        + (max(0.0, min(1.0, item.theme_strength)) * 0.07)
        + (max(-1.0, min(1.0, item.theme_strength_delta)) * 0.02)
        + (0.01 if item.has_high_signal_tag else 0.0)
    )
    # Deterministic tie-break by code to ensure strict ordering in tests and idempotent runs.
    tail = (sum(ord(c) for c in item.code) % 1000) / 1_000_000
    return round(score + tail, 6)


def rank_priorities(items: list[PriorityInput]) -> list[dict[str, Any]]:
    rows = [{"code": i.code, "priority": calculate_priority(i)} for i in items]
    rows.sort(key=lambda x: (-x["priority"], x["code"]))
    return rows
