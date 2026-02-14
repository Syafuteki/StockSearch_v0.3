from __future__ import annotations


def compute_session_allowance(
    *,
    daily_budget: int,
    session_cap: int,
    done_total: int,
    done_session: int,
) -> int:
    remaining_total = max(0, daily_budget - done_total)
    remaining_session = max(0, session_cap - done_session)
    return min(remaining_total, remaining_session)


def build_idempotency_key(business_date_iso: str, session_name: str, code: str) -> str:
    return f"{business_date_iso}:{session_name}:{code}"

