from __future__ import annotations

from datetime import date
from typing import Any, Protocol


class TdnetProvider(Protocol):
    def fetch_disclosures(self, target_date: date, code: str | None = None) -> list[dict[str, Any]]:
        ...


class TdnetStubProvider:
    """Paid TDnet API can be plugged in later. Current implementation is a no-op stub."""

    def fetch_disclosures(self, target_date: date, code: str | None = None) -> list[dict[str, Any]]:
        _ = (target_date, code)
        return []

