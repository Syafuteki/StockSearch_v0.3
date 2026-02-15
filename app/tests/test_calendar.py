from __future__ import annotations

from datetime import date

from jpswing.ingest.calendar import business_days_in_range, is_business_day


def test_is_business_day_parses_holdiv_values() -> None:
    rows = [
        {"Date": "2026-02-10", "HolDiv": "1"},
        {"Date": "2026-02-11", "HolDiv": "3"},
        {"Date": "2026-02-12", "HolDiv": "1"},
    ]
    assert is_business_day(date(2026, 2, 10), rows) is True
    assert is_business_day(date(2026, 2, 11), rows) is False
    assert is_business_day(date(2026, 2, 12), rows) is True


def test_business_days_in_range_uses_holdiv() -> None:
    rows = [
        {"Date": "2026-02-10", "HolDiv": "1"},
        {"Date": "2026-02-11", "HolDiv": "3"},
        {"Date": "2026-02-12", "HolDiv": "1"},
    ]
    days = business_days_in_range(rows, date(2026, 2, 10), date(2026, 2, 12))
    assert days == [date(2026, 2, 10), date(2026, 2, 12)]

