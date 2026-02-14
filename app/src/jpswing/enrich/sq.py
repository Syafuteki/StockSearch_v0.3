from __future__ import annotations

from datetime import date, timedelta

from jpswing.ingest.calendar import is_business_day


def second_friday(year: int, month: int) -> date:
    first_day = date(year, month, 1)
    offset = (4 - first_day.weekday()) % 7  # Friday=4
    first_friday = first_day + timedelta(days=offset)
    return first_friday + timedelta(days=7)


def _nth_business_before(target: date, n: int, calendar_rows: list[dict]) -> date:
    d = target
    count = 0
    while count < n:
        d -= timedelta(days=1)
        if is_business_day(d, calendar_rows):
            count += 1
    return d


def _nth_business_after(target: date, n: int, calendar_rows: list[dict]) -> date:
    d = target
    count = 0
    while count < n:
        d += timedelta(days=1)
        if is_business_day(d, calendar_rows):
            count += 1
    return d


def is_sq_window(target_date: date, calendar_rows: list[dict], business_day_window: int = 2) -> bool:
    sq_day = second_friday(target_date.year, target_date.month)
    start = _nth_business_before(sq_day, business_day_window, calendar_rows)
    end = _nth_business_after(sq_day, business_day_window, calendar_rows)
    return start <= target_date <= end

