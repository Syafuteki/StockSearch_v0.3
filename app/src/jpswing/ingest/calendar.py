from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from jpswing.ingest.normalize import to_date

logger = logging.getLogger(__name__)


def _to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    if isinstance(value, str):
        txt = value.strip().lower()
        if txt in {"1", "true", "yes", "open", "business", "営業日"}:
            return True
        if txt in {"0", "false", "no", "closed", "holiday", "休場"}:
            return False
    return None


def is_business_day(target_date: date, rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        row_date = to_date(
            row.get("Date") or row.get("date") or row.get("HolidayDate") or row.get("CalendarDate")
        )
        if row_date != target_date:
            continue

        for key in ("is_business_day", "IsBusinessDay", "BusinessDayFlag"):
            flag = _to_bool(row.get(key))
            if flag is not None:
                return flag

        holiday_div = row.get("HolidayDivision") or row.get("holiday_division")
        if holiday_div is not None:
            flag = _to_bool(holiday_div)
            if flag is not None:
                return flag

        name = str(row.get("HolidayName") or row.get("holiday_name") or "").strip()
        if name:
            return False
        break

    logger.warning("Could not parse market calendar for %s. fallback to weekday.", target_date)
    return target_date.weekday() < 5


def business_days_in_range(rows: list[dict[str, Any]], from_date: date, to_date: date) -> list[date]:
    all_days: list[date] = []
    d = from_date
    while d <= to_date:
        all_days.append(d)
        d += timedelta(days=1)
    result: list[date] = []
    for d in all_days:
        if is_business_day(d, rows):
            result.append(d)
    return result


def previous_business_day(target_date: date, rows: list[dict[str, Any]]) -> date:
    day = target_date - timedelta(days=1)
    while not is_business_day(day, rows):
        day -= timedelta(days=1)
    return day

