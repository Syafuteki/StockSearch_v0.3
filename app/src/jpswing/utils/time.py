from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


JST = ZoneInfo("Asia/Tokyo")


def now_jst() -> datetime:
    return datetime.now(tz=JST)


def today_jst() -> date:
    return now_jst().date()


def ensure_jst(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=JST)
    return dt.astimezone(JST)


def date_to_str(value: date | datetime) -> str:
    if isinstance(value, datetime):
        value = ensure_jst(value).date()
    return value.isoformat()


def previous_weekday(base_date: date) -> date:
    day = base_date - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day

