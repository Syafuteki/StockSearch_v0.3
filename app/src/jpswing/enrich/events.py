from __future__ import annotations

from datetime import date
from typing import Any

from jpswing.ingest.normalize import pick_first, to_date


def extract_code(row: dict[str, Any]) -> str | None:
    code = pick_first(
        row,
        [
            "Code",
            "code",
            "LocalCode",
            "IssueCode",
            "SecurityCode",
            "Ticker",
            "銘柄コード",
        ],
    )
    if code is None:
        return None
    return str(code).strip()


def collect_events_for_codes(
    *,
    trade_date: date,
    codes: list[str],
    earnings_rows: list[dict[str, Any]],
    margin_rows: list[dict[str, Any]],
    short_sale_rows: list[dict[str, Any]],
    short_ratio_rows: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], dict[str, int]]:
    code_set = set(codes)
    event_map: dict[str, list[dict[str, Any]]] = {code: [] for code in codes}
    db_rows: list[dict[str, Any]] = []
    summary = {
        "earnings": 0,
        "margin_alert": 0,
        "short_sale_report": 0,
        "short_ratio": 0,
    }

    def push(event_type: str, row: dict[str, Any], code: str | None) -> None:
        db_rows.append(
            {
                "trade_date": trade_date,
                "code": code,
                "event_type": event_type,
                "payload_json": row,
            }
        )
        if code and code in event_map:
            event_map[code].append({"event_type": event_type, "payload": row})

    for row in earnings_rows:
        code = extract_code(row)
        event_date = to_date(
            row.get("DisclosedDate") or row.get("AnnouncementDate") or row.get("Date") or row.get("date")
        )
        if code in code_set:
            push("earnings_calendar", row, code)
            if event_date and (event_date == trade_date or event_date > trade_date):
                summary["earnings"] += 1
        else:
            push("earnings_calendar", row, code)

    for row in margin_rows:
        code = extract_code(row)
        push("margin_alert", row, code)
        if code in code_set:
            summary["margin_alert"] += 1

    for row in short_sale_rows:
        code = extract_code(row)
        push("short_sale_report", row, code)
        if code in code_set:
            summary["short_sale_report"] += 1

    for row in short_ratio_rows:
        sector = pick_first(row, ["Sector", "sector", "Industry", "industry"])
        code = extract_code(row)
        push("short_ratio", row, code)
        if sector or (code and code in code_set):
            summary["short_ratio"] += 1

    return event_map, db_rows, summary

