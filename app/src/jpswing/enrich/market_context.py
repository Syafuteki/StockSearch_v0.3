from __future__ import annotations

from datetime import date
from typing import Any

from jpswing.ingest.normalize import pick_first, to_date, to_float


def parse_index_row(row: dict[str, Any]) -> dict[str, Any]:
    code = pick_first(row, ["Code", "code", "IndexCode", "Symbol"])
    name = pick_first(row, ["Name", "name", "IndexName"])
    trade_date = to_date(pick_first(row, ["Date", "date", "TradeDate"]))
    close = to_float(pick_first(row, ["Close", "close", "Value", "IndexValue"]))
    open_price = to_float(pick_first(row, ["Open", "open"]))
    return {
        "code": str(code) if code is not None else None,
        "name": str(name) if name is not None else None,
        "date": trade_date.isoformat() if trade_date else None,
        "open": open_price,
        "close": close,
        "change_pct": ((close / open_price) - 1.0) if close and open_price else None,
        "raw": row,
    }

