from __future__ import annotations

from typing import Any

from jpswing.ingest.normalize import pick_first, to_date, to_float, to_int


def normalize_instrument_row(row: dict[str, Any]) -> dict[str, Any] | None:
    code = pick_first(row, ["Code", "code", "LocalCode", "Ticker", "IssueCode"])
    if code is None:
        return None
    return {
        "code": str(code).strip(),
        "name": pick_first(row, ["CompanyName", "Name", "name", "IssueName"]),
        "market": pick_first(row, ["MarketCodeName", "MarketName", "MarketCode", "market"]),
        "issued_shares": to_int(pick_first(row, ["IssuedShares", "issued_shares", "NumberOfIssuedAndOutstandingSharesAtTheEnd"])),
        "market_cap": to_float(pick_first(row, ["MarketCapitalization", "market_cap", "MarketCap"])),
        "raw_json": row,
    }


def normalize_bar_row(row: dict[str, Any]) -> dict[str, Any] | None:
    code = pick_first(row, ["Code", "code", "LocalCode", "Ticker", "IssueCode"])
    trade_date = to_date(pick_first(row, ["Date", "date", "TradeDate", "TargetDate"]))
    if code is None or trade_date is None:
        return None
    open_price = to_float(pick_first(row, ["Open", "OpenPrice", "open", "opening_price", "O"]))
    high_price = to_float(pick_first(row, ["High", "high", "HighPrice", "H"]))
    low_price = to_float(pick_first(row, ["Low", "low", "LowPrice", "L"]))
    close_price = to_float(pick_first(row, ["Close", "close", "ClosePrice", "C"]))
    adj_close = to_float(
        pick_first(
            row,
            [
                "AdjustmentClose",
                "AdjustedClose",
                "AdjClose",
                "adjusted_close",
                "close_adjusted",
                "AdjC",
            ],
        )
    )
    if adj_close is None:
        adj_close = close_price
    return {
        "trade_date": trade_date,
        "code": str(code).strip(),
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "adj_close": adj_close,
        "volume": to_int(pick_first(row, ["Volume", "volume", "TradingVolume", "Vo", "AdjVo"])),
        "market_cap": to_float(pick_first(row, ["MarketCapitalization", "market_cap", "MarketCap"])),
        "raw_json": row,
    }


def normalize_index_row(row: dict[str, Any]) -> dict[str, Any] | None:
    code = pick_first(row, ["Code", "code", "IndexCode", "Symbol"])
    trade_date = to_date(pick_first(row, ["Date", "date", "TradeDate"]))
    if trade_date is None:
        return None
    close_price = to_float(pick_first(row, ["Close", "close", "Value", "IndexValue"]))
    open_price = to_float(pick_first(row, ["Open", "open"]))
    return {
        "trade_date": trade_date,
        "code": str(code).strip() if code is not None else None,
        "open": open_price,
        "close": close_price,
        "raw_json": row,
    }
