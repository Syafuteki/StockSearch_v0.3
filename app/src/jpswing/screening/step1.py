from __future__ import annotations

from typing import Any

import pandas as pd


def _is_common_stock(name: str | None, raw: dict[str, Any] | None, require_common: bool) -> bool:
    if not require_common:
        return True
    txt = (name or "").upper()
    ng_keywords = ["ETF", "ETN", "REIT", "優先", "INVESTMENT", "投資法人"]
    if any(k in txt for k in ng_keywords):
        return False
    if raw:
        sec_type = str(
            raw.get("SecurityType")
            or raw.get("security_type")
            or raw.get("Type")
            or raw.get("IssueType")
            or ""
        ).upper()
        if any(k in sec_type for k in ["ETF", "ETN", "REIT", "PREFERRED"]):
            return False
    return True


def build_universe(
    latest_bars_df: pd.DataFrame,
    instruments_df: pd.DataFrame,
    rules: dict[str, Any],
    *,
    use_adj_close: bool = True,
) -> pd.DataFrame:
    if latest_bars_df.empty:
        return pd.DataFrame()

    step1 = rules.get("step1", {})
    min_price = float(step1.get("min_price", 300))
    min_volume = int(step1.get("min_volume", 200_000))
    min_traded_value = float(step1.get("min_traded_value", 60_000_000))
    min_market_cap = float(step1.get("min_market_cap", 10_000_000_000))
    market_cap_filter = bool(step1.get("market_cap_filter", True))
    allow_disable_market_cap_filter = bool(step1.get("allow_disable_market_cap_filter", True))
    require_common_stock = bool(step1.get("require_common_stock", True))

    bars = latest_bars_df.copy()
    master_cols = ["code", "name", "market", "issued_shares", "market_cap", "raw_json"]
    master = instruments_df[master_cols].copy() if not instruments_df.empty else pd.DataFrame(columns=master_cols)
    merged = bars.merge(master, on="code", how="left", suffixes=("", "_m"))

    price_col = "adj_close" if use_adj_close else "close"
    merged["price_for_filter"] = merged[price_col].fillna(merged["close"])
    merged["traded_value"] = merged["price_for_filter"] * merged["volume"].fillna(0)

    raw_market_cap = merged["market_cap"].fillna(merged["market_cap_m"])
    issued = merged["issued_shares"].fillna(0)
    estimated_market_cap = merged["price_for_filter"] * issued
    market_cap_estimated = raw_market_cap.isna() & estimated_market_cap.notna() & (issued > 0)
    merged["market_cap_effective"] = raw_market_cap.fillna(estimated_market_cap)
    merged["market_cap_estimated"] = market_cap_estimated

    has_market_cap_data = merged["market_cap_effective"].notna().any()
    market_cap_filter_active = market_cap_filter and (has_market_cap_data or not allow_disable_market_cap_filter)

    merged["is_common_stock"] = merged.apply(
        lambda r: _is_common_stock(r.get("name"), r.get("raw_json"), require_common_stock),
        axis=1,
    )
    merged["pass_price"] = merged["price_for_filter"] >= min_price
    merged["pass_volume"] = merged["volume"].fillna(0) >= min_volume
    merged["pass_traded_value"] = merged["traded_value"].fillna(0) >= min_traded_value
    if market_cap_filter_active:
        merged["pass_market_cap"] = merged["market_cap_effective"].fillna(0) >= min_market_cap
    else:
        merged["pass_market_cap"] = True

    merged["passed"] = (
        merged["is_common_stock"]
        & merged["pass_price"]
        & merged["pass_volume"]
        & merged["pass_traded_value"]
        & merged["pass_market_cap"]
    )
    passed = merged[merged["passed"]].copy()
    if passed.empty:
        return passed

    passed["details_json"] = passed.apply(
        lambda r: {
            "price_for_filter": r.get("price_for_filter"),
            "volume": r.get("volume"),
            "traded_value": r.get("traded_value"),
            "market_cap": r.get("market_cap_effective"),
            "market_cap_estimated": bool(r.get("market_cap_estimated")),
            "market_cap_filter_active": market_cap_filter_active,
        },
        axis=1,
    )
    return passed

