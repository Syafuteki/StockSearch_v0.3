from __future__ import annotations

import numpy as np
import pandas as pd


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def compute_features(bars_df: pd.DataFrame, *, use_adj_close: bool = True) -> pd.DataFrame:
    required_cols = {"trade_date", "code", "open", "high", "low", "close", "adj_close", "volume"}
    missing = required_cols - set(bars_df.columns)
    if missing:
        raise ValueError(f"bars_df missing columns: {sorted(missing)}")
    if bars_df.empty:
        return bars_df.copy()

    df = bars_df.copy()
    df = df.sort_values(["code", "trade_date"]).reset_index(drop=True)

    price_col = "adj_close" if use_adj_close else "close"
    group = df.groupby("code", sort=False)
    df["ma10"] = group[price_col].transform(lambda s: s.rolling(10, min_periods=10).mean())
    df["ma25"] = group[price_col].transform(lambda s: s.rolling(25, min_periods=25).mean())
    df["ma75"] = group[price_col].transform(lambda s: s.rolling(75, min_periods=75).mean())
    df["ma75_slope_5"] = group["ma75"].transform(lambda s: s - s.shift(5))
    df["roc20"] = group[price_col].transform(lambda s: s.pct_change(20))
    df["roc60"] = group[price_col].transform(lambda s: s.pct_change(60))
    df["rsi14"] = group[price_col].transform(_rsi)

    prev_close = group[price_col].shift(1)
    tr_1 = (df["high"] - df["low"]).abs()
    tr_2 = (df["high"] - prev_close).abs()
    tr_3 = (df["low"] - prev_close).abs()
    df["tr"] = pd.concat([tr_1, tr_2, tr_3], axis=1).max(axis=1)
    df["atr14"] = group["tr"].transform(lambda s: s.rolling(14, min_periods=14).mean())

    df["volume_sma20"] = group["volume"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["volume_ratio20"] = df["volume"] / df["volume_sma20"].replace(0.0, np.nan)

    rolling_high20 = group["high"].transform(lambda s: s.rolling(20, min_periods=20).max())
    df["breakout_strength20"] = (df[price_col] / rolling_high20.replace(0.0, np.nan)) - 1.0
    df["volatility_penalty"] = df["atr14"] / df[price_col].replace(0.0, np.nan)
    return df.drop(columns=["tr"])

