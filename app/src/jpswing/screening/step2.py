from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def _zscore(series: pd.Series) -> pd.Series:
    std = series.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series([0.0] * len(series), index=series.index, dtype=float)
    return (series - series.mean()) / std


def screen_top30(
    latest_features_df: pd.DataFrame,
    universe_codes: set[str],
    rules: dict[str, Any],
) -> pd.DataFrame:
    if latest_features_df.empty or not universe_codes:
        return pd.DataFrame()

    step2 = rules.get("step2", {})
    filter_cfg = step2.get("filters", {})
    weight_cfg = step2.get("weights", {})
    top_n = int(step2.get("top_n", 30))

    df = latest_features_df[latest_features_df["code"].isin(universe_codes)].copy()
    if df.empty:
        return df

    trend_required = bool(filter_cfg.get("trend_required", True))
    breakout_min = float(filter_cfg.get("breakout_min", -0.01))
    rsi_overheat = float(filter_cfg.get("rsi_overheat", 80.0))
    rsi_penalty = float(filter_cfg.get("rsi_penalty", 0.5))

    if trend_required:
        trend_mask = (
            (df["adj_close"] > df["ma25"])
            & (df["ma25"] > df["ma75"])
            & (df["ma75_slope_5"] > 0)
        )
        df = df[trend_mask].copy()
    if df.empty:
        return df

    df = df[df["breakout_strength20"].fillna(-999) >= breakout_min].copy()
    if df.empty:
        return df

    z_roc20 = _zscore(df["roc20"].fillna(0.0))
    z_roc60 = _zscore(df["roc60"].fillna(0.0))
    z_vol_ratio = _zscore(df["volume_ratio20"].fillna(0.0))
    z_breakout = _zscore(df["breakout_strength20"].fillna(0.0))
    z_vol_penalty = _zscore(df["volatility_penalty"].fillna(0.0))

    w_roc20 = float(weight_cfg.get("roc20", 1.0))
    w_roc60 = float(weight_cfg.get("roc60", 1.0))
    w_volume_ratio = float(weight_cfg.get("volume_ratio", 1.0))
    w_breakout = float(weight_cfg.get("breakout_strength", 1.0))
    w_vol_penalty = float(weight_cfg.get("volatility_penalty", 1.0))

    score = (
        (w_roc20 * z_roc20)
        + (w_roc60 * z_roc60)
        + (w_volume_ratio * z_vol_ratio)
        + (w_breakout * z_breakout)
        - (w_vol_penalty * z_vol_penalty)
    )
    overheat_penalty = (df["rsi14"] > rsi_overheat).astype(float) * rsi_penalty
    df["score"] = score - overheat_penalty

    df["score_breakdown"] = [
        {
            "z_roc20": float(a),
            "z_roc60": float(b),
            "z_volume_ratio20": float(c),
            "z_breakout_strength20": float(d),
            "z_volatility_penalty": float(e),
            "rsi14": float(r) if pd.notna(r) else None,
            "overheat_penalty": float(op),
        }
        for a, b, c, d, e, r, op in zip(
            z_roc20.tolist(),
            z_roc60.tolist(),
            z_vol_ratio.tolist(),
            z_breakout.tolist(),
            z_vol_penalty.tolist(),
            df["rsi14"].tolist(),
            overheat_penalty.tolist(),
        )
    ]

    df = df.sort_values("score", ascending=False).head(top_n).copy()
    df["rank"] = list(range(1, len(df) + 1))
    return df

