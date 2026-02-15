from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from jpswing.features.indicators import compute_features


def test_compute_features_handles_none_values_without_crash() -> None:
    rows: list[dict[str, object]] = []
    base = date(2026, 1, 1)
    for i in range(25):
        close = 100.0 + i
        adj = close if i != 10 else None
        rows.append(
            {
                "trade_date": base + timedelta(days=i),
                "code": "7203",
                "open": close - 1.0,
                "high": close + 1.0,
                "low": close - 2.0,
                "close": close,
                "adj_close": adj,
                "volume": 100000 + i * 100,
            }
        )
    df = pd.DataFrame(rows)
    out = compute_features(df, use_adj_close=True)
    assert not out.empty
    assert "rsi14" in out.columns
