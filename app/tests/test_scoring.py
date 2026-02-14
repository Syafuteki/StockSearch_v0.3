import pandas as pd

from jpswing.screening.step2 import screen_top30


def test_screen_top30_scores_and_filters() -> None:
    df = pd.DataFrame(
        [
            {
                "trade_date": "2026-02-13",
                "code": "1111",
                "adj_close": 1200.0,
                "ma25": 1100.0,
                "ma75": 1000.0,
                "ma75_slope_5": 20.0,
                "roc20": 0.12,
                "roc60": 0.18,
                "volume_ratio20": 1.8,
                "breakout_strength20": 0.02,
                "volatility_penalty": 0.03,
                "rsi14": 65.0,
            },
            {
                "trade_date": "2026-02-13",
                "code": "2222",
                "adj_close": 1000.0,
                "ma25": 1010.0,  # trend fail
                "ma75": 990.0,
                "ma75_slope_5": 10.0,
                "roc20": 0.15,
                "roc60": 0.20,
                "volume_ratio20": 2.0,
                "breakout_strength20": 0.03,
                "volatility_penalty": 0.02,
                "rsi14": 70.0,
            },
            {
                "trade_date": "2026-02-13",
                "code": "3333",
                "adj_close": 1500.0,
                "ma25": 1400.0,
                "ma75": 1300.0,
                "ma75_slope_5": 12.0,
                "roc20": 0.05,
                "roc60": 0.08,
                "volume_ratio20": 1.1,
                "breakout_strength20": 0.01,
                "volatility_penalty": 0.04,
                "rsi14": 82.0,  # penalty
            },
        ]
    )
    rules = {
        "step2": {
            "top_n": 30,
            "filters": {
                "trend_required": True,
                "breakout_min": -0.01,
                "rsi_overheat": 80,
                "rsi_penalty": 0.5,
            },
            "weights": {
                "roc20": 1.0,
                "roc60": 1.0,
                "volume_ratio": 1.0,
                "breakout_strength": 1.0,
                "volatility_penalty": 1.0,
            },
        }
    }
    out = screen_top30(df, {"1111", "2222", "3333"}, rules)
    assert set(out["code"]) == {"1111", "3333"}
    assert int(out.iloc[0]["rank"]) == 1
    assert out.iloc[0]["score"] > out.iloc[1]["score"]

