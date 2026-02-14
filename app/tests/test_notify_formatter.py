from datetime import date

import pandas as pd

from jpswing.notify.formatter import format_report_message


def test_format_report_message_contains_disclaimer_and_title() -> None:
    top10_df = pd.DataFrame(
        [
            {
                "code": "7203",
                "name": "トヨタ自動車",
                "rank": 1,
                "ma25": 2900.0,
                "roc20": 0.08,
                "volume_ratio20": 1.25,
                "breakout_strength20": 0.02,
            }
        ]
    )
    llm_map = {
        "7203": {
            "thesis_bull": ["出来高伴う上放れ"],
            "thesis_bear": ["地合い悪化"],
            "key_levels": {
                "entry_idea": "前日高値超え",
                "stop_idea": "ATR下",
                "takeprofit_idea": "2R",
            },
            "event_risks": ["決算"],
            "confidence_0_100": 70,
        }
    }
    msgs = format_report_message(
        report_date=date(2026, 2, 14),
        run_type="morning",
        top10_df=top10_df,
        llm_map=llm_map,
        event_summary={"earnings": 1, "margin_alert": 0, "short_sale_report": 0},
        disclaimer="免責文",
        max_chars=2000,
        max_parts=2,
    )
    assert msgs
    assert "今日の注目Top10" in msgs[0]
    assert "免責文" in msgs[-1]

