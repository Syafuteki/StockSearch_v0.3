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
            "thesis_bull": ["出来高増加", "上方修正期待"],
            "thesis_bear": ["地合い悪化"],
            "key_levels": {
                "entry_idea": "前日高値ブレイク",
                "stop_idea": "ATR基準",
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
    assert "【1】7203 トヨタ自動車" in msgs[0]
    assert "免責文" in msgs[-1]


def test_format_report_message_displays_local_code_as_4digit() -> None:
    top10_df = pd.DataFrame(
        [
            {
                "code": "60850",
                "name": "ＡＳＪ",
                "rank": 1,
                "ma25": 1000.0,
                "roc20": 0.1,
                "volume_ratio20": 2.0,
                "breakout_strength20": 0.05,
            }
        ]
    )
    msgs = format_report_message(
        report_date=date(2026, 2, 14),
        run_type="morning",
        top10_df=top10_df,
        llm_map={},
        event_summary={"earnings": 0, "margin_alert": 0, "short_sale_report": 0},
        disclaimer="免責文",
        max_chars=2000,
        max_parts=2,
    )
    assert msgs
    assert "【1】6085 ＡＳＪ" in msgs[0]


def test_format_report_message_fallbacks_when_llm_values_are_empty() -> None:
    top10_df = pd.DataFrame(
        [
            {
                "code": "72030",
                "name": "Sample",
                "rank": 1,
                "ma25": 1.0,
                "roc20": 0.1,
                "volume_ratio20": 1.2,
                "breakout_strength20": 0.01,
            }
        ]
    )
    llm_map = {
        "72030": {
            "thesis_bull": [],
            "thesis_bear": [""],
            "key_levels": {"entry_idea": "N/A", "stop_idea": "", "takeprofit_idea": "unknown"},
            "confidence_0_100": 80,
        }
    }
    msgs = format_report_message(
        report_date=date(2026, 2, 14),
        run_type="morning",
        top10_df=top10_df,
        llm_map=llm_map,
        event_summary={"earnings": 0, "margin_alert": 0, "short_sale_report": 0},
        disclaimer="免責文",
        max_chars=2000,
        max_parts=2,
    )
    assert msgs
    assert "上昇シナリオ: 未取得" in msgs[0]
    assert "下落シナリオ: 未取得" in msgs[0]
    assert "目安: エントリー=未取得 / 利確=未取得 / 損切り=未取得" in msgs[0]


def test_format_report_message_shows_fallback_note_for_tech() -> None:
    top10_df = pd.DataFrame(
        [
            {
                "code": "72030",
                "name": "Sample",
                "rank": 1,
                "ma25": 1.0,
                "roc20": 0.1,
                "volume_ratio20": 1.2,
                "breakout_strength20": 0.01,
            }
        ]
    )
    llm_map = {
        "72030": {
            "thesis_bull": ["上昇余地あり"],
            "thesis_bear": ["反落リスク"],
            "key_levels": {"entry_idea": "高値更新", "stop_idea": "直近安値割れ", "takeprofit_idea": "2R"},
            "confidence_0_100": 65,
            "data_gaps": ["llm_output_invalid_or_missing"],
        }
    }
    msgs = format_report_message(
        report_date=date(2026, 2, 14),
        run_type="morning",
        top10_df=top10_df,
        llm_map=llm_map,
        event_summary={"earnings": 0, "margin_alert": 0, "short_sale_report": 0},
        disclaimer="免責文",
        max_chars=2000,
        max_parts=2,
    )
    assert msgs
    assert "備考: LLMフォールバック結果" in msgs[0]
