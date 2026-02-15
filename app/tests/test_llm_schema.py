from jpswing.llm.validator import validate_llm_output


def test_validate_llm_output_success() -> None:
    content = """
    {
      "top10": [
        {
          "code": "72030",
          "top10_rank": 1,
          "thesis_bull": ["出来高増加とトレンド継続が追い風"],
          "thesis_bear": ["短期過熱で反落リスク"],
          "key_levels": {
            "entry_idea": "前日高値ブレイク",
            "stop_idea": "直近安値割れ",
            "takeprofit_idea": "2R到達"
          },
          "event_risks": ["決算"],
          "confidence_0_100": 72,
          "data_gaps": [],
          "rule_suggestion": null
        }
      ]
    }
    """
    model, err, payload = validate_llm_output(content)
    assert err is None
    assert model is not None
    assert payload is not None
    assert model.top10[0].code == "72030"


def test_validate_llm_output_rejects_placeholder_values() -> None:
    content = """
    {
      "top10": [
        {
          "code": "72030",
          "top10_rank": 1,
          "thesis_bull": [],
          "thesis_bear": ["N/A"],
          "key_levels": {
            "entry_idea": "N/A",
            "stop_idea": "N/A",
            "takeprofit_idea": "N/A"
          },
          "event_risks": [],
          "confidence_0_100": 80,
          "data_gaps": [],
          "rule_suggestion": null
        }
      ]
    }
    """
    model, err, payload = validate_llm_output(content)
    assert model is None
    assert err is not None
    assert payload is not None


def test_validate_llm_output_accepts_gpt_oss_control_prefix() -> None:
    content = """<|channel|>final <|constrain|>json<|message|>{"top10":[{"code":"72030","top10_rank":1,"thesis_bull":["上昇継続"],"thesis_bear":["急落リスク"],"key_levels":{"entry_idea":"前日高値ブレイク","stop_idea":"直近安値割れ","takeprofit_idea":"2R到達"},"event_risks":[],"confidence_0_100":70,"data_gaps":[],"rule_suggestion":null}]}"""
    model, err, payload = validate_llm_output(content)
    assert err is None
    assert model is not None
    assert payload is not None
    assert model.top10[0].code == "72030"
