from jpswing.llm.validator import validate_llm_output


def test_validate_llm_output_success() -> None:
    content = """
    {
      "top10": [
        {
          "code": "7203",
          "top10_rank": 1,
          "thesis_bull": ["上昇トレンド継続"],
          "thesis_bear": ["決算リスク"],
          "key_levels": {
            "entry_idea": "前日高値ブレイク",
            "stop_idea": "ATR1.5下",
            "takeprofit_idea": "2R目安"
          },
          "event_risks": ["決算"],
          "confidence_0_100": 72,
          "data_gaps": [],
          "rule_suggestion": "RSI過熱時の減点強化"
        }
      ]
    }
    """
    model, err, payload = validate_llm_output(content)
    assert err is None
    assert model is not None
    assert payload is not None
    assert model.top10[0].code == "7203"


def test_validate_llm_output_invalid() -> None:
    model, err, payload = validate_llm_output('{"top10":[{"code":"7203"}]}')
    assert model is None
    assert err is not None
    assert payload is not None

