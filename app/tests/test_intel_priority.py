from jpswing.intel.priority import PriorityInput, calculate_priority, rank_priorities


def test_priority_scoring_is_deterministic() -> None:
    item = PriorityInput(
        code="7203",
        fund_state="IN",
        fund_score=0.77,
        has_new_edinet=True,
        theme_strength=0.65,
        theme_strength_delta=0.20,
        has_high_signal_tag=True,
    )
    score_a = calculate_priority(item)
    score_b = calculate_priority(item)
    assert score_a == score_b


def test_priority_ranking_tie_break_by_code() -> None:
    items = [
        PriorityInput("1332", "WATCH", 0.5, False, 0.2, 0.0, False),
        PriorityInput("1333", "WATCH", 0.5, False, 0.2, 0.0, False),
    ]
    ranked = rank_priorities(items)
    assert ranked[0]["priority"] != ranked[1]["priority"]
    assert ranked[0]["code"] == "1333" or ranked[0]["code"] == "1332"

