from jpswing.fund.service import _dedupe_financial_rows


def test_dedupe_financial_rows_keeps_last_row_per_code() -> None:
    rows = [
        {"Code": "60130", "v": 1},
        {"Code": "60130", "v": 2},
        {"code": "72030", "v": 3},
        {"foo": "bar"},
    ]
    deduped = _dedupe_financial_rows(rows)
    assert set(deduped.keys()) == {"60130", "72030"}
    assert deduped["60130"]["v"] == 2
    assert deduped["72030"]["v"] == 3
