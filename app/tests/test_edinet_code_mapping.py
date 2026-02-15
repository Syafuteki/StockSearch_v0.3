from jpswing.fund_intel_orchestrator import _edinet_code


def test_edinet_code_keeps_five_digit_local_code() -> None:
    doc = {"secCode": "63640"}
    assert _edinet_code(doc) == "63640"


def test_edinet_code_pads_four_digit_to_local_format() -> None:
    doc = {"secCode": "7203"}
    assert _edinet_code(doc) == "72030"


def test_edinet_code_supports_alnum_code() -> None:
    doc = {"secCode": "141A0"}
    assert _edinet_code(doc) == "141A0"
