from jpswing.ingest.transformers import normalize_instrument_row


def test_normalize_instrument_row_supports_jquants_master_v2_keys() -> None:
    row = {
        "Date": "2026-02-13",
        "Code": "60850",
        "CoName": "ARCHITECTS STUDIO JAPAN",
        "MktNm": "Growth",
    }
    out = normalize_instrument_row(row)
    assert out is not None
    assert out["code"] == "60850"
    assert out["name"] == "ARCHITECTS STUDIO JAPAN"
    assert out["market"] == "Growth"
