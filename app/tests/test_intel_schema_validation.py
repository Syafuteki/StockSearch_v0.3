from jpswing.intel.schema import validate_intel_payload


def test_intel_schema_validation_rejects_missing_required_fields() -> None:
    payload = {
        "headline": "test",
        "facts": [],
        "tags": [],
        "risk_flags": [],
        "critical_risk": False,
        "evidence_refs": [],
        "data_gaps": [],
    }
    result = validate_intel_payload(payload)
    assert not result.valid
    assert result.payload is None
    assert result.error is not None


def test_intel_schema_validation_accepts_valid_payload() -> None:
    payload = {
        "headline": "test",
        "summary": "ok",
        "facts": ["fact1"],
        "tags": ["buyback"],
        "risk_flags": [],
        "critical_risk": False,
        "evidence_refs": ["https://example.com"],
        "data_gaps": [],
    }
    result = validate_intel_payload(payload)
    assert result.valid
    assert result.payload is not None
    assert result.error is None
