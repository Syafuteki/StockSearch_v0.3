from __future__ import annotations

import io
import zipfile
from datetime import date

from jpswing.intel.edinet_xbrl import extract_xbrl_key_facts
from jpswing.intel.search import DefaultIntelSearchBackend


def _zip_payload(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _sample_xbrl() -> str:
    return """<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl
  xmlns:xbrli="http://www.xbrl.org/2003/instance"
  xmlns:jppfs_cor="http://disclosure.edinet-fsa.go.jp/taxonomy/jppfs/2023-11-01/jppfs_cor">
  <xbrli:context id="CurrentYearDuration">
    <xbrli:entity>
      <xbrli:identifier scheme="http://example.com">E00000</xbrli:identifier>
    </xbrli:entity>
    <xbrli:period>
      <xbrli:startDate>2025-04-01</xbrli:startDate>
      <xbrli:endDate>2026-03-31</xbrli:endDate>
    </xbrli:period>
  </xbrli:context>
  <xbrli:context id="CurrentYearInstant">
    <xbrli:entity>
      <xbrli:identifier scheme="http://example.com">E00000</xbrli:identifier>
    </xbrli:entity>
    <xbrli:period>
      <xbrli:instant>2026-03-31</xbrli:instant>
    </xbrli:period>
  </xbrli:context>
  <jppfs_cor:NetSales contextRef="CurrentYearDuration" unitRef="JPY">1234567890</jppfs_cor:NetSales>
  <jppfs_cor:OperatingIncome contextRef="CurrentYearDuration" unitRef="JPY">234567890</jppfs_cor:OperatingIncome>
  <jppfs_cor:Assets contextRef="CurrentYearInstant" unitRef="JPY">5000000000</jppfs_cor:Assets>
</xbrli:xbrl>
"""


class _DummyEdinetClient:
    def __init__(self, payload: bytes | dict[int, bytes]) -> None:
        self.base_url = "https://api.edinet-fsa.go.jp"
        if isinstance(payload, bytes):
            self.payload_by_type = {5: payload}
        else:
            self.payload_by_type = payload
        self.calls: list[tuple[str, int]] = []

    def download_document(self, doc_id: str, file_type: int = 5) -> bytes:
        self.calls.append((doc_id, file_type))
        return self.payload_by_type.get(file_type, b"")


def test_extract_xbrl_key_facts_from_zip_instance() -> None:
    payload = _zip_payload({"XBRL/PublicDoc/jpcrp030000-asr-001_E00000-000_2026-03-31_01_2026-06-30.xbrl": _sample_xbrl()})
    facts = extract_xbrl_key_facts(payload, limit=6)
    assert facts
    assert any("1,234,567,890" in f for f in facts)
    assert any("234,567,890" in f for f in facts)
    assert any("2026-03-31" in f for f in facts)


def test_default_intel_search_includes_xbrl_facts() -> None:
    payload = _zip_payload({"XBRL/PublicDoc/main.xbrl": _sample_xbrl()})
    backend = DefaultIntelSearchBackend(
        edinet_client=_DummyEdinetClient(payload),
        whitelist_domains=["api.edinet-fsa.go.jp"],
        company_ir_domains={},
        timeout_sec=5,
        max_items_per_symbol=5,
    )
    rows = backend.fetch(
        code="36790",
        business_date=date(2026, 2, 13),
        seed={"edinet_docs": [{"docID": "S100TEST", "docDescription": "Test filing", "submitDate": "2026-02-13"}]},
    )
    assert len(rows) == 1
    assert rows[0].xbrl_facts
    assert "XBRL key facts:" in rows[0].snippet
    assert rows[0].full_text


def test_default_intel_search_fallbacks_to_next_file_type_when_primary_empty() -> None:
    payload = _zip_payload({"XBRL/PublicDoc/main.xbrl": _sample_xbrl()})
    dummy = _DummyEdinetClient({5: b"", 1: payload})
    backend = DefaultIntelSearchBackend(
        edinet_client=dummy,
        whitelist_domains=["api.edinet-fsa.go.jp"],
        company_ir_domains={},
        timeout_sec=5,
        max_items_per_symbol=5,
        edinet_file_types=[5, 1],
    )
    rows = backend.fetch(
        code="36790",
        business_date=date(2026, 2, 13),
        seed={"edinet_docs": [{"docID": "S100TEST", "docDescription": "Test filing", "submitDate": "2026-02-13"}]},
    )
    assert len(rows) == 1
    assert dummy.calls == [("S100TEST", 5), ("S100TEST", 1)]
    assert rows[0].source_url.endswith("type=1")
    assert rows[0].xbrl_facts
    assert rows[0].full_text


def test_default_intel_search_fallbacks_when_primary_is_binaryish() -> None:
    primary_binary = b"%PDF-1.7\\n1 0 obj\\n<< /Type /Catalog >>\\nendobj\\n"
    secondary_html = _zip_payload(
        {
            "PublicDoc/main.htm": (
                "<html><body><h1>提出書類</h1><p>自己株式の取得状況を報告します。"
                "取得株数と取得価額の内訳を記載しています。</p></body></html>"
            )
        }
    )
    dummy = _DummyEdinetClient({5: primary_binary, 1: secondary_html})
    backend = DefaultIntelSearchBackend(
        edinet_client=dummy,
        whitelist_domains=["api.edinet-fsa.go.jp"],
        company_ir_domains={},
        timeout_sec=5,
        max_items_per_symbol=5,
        edinet_file_types=[5, 1],
    )
    rows = backend.fetch(
        code="36790",
        business_date=date(2026, 2, 13),
        seed={"edinet_docs": [{"docID": "S100TEST", "docDescription": "Test filing", "submitDate": "2026-02-13"}]},
    )
    assert len(rows) == 1
    assert dummy.calls == [("S100TEST", 5), ("S100TEST", 1)]
    assert rows[0].source_url.endswith("type=1")
    assert rows[0].full_text


def test_default_intel_search_fallbacks_when_primary_is_error_page() -> None:
    primary_error = b"<html><body><h1>403 Forbidden</h1><p>access denied</p></body></html>"
    secondary_html = _zip_payload(
        {
            "PublicDoc/main.htm": (
                "<html><body><h1>Disclosure</h1>"
                "<p>This filing includes buyback details and timeline.</p></body></html>"
            )
        }
    )
    dummy = _DummyEdinetClient({5: primary_error, 1: secondary_html})
    backend = DefaultIntelSearchBackend(
        edinet_client=dummy,
        whitelist_domains=["api.edinet-fsa.go.jp"],
        company_ir_domains={},
        timeout_sec=5,
        max_items_per_symbol=5,
        edinet_file_types=[5, 1],
    )
    rows = backend.fetch(
        code="36790",
        business_date=date(2026, 2, 13),
        seed={"edinet_docs": [{"docID": "S100TEST", "docDescription": "Test filing", "submitDate": "2026-02-13"}]},
    )
    assert len(rows) == 1
    assert dummy.calls == [("S100TEST", 5), ("S100TEST", 1)]
    assert rows[0].source_url.endswith("type=1")
    assert "buyback details" in rows[0].snippet
    assert rows[0].full_text


def test_default_intel_search_skips_json_error_payload_even_when_status_is_200() -> None:
    # Simulate EDINET returning JSON error body for a document type that should be binary.
    primary_json_error = b'{"message":"Not Found\\u985e\\u3092\\u53d6\\u5f97\\u3059\\u308b\\u305f\\u3081\\u306eAPI"}'
    secondary_html = _zip_payload(
        {
            "PublicDoc/main.htm": (
                "<html><body><h1>Disclosure</h1>"
                "<p>This filing contains concrete timeline and revision details.</p></body></html>"
            )
        }
    )
    dummy = _DummyEdinetClient({2: primary_json_error, 1: secondary_html})
    backend = DefaultIntelSearchBackend(
        edinet_client=dummy,
        whitelist_domains=["api.edinet-fsa.go.jp"],
        company_ir_domains={},
        timeout_sec=5,
        max_items_per_symbol=5,
        edinet_file_types=[2, 1],
    )
    rows = backend.fetch(
        code="36790",
        business_date=date(2026, 2, 13),
        seed={"edinet_docs": [{"docID": "S100TEST", "docDescription": "Test filing", "submitDate": "2026-02-13"}]},
    )
    assert len(rows) == 1
    assert dummy.calls == [("S100TEST", 2), ("S100TEST", 1)]
    assert rows[0].source_url.endswith("type=1")
    assert "timeline" in rows[0].snippet
