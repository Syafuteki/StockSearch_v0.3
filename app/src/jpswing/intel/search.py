from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from dataclasses import dataclass, field
from datetime import date
from typing import Any
from urllib.parse import urlparse

import httpx

from jpswing.ingest.edinet_client import EdinetClient
from jpswing.intel.edinet_xbrl import extract_xbrl_key_facts


@dataclass(slots=True)
class IntelSource:
    code: str
    source_url: str
    source_type: str
    headline: str
    published_at: str | None
    snippet: str
    evidence_refs: list[str]
    xbrl_facts: list[str] = field(default_factory=list)
    full_text: str = ""


def _domain_of(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def _safe_text(raw: str, limit: int = 600) -> str:
    txt = re.sub(r"\s+", " ", raw).strip()
    return txt[:limit]


def _looks_like_error_snippet(text: str) -> bool:
    t = str(text or "").strip().lower()
    if not t:
        return True
    if "wzek0130.aspx" in t:
        return True
    if any(token in t for token in ("403 forbidden", "404 not found", "access denied")):
        return True
    if "invalid_api_key" in t or "invalid api key" in t:
        return True
    if "subscription-key" in t and "required" in t:
        return True
    if t.startswith("<?xml") and "<error" in t:
        return True
    if "<html" in t and any(
        token in t
        for token in (
            "forbidden",
            "not found",
            "access denied",
            "service unavailable",
            "invalid_api_key",
            "invalid api key",
            "subscription-key",
            "error",
        )
    ):
        return True
    return False


def _has_substantive_snippet(snippet: str, headline: str) -> bool:
    s = str(snippet or "").strip()
    h = str(headline or "").strip()
    if not s:
        return False
    if _looks_like_error_snippet(s):
        return False
    if "\x00" in s:
        return False
    if s.startswith("%PDF-"):
        return False
    ctrl = sum(1 for ch in s if ord(ch) < 32 and ch not in {"\n", "\r", "\t"})
    if ctrl > 0:
        return False
    if not h:
        return len(s) >= 24
    if s == h:
        return False
    if s.startswith(h) and len(s) <= len(h) + 8:
        return False
    return len(s) >= 24


def _is_expected_edinet_payload(payload: bytes, file_type: int) -> bool:
    if not payload:
        return False
    if file_type == 2:
        return payload.startswith(b"%PDF-")
    if file_type in {1, 3, 4, 5}:
        return payload.startswith(b"PK")
    # Unknown type: keep previous behavior.
    return True


def _looks_like_edinet_api_error_payload(payload: bytes) -> bool:
    if not payload:
        return True
    head = payload[:12000]
    stripped = head.lstrip()
    if not stripped:
        return True
    if stripped.startswith((b"{", b"[")):
        txt = _decode_bytes(stripped)
        low = txt.lower()
        if any(
            token in low
            for token in (
                "not found",
                "invalid_api_key",
                "subscription-key",
                "access denied",
                "forbidden",
                "wzek0130.aspx",
            )
        ):
            return True
        try:
            obj = json.loads(txt)
        except Exception:
            return _looks_like_error_snippet(txt)
        if isinstance(obj, dict):
            msg = str(obj.get("message") or "")
            err = obj.get("error")
            code = str(obj.get("code") or "")
            detail = str(obj.get("detail") or "")
            probe = " ".join(
                [
                    msg,
                    str(err) if err is not None else "",
                    code,
                    detail,
                ]
            ).lower()
            if any(
                token in probe
                for token in (
                    "not found",
                    "invalid_api_key",
                    "subscription-key",
                    "forbidden",
                    "access denied",
                    "wzek0130.aspx",
                )
            ):
                return True
        return False
    if stripped.startswith(b"<"):
        txt = _decode_bytes(stripped)
        return _looks_like_error_snippet(txt)
    return False


def _decode_bytes(raw: bytes) -> str:
    if not raw:
        return ""

    # BOM-based UTF-16 payloads are common in EDINET CSV/text files.
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return raw.decode("utf-16")
        except Exception:
            pass

    # Heuristic: many NUL bytes usually indicate UTF-16 without BOM.
    if raw.count(b"\x00") > len(raw) // 8:
        for enc in ("utf-16-le", "utf-16-be"):
            try:
                txt = raw.decode(enc)
                if "\x00" not in txt:
                    return txt
            except Exception:
                continue

    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis", "euc_jp"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")


def _strip_markup(text: str) -> str:
    no_script = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", text)
    no_tag = re.sub(r"(?is)<[^>]+>", " ", no_script)
    return re.sub(r"\s+", " ", no_tag).strip()


def _extract_edinet_text(
    document_bytes: bytes,
    fallback_headline: str,
    *,
    limit: int,
    scan_target_chars: int,
) -> str:
    if not document_bytes:
        return _safe_text(fallback_headline, limit=limit)

    # EDINET document download often returns ZIP payload; extract text-like entries first.
    if document_bytes.startswith(b"PK"):
        try:
            per_file_limit = max(1200, min(30000, scan_target_chars))
            with zipfile.ZipFile(io.BytesIO(document_bytes)) as zf:
                names = sorted(
                    zf.namelist(),
                    key=lambda n: (
                        0 if "publicdoc" in n.lower() else 1,
                        0
                        if n.lower().endswith((".htm", ".html", ".xhtml"))
                        else 1
                        if n.lower().endswith(".txt")
                        else 2
                        if n.lower().endswith(".csv")
                        else 3
                        if n.lower().endswith(".xml")
                        else 9,
                        1 if any(tag in n.lower() for tag in ("_lab", "_pre", "_cal", "_def", ".xsd")) else 0,
                        n.lower(),
                    ),
                )
                text_chunks: list[str] = []
                for name in names:
                    lowered = name.lower()
                    if not lowered.endswith((".htm", ".html", ".xhtml", ".xml", ".txt", ".csv")):
                        continue
                    try:
                        raw = zf.read(name)
                    except Exception:
                        continue
                    if not raw:
                        continue
                    text = _decode_bytes(raw[:300_000])
                    cleaned = _strip_markup(text) if "<" in text and ">" in text else _safe_text(text, limit=per_file_limit)
                    if _looks_like_error_snippet(cleaned):
                        continue
                    if cleaned:
                        text_chunks.append(cleaned)
                    if sum(len(t) for t in text_chunks) >= scan_target_chars:
                        break
                if text_chunks:
                    return _safe_text(" ".join(text_chunks), limit=limit)
        except Exception:
            pass

    try:
        plain = _decode_bytes(document_bytes[:300_000])
        cleaned = _strip_markup(plain) if "<" in plain and ">" in plain else plain
        return _safe_text(cleaned, limit=limit)
    except Exception:
        return _safe_text(fallback_headline, limit=limit)


def extract_edinet_snippet(document_bytes: bytes, fallback_headline: str, limit: int = 600) -> str:
    return _extract_edinet_text(
        document_bytes,
        fallback_headline,
        limit=limit,
        scan_target_chars=1200,
    )


def extract_edinet_full_text(document_bytes: bytes, fallback_headline: str, limit: int = 6000) -> str:
    scan_target = min(max(2400, limit * 2), 30000)
    return _extract_edinet_text(
        document_bytes,
        fallback_headline,
        limit=limit,
        scan_target_chars=scan_target,
    )


class IntelSearchBackend:
    def fetch(self, *, code: str, business_date: date, seed: dict[str, Any]) -> list[IntelSource]:
        raise NotImplementedError


class DefaultIntelSearchBackend(IntelSearchBackend):
    def __init__(
        self,
        *,
        edinet_client: EdinetClient,
        whitelist_domains: list[str],
        company_ir_domains: dict[str, list[str]] | None = None,
        timeout_sec: int = 20,
        max_items_per_symbol: int = 5,
        edinet_file_types: list[int] | None = None,
        edinet_full_text_limit: int = 30000,
        ir_full_text_limit: int = 12000,
    ) -> None:
        self.edinet_client = edinet_client
        self.whitelist = {d.lower() for d in whitelist_domains}
        self.company_ir_domains = company_ir_domains or {}
        self.timeout_sec = timeout_sec
        self.max_items_per_symbol = max_items_per_symbol
        self.edinet_full_text_limit = max(1200, int(edinet_full_text_limit))
        self.ir_full_text_limit = max(800, int(ir_full_text_limit))
        raw_file_types = edinet_file_types or [5, 1, 2]
        self.edinet_file_types = [int(x) for x in raw_file_types if int(x) > 0]
        if not self.edinet_file_types:
            self.edinet_file_types = [5]
        self.logger = logging.getLogger(self.__class__.__name__)

    def fetch(self, *, code: str, business_date: date, seed: dict[str, Any]) -> list[IntelSource]:
        items: list[IntelSource] = []
        docs = seed.get("edinet_docs", [])
        for doc in docs:
            doc_id = str(doc.get("docID") or "").strip()
            if not doc_id:
                continue
            primary_file_type = self.edinet_file_types[0]
            url = f"{self.edinet_client.base_url}/api/v2/documents/{doc_id}?type={primary_file_type}"
            domain = _domain_of(url)
            if domain and self.whitelist and domain not in self.whitelist:
                continue

            headline = str(doc.get("docDescription") or doc.get("docTypeCode") or "EDINET filing")
            published = doc.get("submitDateTime") or doc.get("submitDate")
            snippet = _safe_text(headline)
            full_text = _safe_text(headline, limit=self.edinet_full_text_limit)
            xbrl_facts: list[str] = []
            used_file_type = primary_file_type
            tried_types: list[int] = []
            extracted = False

            for file_type in self.edinet_file_types:
                tried_types.append(file_type)
                try:
                    payload = self.edinet_client.download_document(doc_id, file_type=file_type)
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning(
                        "EDINET download error code=%s doc_id=%s type=%s err=%s",
                        code,
                        doc_id,
                        file_type,
                        exc,
                    )
                    continue
                if not payload:
                    self.logger.info(
                        "EDINET empty payload code=%s doc_id=%s type=%s",
                        code,
                        doc_id,
                        file_type,
                    )
                    continue
                if not _is_expected_edinet_payload(payload, file_type):
                    head_text = _safe_text(_decode_bytes(payload[:1200]), limit=180)
                    self.logger.warning(
                        "EDINET payload mismatch code=%s doc_id=%s type=%s magic=%s looks_error=%s head=%s",
                        code,
                        doc_id,
                        file_type,
                        payload[:8].hex(),
                        _looks_like_edinet_api_error_payload(payload),
                        head_text,
                    )
                    continue

                candidate_full_text = extract_edinet_full_text(
                    payload,
                    fallback_headline=headline,
                    limit=self.edinet_full_text_limit,
                )
                candidate_snippet = _safe_text(candidate_full_text, limit=600)
                candidate_xbrl = extract_xbrl_key_facts(payload, limit=6)
                snippet_ok = _has_substantive_snippet(candidate_snippet, headline)
                xbrl_ok = bool(candidate_xbrl)
                self.logger.info(
                    "EDINET extract try code=%s doc_id=%s type=%s snippet_ok=%s xbrl_ok=%s",
                    code,
                    doc_id,
                    file_type,
                    snippet_ok,
                    xbrl_ok,
                )
                if not snippet_ok and not xbrl_ok:
                    continue

                snippet = candidate_snippet
                full_text = candidate_full_text
                xbrl_facts = candidate_xbrl
                used_file_type = file_type
                extracted = True
                break

            if xbrl_facts:
                xbrl_text = " / ".join(xbrl_facts)
                snippet = _safe_text(f"{snippet} XBRL key facts: {xbrl_text}", limit=700)
            if not extracted:
                self.logger.warning(
                    "EDINET text extraction fallback to metadata code=%s doc_id=%s tried_types=%s",
                    code,
                    doc_id,
                    tried_types,
                )

            final_url = f"{self.edinet_client.base_url}/api/v2/documents/{doc_id}?type={used_file_type}"
            items.append(
                IntelSource(
                    code=code,
                    source_url=final_url,
                    source_type="edinet",
                    headline=headline,
                    published_at=str(published) if published else None,
                    snippet=snippet,
                    evidence_refs=[final_url],
                    xbrl_facts=xbrl_facts,
                    full_text=full_text,
                )
            )
            if len(items) >= self.max_items_per_symbol:
                return items

        for url in self.company_ir_domains.get(code, []):
            domain = _domain_of(url)
            if domain and self.whitelist and domain not in self.whitelist:
                continue
            try:
                resp = httpx.get(url, timeout=self.timeout_sec)
                if resp.status_code >= 400:
                    continue
                raw_text = _strip_markup(resp.text) if "<" in resp.text and ">" in resp.text else resp.text
                full_text = _safe_text(raw_text, limit=self.ir_full_text_limit)
                snippet = _safe_text(full_text, limit=600)
                items.append(
                    IntelSource(
                        code=code,
                        source_url=url,
                        source_type="company_ir",
                        headline=f"{code} IR page",
                        published_at=business_date.isoformat(),
                        snippet=snippet,
                        evidence_refs=[url],
                        full_text=full_text,
                    )
                )
            except Exception:
                self.logger.debug("IR source fetch failed: %s", url)
            if len(items) >= self.max_items_per_symbol:
                break
        return items


class McpIntelSearchBackend(IntelSearchBackend):
    def __init__(self, endpoint: str = "", timeout_sec: int = 20) -> None:
        self.endpoint = endpoint.strip()
        self.timeout_sec = timeout_sec
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def enabled(self) -> bool:
        return bool(self.endpoint)

    def fetch(self, *, code: str, business_date: date, seed: dict[str, Any]) -> list[IntelSource]:
        if not self.enabled:
            return []
        payload = {"code": code, "business_date": business_date.isoformat(), "seed": seed}
        try:
            resp = httpx.post(self.endpoint, json=payload, timeout=self.timeout_sec)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                return []
            out: list[IntelSource] = []
            for row in data:
                if not isinstance(row, dict):
                    continue
                url = str(row.get("source_url") or "")
                if not url:
                    continue
                out.append(
                    IntelSource(
                        code=code,
                        source_url=url,
                        source_type=str(row.get("source_type") or "mcp"),
                        headline=str(row.get("headline") or f"{code} MCP result"),
                        published_at=row.get("published_at"),
                        snippet=_safe_text(str(row.get("snippet") or "")),
                        evidence_refs=list(row.get("evidence_refs") or [url]),
                        full_text=_safe_text(str(row.get("full_text") or row.get("snippet") or ""), limit=4000),
                    )
                )
            return out
        except Exception as exc:  # noqa: BLE001
            self.logger.info("MCP backend skipped for %s: %s", code, exc)
            return []


class CompositeIntelSearchBackend(IntelSearchBackend):
    def __init__(self, backends: list[IntelSearchBackend]) -> None:
        self.backends = backends

    def fetch(self, *, code: str, business_date: date, seed: dict[str, Any]) -> list[IntelSource]:
        out: list[IntelSource] = []
        seen = set()
        for backend in self.backends:
            for item in backend.fetch(code=code, business_date=business_date, seed=seed):
                key = (item.source_url, item.source_type)
                if key in seen:
                    continue
                seen.add(key)
                out.append(item)
        return out
