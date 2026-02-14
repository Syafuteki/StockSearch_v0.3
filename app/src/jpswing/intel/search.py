from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import urlparse

import httpx

from jpswing.ingest.edinet_client import EdinetClient


@dataclass(slots=True)
class IntelSource:
    code: str
    source_url: str
    source_type: str
    headline: str
    published_at: str | None
    snippet: str
    evidence_refs: list[str]


def _domain_of(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def _safe_text(raw: str, limit: int = 600) -> str:
    txt = re.sub(r"\s+", " ", raw).strip()
    return txt[:limit]


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
    ) -> None:
        self.edinet_client = edinet_client
        self.whitelist = {d.lower() for d in whitelist_domains}
        self.company_ir_domains = company_ir_domains or {}
        self.timeout_sec = timeout_sec
        self.max_items_per_symbol = max_items_per_symbol
        self.logger = logging.getLogger(self.__class__.__name__)

    def fetch(self, *, code: str, business_date: date, seed: dict[str, Any]) -> list[IntelSource]:
        items: list[IntelSource] = []
        docs = seed.get("edinet_docs", [])
        for doc in docs:
            url = f"{self.edinet_client.base_url}/api/v2/documents/{doc.get('docID', '')}?type=5"
            domain = _domain_of(url)
            if domain and self.whitelist and domain not in self.whitelist:
                continue
            headline = str(doc.get("docDescription") or doc.get("docTypeCode") or "EDINET filing")
            published = doc.get("submitDateTime") or doc.get("submitDate")
            items.append(
                IntelSource(
                    code=code,
                    source_url=url,
                    source_type="edinet",
                    headline=headline,
                    published_at=str(published) if published else None,
                    snippet=_safe_text(headline),
                    evidence_refs=[url],
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
                snippet = _safe_text(resp.text)
                items.append(
                    IntelSource(
                        code=code,
                        source_url=url,
                        source_type="company_ir",
                        headline=f"{code} IR page",
                        published_at=business_date.isoformat(),
                        snippet=snippet,
                        evidence_refs=[url],
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

