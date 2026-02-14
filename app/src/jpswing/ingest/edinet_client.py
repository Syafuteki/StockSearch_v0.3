from __future__ import annotations

import logging
from datetime import date
from typing import Any

import httpx

from jpswing.utils.retry import retry_with_backoff


class EdinetClient:
    def __init__(self, base_url: str, api_key: str, timeout_sec: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_sec = timeout_sec
        self.logger = logging.getLogger(self.__class__.__name__)

    def _headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.api_key:
            headers["Subscription-Key"] = self.api_key
        return headers

    def fetch_documents_list(self, target_date: date, doc_type: int = 2) -> list[dict[str, Any]]:
        endpoint = f"{self.base_url}/api/v2/documents.json"
        params = {"date": target_date.isoformat(), "type": doc_type}

        def _run() -> list[dict[str, Any]]:
            response = httpx.get(endpoint, params=params, headers=self._headers(), timeout=self.timeout_sec)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"EDINET temporary error: {response.status_code}")
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return []
            results = payload.get("results")
            if isinstance(results, list):
                return [r for r in results if isinstance(r, dict)]
            return []

        return retry_with_backoff(_run, retries=3, base_delay_sec=1.2, backoff=2.0, logger=self.logger)

    def download_document(self, doc_id: str, file_type: int = 5) -> bytes:
        endpoint = f"{self.base_url}/api/v2/documents/{doc_id}"
        params = {"type": file_type}

        def _run() -> bytes:
            response = httpx.get(endpoint, params=params, headers=self._headers(), timeout=self.timeout_sec)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"EDINET temporary error: {response.status_code}")
            response.raise_for_status()
            return response.content

        return retry_with_backoff(_run, retries=3, base_delay_sec=1.2, backoff=2.0, logger=self.logger)

