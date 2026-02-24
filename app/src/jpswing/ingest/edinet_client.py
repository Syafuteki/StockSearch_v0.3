from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any
from urllib.parse import urlsplit

import httpx

from jpswing.utils.retry import retry_with_backoff


class EdinetClient:
    def __init__(self, base_url: str, api_key: str, timeout_sec: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_sec = timeout_sec
        self.logger = logging.getLogger(self.__class__.__name__)

    def _headers(self) -> dict[str, str]:
        return {}

    def _auth_params(self, params: dict[str, Any]) -> dict[str, Any]:
        out = dict(params)
        if self.api_key:
            # EDINET list/download APIs currently accept the subscription key via query param.
            out["Subscription-Key"] = self.api_key
        return out

    @staticmethod
    def _origin(url: str) -> str:
        raw = (url or "").strip().rstrip("/")
        parsed = urlsplit(raw)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return raw

    def _candidate_base_urls(self, *, api_only: bool = False) -> list[str]:
        current = self._origin(self.base_url)
        fallbacks = ["https://api.edinet-fsa.go.jp"]
        if not api_only:
            fallbacks.extend(
                [
                    "https://disclosure.edinet-fsa.go.jp",
                    "https://disclosure2.edinet-fsa.go.jp",
                ]
            )
        out = [current]
        for base in fallbacks:
            if base not in out:
                out.append(base)
        return out

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float:
        header = response.headers.get("Retry-After")
        if header is not None:
            try:
                value = float(header)
                if value > 1000:
                    return max(0.1, value / 1000.0)
                return max(0.1, value)
            except ValueError:
                pass

        try:
            body = response.json()
        except Exception:  # noqa: BLE001
            body = {}
        retry_after = body.get("retry_after")
        if retry_after is None:
            return 1.0
        try:
            value = float(retry_after)
            if value > 1000:
                return max(0.1, value / 1000.0)
            return max(0.1, value)
        except (TypeError, ValueError):
            return 1.0

    def fetch_documents_list(self, target_date: date, doc_type: int = 2) -> list[dict[str, Any]]:
        params = self._auth_params({"date": target_date.isoformat(), "type": doc_type})

        def _run() -> list[dict[str, Any]]:
            for base_url in self._candidate_base_urls(api_only=True):
                endpoint = f"{base_url}/api/v2/documents.json"
                response = httpx.get(
                    endpoint,
                    params=params,
                    headers={**self._headers(), "Accept": "application/json"},
                    timeout=self.timeout_sec,
                    follow_redirects=False,
                )
                if response.status_code == 429:
                    wait_sec = self._retry_after_seconds(response)
                    self.logger.warning(
                        "EDINET rate limited on %s status=429 wait=%.3fs",
                        base_url,
                        wait_sec,
                    )
                    time.sleep(wait_sec)
                    raise RuntimeError("EDINET temporary error: 429")
                if response.status_code in {500, 502, 503, 504}:
                    raise RuntimeError(f"EDINET temporary error: {response.status_code}")
                if response.status_code in {301, 302, 303, 307, 308}:
                    location = response.headers.get("Location", "")
                    aspx_redirect = ".aspx" in location.lower()
                    self.logger.warning(
                        "EDINET documents redirect on %s status=%s location=%s aspx_redirect=%s",
                        base_url,
                        response.status_code,
                        location,
                        aspx_redirect,
                    )
                    continue
                if response.status_code in {400, 401, 403, 404}:
                    self.logger.warning(
                        "EDINET documents unavailable on %s status=%s body=%s",
                        base_url,
                        response.status_code,
                        response.text[:300],
                    )
                    continue
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    self.logger.warning(
                        "EDINET documents invalid payload on %s payload_type=%s",
                        base_url,
                        type(payload).__name__,
                    )
                    continue
                results = payload.get("results")
                if isinstance(results, list):
                    return [r for r in results if isinstance(r, dict)]
                self.logger.warning(
                    "EDINET documents payload missing results list on %s keys=%s",
                    base_url,
                    sorted(payload.keys())[:20],
                )
            return []

        return retry_with_backoff(_run, retries=3, base_delay_sec=1.2, backoff=2.0, logger=self.logger)

    def download_document(self, doc_id: str, file_type: int = 5) -> bytes:
        params = self._auth_params({"type": file_type})

        def _run() -> bytes:
            for base_url in self._candidate_base_urls(api_only=False):
                endpoint = f"{base_url}/api/v2/documents/{doc_id}"
                response = httpx.get(
                    endpoint,
                    params=params,
                    headers=self._headers(),
                    timeout=self.timeout_sec,
                    follow_redirects=False,
                )
                if response.status_code == 429:
                    wait_sec = self._retry_after_seconds(response)
                    self.logger.warning(
                        "EDINET rate limited on %s doc_id=%s type=%s status=429 wait=%.3fs",
                        base_url,
                        doc_id,
                        file_type,
                        wait_sec,
                    )
                    time.sleep(wait_sec)
                    raise RuntimeError("EDINET temporary error: 429")
                if response.status_code in {500, 502, 503, 504}:
                    raise RuntimeError(f"EDINET temporary error: {response.status_code}")
                if response.status_code in {301, 302, 303, 307, 308}:
                    location = response.headers.get("Location", "")
                    self.logger.warning(
                        "EDINET download redirect on %s doc_id=%s type=%s status=%s location=%s",
                        base_url,
                        doc_id,
                        file_type,
                        response.status_code,
                        location,
                    )
                    continue
                if response.status_code in {400, 401, 403, 404}:
                    self.logger.warning(
                        "EDINET download unavailable on %s doc_id=%s type=%s status=%s body=%s",
                        base_url,
                        doc_id,
                        file_type,
                        response.status_code,
                        response.text[:300],
                    )
                    continue
                response.raise_for_status()
                return response.content
            return b""

        return retry_with_backoff(_run, retries=3, base_delay_sec=1.2, backoff=2.0, logger=self.logger)
