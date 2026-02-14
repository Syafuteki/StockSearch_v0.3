from __future__ import annotations

import logging
from typing import Any

import httpx

from jpswing.utils.retry import retry_with_backoff


class LocalEmbedder:
    def __init__(self, *, base_url: str, model: str, api_key: str = "", timeout_sec: int = 60) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.timeout_sec = timeout_sec
        self.logger = logging.getLogger(self.__class__.__name__)

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        endpoint = f"{self.base_url}/embeddings"
        payload = {"model": self.model, "input": texts}

        def _run() -> dict[str, Any]:
            response = httpx.post(endpoint, headers=self._headers(), json=payload, timeout=self.timeout_sec)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"embedding temporary error: {response.status_code}")
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise RuntimeError("embedding response is not dict")
            return data

        try:
            data = retry_with_backoff(_run, retries=2, base_delay_sec=1.0, backoff=2.0, logger=self.logger)
            out = data.get("data")
            if not isinstance(out, list):
                return [[] for _ in texts]
            vectors: list[list[float]] = []
            for item in out:
                if not isinstance(item, dict):
                    vectors.append([])
                    continue
                emb = item.get("embedding")
                if isinstance(emb, list):
                    vectors.append([float(v) for v in emb])
                else:
                    vectors.append([])
            if len(vectors) < len(texts):
                vectors.extend([[] for _ in range(len(texts) - len(vectors))])
            return vectors[: len(texts)]
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("embedding fallback: %s", exc)
            return [[] for _ in texts]

