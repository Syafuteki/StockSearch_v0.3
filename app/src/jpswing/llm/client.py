from __future__ import annotations

import logging
from typing import Any

import httpx

from jpswing.utils.retry import retry_with_backoff


class LlmClient:
    def __init__(
        self,
        *,
        base_url: str,
        model_name: str,
        api_key: str = "",
        temperature: float = 0.1,
        timeout_sec: int = 90,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model_name = model_name
        self.api_key = api_key
        self.temperature = temperature
        self.timeout_sec = timeout_sec
        self.logger = logging.getLogger(self.__class__.__name__)

    def chat_completion(self, messages: list[dict[str, str]]) -> dict[str, Any]:
        endpoint = f"{self.base_url}/chat/completions"
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload: dict[str, Any] = {
            "model": self.model_name,
            "temperature": self.temperature,
            "messages": messages,
            "response_format": {"type": "json_object"},
        }

        def _run() -> dict[str, Any]:
            response = httpx.post(endpoint, headers=headers, json=payload, timeout=self.timeout_sec)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"LLM temporary error: {response.status_code}")
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise RuntimeError("LLM response is not a JSON object")
            return data

        return retry_with_backoff(_run, retries=3, base_delay_sec=1.5, backoff=2.0, logger=self.logger)

