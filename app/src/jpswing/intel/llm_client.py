from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from jpswing.intel.schema import validate_intel_payload
from jpswing.utils.retry import retry_with_backoff


class IntelLlmClient:
    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        api_key: str = "",
        temperature: float = 0.0,
        timeout_sec: int = 90,
        retries: int = 2,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.temperature = temperature
        self.timeout_sec = timeout_sec
        self.retries = retries
        self.logger = logging.getLogger(self.__class__.__name__)

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def summarize_symbol_intel(
        self,
        *,
        code: str,
        source_payload: list[dict[str, Any]],
        existing_tags: list[str],
    ) -> tuple[dict[str, Any], bool, str | None]:
        endpoint = f"{self.base_url}/chat/completions"
        system = (
            "You are a Japanese equity Intel summarizer. "
            "Return strict JSON only. Never fabricate missing facts. "
            "If a value is unknown, set null and record it in data_gaps."
        )
        user_payload = {
            "code": code,
            "existing_tags": existing_tags,
            "sources": source_payload,
            "output_schema_hint": {
                "headline": "string",
                "published_at": "ISO8601|null",
                "source_url": "string",
                "source_type": "string",
                "summary": "string",
                "facts": ["string"],
                "tags": ["string"],
                "risk_flags": ["string"],
                "critical_risk": "boolean",
                "evidence_refs": ["string"],
                "data_gaps": ["string"],
            },
        }
        payload = {
            "model": self.model,
            "temperature": self.temperature,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
        }

        def _run() -> dict[str, Any]:
            resp = httpx.post(endpoint, headers=self._headers(), json=payload, timeout=self.timeout_sec)
            if resp.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"intel llm temporary error: {resp.status_code}")
            resp.raise_for_status()
            raw = resp.json()
            if not isinstance(raw, dict):
                raise RuntimeError("intel llm response is not dict")
            return raw

        try:
            response = retry_with_backoff(
                _run,
                retries=self.retries,
                base_delay_sec=1.0,
                backoff=2.0,
                logger=self.logger,
            )
            content = self._extract_content(response)
            parsed = json.loads(content)
            if not isinstance(parsed, dict):
                raise RuntimeError("intel llm content not object")
            validation = validate_intel_payload(parsed)
            if not validation.valid:
                return self._fallback(code, source_payload, reason=validation.error)
            return parsed, True, None
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Intel LLM fallback for %s: %s", code, exc)
            return self._fallback(code, source_payload, reason=str(exc))

    @staticmethod
    def _extract_content(response: dict[str, Any]) -> str:
        choices = response.get("choices")
        if not isinstance(choices, list) or not choices:
            raise RuntimeError("llm choices missing")
        message = choices[0].get("message")
        if not isinstance(message, dict):
            raise RuntimeError("llm message missing")
        content = message.get("content")
        if not isinstance(content, str):
            raise RuntimeError("llm content missing")
        text = content.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            if len(lines) >= 3 and lines[0].startswith("```") and lines[-1].startswith("```"):
                return "\n".join(lines[1:-1]).strip()
        return text

    @staticmethod
    def _fallback(
        code: str,
        source_payload: list[dict[str, Any]],
        *,
        reason: str | None,
    ) -> tuple[dict[str, Any], bool, str | None]:
        source_url = source_payload[0]["source_url"] if source_payload else "unknown"
        source_type = source_payload[0]["source_type"] if source_payload else "unknown"
        payload = {
            "headline": f"{code} intel summary unavailable",
            "published_at": None,
            "source_url": source_url,
            "source_type": source_type,
            "summary": "LLM validation failed, fallback summary applied.",
            "facts": [],
            "tags": [],
            "risk_flags": [],
            "critical_risk": False,
            "evidence_refs": [source_url] if source_url != "unknown" else [],
            "data_gaps": ["llm_output_invalid_or_unavailable"],
        }
        return payload, False, reason

