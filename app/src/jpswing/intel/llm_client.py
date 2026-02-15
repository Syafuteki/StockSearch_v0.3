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
        use_mcp: bool = False,
        mcp_integrations: list[str | dict[str, Any]] | None = None,
        mcp_chat_endpoint: str = "",
        mcp_context_length: int = 12000,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.temperature = temperature
        self.timeout_sec = timeout_sec
        self.retries = retries
        self.use_mcp = use_mcp
        self.mcp_integrations = self._normalize_integrations(mcp_integrations or [])
        self.mcp_chat_endpoint = mcp_chat_endpoint.strip()
        self.mcp_context_length = max(1024, int(mcp_context_length))
        self.logger = logging.getLogger(self.__class__.__name__)
        self._mcp_warned = False

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
        use_mcp_path = self.use_mcp and bool(self.mcp_integrations)
        system = self._build_system_prompt(use_mcp_path=use_mcp_path)
        user_payload = {
            "code": code,
            "existing_tags": existing_tags,
            "sources": source_payload,
            "analysis_focus": [
                "Which catalysts are likely to affect stock price in the near term?",
                "How do macro factors and event timing change the bull/bear balance?",
                "What concrete risk controls are implied by the evidence?",
            ],
            "rules": [
                "facts must be directly supported by sources[].full_text/headline/snippet/published_at/source_type.",
                "prioritize sources[].full_text when available; snippet is only a short reference.",
                "if sources[].xbrl_facts exists, prioritize those values as objective evidence.",
                "facts should be max 3 items, short and concrete.",
                "if only filing metadata exists, state that clearly in summary and data_gaps.",
                "include at least one explicit link in evidence_refs.",
            ],
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
        chat_payload = {
            "model": self.model,
            "temperature": self.temperature,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
        }

        def _run_openai_chat() -> dict[str, Any]:
            resp = httpx.post(endpoint, headers=self._headers(), json=chat_payload, timeout=self.timeout_sec)
            if resp.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"intel llm temporary error: {resp.status_code}")
            if 400 <= resp.status_code < 500:
                self.logger.error(
                    "Intel LLM client error status=%s body=%s",
                    resp.status_code,
                    resp.text[:800],
                )
            resp.raise_for_status()
            raw = resp.json()
            if not isinstance(raw, dict):
                raise RuntimeError("intel llm response is not dict")
            return raw

        def _run_lmstudio_mcp() -> dict[str, Any]:
            if not self.mcp_integrations:
                raise RuntimeError("intel llm mcp integrations are empty")
            lmstudio_payload = {
                "model": self.model,
                "system_prompt": system,
                "input": json.dumps(user_payload, ensure_ascii=False),
                "temperature": self.temperature,
                "context_length": self.mcp_context_length,
                "integrations": self.mcp_integrations,
            }
            resp = httpx.post(
                self._resolve_mcp_chat_endpoint(),
                headers=self._headers(),
                json=lmstudio_payload,
                timeout=self.timeout_sec,
            )
            if resp.status_code in {429, 500, 502, 503, 504}:
                raise RuntimeError(f"intel llm mcp temporary error: {resp.status_code}")
            if 400 <= resp.status_code < 500:
                self.logger.error(
                    "Intel LLM MCP client error status=%s body=%s",
                    resp.status_code,
                    resp.text[:800],
                )
            resp.raise_for_status()
            raw = resp.json()
            if not isinstance(raw, dict):
                raise RuntimeError("intel llm mcp response is not dict")
            return raw

        try:
            response: dict[str, Any] | None = None
            if self.use_mcp:
                if self.mcp_integrations:
                    try:
                        response = retry_with_backoff(
                            _run_lmstudio_mcp,
                            retries=self.retries,
                            base_delay_sec=1.0,
                            backoff=2.0,
                            logger=self.logger,
                        )
                    except Exception as exc:  # noqa: BLE001
                        self.logger.warning(
                            "Intel LLM MCP path failed. fallback to /v1/chat/completions. err=%s",
                            exc,
                        )
                elif not self._mcp_warned:
                    self.logger.warning(
                        "search.use_mcp=true but no mcp integrations configured. "
                        "set search.mcp_plugin_ids or INTEL_MCP_PLUGIN_IDS."
                    )
                    self._mcp_warned = True

            if response is None:
                response = retry_with_backoff(
                    _run_openai_chat,
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
    def _normalize_integrations(values: list[str | dict[str, Any]]) -> list[str | dict[str, Any]]:
        out: list[str | dict[str, Any]] = []
        for value in values:
            if isinstance(value, str):
                v = value.strip()
                if v:
                    out.append(v)
                continue
            if isinstance(value, dict):
                t = str(value.get("type") or "").strip().lower()
                if t in {"plugin", "ephemeral_mcp"}:
                    out.append(value)
        return out

    def _resolve_mcp_chat_endpoint(self) -> str:
        if self.mcp_chat_endpoint:
            return self.mcp_chat_endpoint
        base = self.base_url.rstrip("/")
        if base.endswith("/v1"):
            return f"{base[:-3]}/api/v1/chat"
        return f"{base}/api/v1/chat"

    @staticmethod
    def _build_system_prompt(*, use_mcp_path: bool) -> str:
        if use_mcp_path:
            return (
                "You are a Japanese equity Intel analyst. Return strict JSON only. "
                "Use provided sources and MCP tools to gather official evidence when needed. "
                "Read sources.full_text first, and use snippet only as fallback. "
                "Do not fabricate numbers, dates, or actions. "
                "If evidence is missing, fill data_gaps explicitly. "
                "Write summary in Japanese and include catalyst, market impact, and risk."
            )
        return (
            "You are a Japanese equity Intel summarizer. Return strict JSON only. "
            "Use ONLY the provided sources. Never fabricate missing facts. "
            "Read sources.full_text first, and use snippet only as fallback. "
            "Do not invent numbers, dates, or company actions not explicitly present in sources. "
            "If evidence is insufficient, keep facts concise and add data_gaps. "
            "Write summary in Japanese and include catalyst, market impact, and risk."
        )

    @staticmethod
    def _extract_content(response: dict[str, Any]) -> str:
        output = response.get("output")
        if isinstance(output, list):
            messages: list[str] = []
            for item in output:
                if not isinstance(item, dict):
                    continue
                if item.get("type") != "message":
                    continue
                content = item.get("content")
                if isinstance(content, str):
                    messages.append(content)
                    continue
                if isinstance(content, list):
                    parts: list[str] = []
                    for part in content:
                        if not isinstance(part, dict):
                            continue
                        txt = part.get("text") or part.get("content")
                        if isinstance(txt, str) and txt.strip():
                            parts.append(txt)
                    if parts:
                        messages.append("\n".join(parts))
            if messages:
                return IntelLlmClient._cleanup_text(messages[-1])

        choices = response.get("choices")
        if not isinstance(choices, list) or not choices:
            raise RuntimeError("llm choices missing")
        message = choices[0].get("message")
        if not isinstance(message, dict):
            raise RuntimeError("llm message missing")
        content = message.get("content")
        if not isinstance(content, str):
            raise RuntimeError("llm content missing")
        return IntelLlmClient._cleanup_text(content)

    @staticmethod
    def _cleanup_text(content: str) -> str:
        text = content.strip()
        if text.startswith("```"):
            lines = text.splitlines()
            if len(lines) >= 3 and lines[0].startswith("```") and lines[-1].startswith("```"):
                text = "\n".join(lines[1:-1]).strip()
        marker = "<|message|>"
        marker_pos = text.find(marker)
        if marker_pos >= 0:
            text = text[marker_pos + len(marker) :].strip()
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
