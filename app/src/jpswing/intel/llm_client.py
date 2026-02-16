from __future__ import annotations

from collections.abc import Callable
import json
import logging
import re
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

        def _run_openai_chat(payload: dict[str, Any]) -> dict[str, Any]:
            resp = httpx.post(endpoint, headers=self._headers(), json=payload, timeout=self.timeout_sec)
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

        def _run_openai_chat_default() -> dict[str, Any]:
            return _run_openai_chat(chat_payload)

        def _run_lmstudio_mcp(payload: dict[str, Any]) -> dict[str, Any]:
            if not self.mcp_integrations:
                raise RuntimeError("intel llm mcp integrations are empty")
            resp = httpx.post(
                self._resolve_mcp_chat_endpoint(),
                headers=self._headers(),
                json=payload,
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

        def _build_mcp_payload(system_prompt: str, input_payload: dict[str, Any]) -> dict[str, Any]:
            return {
                "model": self.model,
                "system_prompt": system_prompt,
                "input": json.dumps(input_payload, ensure_ascii=False),
                "temperature": self.temperature,
                "context_length": self.mcp_context_length,
                "integrations": self.mcp_integrations,
            }

        try:
            response: dict[str, Any] | None = None
            used_mcp_response = False
            if self.use_mcp:
                if self.mcp_integrations:
                    try:
                        response = retry_with_backoff(
                            lambda: _run_lmstudio_mcp(_build_mcp_payload(system, user_payload)),
                            retries=self.retries,
                            base_delay_sec=1.0,
                            backoff=2.0,
                            logger=self.logger,
                        )
                        used_mcp_response = response is not None
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
                    _run_openai_chat_default,
                    retries=self.retries,
                    base_delay_sec=1.0,
                    backoff=2.0,
                    logger=self.logger,
                )

            content = self._extract_content(response)
            parsed, validation_err = self._parse_and_validate(content)
            if parsed is None:
                repaired = self._attempt_repair(
                    original_content=content,
                    validation_error=validation_err,
                    run_openai_chat=_run_openai_chat,
                    run_lmstudio_mcp=(
                        (lambda payload: _run_lmstudio_mcp(payload))
                        if used_mcp_response and bool(self.mcp_integrations)
                        else None
                    ),
                    use_mcp_path=used_mcp_response and bool(self.mcp_integrations),
                )
                if repaired is None:
                    return self._fallback(code, source_payload, reason=validation_err)
                parsed = repaired

            merged = self._merge_source_fields(code=code, parsed=parsed, source_payload=source_payload)
            return merged, True, None
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

    def _parse_and_validate(self, content: str) -> tuple[dict[str, Any] | None, str | None]:
        try:
            parsed = json.loads(content)
        except Exception as exc:  # noqa: BLE001
            return None, f"json_parse_error: {exc}"
        if not isinstance(parsed, dict):
            return None, "intel llm content not object"
        validation = validate_intel_payload(parsed)
        if not validation.valid:
            return None, validation.error
        return parsed, None

    def _attempt_repair(
        self,
        *,
        original_content: str,
        validation_error: str | None,
        run_openai_chat: Callable[[dict[str, Any]], dict[str, Any]],
        run_lmstudio_mcp: Callable[[dict[str, Any]], dict[str, Any]] | None,
        use_mcp_path: bool,
    ) -> dict[str, Any] | None:
        repair_system = (
            "You repair malformed JSON for a Japanese equity Intel pipeline. "
            "Return strict JSON object only. No markdown, no comments."
        )
        repair_user_payload = {
            "task": "repair_json",
            "required_fields": [
                "headline",
                "summary",
                "facts",
                "tags",
                "risk_flags",
                "critical_risk",
                "evidence_refs",
                "data_gaps",
            ],
            "rules": [
                "Keep only valid JSON object.",
                "facts/tags/risk_flags/evidence_refs/data_gaps must be arrays of strings.",
                "critical_risk must be boolean.",
                "Do not fabricate new facts. Use empty values and data_gaps if needed.",
            ],
            "validation_error": str(validation_error or "")[:1000],
            "original_response": str(original_content or "")[:12000],
        }
        chat_payload = {
            "model": self.model,
            "temperature": 0.0,
            "messages": [
                {"role": "system", "content": repair_system},
                {"role": "user", "content": json.dumps(repair_user_payload, ensure_ascii=False)},
            ],
        }

        def _run() -> dict[str, Any]:
            if use_mcp_path and run_lmstudio_mcp is not None:
                mcp_payload = {
                    "model": self.model,
                    "system_prompt": repair_system,
                    "input": json.dumps(repair_user_payload, ensure_ascii=False),
                    "temperature": 0.0,
                    "context_length": self.mcp_context_length,
                    "integrations": self.mcp_integrations,
                }
                return run_lmstudio_mcp(mcp_payload)
            return run_openai_chat(chat_payload)

        try:
            response = retry_with_backoff(
                _run,
                retries=1,
                base_delay_sec=0.8,
                backoff=2.0,
                logger=self.logger,
            )
            repaired_content = self._extract_content(response)
            repaired, err = self._parse_and_validate(repaired_content)
            if repaired is None:
                self.logger.warning("Intel LLM repair validation failed: %s", err)
            return repaired
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Intel LLM repair call failed: %s", exc)
            return None

    @classmethod
    def _fallback(
        cls,
        code: str,
        source_payload: list[dict[str, Any]],
        *,
        reason: str | None,
    ) -> tuple[dict[str, Any], bool, str | None]:
        meta = cls._resolve_source_meta(source_payload)
        facts = cls._build_deterministic_facts(source_payload, limit=3)
        summary = cls._build_deterministic_summary(code=code, source_payload=source_payload, meta=meta, facts=facts)

        data_gaps: list[str] = []
        if not cls._has_substantive_full_text(source_payload):
            data_gaps.append("報告書本文の取得または抽出が不十分")
        if not cls._has_xbrl_facts(source_payload):
            data_gaps.append("XBRLの主要数値は未取得")
        if not meta["published_at"]:
            data_gaps.append("公開日時が未取得")
        if reason:
            data_gaps.append(f"llm_error: {cls._clean_text(reason, limit=140)}")
        if not facts:
            data_gaps.append("本文から有効な要点を抽出できず")

        payload = {
            "headline": meta["headline"] or f"{code} 開示情報",
            "published_at": meta["published_at"],
            "source_url": meta["source_url"],
            "source_type": meta["source_type"],
            "summary": summary,
            "facts": facts,
            "tags": [],
            "risk_flags": [],
            "critical_risk": False,
            "evidence_refs": meta["evidence_refs"],
            "data_gaps": cls._normalize_text_list(data_gaps, limit=4, item_limit=140),
        }
        return payload, False, reason

    @classmethod
    def _merge_source_fields(
        cls,
        *,
        code: str,
        parsed: dict[str, Any],
        source_payload: list[dict[str, Any]],
    ) -> dict[str, Any]:
        meta = cls._resolve_source_meta(source_payload)
        facts = cls._normalize_text_list(parsed.get("facts"), limit=3, item_limit=120)
        if not facts:
            facts = cls._build_deterministic_facts(source_payload, limit=3)

        summary = cls._clean_text(parsed.get("summary"), limit=360)
        if not summary:
            summary = cls._build_deterministic_summary(code=code, source_payload=source_payload, meta=meta, facts=facts)

        evidence_refs = cls._normalize_text_list(parsed.get("evidence_refs"), limit=5, item_limit=240)
        for ref in meta["evidence_refs"]:
            if ref and ref not in evidence_refs:
                evidence_refs.append(ref)
            if len(evidence_refs) >= 5:
                break
        if not evidence_refs and meta["source_url"] != "unknown":
            evidence_refs = [meta["source_url"]]

        data_gaps = cls._normalize_text_list(parsed.get("data_gaps"), limit=4, item_limit=140)
        if not cls._has_substantive_full_text(source_payload) and "報告書本文の取得または抽出が不十分" not in data_gaps:
            data_gaps.append("報告書本文の取得または抽出が不十分")
        data_gaps = cls._normalize_text_list(data_gaps, limit=4, item_limit=140)

        payload = {
            "headline": cls._clean_text(parsed.get("headline"), limit=160) or meta["headline"] or f"{code} 開示情報",
            "published_at": meta["published_at"],
            "source_url": meta["source_url"],
            "source_type": meta["source_type"],
            "summary": summary,
            "facts": facts,
            "tags": cls._normalize_text_list(parsed.get("tags"), limit=8, item_limit=40),
            "risk_flags": cls._normalize_text_list(parsed.get("risk_flags"), limit=8, item_limit=40),
            "critical_risk": bool(parsed.get("critical_risk")),
            "evidence_refs": evidence_refs,
            "data_gaps": data_gaps,
        }
        return payload

    @classmethod
    def _resolve_source_meta(cls, source_payload: list[dict[str, Any]]) -> dict[str, Any]:
        source_url = ""
        source_type = ""
        published_at: str | None = None
        headline = ""
        evidence_refs: list[str] = []
        for row in source_payload:
            if not isinstance(row, dict):
                continue
            row_url = cls._clean_text(row.get("source_url"), limit=240)
            row_type = cls._clean_text(row.get("source_type"), limit=40)
            row_headline = cls._clean_text(row.get("headline"), limit=160)
            row_published = cls._clean_text(row.get("published_at"), limit=40)
            if not source_url and row_url:
                source_url = row_url
            if not source_type and row_type:
                source_type = row_type
            if published_at is None and row_published:
                published_at = row_published
            if not headline and row_headline:
                headline = row_headline
            if row_url and row_url not in evidence_refs:
                evidence_refs.append(row_url)
            refs = row.get("evidence_refs")
            if isinstance(refs, list):
                for ref in refs:
                    ref_txt = cls._clean_text(ref, limit=240)
                    if ref_txt and ref_txt not in evidence_refs:
                        evidence_refs.append(ref_txt)
            if len(evidence_refs) >= 5:
                break
        if not source_url:
            source_url = "unknown"
        if not source_type:
            source_type = "unknown"
        if not evidence_refs and source_url != "unknown":
            evidence_refs = [source_url]
        return {
            "source_url": source_url,
            "source_type": source_type,
            "published_at": published_at,
            "headline": headline,
            "evidence_refs": evidence_refs[:5],
        }

    @classmethod
    def _build_deterministic_summary(
        cls,
        *,
        code: str,
        source_payload: list[dict[str, Any]],
        meta: dict[str, Any],
        facts: list[str],
    ) -> str:
        headline = str(meta.get("headline") or f"{code} 開示情報")
        if facts:
            top_facts = " / ".join(facts[:2])
            return cls._clean_text(f"{headline}について確認。主な要点は {top_facts}。", limit=360)
        if cls._has_substantive_full_text(source_payload):
            return cls._clean_text(f"{headline}について本文は取得済みですが、機械抽出で有効な要点が限定的でした。", limit=360)
        return cls._clean_text(f"{headline}について開示は確認できましたが、本文の取得または抽出が不十分なため詳細分析は未完了です。", limit=360)

    @classmethod
    def _build_deterministic_facts(cls, source_payload: list[dict[str, Any]], *, limit: int) -> list[str]:
        out: list[str] = []
        for row in source_payload:
            if not isinstance(row, dict):
                continue
            xbrl_facts = row.get("xbrl_facts")
            if isinstance(xbrl_facts, list):
                for item in xbrl_facts:
                    txt = cls._clean_text(item, limit=96)
                    if not txt:
                        continue
                    fact = f"XBRL: {txt}"
                    if fact not in out:
                        out.append(fact)
                    if len(out) >= limit:
                        return out

        for row in source_payload:
            if not isinstance(row, dict):
                continue
            headline = cls._clean_text(row.get("headline"), limit=70)
            full_text = str(row.get("full_text") or "")
            snippet = str(row.get("snippet") or "")
            chosen = ""
            if cls._is_substantive_text(full_text):
                chosen = cls._first_sentence(full_text, limit=108)
            elif cls._is_substantive_text(snippet):
                chosen = cls._first_sentence(snippet, limit=108)
            if not chosen:
                continue
            fact = cls._clean_text(f"{headline}: {chosen}" if headline else chosen, limit=120)
            if fact and fact not in out:
                out.append(fact)
            if len(out) >= limit:
                return out
        return out

    @classmethod
    def _has_substantive_full_text(cls, source_payload: list[dict[str, Any]]) -> bool:
        for row in source_payload:
            if not isinstance(row, dict):
                continue
            if cls._is_substantive_text(row.get("full_text")):
                return True
        return False

    @staticmethod
    def _has_xbrl_facts(source_payload: list[dict[str, Any]]) -> bool:
        for row in source_payload:
            if not isinstance(row, dict):
                continue
            xbrl_facts = row.get("xbrl_facts")
            if isinstance(xbrl_facts, list) and any(str(item).strip() for item in xbrl_facts):
                return True
        return False

    @classmethod
    def _is_substantive_text(cls, value: Any) -> bool:
        text = cls._clean_text(value, limit=2000)
        if len(text) < 24:
            return False
        low = text.lower()
        error_tokens = (
            "not found",
            "forbidden",
            "access denied",
            "invalid_api_key",
            "subscription-key",
            "wzek0130.aspx",
            "llm validation failed",
            "fallback summary applied",
        )
        if any(token in low for token in error_tokens):
            return False
        return True

    @classmethod
    def _first_sentence(cls, value: Any, *, limit: int = 108) -> str:
        raw = str(value or "")
        raw = re.sub(r"\s+", " ", raw).strip()
        if not raw:
            return ""
        pieces = re.split(r"[。！？!?]", raw)
        for piece in pieces:
            txt = cls._clean_text(piece, limit=limit)
            if len(txt) >= 12:
                return txt
        return cls._clean_text(raw, limit=limit)

    @staticmethod
    def _clean_text(value: Any, *, limit: int) -> str:
        text = re.sub(r"\s+", " ", str(value or "")).strip()
        if not text:
            return ""
        if len(text) <= limit:
            return text
        if limit <= 3:
            return text[:limit]
        return f"{text[: limit - 3]}..."

    @classmethod
    def _normalize_text_list(cls, value: Any, *, limit: int, item_limit: int) -> list[str]:
        if not isinstance(value, list):
            return []
        out: list[str] = []
        for item in value:
            txt = cls._clean_text(item, limit=item_limit)
            if not txt:
                continue
            if txt.lower() in {"none", "n/a", "na", "null", "unknown", "not available"}:
                continue
            if txt not in out:
                out.append(txt)
            if len(out) >= limit:
                break
        return out
