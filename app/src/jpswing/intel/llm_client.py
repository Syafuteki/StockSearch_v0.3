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
        company_name: str = "",
        source_payload: list[dict[str, Any]],
        existing_tags: list[str],
    ) -> tuple[dict[str, Any], bool, str | None]:
        endpoint = f"{self.base_url}/chat/completions"
        use_mcp_path = self.use_mcp and bool(self.mcp_integrations)
        system = self._build_system_prompt(use_mcp_path=use_mcp_path)
        company_name = self._clean_text(company_name, limit=120)
        mcp_research_hints = self._build_mcp_research_hints(
            code=code,
            company_name=company_name,
            source_payload=source_payload,
        )
        user_payload = {
            "code": code,
            "company_name": company_name,
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
        if mcp_research_hints:
            user_payload["mcp_research_hints"] = mcp_research_hints
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

        current_payload: dict[str, Any]
        current_valid = False
        current_err: str | None = None
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
                    current_payload, current_valid, current_err = self._fallback(code, source_payload, reason=validation_err)
                else:
                    parsed = repaired
            if parsed is not None:
                current_payload = self._merge_source_fields(
                    code=code,
                    parsed=parsed,
                    source_payload=source_payload,
                    append_source_gaps=not used_mcp_response,
                )
                current_valid = True
                current_err = None
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Intel LLM fallback for %s: %s", code, exc)
            current_payload, current_valid, current_err = self._fallback(code, source_payload, reason=str(exc))

        researched = self._attempt_gap_research(
            code=code,
            company_name=company_name,
            source_payload=source_payload,
            existing_tags=existing_tags,
            current_payload=current_payload,
            run_openai_chat=_run_openai_chat,
            run_lmstudio_mcp=(lambda payload: _run_lmstudio_mcp(payload)) if bool(self.mcp_integrations) else None,
        )
        if researched is not None and self._prefer_gap_research_result(current=current_payload, candidate=researched):
            return researched, True, None
        return current_payload, current_valid, current_err

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
                "Some source_url values may be API or file-download endpoints, so do not rely on opening them directly in a browser tool. "
                "When browser navigation is needed, use company name, code, doc id, headline, and official-site search hints to reach browser-accessible pages. "
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

    def _attempt_gap_research(
        self,
        *,
        code: str,
        company_name: str,
        source_payload: list[dict[str, Any]],
        existing_tags: list[str],
        current_payload: dict[str, Any],
        run_openai_chat: Callable[[dict[str, Any]], dict[str, Any]],
        run_lmstudio_mcp: Callable[[dict[str, Any]], dict[str, Any]] | None,
    ) -> dict[str, Any] | None:
        if not self.use_mcp or not self.mcp_integrations or run_lmstudio_mcp is None:
            return None
        unresolved_gaps = self._normalize_text_list(current_payload.get("data_gaps"), limit=4, item_limit=140)
        if not unresolved_gaps:
            return None

        research_system = (
            "You are a Japanese equity Intel analyst. Return strict JSON only. "
            "You MUST use MCP tools to investigate unresolved data gaps using official sources when possible. "
            "Prefer EDINET, company IR, exchange releases, and official agency pages. "
            "Some source_url values may be API or file-download endpoints, so do not rely on opening them directly in a browser tool. "
            "Use company name, code, doc id, headline, submit date, and search hints to reach browser-accessible official pages. "
            "Treat gap_resolution_targets as a mandatory checklist and search separately for each gap using the most relevant official document type. "
            "Do not stop at the original filing when a related official filing or IR release is more likely to contain the missing detail. "
            "Do not fabricate numbers, dates, or company actions. "
            "Preserve already-supported facts unless contradicted by newly verified evidence. "
            "If a gap cannot be resolved, keep it in data_gaps explicitly. "
            "Write summary in Japanese and include catalyst, market impact, and risk."
        )
        research_payload = {
            "task": "resolve_data_gaps_with_mcp",
            "code": code,
            "company_name": company_name,
            "existing_tags": existing_tags,
            "current_result": {
                "headline": current_payload.get("headline"),
                "summary": current_payload.get("summary"),
                "facts": current_payload.get("facts"),
                "tags": current_payload.get("tags"),
                "risk_flags": current_payload.get("risk_flags"),
                "critical_risk": bool(current_payload.get("critical_risk")),
                "evidence_refs": current_payload.get("evidence_refs"),
                "data_gaps": unresolved_gaps,
            },
            "sources": self._build_gap_research_sources(source_payload),
            "mcp_research_hints": self._build_mcp_research_hints(
                code=code,
                company_name=company_name,
                source_payload=source_payload,
            ),
            "gap_resolution_targets": self._build_gap_resolution_targets(
                code=code,
                company_name=company_name,
                unresolved_gaps=unresolved_gaps,
                source_payload=source_payload,
            ),
            "research_instructions": [
                "Use MCP tools explicitly to investigate each unresolved gap.",
                "Work gap-by-gap using gap_resolution_targets instead of only rereading the original filing.",
                "For financing terms, prioritize prospectus / shelf registration supplement / securities registration / condition determination notices.",
                "For M&A, subsidiary, or business impact gaps, prioritize timely disclosure, extraordinary reports, and IR materials that mention earnings impact or consolidation effects.",
                "Add concrete URLs for any newly found supporting evidence.",
                "Do not drop already-supported facts only because the new source is silent.",
                "Only remove an item from data_gaps if you found direct evidence.",
                "If official sources confirm that a detail is not yet disclosed or undetermined, keep the gap but narrow it to that exact missing item.",
                "Keep facts concise and max 3 items.",
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
        mcp_payload = {
            "model": self.model,
            "system_prompt": research_system,
            "input": json.dumps(research_payload, ensure_ascii=False),
            "temperature": self.temperature,
            "context_length": self.mcp_context_length,
            "integrations": self.mcp_integrations,
        }

        try:
            self.logger.info("Intel LLM MCP gap research start code=%s gaps=%s", code, len(unresolved_gaps))
            response = retry_with_backoff(
                lambda: run_lmstudio_mcp(mcp_payload),
                retries=self.retries,
                base_delay_sec=1.0,
                backoff=2.0,
                logger=self.logger,
            )
            content = self._extract_content(response)
            parsed, validation_err = self._parse_and_validate(content)
            if parsed is None:
                parsed = self._attempt_repair(
                    original_content=content,
                    validation_error=validation_err,
                    run_openai_chat=run_openai_chat,
                    run_lmstudio_mcp=run_lmstudio_mcp,
                    use_mcp_path=True,
                )
                if parsed is None:
                    self.logger.warning("Intel LLM MCP gap research validation failed for %s: %s", code, validation_err)
                    return None
            return self._merge_source_fields(
                code=code,
                parsed=parsed,
                source_payload=source_payload,
                append_source_gaps=False,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Intel LLM MCP gap research failed for %s: %s", code, exc)
            return None

    @staticmethod
    def _prefer_gap_research_result(*, current: dict[str, Any], candidate: dict[str, Any]) -> bool:
        current_gaps = len(IntelLlmClient._normalize_text_list(current.get("data_gaps"), limit=4, item_limit=140))
        candidate_gaps = len(IntelLlmClient._normalize_text_list(candidate.get("data_gaps"), limit=4, item_limit=140))
        if candidate_gaps < current_gaps:
            return True
        if candidate_gaps > current_gaps:
            return False

        current_refs = len(IntelLlmClient._normalize_text_list(current.get("evidence_refs"), limit=5, item_limit=240))
        candidate_refs = len(IntelLlmClient._normalize_text_list(candidate.get("evidence_refs"), limit=5, item_limit=240))
        if candidate_refs > current_refs:
            return True
        if candidate_refs < current_refs:
            return False

        current_facts = len(IntelLlmClient._normalize_text_list(current.get("facts"), limit=3, item_limit=120))
        candidate_facts = len(IntelLlmClient._normalize_text_list(candidate.get("facts"), limit=3, item_limit=120))
        return candidate_facts >= current_facts

    @staticmethod
    def _extract_edinet_doc_id(source_url: str) -> str:
        match = re.search(r"/documents/([^/?]+)", str(source_url or ""))
        if not match:
            return ""
        return match.group(1).strip()

    @classmethod
    def _build_mcp_research_hints(
        cls,
        *,
        code: str,
        company_name: str,
        source_payload: list[dict[str, Any]],
    ) -> dict[str, Any]:
        search_queries = cls._build_mcp_search_queries(
            code=code,
            company_name=company_name,
            source_payload=source_payload,
        )
        source_hints = cls._build_source_navigation_hints(
            code=code,
            company_name=company_name,
            source_payload=source_payload,
        )
        hints: dict[str, Any] = {
            "browser_constraints": [
                "Some source_url values may be API endpoints or file downloads and may not open directly in browser MCP sessions.",
                "If a source_url is not browser-friendly, search official sites by company_name, code, doc_id, headline, and submit date instead of opening the API URL directly.",
            ],
            "official_site_preferences": [
                "company IR",
                "JPX timely disclosure",
                "EDINET public disclosure pages",
                "official agency pages",
            ],
        }
        if company_name:
            hints["company_name"] = company_name
        if search_queries:
            hints["search_queries"] = search_queries
        if source_hints:
            hints["source_navigation_hints"] = source_hints
        return hints

    @classmethod
    def _build_gap_resolution_targets(
        cls,
        *,
        code: str,
        company_name: str,
        unresolved_gaps: list[str],
        source_payload: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        headlines = [
            cls._clean_text(row.get("headline"), limit=160)
            for row in source_payload
            if isinstance(row, dict) and cls._clean_text(row.get("headline"), limit=160)
        ]
        out: list[dict[str, Any]] = []
        seed = company_name or code

        for idx, raw_gap in enumerate(unresolved_gaps, start=1):
            gap = cls._clean_text(raw_gap, limit=160)
            if not gap:
                continue

            components = cls._extract_gap_components(gap)
            categories = cls._infer_gap_categories(gap=gap, components=components, headlines=headlines)
            target_fact_types = cls._infer_gap_fact_types(gap=gap, components=components)
            suggested_queries: list[str] = []
            likely_sources: list[str] = []
            search_focus: list[str] = []
            document_hints: list[str] = []
            for category in categories:
                profile = cls._gap_category_profile(category)
                for item in profile["likely_sources"]:
                    cls._append_unique_clean(likely_sources, item, limit=8, item_limit=160)
                for item in profile["search_focus"]:
                    cls._append_unique_clean(search_focus, item, limit=8, item_limit=180)
                for item in profile["document_hints"]:
                    cls._append_unique_clean(document_hints, item, limit=8, item_limit=220)
                if seed:
                    for suffix in profile["query_suffixes"]:
                        cls._append_unique_clean(suggested_queries, f"{seed} {suffix}", limit=12, item_limit=140)

            if seed:
                for component in components:
                    cls._append_unique_clean(suggested_queries, f"{seed} {component}", limit=12, item_limit=140)
                    cls._append_unique_clean(suggested_queries, f"{seed} site:jpx.co.jp {component}", limit=12, item_limit=140)
                    cls._append_unique_clean(
                        suggested_queries,
                        f"{seed} site:disclosure2.edinet-fsa.go.jp {component}",
                        limit=12,
                        item_limit=140,
                    )

            for headline in headlines:
                if "臨時報告書" in headline and "impact" in categories:
                    cls._append_unique_clean(
                        document_hints,
                        "Use the extraordinary report to identify the transaction structure and date, then search related timely disclosure or IR pages for quantified impact.",
                        limit=8,
                        item_limit=220,
                    )
                    if seed:
                        cls._append_unique_clean(suggested_queries, f"{seed} 臨時報告書 子会社化", limit=12, item_limit=140)
                if any(token in headline for token in ("訂正発行登録書", "発行登録書", "発行登録追補書類")) and "financing_terms" in categories:
                    cls._append_unique_clean(
                        document_hints,
                        "Search the related shelf registration supplement or securities registration filings for final financing terms.",
                        limit=8,
                        item_limit=220,
                    )
                    if seed:
                        cls._append_unique_clean(suggested_queries, f"{seed} 発行登録追補書類 社債", limit=12, item_limit=140)
                        cls._append_unique_clean(suggested_queries, f"{seed} 社債 条件決定 お知らせ", limit=12, item_limit=140)

            resolution_questions = cls._build_gap_resolution_questions(
                gap=gap,
                components=components,
                categories=categories,
                target_fact_types=target_fact_types,
            )

            target = {
                "priority": idx,
                "gap": gap,
                "gap_components": components,
                "inferred_categories": categories,
                "target_fact_types": target_fact_types,
                "resolution_goal": "Find direct official evidence that resolves this gap or narrows it to the exact still-missing detail.",
                "resolution_questions": resolution_questions,
                "likely_sources": likely_sources,
                "suggested_queries": suggested_queries,
                "search_focus": search_focus,
                "document_hints": document_hints,
                "keep_gap_when": (
                    "Keep this gap only when official sources remain silent or explicitly say the detail is undisclosed / undecided."
                ),
            }
            out.append(target)
        return out

    @classmethod
    def _append_unique_clean(cls, items: list[str], value: str, *, limit: int, item_limit: int) -> None:
        text = cls._clean_text(value, limit=item_limit)
        if text and text not in items and len(items) < limit:
            items.append(text)

    @classmethod
    def _extract_gap_components(cls, gap: str) -> list[str]:
        text = cls._clean_text(gap, limit=180)
        if not text:
            return []

        out: list[str] = []

        def _push(value: str) -> None:
            cleaned = cls._clean_text(value, limit=80)
            cleaned = re.sub(
                r"(等)?(の)?(詳細情報|詳細|情報)?(が|は)?(不足|不明|未開示|未取得|未公表|未確定|不透明)$",
                "",
                cleaned,
            ).strip(" 　()（）")
            if cleaned and cleaned not in out:
                out.append(cleaned)

        _push(text)

        slash_parts = re.split(r"\s*[／/]\s*", text)
        for part in slash_parts:
            _push(part)

        paren_parts = re.findall(r"[（(]([^）)]+)[）)]", text)
        for part in paren_parts:
            for token in re.split(r"\s*[・／/、，,]\s*", part):
                _push(token)

        return out[:6]

    @classmethod
    def _infer_gap_categories(
        cls,
        *,
        gap: str,
        components: list[str],
        headlines: list[str],
    ) -> list[str]:
        probe = " ".join([gap, *components, *headlines]).lower()
        rules = [
            ("impact", ("子会社", "買収", "m&a", "収益", "業績", "利益", "eps", "連結", "影響", "シナジー", "希薄化")),
            ("financing_terms", ("社債", "利率", "償還", "発行条件", "発行登録", "資金使途", "借入", "調達", "転換価額", "行使価額")),
            ("ownership_control", ("持分", "議決権", "出資比率", "保有比率", "株式数", "親会社", "子会社", "自己株", "消却", "株主還元")),
            ("timeline_conditions", ("予定日", "時期", "スケジュール", "効力発生日", "完了", "クロージング", "承認", "認可", "前提条件", "条件成就")),
            ("contract_terms", ("契約", "期間", "対価", "価格", "単価", "相手先", "最低保証", "解約", "更新条件", "取引条件")),
            ("guidance_progress", ("見通し", "ガイダンス", "進捗", "達成率", "上方修正", "下方修正", "通期", "受注", "稼働率")),
            ("accounting_valuation", ("簿価", "売却益", "減損", "評価", "のれん", "ppa", "会計処理", "税効果")),
            ("regulatory_legal", ("規制", "許認可", "届出", "訴訟", "行政", "独禁", "監査", "不祥事", "コンプライアンス")),
            ("asset_scope", ("対象資産", "対象事業", "案件規模", "設備", "拠点", "地域", "用途", "生産能力")),
        ]
        out: list[str] = []
        for name, keywords in rules:
            if any(token in probe for token in keywords):
                out.append(name)
        if not out:
            out.append("generic")
        elif "generic" not in out:
            out.append("generic")
        return out[:4]

    @staticmethod
    def _gap_category_profile(category: str) -> dict[str, list[str]]:
        profiles = {
            "impact": {
                "likely_sources": [
                    "company IR release",
                    "JPX timely disclosure",
                    "EDINET extraordinary report",
                    "earnings guidance / presentation materials",
                ],
                "search_focus": [
                    "Find explicit statements about earnings impact, consolidation timing, quantified effect, and guidance impact.",
                ],
                "query_suffixes": [
                    "子会社化 業績影響",
                    "収益影響",
                    "連結影響",
                    "適時開示",
                    "補足説明資料",
                ],
                "document_hints": [
                    "Look for timely disclosure or IR materials that quantify business or earnings impact.",
                ],
            },
            "financing_terms": {
                "likely_sources": [
                    "EDINET shelf registration supplement / prospectus",
                    "EDINET securities registration statement",
                    "company IR financing release",
                    "JPX timely disclosure",
                ],
                "search_focus": [
                    "Find coupon, maturity, issue amount, issue date, use of proceeds, and condition determination details.",
                ],
                "query_suffixes": [
                    "社債 条件決定",
                    "社債 利率 償還",
                    "発行登録追補書類",
                    "有価証券届出書",
                    "資金使途",
                ],
                "document_hints": [
                    "A correction to a shelf registration often omits final terms; search the related supplement, prospectus, or condition-determination notice.",
                ],
            },
            "ownership_control": {
                "likely_sources": [
                    "large shareholding report",
                    "company IR release",
                    "JPX timely disclosure",
                    "EDINET extraordinary report",
                ],
                "search_focus": [
                    "Find ownership ratio, voting rights ratio, share count, and post-transaction control structure.",
                ],
                "query_suffixes": [
                    "持分比率",
                    "議決権比率",
                    "株式数",
                    "大株主",
                    "自己株式",
                ],
                "document_hints": [
                    "Large-shareholding and extraordinary-report filings often contain exact ownership and voting-right percentages.",
                ],
            },
            "timeline_conditions": {
                "likely_sources": [
                    "JPX timely disclosure",
                    "company IR release",
                    "EDINET filing",
                    "official approval notice",
                ],
                "search_focus": [
                    "Find scheduled date, effective date, closing conditions, approvals, and dependency conditions.",
                ],
                "query_suffixes": [
                    "予定日",
                    "効力発生日",
                    "承認",
                    "前提条件",
                    "スケジュール",
                ],
                "document_hints": [
                    "If a transaction is pending, look for approval timelines and closing conditions in the related timely disclosure or notice.",
                ],
            },
            "contract_terms": {
                "likely_sources": [
                    "company IR release",
                    "JPX timely disclosure",
                    "EDINET securities registration statement",
                    "contract-related official notice",
                ],
                "search_focus": [
                    "Find contract amount, term, pricing, counterparties, cancellation terms, and minimum commitments.",
                ],
                "query_suffixes": [
                    "契約 条件",
                    "契約期間",
                    "対価",
                    "相手先",
                    "取引条件",
                ],
                "document_hints": [
                    "IR releases and timely disclosures often summarize contract economics more explicitly than the base filing.",
                ],
            },
            "guidance_progress": {
                "likely_sources": [
                    "earnings release",
                    "earnings presentation materials",
                    "company IR release",
                    "JPX timely disclosure",
                ],
                "search_focus": [
                    "Find guidance assumptions, revision background, progress rate, and management commentary on outlook.",
                ],
                "query_suffixes": [
                    "業績予想",
                    "進捗率",
                    "上方修正",
                    "下方修正",
                    "決算説明資料",
                ],
                "document_hints": [
                    "Earnings materials often contain quantitative explanation that is absent from shorter disclosure notices.",
                ],
            },
            "accounting_valuation": {
                "likely_sources": [
                    "financial statements / notes",
                    "earnings materials",
                    "EDINET filing",
                    "company IR release",
                ],
                "search_focus": [
                    "Find book value, gain/loss recognition, impairment, goodwill treatment, and accounting assumptions.",
                ],
                "query_suffixes": [
                    "簿価",
                    "売却益",
                    "減損",
                    "のれん",
                    "会計処理",
                ],
                "document_hints": [
                    "Financial statement notes often contain the accounting detail missing from event-style releases.",
                ],
            },
            "regulatory_legal": {
                "likely_sources": [
                    "official agency page",
                    "company IR release",
                    "JPX timely disclosure",
                    "EDINET filing",
                ],
                "search_focus": [
                    "Find approvals, regulatory status, legal proceedings, agency notices, and disclosed compliance risk.",
                ],
                "query_suffixes": [
                    "許認可",
                    "届出",
                    "規制",
                    "行政処分",
                    "訴訟",
                ],
                "document_hints": [
                    "Agency notices or official approval pages may be more definitive than company-issued summaries.",
                ],
            },
            "asset_scope": {
                "likely_sources": [
                    "company IR release",
                    "JPX timely disclosure",
                    "EDINET filing",
                    "project overview / explanatory materials",
                ],
                "search_focus": [
                    "Find which assets, business scope, capacity, geography, and operational boundaries are covered.",
                ],
                "query_suffixes": [
                    "対象資産",
                    "対象事業",
                    "案件概要",
                    "設備",
                    "地域",
                ],
                "document_hints": [
                    "Project overviews or explanatory materials may contain operational scope detail missing from the formal filing.",
                ],
            },
            "generic": {
                "likely_sources": [
                    "company IR release",
                    "JPX timely disclosure",
                    "EDINET filing",
                    "official agency page",
                ],
                "search_focus": [
                    "Identify the exact missing number, date, ratio, party, condition, or impact statement and search the official source most likely to contain it.",
                ],
                "query_suffixes": [
                    "IR",
                    "適時開示",
                    "site:jpx.co.jp",
                    "site:disclosure2.edinet-fsa.go.jp",
                ],
                "document_hints": [
                    "If the current filing type is too summary-level, search adjacent official disclosures or related IR materials from the same period.",
                ],
            },
        }
        return profiles.get(category, profiles["generic"])

    @classmethod
    def _infer_gap_fact_types(cls, *, gap: str, components: list[str]) -> list[str]:
        probe = " ".join([gap, *components]).lower()
        rules = [
            ("amount", ("金額", "総額", "価格", "対価", "売上", "利益", "収益", "調達", "簿価")),
            ("date_or_timeline", ("日", "時期", "予定", "効力", "完了", "期間", "期限", "償還")),
            ("ratio_or_rate", ("率", "比率", "利率", "議決権", "持分", "進捗率", "希薄化")),
            ("quantity", ("株", "株式数", "発行数", "数量", "件数", "枠", "口数")),
            ("counterparty_or_scope", ("相手先", "対象", "会社", "事業", "資産", "地域", "案件")),
            ("terms_or_conditions", ("条件", "前提", "契約", "承認", "許認可", "使途", "用途")),
            ("impact_or_outlook", ("影響", "見通し", "ガイダンス", "収益", "業績", "利益", "シナジー")),
        ]
        out: list[str] = []
        for name, keywords in rules:
            if any(token in probe for token in keywords):
                out.append(name)
        if not out:
            out.append("unspecified_detail")
        return out[:4]

    @classmethod
    def _build_gap_resolution_questions(
        cls,
        *,
        gap: str,
        components: list[str],
        categories: list[str],
        target_fact_types: list[str],
    ) -> list[str]:
        out: list[str] = []
        cls._append_unique_clean(
            out,
            "What exact missing fact is implied by this gap, and which official source is most likely to contain it?",
            limit=6,
            item_limit=180,
        )
        cls._append_unique_clean(
            out,
            "If the original filing is too summary-level, which related official disclosure from the same event or period should be searched next?",
            limit=6,
            item_limit=180,
        )
        if components:
            joined = " / ".join(components[:4])
            cls._append_unique_clean(
                out,
                f"Can each missing element be resolved separately: {joined}?",
                limit=6,
                item_limit=180,
            )
        if "impact" in categories:
            cls._append_unique_clean(
                out,
                "Is there an official statement quantifying earnings impact, consolidation timing, or guidance effect?",
                limit=6,
                item_limit=180,
            )
        if "financing_terms" in categories:
            cls._append_unique_clean(
                out,
                "Are coupon, maturity, issue amount, payment date, or use-of-proceeds terms disclosed in a related prospectus or supplement?",
                limit=6,
                item_limit=180,
            )
        if "timeline_conditions" in categories:
            cls._append_unique_clean(
                out,
                "Are closing conditions, approval requirements, or effective dates explicitly disclosed?",
                limit=6,
                item_limit=180,
            )
        if "counterparty_or_scope" in target_fact_types:
            cls._append_unique_clean(
                out,
                "Is the missing counterparty, target asset, or business scope named explicitly in any official source?",
                limit=6,
                item_limit=180,
            )
        cls._append_unique_clean(
            out,
            "If the detail remains unavailable, does any official source explicitly say it is undisclosed, undecided, or to be determined later?",
            limit=6,
            item_limit=180,
        )
        return out

    @classmethod
    def _build_mcp_search_queries(
        cls,
        *,
        code: str,
        company_name: str,
        source_payload: list[dict[str, Any]],
    ) -> list[str]:
        seeds: list[str] = []
        if company_name and code:
            seeds.append(f"{company_name} {code}")
        if company_name:
            seeds.append(company_name)
        elif code:
            seeds.append(code)

        out: list[str] = []

        def _push(query: str) -> None:
            text = cls._clean_text(query, limit=120)
            if text and text not in out:
                out.append(text)

        for seed in seeds:
            _push(f"{seed} IR")
            _push(f"{seed} 適時開示")
            _push(f"{seed} 決算短信")
            _push(f"{seed} site:jpx.co.jp")
            _push(f"{seed} site:disclosure2.edinet-fsa.go.jp")
            if len(out) >= 8:
                return out[:8]

        for row in source_payload:
            if len(out) >= 8:
                break
            if not isinstance(row, dict):
                continue
            headline = cls._clean_text(row.get("headline"), limit=80)
            doc_id = cls._extract_edinet_doc_id(cls._clean_text(row.get("source_url"), limit=240))
            if headline:
                if company_name:
                    _push(f"{company_name} {headline}")
                if code:
                    _push(f"{code} {headline}")
            if doc_id:
                _push(f"{doc_id} EDINET")
                if company_name:
                    _push(f"{company_name} {doc_id}")
        return out[:8]

    @classmethod
    def _build_source_navigation_hints(
        cls,
        *,
        code: str,
        company_name: str,
        source_payload: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in source_payload:
            if not isinstance(row, dict):
                continue
            source_url = cls._clean_text(row.get("source_url"), limit=240)
            source_type = cls._clean_text(row.get("source_type"), limit=40)
            headline = cls._clean_text(row.get("headline"), limit=160)
            published_at = cls._clean_text(row.get("published_at"), limit=40)
            if not source_url and not headline:
                continue

            item: dict[str, Any] = {
                "source_type": source_type,
                "headline": headline,
                "published_at": published_at,
            }
            if source_type == "edinet":
                doc_id = cls._extract_edinet_doc_id(source_url)
                if doc_id:
                    item["doc_id"] = doc_id
                item["browser_access_note"] = (
                    "This source_url is typically an EDINET API or download endpoint. "
                    "Use browser search on official EDINET, JPX, or company IR pages instead of opening the API URL directly."
                )
            else:
                item["browser_access_note"] = "Open the official page directly if the URL is browser-accessible."
                if source_url:
                    item["browser_ready_url"] = source_url

            hint_queries: list[str] = []
            query_seed = company_name or code
            if query_seed and headline:
                hint_queries.append(cls._clean_text(f"{query_seed} {headline}", limit=120))
            if query_seed and source_type == "company_ir":
                hint_queries.append(cls._clean_text(f"{query_seed} IR", limit=120))
            if hint_queries:
                item["recommended_queries"] = [q for q in hint_queries if q]

            out.append(item)
            if len(out) >= 5:
                break
        return out

    @classmethod
    def _build_gap_research_sources(cls, source_payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in source_payload:
            if not isinstance(row, dict):
                continue
            item = {
                "source_url": cls._clean_text(row.get("source_url"), limit=240),
                "source_type": cls._clean_text(row.get("source_type"), limit=40),
                "headline": cls._clean_text(row.get("headline"), limit=160),
                "published_at": cls._clean_text(row.get("published_at"), limit=40),
                "snippet": cls._clean_text(row.get("snippet"), limit=320),
                "evidence_refs": cls._normalize_text_list(row.get("evidence_refs"), limit=5, item_limit=240),
            }
            out.append(item)
        return out

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
        append_source_gaps: bool = True,
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
        if append_source_gaps and not cls._has_substantive_full_text(source_payload) and "報告書本文の取得または抽出が不十分" not in data_gaps:
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
