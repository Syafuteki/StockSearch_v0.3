from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select
from sqlalchemy.orm import Session

from jpswing.config import Settings
from jpswing.db.locks import try_advisory_xact_lock
from jpswing.db.models import (
    FundUniverseState,
    Instrument,
    FundRuleSuggestion,
    IntelDailyBudget,
    IntelItem,
    IntelQueue,
    IntelRuleSuggestion,
    Notification,
    ThemeStrengthDaily,
    ThemeSymbolMap,
)
from jpswing.db.session import DBSessionManager
from jpswing.fund.service import FundService
from jpswing.ingest.calendar import business_days_in_range
from jpswing.ingest.edinet_client import EdinetClient
from jpswing.ingest.jquants_client import JQuantsClient
from jpswing.intel.llm_client import IntelLlmClient
from jpswing.intel.budget import build_idempotency_key, compute_session_allowance
from jpswing.intel.priority import PriorityInput, rank_priorities
from jpswing.intel.schema import parse_published_at
from jpswing.intel.search import CompositeIntelSearchBackend, DefaultIntelSearchBackend, McpIntelSearchBackend
from jpswing.intel.tag_policy import map_tags_to_display
from jpswing.intel.tdnet import TdnetStubProvider
from jpswing.notify.discord_router import DiscordRouter, Topic
from jpswing.theme.service import ThemeService


def _edinet_code(doc: dict[str, Any]) -> str | None:
    for key in ("secCode", "sec_code", "securityCode", "securitiesCode"):
        raw = doc.get(key)
        if raw is None:
            continue
        s = "".join(ch for ch in str(raw).upper() if ch.isalnum())
        if len(s) >= 5:
            return s[:5]
        if len(s) == 4:
            return f"{s}0"
    return None


def _display_code(raw_code: str) -> str:
    code = str(raw_code or "").strip()
    if len(code) == 5 and code.isdigit() and code.endswith("0"):
        return code[:-1]
    return code


def _clip_text(value: Any, limit: int = 180) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return f"{text[: limit - 3]}..."


def _clean_text(value: Any, limit: int = 180) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return f"{text[: limit - 3]}..."


def _normalize_mcp_integrations(search_cfg: dict[str, Any]) -> list[str | dict[str, Any]]:
    out: list[str | dict[str, Any]] = []
    raw = search_cfg.get("mcp_plugin_ids", [])
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                token = item.strip()
                if token:
                    out.append(token)
    server = str(search_cfg.get("mcp_server", "")).strip()
    if server:
        plugin_id = server if server.startswith("mcp/") else f"mcp/{server}"
        if plugin_id not in out:
            out.append(plugin_id)
    extra = search_cfg.get("mcp_integrations", [])
    if isinstance(extra, list):
        for item in extra:
            if isinstance(item, str):
                token = item.strip()
                if token and token not in out:
                    out.append(token)
            elif isinstance(item, dict):
                out.append(item)
    return out


class FundIntelOrchestrator:
    def __init__(
        self,
        *,
        settings: Settings,
        db: DBSessionManager,
        jquants: JQuantsClient,
        notifier: DiscordRouter,
    ) -> None:
        self.settings = settings
        self.db = db
        self.jquants = jquants
        self.notifier = notifier
        self.logger = logging.getLogger(self.__class__.__name__)

        self.fund_service = FundService(settings.fund_config)
        self.theme_service = ThemeService(settings.theme_config)
        self.edinet = EdinetClient(
            base_url=settings.app_config.edinet.base_url,
            api_key=settings.app_config.edinet.api_key,
            timeout_sec=settings.app_config.edinet.timeout_sec,
        )
        intel_cfg = settings.intel_config
        budget_cfg = intel_cfg.get("budget", {})
        self.daily_budget = int(budget_cfg.get("daily_budget", 10))
        self.morning_cap = int(budget_cfg.get("morning_cap", 4))
        self.close_cap = int(budget_cfg.get("close_cap", 6))
        processing_cfg = intel_cfg.get("processing", {})
        self.process_all_candidates = bool(processing_cfg.get("process_all_candidates", False))
        self.pause_for_tech = bool(processing_cfg.get("pause_for_tech", True))
        self.pause_lead_minutes = int(processing_cfg.get("pause_lead_minutes", 3))
        search_cfg = intel_cfg.get("search", {})
        self.high_signal_tags = set(intel_cfg.get("notify", {}).get("high_signal_tags", []))
        self.risk_hard_keys = set(intel_cfg.get("notify", {}).get("risk_hard_keys", []))
        llm_cfg = intel_cfg.get("llm", {})
        mcp_integrations = _normalize_mcp_integrations(search_cfg)
        self.intel_llm = IntelLlmClient(
            base_url=settings.app_config.llm.base_url,
            model=settings.app_config.llm.model_name,
            api_key=settings.app_config.llm.api_key,
            temperature=float(llm_cfg.get("temperature", 0.0)),
            timeout_sec=settings.app_config.llm.timeout_sec,
            retries=int(llm_cfg.get("retries", 2)),
            use_mcp=bool(search_cfg.get("use_mcp", False)),
            mcp_integrations=mcp_integrations,
            mcp_chat_endpoint=str(search_cfg.get("lmstudio_chat_endpoint", "")).strip(),
            mcp_context_length=int(search_cfg.get("mcp_context_length", 64000)),
        )
        self.tdnet = TdnetStubProvider()
        default_backend = DefaultIntelSearchBackend(
            edinet_client=self.edinet,
            whitelist_domains=list(search_cfg.get("whitelist_domains", [])),
            company_ir_domains=search_cfg.get("company_ir_domains", {}),
            timeout_sec=int(search_cfg.get("request_timeout_sec", 20)),
            max_items_per_symbol=int(search_cfg.get("max_items_per_symbol", 5)),
            edinet_file_types=list(search_cfg.get("edinet_file_types", [5, 1, 2])),
            edinet_full_text_limit=int(search_cfg.get("edinet_full_text_limit", 30000)),
            ir_full_text_limit=int(search_cfg.get("ir_full_text_limit", 12000)),
        )
        mcp_backend = McpIntelSearchBackend(endpoint=str(search_cfg.get("mcp_endpoint", "")).strip())
        self.search = CompositeIntelSearchBackend([default_backend, mcp_backend])

    def run(self, *, session_name: str, business_date: date) -> dict[str, Any]:
        if session_name not in {"morning", "close"}:
            return {"status": "skipped", "reason": "unsupported_session"}
        with self.db.session_scope() as session:
            if not try_advisory_xact_lock(session, f"fund_intel:{business_date}:{session_name}"):
                self.logger.info("Skip fund/intel run due to advisory lock %s %s", business_date, session_name)
                return {"status": "locked"}

            # Weekly theme discovery and fund full scan on Monday.
            if business_date.weekday() == 0 and session_name == "morning":
                self.theme_service.weekly_discover(session, business_date, self.jquants)
                fund_changes = self.fund_service.refresh_states(
                    session,
                    business_date=business_date,
                    jquants=self.jquants,
                    force=True,
                )
            else:
                fund_changes = self.fund_service.refresh_states(
                    session,
                    business_date=business_date,
                    jquants=self.jquants,
                    force=False,
                )
            _ = self.theme_service.update_daily_strength(session, business_date)
            intel_result = self._intel_deepdive(session, business_date=business_date, session_name=session_name)

            # Notify only on proposals/signals/state changes.
            fund_state_changed = [c for c in fund_changes if c.changed]
            code_name_map = self._load_code_name_map(session, business_date)
            fund_intel_messages = self._build_fund_intel_notifications(
                session_name=session_name,
                business_date=business_date,
                intel_result=intel_result,
                fund_state_changed=fund_state_changed,
                code_name_map=code_name_map,
            )
            proposal_messages = self._build_proposal_notifications(session, business_date)
            self._send_notifications(
                session,
                business_date,
                fund_intel_messages,
                topic=Topic.FUND_INTEL_FLASH,
                run_type="fund_intel",
            )
            self._send_notifications(
                session,
                business_date,
                proposal_messages,
                topic=Topic.PROPOSALS,
                run_type="proposals",
            )
            return {
                "status": "ok",
                "session": session_name,
                "business_date": business_date.isoformat(),
                "intel_done": intel_result["done"],
                "fund_changes": len(fund_state_changed),
            }

    def run_intel_only(self, *, session_name: str, business_date: date) -> dict[str, Any]:
        if session_name not in {"morning", "close"}:
            return {"status": "skipped", "reason": "unsupported_session"}
        with self.db.session_scope() as session:
            if not try_advisory_xact_lock(session, f"fund_intel:{business_date}:{session_name}"):
                self.logger.info("Skip intel-only run due to advisory lock %s %s", business_date, session_name)
                return {"status": "locked"}

            intel_result = self._intel_deepdive(session, business_date=business_date, session_name=session_name)
            code_name_map = self._load_code_name_map(session, business_date)
            fund_intel_messages = self._build_fund_intel_notifications(
                session_name=session_name,
                business_date=business_date,
                intel_result=intel_result,
                fund_state_changed=[],
                code_name_map=code_name_map,
            )
            self._send_notifications(
                session,
                business_date,
                fund_intel_messages,
                topic=Topic.FUND_INTEL_FLASH,
                run_type="fund_intel",
            )
            return {
                "status": "ok",
                "session": session_name,
                "business_date": business_date.isoformat(),
                "intel_done": intel_result["done"],
                "fund_changes": 0,
                "mode": "intel_only",
            }

    def run_fund_weekly(self, *, business_date: date) -> dict[str, Any]:
        with self.db.session_scope() as session:
            if not try_advisory_xact_lock(session, f"fund_weekly:{business_date}"):
                return {"status": "locked"}
            changes = self.fund_service.refresh_states(
                session,
                business_date=business_date,
                jquants=self.jquants,
                force=True,
            )
            return {"status": "ok", "changes": len([c for c in changes if c.changed])}

    def run_fund_backfill(self, *, business_date: date) -> dict[str, Any]:
        cfg = self.settings.fund_config.get("bootstrap", {})
        lookback_business_days = int(cfg.get("lookback_business_days", 260))
        interval_sec = float(cfg.get("request_interval_sec", 0.0))
        if lookback_business_days <= 0:
            return {"status": "skipped", "reason": "lookback_business_days<=0"}
        with self.db.session_scope() as session:
            if not try_advisory_xact_lock(session, f"fund_backfill:{business_date}"):
                return {"status": "locked"}
            horizon_days = max(lookback_business_days * 2 + 32, 120)
            cal_from = business_date - timedelta(days=horizon_days)
            calendar_rows = self.jquants.fetch_calendar(cal_from, business_date)
            biz_days = business_days_in_range(calendar_rows, cal_from, business_date)
            target_days = biz_days[-lookback_business_days:] if len(biz_days) > lookback_business_days else biz_days
            if not target_days:
                return {"status": "skipped", "reason": "no_business_days"}

            master_rows = self.jquants.fetch_equities_master(business_date)
            total_changed = 0
            total_rows = 0
            for idx, d in enumerate(target_days, start=1):
                changes = self.fund_service.refresh_states(
                    session,
                    business_date=d,
                    jquants=self.jquants,
                    force=True,
                    master_rows=master_rows,
                    carry_forward=False,
                )
                changed_count = len([c for c in changes if c.changed])
                total_changed += changed_count
                total_rows += len(changes)
                if idx % 20 == 0 or idx == len(target_days):
                    self.logger.info(
                        "Fund backfill progress: %s/%s day=%s rows=%s changed=%s",
                        idx,
                        len(target_days),
                        d,
                        len(changes),
                        changed_count,
                    )
                if interval_sec > 0:
                    import time

                    time.sleep(interval_sec)

            # Ensure latest day has carry-forward snapshots for configured states.
            self.fund_service.refresh_states(
                session,
                business_date=business_date,
                jquants=self.jquants,
                force=False,
                master_rows=master_rows,
                carry_forward=True,
            )
            return {
                "status": "ok",
                "days": len(target_days),
                "rows_processed": total_rows,
                "state_changes": total_changed,
                "from": target_days[0].isoformat(),
                "to": target_days[-1].isoformat(),
            }

    def run_fund_daily_refresh(self, *, business_date: date) -> dict[str, Any]:
        with self.db.session_scope() as session:
            if not try_advisory_xact_lock(session, f"fund_daily:{business_date}"):
                return {"status": "locked"}
            changes = self.fund_service.refresh_states(
                session,
                business_date=business_date,
                jquants=self.jquants,
                force=False,
            )
            return {"status": "ok", "changes": len([c for c in changes if c.changed])}

    def run_theme_weekly(self, *, business_date: date) -> dict[str, Any]:
        with self.db.session_scope() as session:
            if not try_advisory_xact_lock(session, f"theme_weekly:{business_date}"):
                return {"status": "locked"}
            self.theme_service.weekly_discover(session, business_date, self.jquants)
            return {"status": "ok"}

    def run_theme_daily(self, *, business_date: date) -> dict[str, Any]:
        with self.db.session_scope() as session:
            if not try_advisory_xact_lock(session, f"theme_daily:{business_date}"):
                return {"status": "locked"}
            impacts = self.theme_service.update_daily_strength(session, business_date)
            sig = [i for i in impacts if i.significant]
            return {"status": "ok", "significant_count": len(sig)}

    def _intel_deepdive(self, session: Session, *, business_date: date, session_name: str) -> dict[str, Any]:
        budget = session.get(IntelDailyBudget, business_date)
        if budget is None:
            budget = IntelDailyBudget(business_date=business_date, done_count=0, morning_done=0, close_done=0)
            session.add(budget)
            session.flush()

        if self.process_all_candidates:
            max_run = 1_000_000
        else:
            session_cap = self.morning_cap if session_name == "morning" else self.close_cap
            session_done = budget.morning_done if session_name == "morning" else budget.close_done
            max_run = compute_session_allowance(
                daily_budget=self.daily_budget,
                session_cap=session_cap,
                done_total=budget.done_count,
                done_session=session_done,
            )
            if max_run <= 0:
                return {"queued": 0, "done": 0, "signals": []}

        try:
            docs = self.edinet.fetch_documents_list(business_date)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("EDINET documents unavailable. continue without docs. date=%s err=%s", business_date, exc)
            docs = []
        docs_by_code: dict[str, list[dict[str, Any]]] = {}
        new_doc_codes: set[str] = set()
        for d in docs:
            code = _edinet_code(d)
            if not code:
                continue
            new_doc_codes.add(code)
            docs_by_code.setdefault(code, []).append(d)

        fund_rows = session.execute(select(FundUniverseState)).scalars().all()
        fund_map = {r.code: r for r in fund_rows}
        a_codes = {r.code for r in fund_rows if r.state in {"IN", "WATCH"}} | new_doc_codes
        b_codes = self.theme_service.high_or_rising_theme_codes(session, business_date)
        candidate_codes = sorted(a_codes | b_codes)

        already_seen = session.execute(
            select(IntelQueue.code).where(
                IntelQueue.business_date == business_date,
                IntelQueue.status.in_(["pending", "done"]),
            )
        ).all()
        seen_codes = {r[0] for r in already_seen}

        ranking_inputs: list[PriorityInput] = []
        for code in candidate_codes:
            fund = fund_map.get(code)
            fund_state = fund.state if fund else "OUT"
            fund_score = float(fund.fund_score or 0.0) if fund else 0.0
            existing_tags = set((fund.tags or {}).get("items", []) if fund and isinstance(fund.tags, dict) else [])
            theme_strength, delta = self._theme_strength_for_code(session, code, business_date)
            ranking_inputs.append(
                PriorityInput(
                    code=code,
                    fund_state=fund_state,
                    fund_score=fund_score,
                    has_new_edinet=code in new_doc_codes,
                    theme_strength=theme_strength,
                    theme_strength_delta=delta,
                    has_high_signal_tag=bool(existing_tags & self.high_signal_tags),
                )
            )
        ranked = [r for r in rank_priorities(ranking_inputs) if r["code"] not in seen_codes]
        selected = ranked[:max_run]

        # enqueue idempotently
        queued = 0
        for item in selected:
            code = item["code"]
            idem = build_idempotency_key(business_date.isoformat(), session_name, code)
            existing = session.scalar(select(IntelQueue).where(IntelQueue.idempotency_key == idem))
            if existing:
                continue
            row = IntelQueue(
                business_date=business_date,
                session=session_name,
                code=code,
                priority=float(item["priority"]),
                sources_seed={"edinet_docs": docs_by_code.get(code, [])},
                status="pending",
                idempotency_key=idem,
            )
            session.add(row)
            queued += 1
        session.flush()

        pending_stmt = (
            select(IntelQueue)
            .where(
                IntelQueue.business_date == business_date,
                IntelQueue.status == "pending",
            )
            .order_by(IntelQueue.priority.desc(), IntelQueue.code.asc())
        )
        if not self.process_all_candidates:
            pending_stmt = pending_stmt.where(IntelQueue.session == session_name)

        pending = session.execute(pending_stmt).scalars().all()
        code_name_map = self._load_code_name_map(session, business_date)
        signals: list[dict[str, Any]] = []
        done = 0
        for q in pending[:max_run]:
            if self.process_all_candidates and self._should_pause_for_upcoming_tech(business_date):
                self.logger.info(
                    "Intel deep-dive paused for upcoming TECH run. date=%s session=%s remaining=%s",
                    business_date,
                    session_name,
                    max(0, len(pending) - done),
                )
                break
            code = q.code
            seed = q.sources_seed if isinstance(q.sources_seed, dict) else {}
            sources = self.search.fetch(code=code, business_date=business_date, seed=seed)
            if not sources:
                q.status = "skipped"
                continue
            source_payload = [
                {
                    "source_url": s.source_url,
                    "source_type": s.source_type,
                    "headline": s.headline,
                    "published_at": s.published_at,
                    "full_text": s.full_text,
                    "snippet": s.snippet,
                    "xbrl_facts": s.xbrl_facts,
                    "evidence_refs": s.evidence_refs,
                }
                for s in sources
            ]
            fund = fund_map.get(code)
            existing_tags = list((fund.tags or {}).get("items", []) if fund and isinstance(fund.tags, dict) else [])
            payload, valid, err = self.intel_llm.summarize_symbol_intel(
                code=code,
                source_payload=source_payload,
                existing_tags=existing_tags,
            )
            try:
                item = IntelItem(
                    code=code,
                    published_at=parse_published_at(payload.get("published_at")),
                    source_url=payload["source_url"],
                    source_type=payload["source_type"],
                    headline=payload["headline"],
                    summary=str(payload.get("summary") or ""),
                    facts={"items": payload.get("facts", [])},
                    tags={"items": payload.get("tags", [])},
                    risk_flags={"items": payload.get("risk_flags", [])},
                    critical_risk=bool(payload.get("critical_risk")),
                    evidence_refs={"items": payload.get("evidence_refs", [])},
                )
                session.add(item)
                session.flush()

                changed_fund = self.fund_service.apply_intel_aggregate(
                    session,
                    code=code,
                    tags_add=list(payload.get("tags", [])),
                    risk_flags=list(payload.get("risk_flags", [])),
                    critical_risk=bool(payload.get("critical_risk")),
                    evidence_refs=list(payload.get("evidence_refs", [])),
                )
                q.status = "done"
                done += 1

                new_high_signal = sorted(self.high_signal_tags.intersection(set(payload.get("tags", []))))
                hard_risks = set(payload.get("risk_flags", [])) & self.risk_hard_keys
                signal = {
                    "code": code,
                    "critical_risk": bool(payload.get("critical_risk")),
                    "high_signal_tags": new_high_signal,
                    "hard_risks": sorted(hard_risks),
                    "fund_state_changed": changed_fund,
                    "headline": str(payload.get("headline") or ""),
                    "summary": str(payload.get("summary") or ""),
                    "source_url": str(payload.get("source_url") or ""),
                    "source_type": str(payload.get("source_type") or ""),
                    "published_at": str(payload.get("published_at") or ""),
                    "facts": list(payload.get("facts") or []) if isinstance(payload.get("facts"), list) else [],
                    "data_gaps": list(payload.get("data_gaps") or []) if isinstance(payload.get("data_gaps"), list) else [],
                    "llm_valid": valid,
                    "llm_error": err,
                }
                signals.append(signal)
                detail_message = self._build_fund_intel_detail_notification(
                    session_name=session_name,
                    business_date=business_date,
                    signal=signal,
                    code_name_map=code_name_map,
                )
                self._send_notifications(
                    session,
                    business_date,
                    [detail_message],
                    topic=Topic.FUND_INTEL_DETAIL,
                    run_type="fund_intel_detail",
                )
            except Exception as exc:  # noqa: BLE001
                q.status = "failed"
                self.logger.exception("Intel queue item failed: %s %s", code, exc)

        budget.done_count += done
        if session_name == "morning":
            budget.morning_done += done
        else:
            budget.close_done += done

        return {"queued": queued, "done": done, "signals": signals}

    def _should_pause_for_upcoming_tech(self, business_date: date) -> bool:
        if not self.pause_for_tech:
            return False
        tz_name = str(self.settings.app_config.scheduler.timezone or "Asia/Tokyo")
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo("Asia/Tokyo")
        now = datetime.now(tz)
        if now.date() != business_date:
            return False
        lead_sec = max(0, self.pause_lead_minutes) * 60
        cron_exprs = [
            str(self.settings.app_config.scheduler.morning_cron or "").strip(),
            str(self.settings.app_config.scheduler.close_cron or "").strip(),
        ]
        for expr in cron_exprs:
            if not expr:
                continue
            try:
                trigger = CronTrigger.from_crontab(expr, timezone=tz)
                next_fire = trigger.get_next_fire_time(None, now)
            except Exception:
                continue
            if next_fire is None:
                continue
            delta = (next_fire - now).total_seconds()
            if 0 <= delta <= lead_sec:
                return True
        return False

    def _theme_strength_for_code(self, session: Session, code: str, business_date: date) -> tuple[float, float]:
        theme_ids = session.execute(select(ThemeSymbolMap.theme_id).where(ThemeSymbolMap.code == code)).all()
        if not theme_ids:
            return 0.0, 0.0
        strengths: list[float] = []
        deltas: list[float] = []
        for (theme_id,) in theme_ids:
            cur = session.scalar(
                select(ThemeStrengthDaily).where(
                    ThemeStrengthDaily.theme_id == theme_id,
                    ThemeStrengthDaily.asof_date == business_date,
                )
            )
            if cur is None:
                continue
            prev = session.scalar(
                select(ThemeStrengthDaily)
                .where(ThemeStrengthDaily.theme_id == theme_id, ThemeStrengthDaily.asof_date < business_date)
                .order_by(ThemeStrengthDaily.asof_date.desc())
                .limit(1)
            )
            strengths.append(cur.strength)
            deltas.append(cur.strength - (prev.strength if prev else 0.0))
        if not strengths:
            return 0.0, 0.0
        return sum(strengths) / len(strengths), sum(deltas) / len(deltas)

    def _build_fund_intel_notifications(
        self,
        *,
        session_name: str,
        business_date: date,
        intel_result: dict[str, Any],
        fund_state_changed: list[Any],
        code_name_map: dict[str, str] | None = None,
    ) -> list[str]:
        names = code_name_map or {}
        trigger_lines: list[str] = []
        for s in intel_result.get("signals", []):
            should_notify = bool(s["critical_risk"] or s["high_signal_tags"] or s["fund_state_changed"])
            if not should_notify:
                continue
            marker = "重大リスク" if s["critical_risk"] else "情報"
            tag_display = map_tags_to_display(list(s["high_signal_tags"]), self.settings.tag_policy)
            tags = ",".join(tag_display) if tag_display else "なし"
            hard = ",".join(s["hard_risks"]) if s["hard_risks"] else "なし"
            code = str(s["code"])
            display_code = _display_code(code)
            name = names.get(code, "")
            trigger_lines.append(
                f"【{marker}】{display_code} {name} / 注目タグ={tags} / ハードリスク={hard} / FUND変化={'あり' if s['fund_state_changed'] else 'なし'}".strip()
            )
            trigger_lines.append(f"  判定: {self._signal_assessment(s)}")

        for c in fund_state_changed:
            code = str(c.code)
            display_code = _display_code(code)
            name = names.get(code, "")
            trigger_lines.append(f"FUND状態変更 {display_code} {name}: {c.before_state or '-'} -> {c.after_state}".strip())

        if not trigger_lines:
            return []
        session_label = "朝" if session_name == "morning" else "引け後"
        return ["\n".join([f"FUND/Intel速報 {business_date.isoformat()}（{session_label}）"] + trigger_lines[:40])]

    def _build_fund_intel_detail_notification(
        self,
        *,
        session_name: str,
        business_date: date,
        signal: dict[str, Any],
        code_name_map: dict[str, str] | None = None,
    ) -> str:
        names = code_name_map or {}
        code = str(signal.get("code") or "")
        display_code = _display_code(code)
        name = names.get(code, "")
        tags = ",".join(map_tags_to_display(list(signal.get("high_signal_tags") or []), self.settings.tag_policy)) or "なし"
        hard = ",".join(list(signal.get("hard_risks") or [])) or "なし"
        headline = _clip_text(signal.get("headline"), 120)
        summary = _clip_text(signal.get("summary"), 220)
        source_url = str(signal.get("source_url") or "").strip()
        source_type = str(signal.get("source_type") or "").strip()
        published_at = str(signal.get("published_at") or "").strip()
        facts = self._normalize_fact_items(signal.get("facts"))
        data_gaps = self._normalize_fact_items(signal.get("data_gaps"), limit=2)
        session_label = "朝" if session_name == "morning" else "引け後"
        lines = [
            f"FUND/Intel深掘り {business_date.isoformat()}（{session_label}）",
            f"【深掘り】{display_code} {name} / 注目タグ={tags} / ハードリスク={hard} / FUND変化={'あり' if signal.get('fund_state_changed') else 'なし'}".strip(),
            f"  判定: {self._signal_assessment(signal)}",
        ]
        if published_at or source_type:
            lines.append(f"  公開: {published_at or '未取得'} / 種別: {source_type or '未取得'}")
        if headline:
            lines.append(f"  材料: {headline}")
        if facts:
            lines.append(f"  要点: {' / '.join(facts)}")
        if summary:
            lines.append(f"  分析: {summary}")
        if source_url:
            lines.append(f"  根拠: {source_url}")
        if data_gaps:
            lines.append(f"  欠損: {' / '.join(data_gaps)}")
        if not signal.get("llm_valid", False):
            lines.append("  備考: LLMフォールバック結果")
        lines.append("＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝")
        return "\n".join(lines)

    def _build_proposal_notifications(self, session: Session, business_date: date) -> list[str]:
        fund_rows, intel_rows = self._fetch_new_proposals(session, business_date)
        if not fund_rows and not intel_rows:
            return []

        lines: list[str] = [f"Proposals {business_date.isoformat()}"]
        for row in fund_rows:
            lines.append(f"- [fund] id={row.proposal_id} scope={row.scope}")
            for diff_line in self._proposal_diff_summary(row.diff):
                lines.append(f"  {diff_line}")
            if row.expected_effect:
                lines.append(f"  expected_effect: {row.expected_effect}")
            if row.risk:
                lines.append(f"  risk: {row.risk}")

        for row in intel_rows:
            lines.append(f"- [intel] id={row.proposal_id} scope={row.scope}")
            for diff_line in self._proposal_diff_summary(row.diff):
                lines.append(f"  {diff_line}")

        body = "\n".join(lines)
        return [body]

    def _send_notifications(
        self,
        session: Session,
        business_date: date,
        messages: list[str],
        *,
        topic: Topic,
        run_type: str,
    ) -> None:
        if not messages:
            return
        for msg in messages:
            ok, err = self.notifier.send(topic, {"content": msg})
            session.add(
                Notification(
                    report_date=business_date,
                    run_type=run_type,
                    content=msg,
                    success=ok,
                    error_message=err,
                )
            )

    @staticmethod
    def _fetch_new_proposals(session: Session, business_date: date) -> tuple[list[FundRuleSuggestion], list[IntelRuleSuggestion]]:
        boundary = datetime.combine(business_date, datetime.min.time())
        next_boundary = boundary.replace(hour=23, minute=59, second=59)
        fund = (
            session.execute(
                select(FundRuleSuggestion).where(
                    FundRuleSuggestion.created_at >= boundary,
                    FundRuleSuggestion.created_at <= next_boundary,
                )
            )
            .scalars()
            .all()
        )
        intel = (
            session.execute(
                select(IntelRuleSuggestion).where(
                    IntelRuleSuggestion.created_at >= boundary,
                    IntelRuleSuggestion.created_at <= next_boundary,
                )
            )
            .scalars()
            .all()
        )
        return fund, intel

    @staticmethod
    def _proposal_diff_summary(diff: Any) -> list[str]:
        if isinstance(diff, dict):
            return [f"- {k}: {str(v)[:160]}" for k, v in list(diff.items())[:3]]
        if isinstance(diff, list):
            return [f"- {str(item)[:160]}" for item in diff[:3]]
        if diff is None:
            return []
        return [f"- {str(diff)[:160]}"]

    @staticmethod
    def _normalize_fact_items(value: Any, *, limit: int = 3) -> list[str]:
        if not isinstance(value, list):
            return []
        out: list[str] = []
        for item in value:
            text = _clean_text(item, limit=120)
            if not text:
                continue
            if text.lower() in {"none", "n/a", "na", "unknown", "null", "not available", "未取得"}:
                continue
            out.append(text)
            if len(out) >= limit:
                break
        return out

    @staticmethod
    def _signal_assessment(signal: dict[str, Any]) -> str:
        critical = bool(signal.get("critical_risk"))
        hard_risks = list(signal.get("hard_risks") or [])
        high_tags = list(signal.get("high_signal_tags") or [])
        fund_changed = bool(signal.get("fund_state_changed"))
        if critical:
            return "ネガティブ（重大リスク☠️）"
        if hard_risks and high_tags:
            return "強弱混在（注目タグとハードリスクが同時発生）"
        if hard_risks:
            return "注意（ハードリスク）"
        if high_tags:
            return "ポジティブ（注目タグ）"
        if fund_changed:
            return "状態変化（FUND判定更新）"
        return "中立"

    @staticmethod
    def _load_code_name_map(session: Session, business_date: date) -> dict[str, str]:
        rows = session.execute(select(Instrument.code, Instrument.name).where(Instrument.as_of_date == business_date)).all()
        out: dict[str, str] = {}
        for code, name in rows:
            if code is None:
                continue
            out[str(code)] = str(name or "")
        return out

