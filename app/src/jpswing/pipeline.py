from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from jpswing.config import Settings
from jpswing.db.models import (
    DailyBar,
    EventsDaily,
    FeaturesDaily,
    Instrument,
    LlmRun,
    MarketContextDaily,
    Notification,
    RuleSuggestion,
    RuleVersion,
    ScreenTop30Daily,
    ShortlistTop10Daily,
    UniverseDaily,
)
from jpswing.db.session import DBSessionManager, get_latest_shortlist_codes_before, replace_rows_for_date
from jpswing.enrich.events import collect_events_for_codes
from jpswing.enrich.market_context import parse_index_row
from jpswing.enrich.sq import is_sq_window
from jpswing.features.indicators import compute_features
from jpswing.fund_intel_orchestrator import FundIntelOrchestrator
from jpswing.ingest.calendar import business_days_in_range, is_business_day, previous_business_day
from jpswing.ingest.fx_client import FxClient
from jpswing.ingest.jquants_client import JQuantsClient
from jpswing.ingest.transformers import normalize_bar_row, normalize_instrument_row
from jpswing.llm.client import LlmClient
from jpswing.llm.prompts import build_top10_messages
from jpswing.llm.validator import validate_llm_output
from jpswing.notify.discord_router import DiscordRouter, Topic
from jpswing.notify.formatter import format_report_message
from jpswing.screening.step1 import build_universe
from jpswing.screening.step2 import screen_top30


def _as_py(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:  # noqa: BLE001
        pass
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


def _safe_get_latest_content(llm_response: dict[str, Any]) -> str:
    choices = llm_response.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    message = choices[0].get("message")
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if not isinstance(content, str):
        return ""
    return content


class SwingPipeline:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        app_cfg = settings.app_config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db = DBSessionManager(app_cfg.database.url, echo=app_cfg.database.echo)
        self.jquants = JQuantsClient(
            base_url=app_cfg.jquants.base_url,
            api_key=app_cfg.jquants.api_key,
            timeout_sec=app_cfg.jquants.timeout_sec,
        )
        self.fx_client = FxClient(
            base_url=app_cfg.external_fx.alpha_vantage_base_url,
            api_key=app_cfg.external_fx.alpha_vantage_api_key,
            timeout_sec=app_cfg.jquants.timeout_sec,
        )
        self.llm = LlmClient(
            base_url=app_cfg.llm.base_url,
            model_name=app_cfg.llm.model_name,
            api_key=app_cfg.llm.api_key,
            temperature=app_cfg.llm.temperature,
            timeout_sec=app_cfg.llm.timeout_sec,
        )
        self.notifier = DiscordRouter.from_config(app_cfg.discord)
        self.fund_intel_orchestrator = FundIntelOrchestrator(
            settings=settings,
            db=self.db,
            jquants=self.jquants,
            notifier=self.notifier,
        )
        self.db.init_schema()

    def close(self) -> None:
        self.jquants.close()

    def _rule_version(self) -> str:
        return str(self.settings.rules.get("version") or "v0")

    def _ensure_rule_version(self, session: Session, report_date: date) -> None:
        version = self._rule_version()
        existing = session.scalar(select(RuleVersion).where(RuleVersion.version == version))
        if existing:
            return
        row = RuleVersion(
            version=version,
            applied_from=report_date,
            description=self.settings.rules.get("description"),
            active=True,
        )
        session.add(row)

    def _fetch_calendar(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        try:
            return self.jquants.fetch_calendar(from_date, to_date)
        except Exception:  # noqa: BLE001
            self.logger.exception("Failed to fetch market calendar")
            return []

    def _wait_for_close_update(self, trade_date: date) -> list[dict[str, Any]]:
        polling = self.settings.app_config.jquants.polling
        if not polling.enabled:
            return self.jquants.fetch_daily_bars(trade_date)
        deadline = time.time() + (polling.max_wait_minutes * 60)
        interval = max(30, polling.interval_sec)
        while True:
            rows = self.jquants.fetch_daily_bars(trade_date)
            if rows and self.jquants.has_date_in_rows(rows, trade_date):
                self.logger.info("Daily bars updated for %s. rows=%s", trade_date, len(rows))
                return rows
            if time.time() >= deadline:
                self.logger.warning("Timeout waiting for daily bars update. proceed with best effort.")
                return rows
            self.logger.info("Daily bars not ready yet. polling again in %ss", interval)
            time.sleep(interval)

    def run(self, run_type: str, report_date: date) -> dict[str, Any]:
        self.logger.info("Pipeline start run_type=%s report_date=%s", run_type, report_date)
        app_cfg = self.settings.app_config
        calendar_rows = self._fetch_calendar(report_date - timedelta(days=240), report_date + timedelta(days=14))
        business_today = is_business_day(report_date, calendar_rows)

        if not business_today:
            self.logger.info("Today is market holiday: %s", report_date)
            if run_type == "morning" and app_cfg.app.allow_morning_on_holiday:
                trade_date = previous_business_day(report_date, calendar_rows)
            else:
                if app_cfg.app.send_holiday_notice:
                    content = f"譛ｬ譌･({report_date.isoformat()})縺ｯ莨大ｴ縺ｧ縺吶る夂衍繧偵せ繧ｭ繝・・縺励∪縺励◆縲・n{app_cfg.app.disclaimer}"
                    self._send_and_log_notifications(report_date, run_type, [content])
                return {"status": "holiday_skip", "report_date": report_date.isoformat()}
        else:
            if run_type == "morning":
                trade_date = previous_business_day(report_date, calendar_rows)
            else:
                trade_date = report_date

        with self.db.session_scope() as session:
            self._ensure_rule_version(session, report_date)

        if run_type == "close":
            _ = self._wait_for_close_update(trade_date)

        result = self._execute(report_date=report_date, trade_date=trade_date, run_type=run_type, calendar_rows=calendar_rows)
        if result.get("status") == "ok" and run_type in {"morning", "close"} and business_today:
            try:
                self.fund_intel_orchestrator.run(session_name=run_type, business_date=report_date)
            except Exception as exc:  # noqa: BLE001
                self.logger.error("Fund/Intel hook failed and skipped: %s", exc)
        elif result.get("status") == "ok" and run_type in {"morning", "close"} and not business_today:
            self.logger.info("TECH succeeded on non-business day. skip Fund/Intel hook.")
        elif result.get("status") != "ok":
            self.logger.error("TECH run failed. skipping Fund/Intel hook. run_type=%s status=%s", run_type, result.get("status"))
        self.logger.info("Pipeline end run_type=%s report_date=%s status=%s", run_type, report_date, result.get("status"))
        return result

    def _execute(
        self,
        *,
        report_date: date,
        trade_date: date,
        run_type: str,
        calendar_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        rules = self.settings.rules
        rule_version = self._rule_version()
        top10_n = int(rules.get("step3", {}).get("top_n", 10))
        top30_n = int(rules.get("step2", {}).get("top_n", 30))

        history_days = int(self.settings.app_config.app.history_days)
        history_from = trade_date - timedelta(days=history_days * 3)
        business_days = business_days_in_range(calendar_rows, history_from, trade_date)
        history_targets = business_days[-history_days:] if business_days else [trade_date]
        self.logger.info("History targets for bars: %s", len(history_targets))

        master_rows = self._safe_fetch("equities_master", lambda: self.jquants.fetch_equities_master(trade_date))
        normalized_master = [x for x in (normalize_instrument_row(r) for r in master_rows) if x]
        instruments_df = pd.DataFrame(normalized_master)

        bars_raw: list[dict[str, Any]] = []
        for d in history_targets:
            rows = self._safe_fetch(f"daily_bars:{d.isoformat()}", lambda date=d: self.jquants.fetch_daily_bars(date))
            bars_raw.extend(rows)
        normalized_bars = [x for x in (normalize_bar_row(r) for r in bars_raw) if x]
        bars_df = pd.DataFrame(normalized_bars)
        if bars_df.empty:
            self.logger.warning("No bars available for trade_date=%s", trade_date)
            content = f"譬ｪ萓｡繝・・繧ｿ縺梧悴蜿門ｾ励・縺溘ａ蜃ｦ逅・ｒ荳ｭ譁ｭ縺励∪縺励◆: {trade_date.isoformat()}\n{self.settings.app_config.app.disclaimer}"
            self._send_and_log_notifications(report_date, run_type, [content])
            return {"status": "no_bars", "trade_date": trade_date.isoformat()}

        bars_df = bars_df.drop_duplicates(subset=["trade_date", "code"], keep="last").sort_values(["code", "trade_date"])
        use_adj_close = bool(rules.get("step2", {}).get("use_adj_close", True))
        features_df = compute_features(bars_df, use_adj_close=use_adj_close)
        latest_features_df = features_df[features_df["trade_date"] == trade_date].copy()
        latest_bars_df = bars_df[bars_df["trade_date"] == trade_date].copy()

        universe_df = build_universe(latest_bars_df, instruments_df, rules, use_adj_close=use_adj_close)
        if universe_df.empty:
            content = f"Step1騾夐℃驫俶氛縺・莉ｶ縺ｧ縺励◆: {trade_date.isoformat()}\n{self.settings.app_config.app.disclaimer}"
            self._send_and_log_notifications(report_date, run_type, [content])
            with self.db.session_scope() as session:
                self._store_basic_data(
                    session,
                    trade_date=trade_date,
                    rule_version=rule_version,
                    instruments_df=instruments_df,
                    bars_df=bars_df,
                    latest_features_df=latest_features_df,
                    universe_df=universe_df,
                    top30_df=pd.DataFrame(),
                    shortlist_df=pd.DataFrame(),
                    llm_run_id=None,
                    market_context={},
                    market_raw={},
                    events_rows=[],
                )
            return {"status": "empty_universe", "trade_date": trade_date.isoformat()}

        top30_df = screen_top30(latest_features_df, set(universe_df["code"].tolist()), rules).head(top30_n)
        if top30_df.empty:
            self.logger.warning("Top30 empty after step2. fallback to universe by roc20.")
            fallback = latest_features_df[latest_features_df["code"].isin(set(universe_df["code"]))].copy()
            fallback = fallback.sort_values("roc20", ascending=False).head(top30_n)
            fallback["rank"] = list(range(1, len(fallback) + 1))
            fallback["score"] = fallback["roc20"].fillna(0.0)
            fallback["score_breakdown"] = [{"fallback": "roc20"} for _ in range(len(fallback))]
            top30_df = fallback

        top30_df = top30_df.merge(
            instruments_df[["code", "name"]] if not instruments_df.empty else pd.DataFrame(columns=["code", "name"]),
            on="code",
            how="left",
        )

        next_business = trade_date + timedelta(days=1)
        while not is_business_day(next_business, calendar_rows):
            next_business += timedelta(days=1)

        earnings_rows = self._safe_fetch(
            "earnings_calendar",
            lambda: self.jquants.fetch_earnings_calendar(trade_date, next_business),
        )
        margin_rows = self._safe_fetch("margin_alert", lambda: self.jquants.fetch_margin_alert(trade_date))
        short_sale_rows = self._safe_fetch("short_sale_report", lambda: self.jquants.fetch_short_sale_report(trade_date))
        short_ratio_rows = self._safe_fetch("short_ratio", lambda: self.jquants.fetch_short_ratio(trade_date))

        code_list = top30_df["code"].astype(str).tolist()
        event_map, event_rows, event_summary = collect_events_for_codes(
            trade_date=trade_date,
            codes=code_list,
            earnings_rows=earnings_rows,
            margin_rows=margin_rows,
            short_sale_rows=short_sale_rows,
            short_ratio_rows=short_ratio_rows,
        )

        market_context, market_raw = self._collect_market_context(
            trade_date=trade_date,
            calendar_rows=calendar_rows,
            sq_window=int(rules.get("step3", {}).get("sq_week_window_business_days", 2)),
        )

        candidates_payload = self._build_candidates_payload(
            top30_df=top30_df,
            features_df=features_df,
            event_map=event_map,
            market_context=market_context,
            lookback_days=int(self.settings.app_config.app.llm_input_lookback_days),
        )

        llm_map: dict[str, dict[str, Any]] = {}
        llm_run_id: int | None = None
        shortlist_df = pd.DataFrame()
        llm_valid = False
        llm_error = ""
        llm_payload_for_db: dict[str, Any] = {}
        llm_messages = build_top10_messages(
            report_date=report_date,
            run_type=run_type,
            candidates_payload=candidates_payload,
            rules_payload=rules,
        )

        llm_raw_response: dict[str, Any] = {}
        try:
            llm_raw_response = self.llm.chat_completion(llm_messages)
            llm_content = _safe_get_latest_content(llm_raw_response)
            llm_model, llm_error, llm_payload = validate_llm_output(llm_content)
            llm_payload_for_db = llm_payload if llm_payload is not None else {"content": llm_content}
            if llm_model:
                llm_valid = True
                for item in llm_model.top10:
                    llm_map[item.code] = item.model_dump()
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("LLM processing failed")
            llm_error = str(exc)

        if llm_valid:
            rows = []
            for code, info in llm_map.items():
                if code not in set(top30_df["code"].astype(str)):
                    continue
                rows.append({"code": code, "rank": info["top10_rank"], "reason_json": info})
            shortlist_df = pd.DataFrame(rows)
            shortlist_df = (
                shortlist_df.sort_values(["rank", "code"])
                .drop_duplicates(subset=["rank"], keep="first")
                .drop_duplicates(subset=["code"], keep="first")
                .head(top10_n)
                .copy()
            )
            if not shortlist_df.empty:
                shortlist_df["rank"] = list(range(1, len(shortlist_df) + 1))
        if shortlist_df.empty:
            self.logger.warning("LLM invalid or empty. fallback to step2 top scores.")
            shortlist_df = top30_df.sort_values("rank").head(top10_n)[["code", "rank"]].copy()
            shortlist_df["reason_json"] = [{"fallback": "step2_score"} for _ in range(len(shortlist_df))]

        shortlist_df = shortlist_df.merge(
            top30_df[
                [
                    "code",
                    "name",
                    "score",
                    "ma25",
                    "roc20",
                    "volume_ratio20",
                    "breakout_strength20",
                    "ma10",
                    "ma75",
                    "atr14",
                ]
            ],
            on="code",
            how="left",
        )

        signal_changes: dict[str, list[str]] | None = None
        with self.db.session_scope() as session:
            llm_run_id = self._store_llm_run(
                session=session,
                report_date=report_date,
                run_type=run_type,
                llm_messages=llm_messages,
                llm_output=llm_payload_for_db if llm_payload_for_db else llm_raw_response,
                validation_ok=llm_valid,
                validation_error=llm_error or None,
                usage=llm_raw_response.get("usage") if isinstance(llm_raw_response, dict) else None,
            )
            self._store_rule_suggestions(session, report_date, llm_map, llm_run_id)
            self._store_basic_data(
                session,
                trade_date=trade_date,
                rule_version=rule_version,
                instruments_df=instruments_df,
                bars_df=bars_df,
                latest_features_df=latest_features_df,
                universe_df=universe_df,
                top30_df=top30_df,
                shortlist_df=shortlist_df,
                llm_run_id=llm_run_id,
                market_context=market_context,
                market_raw=market_raw,
                events_rows=event_rows,
            )

            if run_type == "close":
                new_codes = set(shortlist_df["code"].astype(str).tolist())
                prev_codes = get_latest_shortlist_codes_before(session, ShortlistTop10Daily, trade_date, rule_version)
                signal_changes = {
                    "in": sorted(list(new_codes - prev_codes)),
                    "out": sorted(list(prev_codes - new_codes)),
                }

        notifications = format_report_message(
            report_date=report_date,
            run_type=run_type,
            top10_df=shortlist_df.sort_values("rank").head(top10_n),
            llm_map=llm_map,
            event_summary=event_summary,
            disclaimer=self.settings.app_config.app.disclaimer,
            tag_policy=self.settings.tag_policy,
            signal_changes=signal_changes,
            max_chars=self.settings.app_config.discord.max_message_chars,
            max_parts=self.settings.app_config.discord.split_max_parts,
        )
        self._send_and_log_notifications(report_date, run_type, notifications)
        return {
            "status": "ok",
            "report_date": report_date.isoformat(),
            "trade_date": trade_date.isoformat(),
            "top30_count": int(len(top30_df)),
            "top10_count": int(len(shortlist_df)),
            "llm_valid": llm_valid,
        }

    def _collect_market_context(
        self,
        *,
        trade_date: date,
        calendar_rows: list[dict[str, Any]],
        sq_window: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        index_items: list[dict[str, Any]] = []
        index_raw: dict[str, Any] = {}
        for code in self.settings.app_config.market.index_codes:
            rows = self._safe_fetch(f"indices:{code}", lambda c=code: self.jquants.fetch_indices_bars_daily(trade_date, c))
            index_raw[code] = rows
            for row in rows:
                parsed = parse_index_row(row)
                parsed["requested_code"] = code
                index_items.append(parsed)

        usdjpy_rows = self._safe_fetch(
            "indices:usdjpy",
            lambda: self.jquants.fetch_indices_bars_daily(trade_date, self.settings.app_config.market.usd_jpy_symbol),
        )
        usdjpy = None
        if usdjpy_rows:
            parsed = parse_index_row(usdjpy_rows[0])
            usdjpy = {
                "date": parsed.get("date"),
                "open": parsed.get("open"),
                "close": parsed.get("close"),
                "source": "jquants_indices",
            }
        elif self.settings.app_config.external_fx.use_fallback:
            fx = self.fx_client.fetch_usdjpy_daily(trade_date)
            if fx:
                usdjpy = {
                    "date": fx["date"].isoformat() if fx.get("date") else trade_date.isoformat(),
                    "open": fx.get("open"),
                    "close": fx.get("close"),
                    "source": "alphavantage",
                }
        options225 = self._safe_fetch("options225", lambda: self.jquants.fetch_225_options(trade_date))
        sq_flag = is_sq_window(trade_date, calendar_rows, business_day_window=sq_window)
        context = {
            "indices": index_items,
            "usdjpy": usdjpy,
            "sq_week_flag": sq_flag,
            "options225_count": len(options225),
        }
        raw = {
            "indices": index_raw,
            "usdjpy_rows": usdjpy_rows,
            "options225": options225,
        }
        return context, raw

    def _build_candidates_payload(
        self,
        *,
        top30_df: pd.DataFrame,
        features_df: pd.DataFrame,
        event_map: dict[str, list[dict[str, Any]]],
        market_context: dict[str, Any],
        lookback_days: int,
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for _, row in top30_df.iterrows():
            code = str(row["code"])
            hist = features_df[features_df["code"] == code].tail(lookback_days)
            latest = hist.iloc[-1] if not hist.empty else row
            data_gaps: list[str] = []
            for field in ("ma25", "ma75", "roc20", "roc60", "rsi14", "atr14", "volume_ratio20"):
                if _as_py(latest.get(field)) is None:
                    data_gaps.append(field)
            result.append(
                {
                    "code": code,
                    "name": row.get("name"),
                    "step2_rank": int(row.get("rank")),
                    "step2_score": float(row.get("score")),
                    "technical_summary": {
                        "adj_close": _as_py(latest.get("adj_close")),
                        "ma10": _as_py(latest.get("ma10")),
                        "ma25": _as_py(latest.get("ma25")),
                        "ma75": _as_py(latest.get("ma75")),
                        "roc20": _as_py(latest.get("roc20")),
                        "roc60": _as_py(latest.get("roc60")),
                        "rsi14": _as_py(latest.get("rsi14")),
                        "atr14": _as_py(latest.get("atr14")),
                        "volume_ratio20": _as_py(latest.get("volume_ratio20")),
                        "breakout_strength20": _as_py(latest.get("breakout_strength20")),
                    },
                    "events": event_map.get(code, []),
                    "market_context": market_context,
                    "data_gaps": data_gaps,
                }
            )
        return result

    def _store_llm_run(
        self,
        *,
        session: Session,
        report_date: date,
        run_type: str,
        llm_messages: list[dict[str, Any]],
        llm_output: dict[str, Any],
        validation_ok: bool,
        validation_error: str | None,
        usage: dict[str, Any] | None,
    ) -> int:
        row = LlmRun(
            report_date=report_date,
            run_type=run_type,
            model=self.settings.app_config.llm.model_name,
            temperature=self.settings.app_config.llm.temperature,
            prompt_json={"messages": llm_messages},
            output_json=llm_output,
            validation_ok=validation_ok,
            validation_errors=validation_error,
            token_usage_json=usage,
        )
        session.add(row)
        session.flush()
        return int(row.id)

    def _store_rule_suggestions(
        self,
        session: Session,
        report_date: date,
        llm_map: dict[str, dict[str, Any]],
        llm_run_id: int | None,
    ) -> None:
        for code, info in llm_map.items():
            suggestion = info.get("rule_suggestion")
            if not suggestion:
                continue
            session.add(
                RuleSuggestion(
                    report_date=report_date,
                    code=code,
                    suggestion_text=str(suggestion),
                    source_llm_run_id=llm_run_id,
                    status="pending",
                    raw_json=info,
                )
            )

    def _store_basic_data(
        self,
        session: Session,
        *,
        trade_date: date,
        rule_version: str,
        instruments_df: pd.DataFrame,
        bars_df: pd.DataFrame,
        latest_features_df: pd.DataFrame,
        universe_df: pd.DataFrame,
        top30_df: pd.DataFrame,
        shortlist_df: pd.DataFrame,
        llm_run_id: int | None,
        market_context: dict[str, Any],
        market_raw: dict[str, Any],
        events_rows: list[dict[str, Any]],
    ) -> None:
        if not instruments_df.empty:
            replace_rows_for_date(session, Instrument, trade_date, date_field="as_of_date")
            session.bulk_insert_mappings(
                Instrument,
                [
                    {
                        "as_of_date": trade_date,
                        "code": str(r["code"]),
                        "name": _as_py(r.get("name")),
                        "market": _as_py(r.get("market")),
                        "issued_shares": _as_py(r.get("issued_shares")),
                        "market_cap": _as_py(r.get("market_cap")),
                        "raw_json": _as_py(r.get("raw_json")) or {},
                    }
                    for r in instruments_df.to_dict("records")
                ],
            )

        if not bars_df.empty:
            for d in sorted(set(pd.to_datetime(bars_df["trade_date"]).dt.date.tolist())):
                replace_rows_for_date(session, DailyBar, d)
            session.bulk_insert_mappings(
                DailyBar,
                [
                    {
                        "trade_date": _as_py(r.get("trade_date")),
                        "code": str(r["code"]),
                        "open": _as_py(r.get("open")),
                        "high": _as_py(r.get("high")),
                        "low": _as_py(r.get("low")),
                        "close": _as_py(r.get("close")),
                        "adj_close": _as_py(r.get("adj_close")),
                        "volume": _as_py(r.get("volume")),
                        "market_cap": _as_py(r.get("market_cap")),
                        "raw_json": _as_py(r.get("raw_json")) or {},
                    }
                    for r in bars_df.to_dict("records")
                ],
            )

        replace_rows_for_date(session, FeaturesDaily, trade_date)
        if not latest_features_df.empty:
            session.bulk_insert_mappings(
                FeaturesDaily,
                [
                    {
                        "trade_date": _as_py(r.get("trade_date")),
                        "code": str(r["code"]),
                        "ma10": _as_py(r.get("ma10")),
                        "ma25": _as_py(r.get("ma25")),
                        "ma75": _as_py(r.get("ma75")),
                        "ma75_slope_5": _as_py(r.get("ma75_slope_5")),
                        "roc20": _as_py(r.get("roc20")),
                        "roc60": _as_py(r.get("roc60")),
                        "rsi14": _as_py(r.get("rsi14")),
                        "atr14": _as_py(r.get("atr14")),
                        "volume_ratio20": _as_py(r.get("volume_ratio20")),
                        "breakout_strength20": _as_py(r.get("breakout_strength20")),
                        "volatility_penalty": _as_py(r.get("volatility_penalty")),
                        "raw_json": {k: _as_py(v) for k, v in r.items()},
                    }
                    for r in latest_features_df.to_dict("records")
                ],
            )

        replace_rows_for_date(session, UniverseDaily, trade_date, extra_filters={"rule_version": rule_version})
        if not universe_df.empty:
            session.bulk_insert_mappings(
                UniverseDaily,
                [
                    {
                        "trade_date": trade_date,
                        "code": str(r["code"]),
                        "passed": True,
                        "market_cap": _as_py(r.get("market_cap_effective")),
                        "market_cap_estimated": bool(r.get("market_cap_estimated")),
                        "details_json": _as_py(r.get("details_json")) or {},
                        "rule_version": rule_version,
                    }
                    for r in universe_df.to_dict("records")
                ],
            )

        replace_rows_for_date(session, ScreenTop30Daily, trade_date, extra_filters={"rule_version": rule_version})
        if not top30_df.empty:
            session.bulk_insert_mappings(
                ScreenTop30Daily,
                [
                    {
                        "trade_date": trade_date,
                        "code": str(r["code"]),
                        "rank": int(r["rank"]),
                        "score": float(r["score"]),
                        "score_breakdown": _as_py(r.get("score_breakdown")) or {},
                        "rule_version": rule_version,
                    }
                    for r in top30_df.to_dict("records")
                ],
            )

        replace_rows_for_date(session, ShortlistTop10Daily, trade_date, extra_filters={"rule_version": rule_version})
        if not shortlist_df.empty:
            session.bulk_insert_mappings(
                ShortlistTop10Daily,
                [
                    {
                        "trade_date": trade_date,
                        "code": str(r["code"]),
                        "rank": int(r["rank"]),
                        "llm_run_id": llm_run_id,
                        "reason_json": _as_py(r.get("reason_json")) or {},
                        "rule_version": rule_version,
                    }
                    for r in shortlist_df.to_dict("records")
                ],
            )

        replace_rows_for_date(session, MarketContextDaily, trade_date)
        session.add(
            MarketContextDaily(
                trade_date=trade_date,
                sq_week_flag=bool(market_context.get("sq_week_flag")),
                context_json=market_context,
                raw_json=market_raw,
            )
        )

        replace_rows_for_date(session, EventsDaily, trade_date)
        if events_rows:
            session.bulk_insert_mappings(EventsDaily, events_rows)

    def _send_and_log_notifications(self, report_date: date, run_type: str, contents: list[str]) -> None:
        with self.db.session_scope() as session:
            for content in contents:
                ok, error = self.notifier.send(Topic.TECH, {"content": content})
                session.add(
                    Notification(
                        report_date=report_date,
                        run_type=run_type,
                        content=content,
                        success=ok,
                        error_message=error,
                    )
                )

    def _safe_fetch(self, name: str, fn: Any) -> list[dict[str, Any]]:
        try:
            rows = fn()
            if rows is None:
                self.logger.warning("Fetch %s returned None", name)
                return []
            if not isinstance(rows, list):
                self.logger.warning("Fetch %s returned non-list payload", name)
                return []
            self.logger.info("Fetched %s rows=%s", name, len(rows))
            return rows
        except Exception:  # noqa: BLE001
            self.logger.exception("Fetch failed: %s", name)
            return []

