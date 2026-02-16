from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
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
from jpswing.llm.prompts import build_single_candidate_messages, build_single_candidate_repair_messages
from jpswing.llm.validator import validate_single_candidate_output
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


def _json_safe(value: Any) -> Any:
    value = _as_py(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


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


_TEXT_PLACEHOLDERS = {
    "",
    "-",
    "n/a",
    "na",
    "none",
    "null",
    "unknown",
    "tbd",
    "not available",
    "not_applicable",
    "未取得",
}


def _is_placeholder_text(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in _TEXT_PLACEHOLDERS


def _as_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except Exception:  # noqa: BLE001
        return None


def _fallback_key_levels(candidate_payload: dict[str, Any]) -> dict[str, str]:
    tech = candidate_payload.get("technical_summary", {}) if isinstance(candidate_payload, dict) else {}
    price = _as_float(tech.get("adj_close"))
    atr = _as_float(tech.get("atr14"))
    ma25 = _as_float(tech.get("ma25"))

    if price is not None and atr is not None and atr > 0:
        stop = max(price - (1.2 * atr), 0.0)
        take = price + (2.0 * atr)
        return {
            "entry_idea": f"{price:.2f}円付近。出来高を伴う高値更新でエントリーを検討",
            "stop_idea": f"{stop:.2f}円（ATR14ベースの目安）",
            "takeprofit_idea": f"{take:.2f}円（ATR14の約2倍幅を目安）",
        }
    if price is not None and ma25 is not None:
        return {
            "entry_idea": f"{price:.2f}円付近。MA25({ma25:.2f}円)上を維持できるか確認",
            "stop_idea": f"MA25({ma25:.2f}円)明確割れで見直し",
            "takeprofit_idea": "直近高値更新時の分割利確を目安",
        }
    if price is not None:
        return {
            "entry_idea": f"{price:.2f}円付近で出来高増を確認してエントリー検討",
            "stop_idea": "直近安値割れで撤退を検討",
            "takeprofit_idea": "リスクリワード1:2以上を目安に分割利確",
        }
    return {
        "entry_idea": "出来高増を伴う高値更新でエントリー検討",
        "stop_idea": "直近安値割れで撤退を検討",
        "takeprofit_idea": "リスクリワード1:2以上を目安に分割利確",
    }


def _normalize_text_list(value: Any, *, default_items: list[str], limit: int = 3) -> list[str]:
    out: list[str] = []
    if isinstance(value, list):
        for item in value:
            text = str(item or "").strip()
            if not text or _is_placeholder_text(text):
                continue
            out.append(text)
            if len(out) >= limit:
                break
    if out:
        return out
    return default_items[:limit]


def _build_event_risks_from_candidate(candidate_payload: dict[str, Any]) -> list[str]:
    events = candidate_payload.get("events", []) if isinstance(candidate_payload, dict) else []
    if not isinstance(events, list) or not events:
        return []
    labels: list[str] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        etype = str(ev.get("event_type") or ev.get("type") or "").strip()
        if not etype:
            continue
        labels.append(etype)
        if len(labels) >= 2:
            break
    return labels


def _normalize_single_candidate_result(
    *,
    code: str,
    candidate_payload: dict[str, Any],
    parsed: dict[str, Any] | None,
    step2_rank: int,
    validation_error: str | None,
) -> dict[str, Any]:
    fallback_levels = _fallback_key_levels(candidate_payload)
    thesis_bull = _normalize_text_list(
        parsed.get("thesis_bull") if isinstance(parsed, dict) else None,
        default_items=["トレンドと出来高が維持される場合、上昇継続の余地"],
        limit=2,
    )
    thesis_bear = _normalize_text_list(
        parsed.get("thesis_bear") if isinstance(parsed, dict) else None,
        default_items=["ボラティリティ拡大時は急反落のリスク"],
        limit=2,
    )

    key_levels_raw = parsed.get("key_levels", {}) if isinstance(parsed, dict) else {}
    if not isinstance(key_levels_raw, dict):
        key_levels_raw = {}
    key_levels = {
        "entry_idea": key_levels_raw.get("entry_idea"),
        "stop_idea": key_levels_raw.get("stop_idea"),
        "takeprofit_idea": key_levels_raw.get("takeprofit_idea"),
    }
    for k, default_text in fallback_levels.items():
        if _is_placeholder_text(key_levels.get(k)):
            key_levels[k] = default_text
        else:
            key_levels[k] = str(key_levels[k]).strip()

    event_risks = _normalize_text_list(
        parsed.get("event_risks") if isinstance(parsed, dict) else None,
        default_items=_build_event_risks_from_candidate(candidate_payload),
        limit=3,
    )

    conf = None
    if isinstance(parsed, dict):
        conf = parsed.get("confidence_0_100")
    try:
        confidence = int(conf) if conf is not None else None
    except Exception:  # noqa: BLE001
        confidence = None
    if confidence is None:
        confidence = max(35, 76 - max(0, step2_rank - 1))
    confidence = min(max(confidence, 0), 100)

    data_gaps = _normalize_text_list(
        parsed.get("data_gaps") if isinstance(parsed, dict) else None,
        default_items=[],
        limit=6,
    )
    if isinstance(candidate_payload.get("data_gaps"), list):
        for gap in candidate_payload.get("data_gaps", []):
            text = str(gap or "").strip()
            if text and text not in data_gaps:
                data_gaps.append(text)
    if validation_error and "llm_output_invalid_or_missing" not in data_gaps:
        data_gaps.append("llm_output_invalid_or_missing")

    rule_suggestion = None
    if isinstance(parsed, dict):
        rs = parsed.get("rule_suggestion")
        if rs is not None:
            text = str(rs).strip()
            rule_suggestion = text or None

    return {
        "code": code,
        "thesis_bull": thesis_bull,
        "thesis_bear": thesis_bear,
        "key_levels": key_levels,
        "event_risks": event_risks,
        "confidence_0_100": confidence,
        "data_gaps": data_gaps,
        "rule_suggestion": rule_suggestion,
    }


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

    def _load_cached_bars(self, target_dates: list[date]) -> pd.DataFrame:
        if not target_dates:
            return pd.DataFrame()
        with self.db.session_scope() as session:
            rows = session.execute(
                select(
                    DailyBar.trade_date,
                    DailyBar.code,
                    DailyBar.open,
                    DailyBar.high,
                    DailyBar.low,
                    DailyBar.close,
                    DailyBar.adj_close,
                    DailyBar.volume,
                    DailyBar.market_cap,
                    DailyBar.raw_json,
                ).where(DailyBar.trade_date.in_(target_dates))
            ).all()
        if not rows:
            return pd.DataFrame()
        records = [
            {
                "trade_date": row[0],
                "code": str(row[1]),
                "open": _as_py(row[2]),
                "high": _as_py(row[3]),
                "low": _as_py(row[4]),
                "close": _as_py(row[5]),
                "adj_close": _as_py(row[6]),
                "volume": _as_py(row[7]),
                "market_cap": _as_py(row[8]),
                "raw_json": row[9] if isinstance(row[9], dict) else {},
            }
            for row in rows
        ]
        return pd.DataFrame(records)

    def run(self, run_type: str, report_date: date, *, run_post_hooks: bool = True) -> dict[str, Any]:
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
                    content = f"本日({report_date.isoformat()})は休場です。処理をスキップしました。\n{app_cfg.app.disclaimer}"
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
        if run_post_hooks:
            if result.get("status") == "ok" and run_type in {"morning", "close"} and business_today:
                try:
                    self.fund_intel_orchestrator.run(session_name=run_type, business_date=report_date)
                except Exception as exc:  # noqa: BLE001
                    self.logger.error("Fund/Intel hook failed and skipped: %s", exc)
            elif result.get("status") == "ok" and run_type in {"morning", "close"} and not business_today:
                self.logger.info("TECH succeeded on non-business day. skip Fund/Intel hook.")
            elif result.get("status") != "ok":
                self.logger.error("TECH run failed. skipping Fund/Intel hook. run_type=%s status=%s", run_type, result.get("status"))
        else:
            if result.get("status") == "ok" and run_type in {"morning", "close"}:
                self.logger.info("TECH post hook skipped by caller. run_type=%s report_date=%s", run_type, report_date)
            elif result.get("status") != "ok":
                self.logger.error("TECH run failed. run_type=%s status=%s", run_type, result.get("status"))
        self.logger.info("Pipeline end run_type=%s report_date=%s status=%s", run_type, report_date, result.get("status"))
        return result

    def run_intel_background(self, report_date: date) -> dict[str, Any]:
        intel_schedule = self.settings.intel_config.get("schedule", {})
        session_name = str(intel_schedule.get("session", "close")).strip() or "close"
        run_on_holiday = bool(intel_schedule.get("run_on_holiday", False))
        use_prev_on_holiday = bool(intel_schedule.get("use_previous_business_day_on_holiday", False))

        calendar_rows = self._fetch_calendar(report_date - timedelta(days=240), report_date + timedelta(days=14))
        business_today = is_business_day(report_date, calendar_rows)
        if business_today:
            business_date = report_date
        else:
            if not run_on_holiday:
                self.logger.info("Intel background skipped on holiday: %s", report_date)
                return {"status": "holiday_skip", "report_date": report_date.isoformat(), "reason": "run_on_holiday=false"}
            if use_prev_on_holiday:
                business_date = previous_business_day(report_date, calendar_rows)
            else:
                business_date = report_date

        self.logger.info(
            "Intel background run start report_date=%s business_date=%s session=%s",
            report_date,
            business_date,
            session_name,
        )
        try:
            result = self.fund_intel_orchestrator.run_intel_only(
                session_name=session_name,
                business_date=business_date,
            )
            self.logger.info("Intel background run end result=%s", result)
            return result
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Intel background run failed: %s", exc)
            return {"status": "error", "error": str(exc), "report_date": report_date.isoformat()}

    def run_backfill_range(
        self,
        *,
        start_date: date,
        end_date: date,
        mode: str = "close_only",
    ) -> dict[str, Any]:
        if end_date < start_date:
            return {
                "status": "invalid_range",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            }
        mode_map = {
            "close_only": ["close"],
            "morning_close": ["morning", "close"],
        }
        run_types = mode_map.get(mode)
        if not run_types:
            return {"status": "invalid_mode", "mode": mode}

        calendar_rows = self._fetch_calendar(start_date - timedelta(days=30), end_date + timedelta(days=14))
        target_days = business_days_in_range(calendar_rows, start_date, end_date)
        if not target_days:
            return {
                "status": "no_business_days",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            }

        self.logger.info(
            "Backfill range start from=%s to=%s mode=%s business_days=%s",
            start_date,
            end_date,
            mode,
            len(target_days),
        )
        summary_rows: list[dict[str, Any]] = []
        ok_days = 0
        failed_days = 0
        for i, d in enumerate(target_days, start=1):
            row: dict[str, Any] = {"date": d.isoformat(), "runs": {}}
            day_ok = True
            self.logger.info("Backfill day %s/%s date=%s mode=%s", i, len(target_days), d, mode)
            for rt in run_types:
                result = self.run(rt, d, run_post_hooks=False)
                status = str(result.get("status"))
                row["runs"][rt] = status
                if status != "ok":
                    day_ok = False
            if day_ok:
                ok_days += 1
            else:
                failed_days += 1
                summary_rows.append(row)

        self.logger.info(
            "Backfill range end from=%s to=%s mode=%s ok_days=%s failed_days=%s",
            start_date,
            end_date,
            mode,
            ok_days,
            failed_days,
        )
        return {
            "status": "ok",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "mode": mode,
            "business_days": len(target_days),
            "ok_days": ok_days,
            "failed_days": failed_days,
            "failed_details": summary_rows[:20],
        }

    def run_auto_recover(self, report_date: date) -> dict[str, Any]:
        cfg = self.settings.intel_config.get("recovery", {})
        if not bool(cfg.get("enabled", False)):
            return {"status": "disabled"}

        lookback_business_days = max(1, int(cfg.get("lookback_business_days", 40)))
        max_days_per_run = max(1, int(cfg.get("max_days_per_run", 3)))
        mode = str(cfg.get("mode", "close_only")).strip() or "close_only"
        run_on_holiday = bool(cfg.get("run_on_holiday", True))

        calendar_rows = self._fetch_calendar(report_date - timedelta(days=lookback_business_days * 4), report_date + timedelta(days=14))
        business_today = is_business_day(report_date, calendar_rows)
        if not business_today and not run_on_holiday:
            return {"status": "holiday_skip", "report_date": report_date.isoformat(), "reason": "run_on_holiday=false"}

        # Avoid colliding with intraday close data update timing by recovering up to previous business day.
        end_date = previous_business_day(report_date, calendar_rows)
        if end_date >= report_date and business_today:
            end_date = previous_business_day(end_date, calendar_rows)
        if end_date is None:
            return {"status": "no_target"}

        biz_days = business_days_in_range(
            calendar_rows,
            end_date - timedelta(days=lookback_business_days * 4),
            end_date,
        )
        if len(biz_days) > lookback_business_days:
            biz_days = biz_days[-lookback_business_days:]
        if not biz_days:
            return {"status": "no_business_days"}

        with self.db.session_scope() as session:
            done_rows = session.execute(
                select(ScreenTop30Daily.trade_date)
                .where(
                    ScreenTop30Daily.trade_date >= biz_days[0],
                    ScreenTop30Daily.trade_date <= biz_days[-1],
                )
                .distinct()
            ).all()
        done_dates = {r[0] for r in done_rows if r and r[0] is not None}
        missing_dates = [d for d in biz_days if d not in done_dates]
        if not missing_dates:
            return {
                "status": "no_gap",
                "from": biz_days[0].isoformat(),
                "to": biz_days[-1].isoformat(),
                "checked_days": len(biz_days),
            }

        targets = missing_dates[:max_days_per_run]
        self.logger.info(
            "Auto recover start report_date=%s mode=%s targets=%s",
            report_date,
            mode,
            [d.isoformat() for d in targets],
        )
        mode_map = {
            "close_only": ["close"],
            "morning_close": ["morning", "close"],
        }
        run_types = mode_map.get(mode, ["close"])
        repaired: list[dict[str, Any]] = []
        for d in targets:
            row: dict[str, Any] = {"date": d.isoformat(), "runs": {}}
            for rt in run_types:
                result = self.run(rt, d, run_post_hooks=False)
                row["runs"][rt] = str(result.get("status"))
            repaired.append(row)
        return {
            "status": "ok",
            "report_date": report_date.isoformat(),
            "mode": mode,
            "checked_days": len(biz_days),
            "missing_days": len(missing_dates),
            "repaired_days": len(targets),
            "details": repaired,
        }

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

        cached_bars_df = self._load_cached_bars(history_targets)
        invalid_cached_dates: set[date] = set()
        if not cached_bars_df.empty:
            quality_df = cached_bars_df.copy()
            quality_df["price_for_cache"] = pd.to_numeric(quality_df["adj_close"], errors="coerce").fillna(
                pd.to_numeric(quality_df["close"], errors="coerce")
            )
            quality_df["volume_num"] = pd.to_numeric(quality_df["volume"], errors="coerce")
            by_day = (
                quality_df.groupby(pd.to_datetime(quality_df["trade_date"]).dt.date)
                .agg(price_non_null=("price_for_cache", lambda s: int(s.notna().sum())))
                .reset_index()
            )
            for _, row in by_day.iterrows():
                if int(row.get("price_non_null", 0)) <= 0:
                    invalid_cached_dates.add(row["trade_date"])
            if invalid_cached_dates:
                cached_bars_df = cached_bars_df[
                    ~pd.to_datetime(cached_bars_df["trade_date"]).dt.date.isin(list(invalid_cached_dates))
                ].copy()

        cached_dates = (
            set(pd.to_datetime(cached_bars_df["trade_date"]).dt.date.tolist()) if not cached_bars_df.empty else set()
        )
        missing_dates = [d for d in history_targets if (d not in cached_dates or d in invalid_cached_dates)]
        # Always refresh trade_date once to keep latest value up to date.
        if trade_date not in missing_dates:
            missing_dates.append(trade_date)
        self.logger.info(
            "Daily bars incremental fetch: cache_days=%s invalid_cache_days=%s missing_days=%s",
            len(cached_dates),
            len(invalid_cached_dates),
            len(missing_dates),
        )

        bars_raw: list[dict[str, Any]] = []
        for d in missing_dates:
            rows = self._safe_fetch(f"daily_bars:{d.isoformat()}", lambda date=d: self.jquants.fetch_daily_bars(date))
            bars_raw.extend(rows)
        fetched_bars = [x for x in (normalize_bar_row(r) for r in bars_raw) if x]
        fetched_bars_df = pd.DataFrame(fetched_bars)

        if cached_bars_df.empty and fetched_bars_df.empty:
            bars_df = pd.DataFrame()
        elif cached_bars_df.empty:
            bars_df = fetched_bars_df.copy()
        elif fetched_bars_df.empty:
            bars_df = cached_bars_df.copy()
        else:
            bars_df = pd.concat([cached_bars_df, fetched_bars_df], ignore_index=True)

        if bars_df.empty:
            self.logger.warning("No bars available for trade_date=%s", trade_date)
            content = f"株価データが取得できないため処理を中断しました: {trade_date.isoformat()}\n{self.settings.app_config.app.disclaimer}"
            self._send_and_log_notifications(report_date, run_type, [content])
            return {"status": "no_bars", "trade_date": trade_date.isoformat()}

        bars_df = bars_df.drop_duplicates(subset=["trade_date", "code"], keep="last").sort_values(["code", "trade_date"])
        use_adj_close = bool(rules.get("step2", {}).get("use_adj_close", True))
        features_df = compute_features(bars_df, use_adj_close=use_adj_close)
        latest_features_df = features_df[features_df["trade_date"] == trade_date].copy()
        latest_bars_df = bars_df[bars_df["trade_date"] == trade_date].copy()

        universe_df = build_universe(latest_bars_df, instruments_df, rules, use_adj_close=use_adj_close)
        if universe_df.empty:
            content = f"Step1通過銘柄が0件のため処理を終了しました: {trade_date.isoformat()}\n{self.settings.app_config.app.disclaimer}"
            self._send_and_log_notifications(report_date, run_type, [content])
            with self.db.session_scope() as session:
                self._store_basic_data(
                    session,
                    trade_date=trade_date,
                    rule_version=rule_version,
                    instruments_df=instruments_df,
                    bars_df=fetched_bars_df,
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
        llm_messages: list[dict[str, Any]] = []
        llm_raw_response: dict[str, Any] = {}
        llm_payload_for_db: dict[str, Any] = {
            "mode": "per_symbol",
            "results": [],
        }
        step2_rank_map = {str(r["code"]): int(r["rank"]) for r in top30_df.to_dict("records")}
        for candidate in candidates_payload:
            code = str(candidate.get("code") or "")
            step2_rank = step2_rank_map.get(code, 9999)
            messages = build_single_candidate_messages(
                report_date=report_date,
                run_type=run_type,
                candidate_payload=candidate,
                rules_payload=rules,
            )
            llm_messages.append({"code": code, "stage": "main", "messages": messages})
            parsed: dict[str, Any] | None = None
            validation_error: str | None = None
            payload_for_log: dict[str, Any] | None = None
            last_content = ""
            try:
                raw = self.llm.chat_completion(messages)
                llm_raw_response = raw
                last_content = _safe_get_latest_content(raw)
                parsed, validation_error, payload_for_log = validate_single_candidate_output(last_content)
                if parsed is None:
                    repair_messages = build_single_candidate_repair_messages(
                        report_date=report_date,
                        run_type=run_type,
                        candidate_payload=candidate,
                        rules_payload=rules,
                        previous_output=last_content[:8000],
                        validation_error=validation_error or "unknown_validation_error",
                    )
                    llm_messages.append({"code": code, "stage": "repair", "messages": repair_messages})
                    repair_raw = self.llm.chat_completion(repair_messages)
                    llm_raw_response = repair_raw
                    repair_content = _safe_get_latest_content(repair_raw)
                    parsed2, validation_error2, payload_for_log2 = validate_single_candidate_output(repair_content)
                    if parsed2 is not None:
                        parsed = parsed2
                        validation_error = None
                        payload_for_log = payload_for_log2
                    else:
                        validation_error = validation_error2 or validation_error
                        payload_for_log = payload_for_log2 if payload_for_log2 is not None else payload_for_log
            except Exception as exc:  # noqa: BLE001
                self.logger.exception("LLM processing failed for code=%s", code)
                validation_error = str(exc)

            normalized = _normalize_single_candidate_result(
                code=code,
                candidate_payload=candidate,
                parsed=parsed,
                step2_rank=step2_rank,
                validation_error=validation_error,
            )
            llm_map[code] = normalized

            row: dict[str, Any] = {
                "code": code,
                "validation_ok": parsed is not None,
                "validation_error": validation_error,
                "payload": payload_for_log if payload_for_log is not None else ({"content": last_content} if last_content else None),
            }
            llm_payload_for_db["results"].append(row)
            if parsed is None:
                self.logger.warning("LLM validation fallback used for code=%s err=%s", code, validation_error)

        llm_valid = bool(llm_map)
        if not llm_valid:
            llm_error = "no_per_symbol_candidates"

        if llm_valid:
            rows = []
            for code, info in llm_map.items():
                if code not in step2_rank_map:
                    continue
                conf = int(info.get("confidence_0_100", 0) or 0)
                rows.append(
                    {
                        "code": code,
                        "confidence_0_100": conf,
                        "step2_rank": step2_rank_map.get(code, 9999),
                        "reason_json": info,
                    }
                )
            shortlist_df = pd.DataFrame(rows)
            shortlist_df = (
                shortlist_df.sort_values(["confidence_0_100", "step2_rank", "code"], ascending=[False, True, True])
                .drop_duplicates(subset=["code"], keep="first")
                .head(top10_n)
                .copy()
            )
            if not shortlist_df.empty:
                shortlist_df["rank"] = list(range(1, len(shortlist_df) + 1))
                shortlist_df = shortlist_df[["code", "rank", "reason_json"]]
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
                bars_df=fetched_bars_df,
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
                        "raw_json": _json_safe(r.get("raw_json")) or {},
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
                        "raw_json": _json_safe(r.get("raw_json")) or {},
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
                        "raw_json": _json_safe(r),
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
                        "details_json": _json_safe(r.get("details_json")) or {},
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
                        "score_breakdown": _json_safe(r.get("score_breakdown")) or {},
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
                        "reason_json": _json_safe(r.get("reason_json")) or {},
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
                context_json=_json_safe(market_context),
                raw_json=_json_safe(market_raw),
            )
        )

        replace_rows_for_date(session, EventsDaily, trade_date)
        if events_rows:
            session.bulk_insert_mappings(
                EventsDaily,
                [
                    {
                        "trade_date": row.get("trade_date"),
                        "code": row.get("code"),
                        "event_type": row.get("event_type"),
                        "payload_json": _json_safe(row.get("payload_json")),
                    }
                    for row in events_rows
                ],
            )

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


