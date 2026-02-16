from __future__ import annotations

import argparse
import logging
import threading
from datetime import date, datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from jpswing.config import load_settings
from jpswing.pipeline import SwingPipeline
from jpswing.rag.embedder import LocalEmbedder
from jpswing.rag.indexer import KbIndexer
from jpswing.utils.logging import setup_logging
from jpswing.utils.time import today_jst


_SCHEDULER_MUTEX = threading.Lock()
_STARTUP_CATCHUP_MUTEX = threading.Lock()
_STARTUP_CATCHUP_PHASE = "done"  # tech -> fund -> done


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="JP stock swing daily agent")
    parser.add_argument("--config-dir", default="config", help="Path to config directory")
    parser.add_argument("--once", action="store_true", help="Run one time and exit")
    parser.add_argument(
        "--run-type",
        choices=[
            "morning",
            "close",
            "all",
            "fund_weekly",
            "fund_daily",
            "fund_backfill",
            "fund_auto_recover",
            "theme_weekly",
            "theme_daily",
            "intel_background",
            "auto_recover",
            "recover_range",
            "rag_index",
        ],
        default="morning",
    )
    parser.add_argument("--date", help="Target report date (YYYY-MM-DD)")
    parser.add_argument("--from-date", help="Range start date for recover_range (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="Range end date for recover_range (YYYY-MM-DD)")
    parser.add_argument(
        "--recover-mode",
        choices=["close_only", "morning_close"],
        default="close_only",
        help="Recover mode for recover_range",
    )
    return parser.parse_args()


def _run_serialized(job_name: str, fn) -> None:
    log = logging.getLogger(__name__)
    if not _SCHEDULER_MUTEX.acquire(blocking=False):
        log.warning("Scheduled job skipped due to active run: %s", job_name)
        return
    try:
        fn()
    finally:
        _SCHEDULER_MUTEX.release()


def _parse_date(raw: str | None) -> date:
    if not raw:
        return today_jst()
    return date.fromisoformat(raw)


def _get_startup_catchup_phase() -> str:
    with _STARTUP_CATCHUP_MUTEX:
        return _STARTUP_CATCHUP_PHASE


def _set_startup_catchup_phase(phase: str) -> None:
    global _STARTUP_CATCHUP_PHASE
    with _STARTUP_CATCHUP_MUTEX:
        _STARTUP_CATCHUP_PHASE = phase


def _is_startup_catchup_done() -> bool:
    return _get_startup_catchup_phase() == "done"


def _is_recovery_phase_complete(result: dict[str, object]) -> bool:
    status = str(result.get("status", "")).strip()
    if status in {"disabled", "no_gap", "no_business_days", "no_target", "holiday_skip"}:
        return True
    if status != "ok":
        return False
    missing_days = int(result.get("missing_days", 0) or 0)
    repaired_days = int(result.get("repaired_days", 0) or 0)
    return missing_days <= repaired_days


def _should_pause_startup_catchup(pipeline: SwingPipeline, report_date: date) -> bool:
    cfg = pipeline.settings.intel_config.get("startup_catchup", {})
    if not bool(cfg.get("enabled", True)):
        return False
    lead_minutes = max(0, int(cfg.get("pause_lead_minutes", 3)))
    if lead_minutes <= 0:
        return False

    tz_name = str(pipeline.settings.app_config.scheduler.timezone or "Asia/Tokyo")
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    if now.date() != report_date:
        return False
    lead_sec = lead_minutes * 60

    fund_cfg = pipeline.settings.fund_config.get("schedule", {})
    cron_exprs = [
        str(pipeline.settings.app_config.scheduler.morning_cron or "").strip(),
        str(pipeline.settings.app_config.scheduler.close_cron or "").strip(),
        str(fund_cfg.get("weekly_cron", "0 7 * * 1")).strip(),
        str(fund_cfg.get("daily_refresh_cron", "10 7 * * 1-5")).strip(),
    ]
    for expr in cron_exprs:
        if not expr:
            continue
        try:
            trigger = CronTrigger.from_crontab(expr, timezone=tz)
            next_fire = trigger.get_next_fire_time(None, now)
        except Exception:  # noqa: BLE001
            continue
        if next_fire is None:
            continue
        delta = (next_fire - now).total_seconds()
        if 0 <= delta <= lead_sec:
            return True
    return False


def _run_job(pipeline: SwingPipeline, run_type: str) -> None:
    def _do() -> None:
        report_date = today_jst()
        logging.getLogger(__name__).info("Scheduled run start type=%s date=%s", run_type, report_date)
        pipeline.run(run_type, report_date)

    _run_serialized(f"tech:{run_type}", _do)


def _run_aux_job(pipeline: SwingPipeline, job_type: str) -> None:
    def _do() -> None:
        report_date = today_jst()
        log = logging.getLogger(__name__)
        log.info("Scheduled aux job start type=%s date=%s", job_type, report_date)
        orch = pipeline.fund_intel_orchestrator
        if job_type == "fund_weekly":
            result = orch.run_fund_weekly(business_date=report_date)
        elif job_type == "fund_daily":
            result = orch.run_fund_daily_refresh(business_date=report_date)
        elif job_type == "theme_weekly":
            result = orch.run_theme_weekly(business_date=report_date)
        else:
            result = orch.run_theme_daily(business_date=report_date)
        log.info("Scheduled aux job end type=%s result=%s", job_type, result)

    _run_serialized(f"aux:{job_type}", _do)


def _run_intel_background_job(pipeline: SwingPipeline) -> None:
    def _do() -> None:
        report_date = today_jst()
        log = logging.getLogger(__name__)
        if not _is_startup_catchup_done():
            log.info(
                "Scheduled intel background skipped until startup catch-up completes. phase=%s",
                _get_startup_catchup_phase(),
            )
            return
        log.info("Scheduled intel background start date=%s", report_date)
        result = pipeline.run_intel_background(report_date)
        log.info("Scheduled intel background end result=%s", result)

    _run_serialized("intel_background", _do)


def _run_auto_recover_job(pipeline: SwingPipeline) -> None:
    def _do() -> None:
        report_date = today_jst()
        log = logging.getLogger(__name__)
        log.info("Scheduled auto recover start date=%s", report_date)
        result = pipeline.run_auto_recover(report_date)
        log.info("Scheduled auto recover end result=%s", result)

    _run_serialized("auto_recover", _do)


def _run_startup_catchup_step_job(pipeline: SwingPipeline) -> None:
    def _do() -> None:
        logger = logging.getLogger(__name__)
        report_date = today_jst()
        phase = _get_startup_catchup_phase()
        if phase == "done":
            return
        if _should_pause_startup_catchup(pipeline, report_date):
            logger.info("Startup catch-up paused for upcoming TECH/FUND window. phase=%s", phase)
            return

        while True:
            if _should_pause_startup_catchup(pipeline, report_date):
                logger.info("Startup catch-up paused for upcoming TECH/FUND window. phase=%s", _get_startup_catchup_phase())
                return
            phase = _get_startup_catchup_phase()
            if phase == "done":
                return
            if phase == "tech":
                logger.info("Startup catch-up step start phase=tech date=%s", report_date)
                result = pipeline.run_auto_recover(report_date)
                logger.info("Startup catch-up step end phase=tech result=%s", result)
                if _is_recovery_phase_complete(result):
                    fund_recovery_cfg = pipeline.settings.fund_config.get("recovery", {})
                    if bool(fund_recovery_cfg.get("run_on_startup", True)):
                        _set_startup_catchup_phase("fund")
                    else:
                        _set_startup_catchup_phase("done")
                    continue
                return
            if phase == "fund":
                logger.info("Startup catch-up step start phase=fund date=%s", report_date)
                result = pipeline.fund_intel_orchestrator.run_fund_auto_recover(report_date=report_date)
                logger.info("Startup catch-up step end phase=fund result=%s", result)
                if _is_recovery_phase_complete(result):
                    _set_startup_catchup_phase("done")
                    logger.info("Startup catch-up completed")
                return
            logger.warning("Unknown startup catch-up phase: %s", phase)
            _set_startup_catchup_phase("done")
            return

    _run_serialized("startup_catchup", _do)


def _init_startup_catchup_state(pipeline: SwingPipeline) -> None:
    logger = logging.getLogger(__name__)
    cfg = pipeline.settings.intel_config.get("startup_catchup", {})
    enabled = bool(cfg.get("enabled", True))
    tech_recovery_cfg = pipeline.settings.intel_config.get("recovery", {})
    fund_recovery_cfg = pipeline.settings.fund_config.get("recovery", {})
    tech_on_startup = bool(tech_recovery_cfg.get("run_on_startup", True))
    fund_on_startup = bool(fund_recovery_cfg.get("run_on_startup", True))
    if enabled and (tech_on_startup or fund_on_startup):
        if tech_on_startup:
            _set_startup_catchup_phase("tech")
            logger.info("Startup catch-up initialized phase=tech")
        else:
            _set_startup_catchup_phase("fund")
            logger.info("Startup catch-up initialized phase=fund")
    elif enabled:
        _set_startup_catchup_phase("done")
        logger.info("Startup catch-up disabled by run_on_startup flags")
    else:
        _set_startup_catchup_phase("done")
        logger.info("Startup catch-up disabled by config")


def main() -> None:
    args = _parse_args()
    settings = load_settings(args.config_dir)
    setup_logging(settings.app_config.app.log_level)
    logger = logging.getLogger(__name__)
    pipeline = SwingPipeline(settings)

    try:
        if args.once:
            report_date = _parse_date(args.date)
            run_types = ["morning", "close"] if args.run_type == "all" else [args.run_type]
            for rt in run_types:
                if rt in {"morning", "close"}:
                    result = pipeline.run(rt, report_date)
                elif rt == "fund_weekly":
                    result = pipeline.fund_intel_orchestrator.run_fund_weekly(business_date=report_date)
                elif rt == "fund_daily":
                    result = pipeline.fund_intel_orchestrator.run_fund_daily_refresh(business_date=report_date)
                elif rt == "fund_backfill":
                    result = pipeline.fund_intel_orchestrator.run_fund_backfill(business_date=report_date)
                elif rt == "fund_auto_recover":
                    result = pipeline.fund_intel_orchestrator.run_fund_auto_recover(report_date=report_date)
                elif rt == "theme_weekly":
                    result = pipeline.fund_intel_orchestrator.run_theme_weekly(business_date=report_date)
                elif rt == "intel_background":
                    result = pipeline.run_intel_background(report_date)
                elif rt == "auto_recover":
                    result = pipeline.run_auto_recover(report_date)
                elif rt == "recover_range":
                    start = _parse_date(args.from_date or args.date)
                    end = _parse_date(args.to_date or args.date)
                    result = pipeline.run_backfill_range(
                        start_date=start,
                        end_date=end,
                        mode=args.recover_mode,
                    )
                elif rt == "rag_index":
                    embedder = LocalEmbedder(
                        base_url=settings.app_config.rag.embedding_base_url,
                        api_key=settings.app_config.rag.embedding_api_key,
                        model=settings.app_config.rag.embedding_model,
                    )
                    indexer = KbIndexer(
                        embedder=embedder,
                        chunk_size=settings.app_config.rag.chunk_size,
                        chunk_overlap=settings.app_config.rag.chunk_overlap,
                    )
                    with pipeline.db.session_scope() as dbs:
                        files = indexer.index_markdown_dir(dbs, kb_dir="kb")
                        promoted = indexer.promote_approved_items(dbs)
                    result = {"indexed_files": files, "promoted_items": promoted}
                else:
                    result = pipeline.fund_intel_orchestrator.run_theme_daily(business_date=report_date)
                logger.info("One-shot result run_type=%s result=%s", rt, result)
            return

        _init_startup_catchup_state(pipeline)
        tz = ZoneInfo(settings.app_config.scheduler.timezone)
        scheduler = BlockingScheduler(
            timezone=tz,
            job_defaults={
                "coalesce": True,
                "max_instances": 1,
                "misfire_grace_time": 300,
            },
        )
        scheduler.add_job(
            _run_job,
            CronTrigger.from_crontab(settings.app_config.scheduler.morning_cron, timezone=tz),
            args=[pipeline, "morning"],
            id="morning_job",
            replace_existing=True,
        )
        scheduler.add_job(
            _run_job,
            CronTrigger.from_crontab(settings.app_config.scheduler.close_cron, timezone=tz),
            args=[pipeline, "close"],
            id="close_job",
            replace_existing=True,
        )
        fund_cfg = settings.fund_config.get("schedule", {})
        theme_cfg = settings.theme_config.get("schedule", {})
        scheduler.add_job(
            _run_aux_job,
            CronTrigger.from_crontab(str(fund_cfg.get("weekly_cron", "0 7 * * 1")), timezone=tz),
            args=[pipeline, "fund_weekly"],
            id="fund_weekly_job",
            replace_existing=True,
        )
        scheduler.add_job(
            _run_aux_job,
            CronTrigger.from_crontab(str(fund_cfg.get("daily_refresh_cron", "10 7 * * 1-5")), timezone=tz),
            args=[pipeline, "fund_daily"],
            id="fund_daily_job",
            replace_existing=True,
        )
        scheduler.add_job(
            _run_aux_job,
            CronTrigger.from_crontab(str(theme_cfg.get("weekly_discovery_cron", "20 7 * * 1")), timezone=tz),
            args=[pipeline, "theme_weekly"],
            id="theme_weekly_job",
            replace_existing=True,
        )
        scheduler.add_job(
            _run_aux_job,
            CronTrigger.from_crontab(str(theme_cfg.get("daily_strength_cron", "40 7 * * 1-5")), timezone=tz),
            args=[pipeline, "theme_daily"],
            id="theme_daily_job",
            replace_existing=True,
        )
        intel_cfg = settings.intel_config.get("schedule", {})
        if bool(intel_cfg.get("enabled", False)):
            scheduler.add_job(
                _run_intel_background_job,
                CronTrigger.from_crontab(str(intel_cfg.get("cron", "*/20 9-15 * * 1-5")), timezone=tz),
                args=[pipeline],
                id="intel_background_job",
                replace_existing=True,
            )
        recovery_cfg = settings.intel_config.get("recovery", {})
        if bool(recovery_cfg.get("enabled", False)):
            scheduler.add_job(
                _run_auto_recover_job,
                CronTrigger.from_crontab(str(recovery_cfg.get("cron", "15 * * * 1-5")), timezone=tz),
                args=[pipeline],
                id="auto_recover_job",
                replace_existing=True,
            )
        startup_catchup_cfg = settings.intel_config.get("startup_catchup", {})
        if bool(startup_catchup_cfg.get("enabled", True)):
            scheduler.add_job(
                _run_startup_catchup_step_job,
                CronTrigger.from_crontab(str(startup_catchup_cfg.get("cron", "*/2 * * * *")), timezone=tz),
                args=[pipeline],
                id="startup_catchup_job",
                replace_existing=True,
            )
        logger.info("Scheduler started timezone=%s", tz)
        scheduler.start()
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
