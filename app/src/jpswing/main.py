from __future__ import annotations

import argparse
import logging
import threading
from datetime import date
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
        logger.info("Scheduler started timezone=%s", tz)
        scheduler.start()
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
