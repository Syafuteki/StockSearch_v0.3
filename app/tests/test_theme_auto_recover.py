from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import select

from jpswing.config import load_settings
from jpswing.db.models import Theme, ThemeStrengthDaily
from jpswing.db.session import DBSessionManager
from jpswing.fund_intel_orchestrator import FundIntelOrchestrator
from jpswing.theme.service import ThemeService


class _DummyJQuants:
    def fetch_calendar(self, from_date: date, to_date: date) -> list[dict]:
        rows: list[dict] = []
        d = from_date
        while d <= to_date:
            rows.append({"Date": d.isoformat(), "HolDiv": "1" if d.weekday() < 5 else "0"})
            d += timedelta(days=1)
        return rows


class _DummyNotifier:
    def send(self, *_args, **_kwargs):  # noqa: ANN001
        return True, None


def _sqlite_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path / 'theme_auto_recover.db'}"


def _build_orchestrator(tmp_path: Path) -> tuple[FundIntelOrchestrator, DBSessionManager]:
    settings = load_settings("config")
    settings.theme_config["recovery"] = {
        "enabled": True,
        "run_on_startup": True,
        "lookback_business_days": 5,
        "max_days_per_run": 2,
        "run_on_holiday": True,
        "refresh_mapping": False,
        "request_interval_sec": 0.0,
    }
    db = DBSessionManager(_sqlite_url(tmp_path))
    db.init_schema()
    orch = object.__new__(FundIntelOrchestrator)
    orch.settings = settings
    orch.db = db
    orch.jquants = _DummyJQuants()
    orch.notifier = _DummyNotifier()  # type: ignore[assignment]
    orch.theme_service = ThemeService(settings.theme_config)
    orch.logger = logging.getLogger("test_theme_auto_recover")
    return orch, db


def test_theme_auto_recover_repairs_missing_days_with_cap(tmp_path: Path) -> None:
    orch, db = _build_orchestrator(tmp_path)

    with db.session_scope() as session:
        session.add(Theme(name="AI", keywords={"items": ["ai"]}, allowed_sources={"items": ["edinet"]}))
        session.flush()
        session.add(
            ThemeStrengthDaily(
                theme_id=1,
                asof_date=date(2026, 2, 13),
                strength=0.1,
                drivers={},
            )
        )

    result = orch.run_theme_auto_recover(report_date=date(2026, 2, 18))
    assert result["status"] == "ok"
    assert result["missing_days"] >= 2
    assert result["repaired_days"] == 2

    with db.session_scope() as session:
        rows = session.execute(
            select(ThemeStrengthDaily.asof_date)
            .where(
                ThemeStrengthDaily.asof_date >= date(2026, 2, 12),
                ThemeStrengthDaily.asof_date <= date(2026, 2, 18),
            )
            .distinct()
            .order_by(ThemeStrengthDaily.asof_date.asc())
        ).all()
    restored_dates = [r[0] for r in rows]
    assert date(2026, 2, 12) in restored_dates
    assert date(2026, 2, 16) in restored_dates
    assert date(2026, 2, 17) not in restored_dates


def test_theme_auto_recover_without_cap_repairs_all_missing_days(tmp_path: Path) -> None:
    orch, db = _build_orchestrator(tmp_path)
    orch.settings.theme_config["recovery"]["max_days_per_run"] = 0

    with db.session_scope() as session:
        session.add(Theme(name="AI", keywords={"items": ["ai"]}, allowed_sources={"items": ["edinet"]}))
        session.flush()
        session.add(
            ThemeStrengthDaily(
                theme_id=1,
                asof_date=date(2026, 2, 13),
                strength=0.1,
                drivers={},
            )
        )

    result = orch.run_theme_auto_recover(report_date=date(2026, 2, 18))
    assert result["status"] == "ok"
    assert result["repaired_days"] == result["missing_days"]

    with db.session_scope() as session:
        rows = session.execute(
            select(ThemeStrengthDaily.asof_date)
            .where(
                ThemeStrengthDaily.asof_date >= date(2026, 2, 12),
                ThemeStrengthDaily.asof_date <= date(2026, 2, 18),
            )
            .distinct()
            .order_by(ThemeStrengthDaily.asof_date.asc())
        ).all()
    restored_dates = [r[0] for r in rows]
    assert date(2026, 2, 12) in restored_dates
    assert date(2026, 2, 16) in restored_dates
    assert date(2026, 2, 17) in restored_dates
