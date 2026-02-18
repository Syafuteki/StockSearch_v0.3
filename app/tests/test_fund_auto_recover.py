from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import select

from jpswing.config import load_settings
from jpswing.db.models import FundFeaturesSnapshot
from jpswing.db.session import DBSessionManager
from jpswing.fund_intel_orchestrator import FundIntelOrchestrator


class _DummyNotifier:
    def send(self, *_args, **_kwargs):  # noqa: ANN001
        return True, None


class _DummyJQuants:
    def __init__(self, *, fin_rows_by_date: dict[date, list[dict]]) -> None:
        self.fin_rows_by_date = fin_rows_by_date

    def fetch_calendar(self, from_date: date, to_date: date) -> list[dict]:
        rows: list[dict] = []
        d = from_date
        while d <= to_date:
            rows.append({"Date": d.isoformat(), "HolDiv": "1" if d.weekday() < 5 else "0"})
            d += timedelta(days=1)
        return rows

    def fetch_financial_summary(self, target_date: date) -> list[dict]:
        return list(self.fin_rows_by_date.get(target_date, []))

    def fetch_equities_master(self, _as_of: date) -> list[dict]:
        return [{"Code": "11110", "IssuedShares": 1_000_000}]


def _sqlite_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path / 'fund_auto_recover.db'}"


def test_fund_auto_recover_repairs_only_missing_days_with_cap(tmp_path: Path) -> None:
    settings = load_settings("config")
    settings.fund_config["states"] = {"in_min": 0.0, "watch_min": -1.0}
    settings.fund_config["recovery"] = {
        "enabled": True,
        "lookback_business_days": 5,
        "max_days_per_run": 2,
        "run_on_holiday": True,
        "force": False,
        "request_interval_sec": 0.0,
    }

    db = DBSessionManager(_sqlite_url(tmp_path))
    db.init_schema()
    jq = _DummyJQuants(
        fin_rows_by_date={
            date(2026, 2, 12): [
                {
                    "Code": "11110",
                    "Sales": 100.0,
                    "OP": 20.0,
                    "NP": 10.0,
                    "FSales": 130.0,
                    "EPS": 100.0,
                    "FEPS": 140.0,
                    "Eq": 100.0,
                    "TA": 150.0,
                    "BPS": 200.0,
                }
            ]
        }
    )
    orch = FundIntelOrchestrator(
        settings=settings,
        db=db,
        jquants=jq,
        notifier=_DummyNotifier(),  # type: ignore[arg-type]
    )

    with db.session_scope() as session:
        session.add(
            FundFeaturesSnapshot(
                code="11110",
                asof_date=date(2026, 2, 13),
                features={"fund_score": 0.8, "state": "IN"},
            )
        )

    result = orch.run_fund_auto_recover(report_date=date(2026, 2, 18))
    assert result["status"] == "ok"
    assert result["missing_days"] >= 2
    assert result["repaired_days"] == 2

    with db.session_scope() as session:
        rows = session.execute(
            select(FundFeaturesSnapshot.asof_date)
            .where(
                FundFeaturesSnapshot.asof_date >= date(2026, 2, 12),
                FundFeaturesSnapshot.asof_date <= date(2026, 2, 18),
            )
            .distinct()
            .order_by(FundFeaturesSnapshot.asof_date.asc())
        ).all()
    restored_dates = [r[0] for r in rows]
    assert date(2026, 2, 12) in restored_dates
    assert date(2026, 2, 16) in restored_dates
    assert date(2026, 2, 17) not in restored_dates


def test_fund_auto_recover_without_cap_repairs_all_missing_days(tmp_path: Path) -> None:
    settings = load_settings("config")
    settings.fund_config["states"] = {"in_min": 0.0, "watch_min": -1.0}
    settings.fund_config["recovery"] = {
        "enabled": True,
        "lookback_business_days": 5,
        "max_days_per_run": 0,
        "run_on_holiday": True,
        "force": False,
        "request_interval_sec": 0.0,
    }

    db = DBSessionManager(_sqlite_url(tmp_path))
    db.init_schema()
    jq = _DummyJQuants(
        fin_rows_by_date={
            date(2026, 2, 12): [
                {
                    "Code": "11110",
                    "Sales": 100.0,
                    "OP": 20.0,
                    "NP": 10.0,
                    "FSales": 130.0,
                    "EPS": 100.0,
                    "FEPS": 140.0,
                    "Eq": 100.0,
                    "TA": 150.0,
                    "BPS": 200.0,
                }
            ]
        }
    )
    orch = FundIntelOrchestrator(
        settings=settings,
        db=db,
        jquants=jq,
        notifier=_DummyNotifier(),  # type: ignore[arg-type]
    )

    with db.session_scope() as session:
        session.add(
            FundFeaturesSnapshot(
                code="11110",
                asof_date=date(2026, 2, 13),
                features={"fund_score": 0.8, "state": "IN"},
            )
        )

    result = orch.run_fund_auto_recover(report_date=date(2026, 2, 18))
    assert result["status"] == "ok"
    assert result["missing_days"] >= 2
    assert result["repaired_days"] == result["missing_days"]

    with db.session_scope() as session:
        rows = session.execute(
            select(FundFeaturesSnapshot.asof_date)
            .where(
                FundFeaturesSnapshot.asof_date >= date(2026, 2, 12),
                FundFeaturesSnapshot.asof_date <= date(2026, 2, 18),
            )
            .distinct()
            .order_by(FundFeaturesSnapshot.asof_date.asc())
        ).all()
    restored_dates = [r[0] for r in rows]
    assert date(2026, 2, 12) in restored_dates
    assert date(2026, 2, 16) in restored_dates
    assert date(2026, 2, 17) in restored_dates
