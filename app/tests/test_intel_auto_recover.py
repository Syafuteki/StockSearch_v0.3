from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from types import MethodType

from sqlalchemy import select

from jpswing.config import load_settings
from jpswing.db.models import IntelDailyBudget, IntelItem, IntelQueue
from jpswing.db.session import DBSessionManager
from jpswing.fund_intel_orchestrator import FundIntelOrchestrator


class _DummyJQuants:
    def fetch_calendar(self, from_date: date, to_date: date) -> list[dict]:
        rows: list[dict] = []
        d = from_date
        while d <= to_date:
            rows.append({"Date": d.isoformat(), "HolDiv": "1" if d.weekday() < 5 else "0"})
            d += timedelta(days=1)
        return rows


class _DummyEdinet:
    def fetch_documents_list(self, target_date: date, doc_type: int = 2) -> list[dict]:  # noqa: ARG002
        _ = target_date
        return []


def _sqlite_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path / 'intel_auto_recover.db'}"


def _build_orchestrator(tmp_path: Path) -> tuple[FundIntelOrchestrator, DBSessionManager]:
    settings = load_settings("config")
    settings.intel_config["recovery"] = {
        "enabled": True,
        "lookback_business_days": 5,
        "max_days_per_run": 2,
        "run_on_holiday": True,
        "mode": "close_only",
    }
    db = DBSessionManager(_sqlite_url(tmp_path))
    db.init_schema()
    orch = object.__new__(FundIntelOrchestrator)
    orch.settings = settings
    orch.db = db
    orch.jquants = _DummyJQuants()
    orch.edinet = _DummyEdinet()
    orch.logger = logging.getLogger("test_intel_auto_recover")
    return orch, db


def test_intel_auto_recover_repairs_missing_days_with_cap(tmp_path: Path) -> None:
    orch, db = _build_orchestrator(tmp_path)

    with db.session_scope() as session:
        session.add(IntelDailyBudget(business_date=date(2026, 2, 13), done_count=1, morning_done=0, close_done=1))

    calls: list[tuple[str, date]] = []

    def _fake_run_intel_only(self: FundIntelOrchestrator, *, session_name: str, business_date: date) -> dict:
        calls.append((session_name, business_date))
        with db.session_scope() as session:
            row = session.get(IntelDailyBudget, business_date)
            if row is None:
                row = IntelDailyBudget(business_date=business_date, done_count=0, morning_done=0, close_done=0)
                session.add(row)
            row.done_count = int(row.done_count or 0) + 1
            if session_name == "morning":
                row.morning_done = int(row.morning_done or 0) + 1
            else:
                row.close_done = int(row.close_done or 0) + 1
        return {"status": "ok", "session": session_name, "business_date": business_date.isoformat()}

    orch.run_intel_only = MethodType(_fake_run_intel_only, orch)

    result = orch.run_intel_auto_recover(report_date=date(2026, 2, 18))
    assert result["status"] == "ok"
    assert result["missing_days"] >= 3
    assert result["attempted_days"] == 2
    assert result["repaired_days"] == 2
    assert len(calls) == 2
    assert all(s == "close" for s, _ in calls)


def test_intel_auto_recover_without_cap_repairs_all_missing_days(tmp_path: Path) -> None:
    orch, db = _build_orchestrator(tmp_path)
    orch.settings.intel_config["recovery"]["max_days_per_run"] = 0
    orch.settings.intel_config["recovery"]["mode"] = "morning_close"

    with db.session_scope() as session:
        session.add(IntelDailyBudget(business_date=date(2026, 2, 13), done_count=1, morning_done=0, close_done=1))

    calls: list[tuple[str, date]] = []

    def _fake_run_intel_only(self: FundIntelOrchestrator, *, session_name: str, business_date: date) -> dict:
        calls.append((session_name, business_date))
        with db.session_scope() as session:
            row = session.get(IntelDailyBudget, business_date)
            if row is None:
                row = IntelDailyBudget(business_date=business_date, done_count=0, morning_done=0, close_done=0)
                session.add(row)
            row.done_count = int(row.done_count or 0) + 1
            if session_name == "morning":
                row.morning_done = int(row.morning_done or 0) + 1
            else:
                row.close_done = int(row.close_done or 0) + 1
        return {"status": "ok", "session": session_name, "business_date": business_date.isoformat()}

    orch.run_intel_only = MethodType(_fake_run_intel_only, orch)

    result = orch.run_intel_auto_recover(report_date=date(2026, 2, 18))
    assert result["status"] == "ok"
    assert result["attempted_days"] == result["missing_days"]
    assert result["repaired_days"] == result["missing_days"]

    # morning_close mode runs 2 sessions per recovered day.
    assert len(calls) == result["missing_days"] * 2
    with db.session_scope() as session:
        restored = {
            r[0]
            for r in session.execute(
                select(IntelDailyBudget.business_date).where(
                    IntelDailyBudget.business_date >= date(2026, 2, 11),
                    IntelDailyBudget.business_date <= date(2026, 2, 17),
                )
            ).all()
        }
    assert date(2026, 2, 11) in restored
    assert date(2026, 2, 12) in restored
    assert date(2026, 2, 16) in restored
    assert date(2026, 2, 17) in restored


def test_intel_auto_recover_retries_zero_done_budget_day(tmp_path: Path) -> None:
    orch, db = _build_orchestrator(tmp_path)
    orch.settings.intel_config["recovery"]["max_days_per_run"] = 0
    orch.settings.intel_config["recovery"]["mode"] = "close_only"

    with db.session_scope() as session:
        # Existing budget row with zero completed Intel items must not be treated as recovered.
        session.add(IntelDailyBudget(business_date=date(2026, 2, 12), done_count=0, morning_done=0, close_done=0))
        session.add(IntelDailyBudget(business_date=date(2026, 2, 13), done_count=1, morning_done=0, close_done=1))

    calls: list[tuple[str, date]] = []

    def _fake_run_intel_only(self: FundIntelOrchestrator, *, session_name: str, business_date: date) -> dict:
        calls.append((session_name, business_date))
        with db.session_scope() as session:
            row = session.get(IntelDailyBudget, business_date)
            if row is None:
                row = IntelDailyBudget(business_date=business_date, done_count=0, morning_done=0, close_done=0)
                session.add(row)
            row.done_count = int(row.done_count or 0) + 1
            row.close_done = int(row.close_done or 0) + 1
        return {"status": "ok", "session": session_name, "business_date": business_date.isoformat()}

    orch.run_intel_only = MethodType(_fake_run_intel_only, orch)

    result = orch.run_intel_auto_recover(report_date=date(2026, 2, 18))
    assert result["status"] == "ok"
    retried_days = {d for _, d in calls}
    assert date(2026, 2, 12) in retried_days


def test_intel_auto_recover_retries_when_queue_pending_exists(tmp_path: Path) -> None:
    orch, db = _build_orchestrator(tmp_path)
    orch.settings.intel_config["recovery"]["max_days_per_run"] = 0
    orch.settings.intel_config["recovery"]["mode"] = "close_only"

    with db.session_scope() as session:
        # 2/12 has pending queue => incomplete
        session.add(IntelDailyBudget(business_date=date(2026, 2, 12), done_count=1, morning_done=0, close_done=1))
        session.add(
            IntelQueue(
                business_date=date(2026, 2, 12),
                session="close",
                code="11110",
                priority=1.0,
                sources_seed={},
                status="done",
                idempotency_key="2026-02-12:close:11110",
            )
        )
        session.add(
            IntelQueue(
                business_date=date(2026, 2, 12),
                session="close",
                code="22220",
                priority=0.9,
                sources_seed={},
                status="pending",
                idempotency_key="2026-02-12:close:22220",
            )
        )
        # 2/13 has no pending/failed and queue exists => complete
        session.add(IntelDailyBudget(business_date=date(2026, 2, 13), done_count=1, morning_done=0, close_done=1))
        session.add(
            IntelQueue(
                business_date=date(2026, 2, 13),
                session="close",
                code="33330",
                priority=0.8,
                sources_seed={},
                status="done",
                idempotency_key="2026-02-13:close:33330",
            )
        )

    calls: list[tuple[str, date]] = []

    def _fake_run_intel_only(self: FundIntelOrchestrator, *, session_name: str, business_date: date) -> dict:
        calls.append((session_name, business_date))
        return {"status": "ok", "session": session_name, "business_date": business_date.isoformat()}

    orch.run_intel_only = MethodType(_fake_run_intel_only, orch)

    result = orch.run_intel_auto_recover(report_date=date(2026, 2, 18))
    assert result["status"] == "ok"
    retried_days = {d for _, d in calls}
    assert date(2026, 2, 12) in retried_days
    assert date(2026, 2, 13) not in retried_days


def test_intel_auto_recover_retries_day_with_unprocessed_edinet_doc(tmp_path: Path) -> None:
    orch, db = _build_orchestrator(tmp_path)
    orch.settings.intel_config["recovery"]["max_days_per_run"] = 0
    orch.settings.intel_config["recovery"]["mode"] = "close_only"

    target_days = [date(2026, 2, 11), date(2026, 2, 12), date(2026, 2, 13), date(2026, 2, 16), date(2026, 2, 17)]
    day_doc = {
        date(2026, 2, 11): "S100A111",
        date(2026, 2, 12): "S100A222",
        date(2026, 2, 13): "S100MISS",
        date(2026, 2, 16): "S100A444",
        date(2026, 2, 17): "S100A555",
    }

    with db.session_scope() as session:
        for d in target_days:
            session.add(IntelDailyBudget(business_date=d, done_count=1, morning_done=0, close_done=1))
            session.add(
                IntelQueue(
                    business_date=d,
                    session="close",
                    code="11110",
                    priority=1.0,
                    sources_seed={},
                    status="done",
                    idempotency_key=f"{d.isoformat()}:close:11110",
                )
            )
            if d != date(2026, 2, 13):
                session.add(
                    IntelItem(
                        code="11110",
                        published_at=None,
                        source_url=f"https://api.edinet-fsa.go.jp/api/v2/documents/{day_doc[d]}?type=1",
                        source_type="edinet",
                        headline="done",
                        summary="done",
                        facts={"items": []},
                        tags={"items": []},
                        risk_flags={"items": []},
                        critical_risk=False,
                        evidence_refs={"items": []},
                    )
                )

    def _fake_fetch_documents_list(target_date: date, doc_type: int = 2) -> list[dict]:  # noqa: ARG001
        doc_id = day_doc.get(target_date)
        if not doc_id:
            return []
        return [{"docID": doc_id, "secCode": "1111"}]

    orch.edinet.fetch_documents_list = _fake_fetch_documents_list  # type: ignore[attr-defined]

    calls: list[tuple[str, date]] = []

    def _fake_run_intel_only(self: FundIntelOrchestrator, *, session_name: str, business_date: date) -> dict:
        calls.append((session_name, business_date))
        return {"status": "ok", "session": session_name, "business_date": business_date.isoformat()}

    orch.run_intel_only = MethodType(_fake_run_intel_only, orch)

    result = orch.run_intel_auto_recover(report_date=date(2026, 2, 18))
    assert result["status"] == "ok"
    assert result["missing_days"] == 1
    assert result["repaired_days"] == 1
    assert result["edinet_gap_days"] == ["2026-02-13"]
    assert calls == [("close", date(2026, 2, 13))]
