from __future__ import annotations

from datetime import date

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from jpswing.db.models import FundFeaturesSnapshot, FundUniverseState
from jpswing.fund.service import FundService


class _DummyJQuants:
    def __init__(self, fin_rows: list[dict] | None = None, master_rows: list[dict] | None = None) -> None:
        self.fin_rows = fin_rows or []
        self.master_rows = master_rows or []
        self.master_called = 0

    def fetch_financial_summary(self, target_date: date):  # noqa: ANN001, ARG002
        return list(self.fin_rows)

    def fetch_equities_master(self, as_of: date):  # noqa: ANN001, ARG002
        self.master_called += 1
        return list(self.master_rows)


def _new_service(enabled: bool = True) -> FundService:
    return FundService(
        {
            "states": {"in_min": 0.65, "watch_min": 0.45},
            "weights": {
                "profitability": 0.30,
                "growth": 0.25,
                "efficiency": 0.20,
                "stability": 0.15,
                "valuation": 0.10,
            },
            "carry_forward": {"enabled": enabled, "states": ["IN", "WATCH"]},
        }
    )


def test_refresh_states_carries_forward_when_no_fin_update() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    FundUniverseState.__table__.create(engine)
    FundFeaturesSnapshot.__table__.create(engine)
    svc = _new_service(enabled=True)
    jq = _DummyJQuants(fin_rows=[])

    with Session(engine) as session:
        session.add(
            FundUniverseState(
                code="11110",
                state="IN",
                fund_score=0.7,
                risk_hard={"items": []},
                risk_soft={"items": []},
                tags={"items": []},
                thesis_bull="",
                thesis_bear="",
                evidence_refs={"items": []},
                data_gaps={"items": []},
            )
        )
        session.add(
            FundFeaturesSnapshot(
                code="11110",
                asof_date=date(2026, 2, 12),
                features={"fund_score": 0.7},
            )
        )
        session.commit()

        changes = svc.refresh_states(session, business_date=date(2026, 2, 13), jquants=jq, force=False)
        session.commit()

        assert changes == []
        snap = session.scalar(
            select(FundFeaturesSnapshot).where(
                FundFeaturesSnapshot.code == "11110",
                FundFeaturesSnapshot.asof_date == date(2026, 2, 13),
            )
        )
        assert snap is not None
        assert isinstance(snap.features, dict)
        assert snap.features.get("carried_forward") is True
        assert snap.features.get("carried_from") == "2026-02-12"


def test_refresh_states_does_not_carry_when_disabled() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    FundUniverseState.__table__.create(engine)
    FundFeaturesSnapshot.__table__.create(engine)
    svc = _new_service(enabled=False)
    jq = _DummyJQuants(fin_rows=[])

    with Session(engine) as session:
        session.add(
            FundUniverseState(
                code="11110",
                state="WATCH",
                fund_score=0.5,
                risk_hard={"items": []},
                risk_soft={"items": []},
                tags={"items": []},
                thesis_bull="",
                thesis_bear="",
                evidence_refs={"items": []},
                data_gaps={"items": []},
            )
        )
        session.add(
            FundFeaturesSnapshot(
                code="11110",
                asof_date=date(2026, 2, 12),
                features={"fund_score": 0.5},
            )
        )
        session.commit()

        _ = svc.refresh_states(session, business_date=date(2026, 2, 13), jquants=jq, force=False)
        session.commit()

        snap = session.scalar(
            select(FundFeaturesSnapshot).where(
                FundFeaturesSnapshot.code == "11110",
                FundFeaturesSnapshot.asof_date == date(2026, 2, 13),
            )
        )
        assert snap is None

