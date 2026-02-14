from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from jpswing.db.models import IntelQueue
from jpswing.intel.budget import build_idempotency_key, compute_session_allowance


def test_budget_allowance_respects_daily_and_session_caps() -> None:
    assert compute_session_allowance(daily_budget=10, session_cap=4, done_total=0, done_session=0) == 4
    assert compute_session_allowance(daily_budget=10, session_cap=4, done_total=8, done_session=1) == 2
    assert compute_session_allowance(daily_budget=10, session_cap=6, done_total=10, done_session=0) == 0
    assert compute_session_allowance(daily_budget=10, session_cap=6, done_total=4, done_session=6) == 0


def test_idempotency_unique_constraint() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    IntelQueue.__table__.create(engine)
    idem = build_idempotency_key(date(2026, 2, 14).isoformat(), "morning", "7203")
    with Session(engine) as session:
        session.add(
            IntelQueue(
                business_date=date(2026, 2, 14),
                session="morning",
                code="7203",
                priority=0.9,
                sources_seed={},
                status="pending",
                idempotency_key=idem,
            )
        )
        session.commit()
        session.add(
            IntelQueue(
                business_date=date(2026, 2, 14),
                session="morning",
                code="7203",
                priority=0.8,
                sources_seed={},
                status="pending",
                idempotency_key=idem,
            )
        )
        with pytest.raises(IntegrityError):
            session.commit()

