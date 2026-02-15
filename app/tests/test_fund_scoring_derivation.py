from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from jpswing.db.models import DailyBar
from jpswing.fund.service import FundService


def test_fund_score_uses_fins_summary_fields_and_derivations() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    DailyBar.__table__.create(bind=engine)

    svc = FundService(
        {
            "states": {"in_min": 0.65, "watch_min": 0.45},
            "weights": {
                "profitability": 0.30,
                "growth": 0.25,
                "efficiency": 0.20,
                "stability": 0.15,
                "valuation": 0.10,
            },
        }
    )

    with Session(engine, future=True) as session:
        session.add(
            DailyBar(
                trade_date=date(2026, 2, 13),
                code="81520",
                open=1000.0,
                high=1100.0,
                low=950.0,
                close=1050.0,
                adj_close=1050.0,
                volume=100000,
                market_cap=None,
                raw_json={},
            )
        )
        session.flush()

        row = {
            "Code": "81520",
            "Sales": "23238000000",
            "FSales": "31900000000",
            "OP": "1979000000",
            "NP": "1405000000",
            "Eq": "21531000000",
            "EqAR": "0.628",
            "TA": "34260000000",
            "EPS": "725.32",
            "FEPS": "959.75",
            "ShOutFY": "1958734",
        }
        _, score, state, _, gaps = svc._score_row(
            session=session,
            code="81520",
            row=row,
            business_date=date(2026, 2, 13),
            issued_shares=None,
            in_min=0.65,
            watch_min=0.45,
            weights={
                "profitability": 0.30,
                "growth": 0.25,
                "efficiency": 0.20,
                "stability": 0.15,
                "valuation": 0.10,
            },
        )

    assert score > 0.45
    assert state in {"WATCH", "IN"}
    assert "roe" not in gaps
    assert "operating_margin" not in gaps
