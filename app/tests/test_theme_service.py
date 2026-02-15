from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from jpswing.db.models import IntelItem, Theme, ThemeSymbolMap
from jpswing.theme.service import ThemeService


class _DummyJQuants:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def fetch_equities_master(self, business_date):  # noqa: ANN001, ARG002
        return self.rows


def test_weekly_discover_maps_by_sector_keyword() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Theme.__table__.create(engine)
    ThemeSymbolMap.__table__.create(engine)
    IntelItem.__table__.create(engine)

    cfg = {
        "seed_themes": [
            {
                "name": "半導体",
                "keywords": ["半導体"],
                "sector_keywords": ["電気機器"],
                "shift_keywords": ["AI", "半導体"],
                "allowed_sources": ["edinet"],
            }
        ],
        "mapping": {
            "min_confidence": 0.2,
            "name_keyword_boost": 0.05,
            "sector_keyword_boost": 0.20,
            "intel_keyword_boost": 0.20,
            "business_shift_bonus": 0.20,
            "intel_lookback_days": 180,
            "intel_shift_min_hits": 2,
        },
    }
    svc = ThemeService(cfg)
    jq = _DummyJQuants(
        [
            {
                "Date": "2026-02-13",
                "Code": "99990",
                "CoName": "サンプル工業",
                "S17Nm": "電気機器",
                "S33Nm": "電気機器",
            }
        ]
    )

    with Session(engine) as session:
        from datetime import date

        svc.weekly_discover(session, date(2026, 2, 13), jq)
        session.commit()
        rows = session.execute(select(ThemeSymbolMap)).scalars().all()
        assert len(rows) == 1
        assert rows[0].code == "99990"
        assert "sector:電気機器" in str(rows[0].rationale)


def test_weekly_discover_allows_shift_signal_when_sector_mismatch() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Theme.__table__.create(engine)
    ThemeSymbolMap.__table__.create(engine)
    IntelItem.__table__.create(engine)

    cfg = {
        "seed_themes": [
            {
                "name": "AIインフラ",
                "keywords": ["AI"],
                "sector_keywords": ["情報・通信業"],
                "shift_keywords": ["AI", "GPU", "データセンター"],
                "allowed_sources": ["edinet"],
            }
        ],
        "mapping": {
            "min_confidence": 0.35,
            "name_keyword_boost": 0.05,
            "sector_keyword_boost": 0.10,
            "intel_keyword_boost": 0.20,
            "business_shift_bonus": 0.20,
            "intel_lookback_days": 180,
            "intel_shift_min_hits": 2,
        },
    }
    svc = ThemeService(cfg)
    jq = _DummyJQuants(
        [
            {
                "Date": "2026-02-13",
                "Code": "88880",
                "CoName": "総合サービスHD",
                "S17Nm": "サービス業",
                "S33Nm": "サービス業",
            }
        ]
    )

    with Session(engine) as session:
        session.add(
            IntelItem(
                code="88880",
                published_at=None,
                source_url="https://example.test",
                source_type="edinet",
                headline="AI向けGPUデータセンター事業開始",
                summary="AI GPU データセンターを新設",
                facts={"items": ["AI", "GPU", "データセンター"]},
                tags={"items": []},
                risk_flags={"items": []},
                critical_risk=False,
                evidence_refs={"items": ["https://example.test"]},
                created_at=datetime.now(timezone.utc),
            )
        )
        session.commit()

        from datetime import date

        svc.weekly_discover(session, date(2026, 2, 13), jq)
        session.commit()
        rows = session.execute(select(ThemeSymbolMap)).scalars().all()
        assert len(rows) == 1
        assert rows[0].code == "88880"
        assert "shift_signal" in str(rows[0].rationale)
        assert "intel:" in str(rows[0].rationale)

