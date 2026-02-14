from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from jpswing.db.models import FundUniverseState, Theme, ThemeStrengthDaily, ThemeSymbolMap
from jpswing.ingest.jquants_client import JQuantsClient
from jpswing.ingest.normalize import pick_first


@dataclass(slots=True)
class ThemeImpact:
    theme_id: int
    name: str
    strength: float
    delta: float
    significant: bool


class ThemeService:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    def weekly_discover(self, session: Session, business_date: date, jquants: JQuantsClient) -> None:
        seeds = list(self.config.get("seed_themes", []))
        mapping_cfg = self.config.get("mapping", {})
        min_conf = float(mapping_cfg.get("min_confidence", 0.5))
        boost = float(mapping_cfg.get("boost_on_keyword_hits", 0.08))

        master_rows = jquants.fetch_equities_master(business_date)
        for seed in seeds:
            name = str(seed.get("name", "")).strip()
            if not name:
                continue
            theme = session.scalar(select(Theme).where(Theme.name == name))
            if theme is None:
                theme = Theme(
                    name=name,
                    keywords={"items": seed.get("keywords", [])},
                    allowed_sources={"items": seed.get("allowed_sources", [])},
                )
                session.add(theme)
                session.flush()
            else:
                theme.keywords = {"items": seed.get("keywords", [])}
                theme.allowed_sources = {"items": seed.get("allowed_sources", [])}

            keywords = [str(k).lower() for k in seed.get("keywords", [])]
            if not keywords:
                continue
            for row in master_rows:
                code = pick_first(row, ["Code", "code", "LocalCode", "IssueCode"])
                name_txt = pick_first(row, ["CompanyName", "Name", "name", "IssueName"])
                if not code or not name_txt:
                    continue
                score = 0.0
                low = str(name_txt).lower()
                for kw in keywords:
                    if kw and kw in low:
                        score += boost
                if score < min_conf:
                    continue
                code_s = str(code)
                mapping = session.scalar(
                    select(ThemeSymbolMap).where(ThemeSymbolMap.theme_id == theme.theme_id, ThemeSymbolMap.code == code_s)
                )
                if mapping is None:
                    mapping = ThemeSymbolMap(
                        theme_id=theme.theme_id,
                        code=code_s,
                        confidence=score,
                        rationale=f"keyword_match:{name}",
                    )
                    session.add(mapping)
                else:
                    mapping.confidence = max(mapping.confidence, score)
                    mapping.rationale = f"keyword_match:{name}"

    def update_daily_strength(self, session: Session, business_date: date) -> list[ThemeImpact]:
        rows = session.execute(select(Theme)).scalars().all()
        impacts: list[ThemeImpact] = []
        sig_delta = float(self.config.get("daily_strength", {}).get("significant_delta", 0.15))
        for theme in rows:
            mapped = session.execute(select(ThemeSymbolMap).where(ThemeSymbolMap.theme_id == theme.theme_id)).scalars().all()
            if not mapped:
                strength = 0.0
            else:
                codes = [m.code for m in mapped]
                states = session.execute(select(FundUniverseState).where(FundUniverseState.code.in_(codes))).scalars().all()
                in_count = sum(1 for s in states if s.state == "IN")
                watch_count = sum(1 for s in states if s.state == "WATCH")
                strength = round((in_count + (0.5 * watch_count)) / max(1, len(codes)), 6)

            prev = session.scalar(
                select(ThemeStrengthDaily)
                .where(ThemeStrengthDaily.theme_id == theme.theme_id, ThemeStrengthDaily.asof_date < business_date)
                .order_by(ThemeStrengthDaily.asof_date.desc())
                .limit(1)
            )
            delta = strength - (prev.strength if prev else 0.0)
            current = session.scalar(
                select(ThemeStrengthDaily).where(
                    ThemeStrengthDaily.theme_id == theme.theme_id,
                    ThemeStrengthDaily.asof_date == business_date,
                )
            )
            payload = {"mapped_symbols": len(mapped), "in_count": int(round(strength * max(1, len(mapped))))}
            if current is None:
                current = ThemeStrengthDaily(
                    theme_id=theme.theme_id,
                    asof_date=business_date,
                    strength=strength,
                    drivers=payload,
                )
                session.add(current)
            else:
                current.strength = strength
                current.drivers = payload
            impacts.append(
                ThemeImpact(
                    theme_id=theme.theme_id,
                    name=theme.name,
                    strength=strength,
                    delta=delta,
                    significant=abs(delta) >= sig_delta,
                )
            )
        return impacts

    def high_or_rising_theme_codes(self, session: Session, business_date: date) -> set[str]:
        sig_delta = float(self.config.get("daily_strength", {}).get("significant_delta", 0.15))
        today = session.execute(select(ThemeStrengthDaily).where(ThemeStrengthDaily.asof_date == business_date)).scalars().all()
        codes: set[str] = set()
        for t in today:
            prev = session.scalar(
                select(ThemeStrengthDaily)
                .where(ThemeStrengthDaily.theme_id == t.theme_id, ThemeStrengthDaily.asof_date < business_date)
                .order_by(ThemeStrengthDaily.asof_date.desc())
                .limit(1)
            )
            delta = t.strength - (prev.strength if prev else 0.0)
            if t.strength >= 0.6 or delta >= sig_delta:
                mapped = session.execute(select(ThemeSymbolMap).where(ThemeSymbolMap.theme_id == t.theme_id)).scalars().all()
                for m in mapped:
                    codes.add(m.code)
        return codes

