from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from jpswing.db.models import FundUniverseState, IntelItem, Theme, ThemeStrengthDaily, ThemeSymbolMap
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
        boost_name = float(mapping_cfg.get("name_keyword_boost", mapping_cfg.get("boost_on_keyword_hits", 0.08)))
        boost_sector = float(mapping_cfg.get("sector_keyword_boost", 0.20))
        boost_intel = float(mapping_cfg.get("intel_keyword_boost", 0.22))
        shift_bonus = float(mapping_cfg.get("business_shift_bonus", 0.20))
        intel_lookback_days = int(mapping_cfg.get("intel_lookback_days", 180))
        intel_shift_min_hits = int(mapping_cfg.get("intel_shift_min_hits", 2))

        master_rows = jquants.fetch_equities_master(business_date)
        intel_text_map = self._recent_intel_text_by_code(session, lookback_days=intel_lookback_days)
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

            keywords = self._clean_keywords(seed.get("keywords", []))
            sector_keywords = self._clean_keywords(seed.get("sector_keywords", [])) or keywords
            shift_keywords = self._clean_keywords(seed.get("shift_keywords", [])) or keywords
            if not keywords and not sector_keywords and not shift_keywords:
                continue
            for row in master_rows:
                code = pick_first(row, ["Code", "code", "LocalCode", "IssueCode"])
                if not code:
                    continue
                code_s = str(code)
                name_ja = str(pick_first(row, ["CompanyName", "CoName", "Name", "name", "IssueName"]) or "")
                name_en = str(pick_first(row, ["CoNameEn"]) or "")
                sector17 = str(pick_first(row, ["S17Nm", "Sector17Name"]) or "")
                sector33 = str(pick_first(row, ["S33Nm", "Sector33Name"]) or "")
                name_text = f"{name_ja} {name_en}".lower()
                sector_text = f"{sector17} {sector33}".lower()
                intel_text = intel_text_map.get(code_s, "")

                name_hits = self._keyword_hits(name_text, keywords)
                sector_hits = self._keyword_hits(sector_text, sector_keywords)
                intel_hits = self._keyword_hits(intel_text, shift_keywords)

                score = (
                    (len(name_hits) * boost_name)
                    + (len(sector_hits) * boost_sector)
                    + (len(intel_hits) * boost_intel)
                )
                shift_detected = len(intel_hits) >= intel_shift_min_hits and len(sector_hits) == 0
                if shift_detected:
                    score += shift_bonus
                score = min(1.0, score)
                if score < min_conf:
                    continue
                rationale_parts: list[str] = []
                if name_hits:
                    rationale_parts.append(f"name:{','.join(sorted(name_hits)[:4])}")
                if sector_hits:
                    rationale_parts.append(f"sector:{','.join(sorted(sector_hits)[:4])}")
                if intel_hits:
                    rationale_parts.append(f"intel:{','.join(sorted(intel_hits)[:4])}")
                if shift_detected:
                    rationale_parts.append("shift_signal")
                rationale = "|".join(rationale_parts) or f"keyword_match:{name}"
                mapping = session.scalar(
                    select(ThemeSymbolMap).where(ThemeSymbolMap.theme_id == theme.theme_id, ThemeSymbolMap.code == code_s)
                )
                if mapping is None:
                    mapping = ThemeSymbolMap(
                        theme_id=theme.theme_id,
                        code=code_s,
                        confidence=score,
                        rationale=rationale,
                    )
                    session.add(mapping)
                else:
                    mapping.confidence = score
                    mapping.rationale = rationale

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

    @staticmethod
    def _clean_keywords(values: list[Any]) -> list[str]:
        out: list[str] = []
        for v in values:
            t = str(v or "").strip().lower()
            if not t:
                continue
            out.append(t)
        # preserve order and dedupe
        seen: set[str] = set()
        uniq: list[str] = []
        for t in out:
            if t in seen:
                continue
            seen.add(t)
            uniq.append(t)
        return uniq

    @staticmethod
    def _keyword_hits(text: str, keywords: list[str]) -> set[str]:
        if not text or not keywords:
            return set()
        low = text.lower()
        hits: set[str] = set()
        for kw in keywords:
            if kw and kw in low:
                hits.add(kw)
        return hits

    @staticmethod
    def _recent_intel_text_by_code(session: Session, *, lookback_days: int) -> dict[str, str]:
        if lookback_days <= 0:
            return {}
        boundary = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        rows = (
            session.execute(
                select(IntelItem.code, IntelItem.headline, IntelItem.summary, IntelItem.facts).where(
                    IntelItem.created_at >= boundary
                )
            )
            .all()
        )
        out: dict[str, str] = {}
        for code, headline, summary, facts in rows:
            code_s = str(code or "").strip()
            if not code_s:
                continue
            facts_text = ""
            if isinstance(facts, dict):
                items = facts.get("items")
                if isinstance(items, list):
                    facts_text = " ".join(str(x) for x in items if str(x).strip())
            payload = " ".join(
                [
                    str(headline or ""),
                    str(summary or ""),
                    facts_text,
                ]
            ).strip()
            if not payload:
                continue
            prev = out.get(code_s, "")
            out[code_s] = f"{prev} {payload}".strip() if prev else payload
        return {k: v.lower() for k, v in out.items()}
