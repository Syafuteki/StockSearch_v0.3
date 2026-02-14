from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from jpswing.db.models import DailyBar, FundFeaturesSnapshot, FundUniverseState
from jpswing.ingest.jquants_client import JQuantsClient
from jpswing.ingest.normalize import pick_first, to_float, to_int


@dataclass(slots=True)
class FundChange:
    code: str
    before_state: str | None
    after_state: str
    changed: bool
    reason: str


def _metric(row: dict[str, Any], keys: list[str]) -> float | None:
    return to_float(pick_first(row, keys))


def _infer_state(score: float, in_min: float, watch_min: float) -> str:
    if score >= in_min:
        return "IN"
    if score >= watch_min:
        return "WATCH"
    return "OUT"


class FundService:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    def refresh_states(
        self,
        session: Session,
        *,
        business_date: date,
        jquants: JQuantsClient,
        force: bool = False,
    ) -> list[FundChange]:
        fin_rows = jquants.fetch_financial_summary(business_date)
        if not fin_rows and not force:
            self.logger.info("No financial summary update at %s", business_date)
            return []

        master_rows = jquants.fetch_equities_master(business_date)
        issued_shares_map: dict[str, int] = {}
        for row in master_rows:
            code = pick_first(row, ["Code", "code", "LocalCode", "IssueCode"])
            shares = to_int(pick_first(row, ["IssuedShares", "issued_shares", "NumberOfIssuedAndOutstandingSharesAtTheEnd"]))
            if code and shares:
                issued_shares_map[str(code)] = shares

        in_min = float(self.config.get("states", {}).get("in_min", 0.65))
        watch_min = float(self.config.get("states", {}).get("watch_min", 0.45))
        w = self.config.get("weights", {})
        weights = {
            "profitability": float(w.get("profitability", 0.30)),
            "growth": float(w.get("growth", 0.25)),
            "efficiency": float(w.get("efficiency", 0.20)),
            "stability": float(w.get("stability", 0.15)),
            "valuation": float(w.get("valuation", 0.10)),
        }

        changes: list[FundChange] = []
        for row in fin_rows:
            code = pick_first(row, ["Code", "code", "LocalCode", "IssueCode"])
            if not code:
                continue
            code_s = str(code)
            features, score, state, tags, gaps = self._score_row(
                session=session,
                code=code_s,
                row=row,
                business_date=business_date,
                issued_shares=issued_shares_map.get(code_s),
                in_min=in_min,
                watch_min=watch_min,
                weights=weights,
            )
            existing = session.get(FundUniverseState, code_s)
            before_state = existing.state if existing else None
            changed = before_state != state
            reason = "state_changed" if changed else "updated"
            if existing is None:
                existing = FundUniverseState(
                    code=code_s,
                    state=state,
                    fund_score=score,
                    risk_hard={"items": []},
                    risk_soft={"items": []},
                    tags={"items": tags},
                    thesis_bull="",
                    thesis_bear="",
                    evidence_refs={"items": []},
                    data_gaps={"items": gaps},
                )
                session.add(existing)
                changed = True
                reason = "new"
            else:
                existing.state = state
                existing.fund_score = score
                existing.tags = {"items": tags}
                existing.data_gaps = {"items": gaps}
            self._upsert_snapshot(session, code_s, business_date, features)
            changes.append(FundChange(code=code_s, before_state=before_state, after_state=state, changed=changed, reason=reason))
        return changes

    def apply_intel_aggregate(
        self,
        session: Session,
        *,
        code: str,
        tags_add: list[str],
        risk_flags: list[str],
        critical_risk: bool,
        evidence_refs: list[str],
    ) -> bool:
        state = session.get(FundUniverseState, code)
        if state is None:
            return False
        existing_tags = list((state.tags or {}).get("items", []))
        merged_tags = sorted(set(existing_tags) | set(tags_add))

        risk_hard = set((state.risk_hard or {}).get("items", []))
        risk_soft = set((state.risk_soft or {}).get("items", []))
        for r in risk_flags:
            if critical_risk:
                risk_hard.add(r)
            else:
                risk_soft.add(r)
        if critical_risk and "critical_risk" not in risk_hard:
            risk_hard.add("critical_risk")

        refs = sorted(set((state.evidence_refs or {}).get("items", [])) | set(evidence_refs))
        changed = (
            merged_tags != existing_tags
            or risk_hard != set((state.risk_hard or {}).get("items", []))
            or risk_soft != set((state.risk_soft or {}).get("items", []))
        )
        if changed:
            state.tags = {"items": merged_tags}
            state.risk_hard = {"items": sorted(risk_hard)}
            state.risk_soft = {"items": sorted(risk_soft)}
            state.evidence_refs = {"items": refs}
            if "critical_risk" in risk_hard:
                state.state = "OUT"
        return changed

    def _score_row(
        self,
        *,
        session: Session,
        code: str,
        row: dict[str, Any],
        business_date: date,
        issued_shares: int | None,
        in_min: float,
        watch_min: float,
        weights: dict[str, float],
    ) -> tuple[dict[str, Any], float, str, list[str], list[str]]:
        roe = _metric(row, ["ROE", "roe", "ResultROE", "ForecastROE"])
        op_margin = _metric(row, ["OperatingMargin", "operating_margin", "ResultOperatingMargin"])
        rev_growth = _metric(row, ["RevenueGrowthRate", "revenue_growth_rate", "ResultRevenueGrowthRate"])
        eps_growth = _metric(row, ["EPSGrowthRate", "eps_growth_rate", "ResultEPSGrowthRate"])
        equity_ratio = _metric(row, ["EquityRatio", "equity_ratio", "ResultEquityRatio"])
        debt_ratio = _metric(row, ["DebtRatio", "debt_ratio", "ResultDebtRatio"])
        pbr = _metric(row, ["PBR", "pbr"])
        per = _metric(row, ["PER", "per"])

        gaps: list[str] = []
        def norm(value: float | None, lo: float, hi: float, name: str) -> float:
            if value is None:
                gaps.append(name)
                return 0.0
            if hi == lo:
                return 0.0
            clamped = min(max(value, lo), hi)
            return (clamped - lo) / (hi - lo)

        profitability = (norm(roe, 0.0, 0.2, "roe") + norm(op_margin, 0.0, 0.2, "operating_margin")) / 2
        growth = (norm(rev_growth, -0.2, 0.3, "revenue_growth") + norm(eps_growth, -0.3, 0.4, "eps_growth")) / 2
        efficiency = norm(roe, 0.0, 0.2, "roe_efficiency")
        stability = (norm(equity_ratio, 0.0, 0.7, "equity_ratio") + (1.0 - norm(debt_ratio, 0.0, 3.0, "debt_ratio"))) / 2
        valuation = 0.0
        if pbr is not None:
            valuation = max(0.0, 1.0 - norm(pbr, 0.5, 4.0, "pbr"))
        elif per is not None:
            valuation = max(0.0, 1.0 - norm(per, 5.0, 40.0, "per"))
        else:
            # Derive rough valuation using market cap if possible.
            latest_bar = session.scalar(
                select(DailyBar)
                .where(DailyBar.code == code, DailyBar.trade_date <= business_date)
                .order_by(DailyBar.trade_date.desc())
                .limit(1)
            )
            if latest_bar and issued_shares:
                market_cap = float((latest_bar.adj_close or latest_bar.close or 0) or 0) * issued_shares
                valuation = max(0.0, 1.0 - min(1.0, market_cap / 1_000_000_000_000))
                gaps.append("valuation_derived_from_market_cap")
            else:
                gaps.append("valuation_unavailable")

        score = round(
            (profitability * weights["profitability"])
            + (growth * weights["growth"])
            + (efficiency * weights["efficiency"])
            + (stability * weights["stability"])
            + (valuation * weights["valuation"]),
            6,
        )
        state = _infer_state(score, in_min, watch_min)
        tags = []
        if growth >= 0.65:
            tags.append("growth")
        if profitability >= 0.65:
            tags.append("profitability")
        if valuation >= 0.65:
            tags.append("valuation")
        features = {
            "roe": roe,
            "operating_margin": op_margin,
            "revenue_growth": rev_growth,
            "eps_growth": eps_growth,
            "equity_ratio": equity_ratio,
            "debt_ratio": debt_ratio,
            "pbr": pbr,
            "per": per,
            "profitability_score": profitability,
            "growth_score": growth,
            "efficiency_score": efficiency,
            "stability_score": stability,
            "valuation_score": valuation,
            "fund_score": score,
            "state": state,
        }
        return features, score, state, tags, sorted(set(gaps))

    @staticmethod
    def _upsert_snapshot(session: Session, code: str, asof_date: date, features: dict[str, Any]) -> None:
        snap = session.scalar(
            select(FundFeaturesSnapshot).where(
                FundFeaturesSnapshot.code == code,
                FundFeaturesSnapshot.asof_date == asof_date,
            )
        )
        if snap is None:
            session.add(
                FundFeaturesSnapshot(
                    code=code,
                    asof_date=asof_date,
                    features=features,
                )
            )
        else:
            snap.features = features

