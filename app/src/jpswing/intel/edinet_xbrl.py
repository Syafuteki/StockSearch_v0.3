from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from xml.etree import ElementTree as ET


XBRLI_NS = "http://www.xbrl.org/2003/instance"
CONTEXT_TAG = f"{{{XBRLI_NS}}}context"
PERIOD_TAG = f"{{{XBRLI_NS}}}period"
INSTANT_TAG = f"{{{XBRLI_NS}}}instant"
END_DATE_TAG = f"{{{XBRLI_NS}}}endDate"
START_DATE_TAG = f"{{{XBRLI_NS}}}startDate"


@dataclass(slots=True)
class ConceptRule:
    key: str
    label: str
    prefixes: tuple[str, ...]


RULES: tuple[ConceptRule, ...] = (
    ConceptRule("net_sales", "売上高", ("NetSales", "Revenue", "Sales")),
    ConceptRule("operating_income", "営業利益", ("OperatingIncome", "OperatingProfit")),
    ConceptRule("ordinary_income", "経常利益", ("OrdinaryIncome",)),
    ConceptRule("profit", "当期純利益", ("ProfitLossAttributableToOwnersOfParent", "ProfitLoss", "NetIncome")),
    ConceptRule("total_assets", "総資産", ("Assets",)),
    ConceptRule("equity", "自己資本", ("Equity", "NetAssets")),
    ConceptRule("eps", "EPS", ("BasicEarningsPerShare", "EarningsPerShare")),
)

RULE_ORDER = [r.key for r in RULES]


@dataclass(slots=True)
class CandidateValue:
    key: str
    label: str
    value: Decimal
    asof: date | None
    score: float


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _try_parse_date(value: str | None) -> date | None:
    text = str(value or "").strip()
    if len(text) < 10:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _collect_context_dates(root: ET.Element) -> dict[str, date | None]:
    out: dict[str, date | None] = {}
    for ctx in root.findall(f".//{CONTEXT_TAG}"):
        ctx_id = str(ctx.attrib.get("id") or "").strip()
        if not ctx_id:
            continue
        period = ctx.find(PERIOD_TAG)
        if period is None:
            out[ctx_id] = None
            continue
        instant = period.find(INSTANT_TAG)
        if instant is not None:
            out[ctx_id] = _try_parse_date(instant.text)
            continue
        end_date = period.find(END_DATE_TAG)
        if end_date is not None:
            out[ctx_id] = _try_parse_date(end_date.text)
            continue
        start_date = period.find(START_DATE_TAG)
        out[ctx_id] = _try_parse_date(start_date.text if start_date is not None else None)
    return out


def _match_rule(local: str) -> ConceptRule | None:
    for rule in RULES:
        for p in rule.prefixes:
            if local == p or local.startswith(p):
                return rule
    return None


def _parse_decimal(text: str) -> Decimal | None:
    cleaned = text.replace(",", "").strip()
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _context_score(context_ref: str, asof: date | None) -> float:
    key = context_ref.lower()
    score = 0.0
    if "currentyear" in key:
        score += 40.0
    elif "currentquarter" in key:
        score += 35.0
    elif "current" in key:
        score += 20.0
    if "duration" in key:
        score += 8.0
    if "instant" in key:
        score += 8.0
    if "prior" in key:
        score -= 20.0
    if asof:
        score += 5.0 + (asof.toordinal() / 1_000_000.0)
    return score


def _format_decimal(value: Decimal) -> str:
    if value == value.to_integral_value():
        return f"{int(value):,}"
    txt = format(value.normalize(), "f")
    if "." in txt:
        txt = txt.rstrip("0").rstrip(".")
    return txt


def _parse_instance_root(root: ET.Element) -> dict[str, CandidateValue]:
    ctx_dates = _collect_context_dates(root)
    best: dict[str, CandidateValue] = {}
    for elem in root.iter():
        local = _local_name(elem.tag)
        rule = _match_rule(local)
        if rule is None:
            continue
        context_ref = str(elem.attrib.get("contextRef") or "").strip()
        if not context_ref:
            continue
        if elem.text is None:
            continue
        value = _parse_decimal(elem.text)
        if value is None:
            continue
        asof = ctx_dates.get(context_ref)
        score = _context_score(context_ref, asof)
        cand = CandidateValue(
            key=rule.key,
            label=rule.label,
            value=value,
            asof=asof,
            score=score,
        )
        prev = best.get(rule.key)
        if prev is None or cand.score > prev.score:
            best[rule.key] = cand
    return best


def _iter_xbrl_buffers(document_bytes: bytes) -> list[bytes]:
    if not document_bytes:
        return []
    if not document_bytes.startswith(b"PK"):
        return [document_bytes]
    try:
        with zipfile.ZipFile(io.BytesIO(document_bytes)) as zf:
            names = sorted(zf.namelist())
            primary = [n for n in names if n.lower().endswith(".xbrl")]
            secondary = [n for n in names if n.lower().endswith(".xml")]
            picked = primary if primary else secondary
            out: list[bytes] = []
            for name in picked[:6]:
                try:
                    payload = zf.read(name)
                except Exception:
                    continue
                if payload:
                    out.append(payload)
            return out
    except Exception:
        return []


def extract_xbrl_key_facts(document_bytes: bytes, limit: int = 6) -> list[str]:
    merged: dict[str, CandidateValue] = {}
    for buf in _iter_xbrl_buffers(document_bytes):
        try:
            root = ET.fromstring(buf)
        except Exception:
            continue
        partial = _parse_instance_root(root)
        for key, cand in partial.items():
            prev = merged.get(key)
            if prev is None or cand.score > prev.score:
                merged[key] = cand

    out: list[str] = []
    for key in RULE_ORDER:
        cand = merged.get(key)
        if cand is None:
            continue
        value_text = _format_decimal(cand.value)
        if cand.asof:
            out.append(f"{cand.label}={value_text} ({cand.asof.isoformat()})")
        else:
            out.append(f"{cand.label}={value_text}")
        if len(out) >= limit:
            break
    return out

