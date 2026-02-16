from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd


def _fmt_num(value: Any, digits: int = 2) -> str:
    try:
        if value is None or pd.isna(value):
            return "N/A"
        return f"{float(value):.{digits}f}"
    except Exception:  # noqa: BLE001
        return "N/A"


def _display_code(raw_code: Any) -> str:
    code = str(raw_code or "").strip()
    # J-Quants local code (5 digits) is often displayed as 4 digits by dropping trailing "0".
    if len(code) == 5 and code.isdigit() and code.endswith("0"):
        return code[:-1]
    return code


def _safe_text_list(value: Any, *, fallback: str = "未取得", limit: int = 2) -> str:
    if not isinstance(value, list):
        return fallback
    cleaned = [str(v).strip() for v in value if str(v).strip()]
    if not cleaned:
        return fallback
    return " / ".join(cleaned[:limit])


def _safe_level_text(value: Any, *, fallback: str = "未取得") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    if text.lower() in {"n/a", "na", "none", "null", "unknown", "-", "not available"}:
        return fallback
    return text


def _is_tech_fallback(llm: dict[str, Any] | None) -> bool:
    if not isinstance(llm, dict):
        return True
    data_gaps = llm.get("data_gaps")
    if isinstance(data_gaps, list):
        for gap in data_gaps:
            if str(gap or "").strip() == "llm_output_invalid_or_missing":
                return True
    suggestion = str(llm.get("rule_suggestion") or "").strip()
    if suggestion.lower().startswith("fallback:"):
        return True
    return False


def split_messages(text: str, max_chars: int, max_parts: int) -> list[str]:
    if len(text) <= max_chars:
        return [text]
    lines = text.splitlines()
    parts: list[str] = []
    current: list[str] = []
    for line in lines:
        candidate = ("\n".join(current + [line])).strip()
        if len(candidate) <= max_chars:
            current.append(line)
            continue
        if current:
            parts.append("\n".join(current).strip())
        current = [line]
        if len(parts) >= max_parts - 1:
            break
    if current and len(parts) < max_parts:
        parts.append("\n".join(current).strip())
    return [p for p in parts if p]


def format_report_message(
    *,
    report_date: date,
    run_type: str,
    top10_df: pd.DataFrame,
    llm_map: dict[str, dict[str, Any]],
    event_summary: dict[str, int],
    disclaimer: str,
    tag_policy: dict[str, Any] | None = None,
    signal_changes: dict[str, list[str]] | None = None,
    max_chars: int = 1900,
    max_parts: int = 2,
) -> list[str]:
    tags = (tag_policy or {}).get("tags", {})

    def _tag_label(key: str, fallback_label: str) -> str:
        item = tags.get(key, {})
        emoji = item.get("emoji", "")
        label = item.get("label", fallback_label)
        return f"{emoji}{label}".strip()

    if run_type == "morning":
        title = f"【08:00】今日の注目Top10（{report_date.isoformat()} / 前日終値基準）"
    else:
        title = f"【引け後】Top10（{report_date.isoformat()} / 当日終値確定）"

    lines = [title]
    lines.append(
        "注目イベント: "
        f"{_tag_label('earnings', '決算')}={event_summary.get('earnings', 0)}件 "
        f"{_tag_label('margin_alert', '信用規制')}={event_summary.get('margin_alert', 0)}件 "
        f"{_tag_label('short_sale', '空売り')}={event_summary.get('short_sale_report', 0)}件"
    )
    if run_type == "close" and signal_changes:
        ins = ",".join(signal_changes.get("in", [])) or "なし"
        outs = ",".join(signal_changes.get("out", [])) or "なし"
        lines.append(f"シグナル変化: 新規IN={ins} / OUT={outs}")

    for _, row in top10_df.iterrows():
        code = str(row.get("code"))
        display_code = _display_code(code)
        name = str(row.get("name") or "")
        rank = int(row.get("rank"))
        llm = llm_map.get(code, {})
        bull = _safe_text_list(llm.get("thesis_bull")) if llm else "未取得"
        bear = _safe_text_list(llm.get("thesis_bear")) if llm else "未取得"
        key_levels = llm.get("key_levels", {})
        events = llm.get("event_risks", [])
        confidence = llm.get("confidence_0_100", "N/A")

        lines.append(f"【{rank}】{display_code} {name}".strip())
        lines.append(
            "テクニカル: "
            f"MA25={_fmt_num(row.get('ma25'))} "
            f"ROC20={_fmt_num(row.get('roc20') * 100 if row.get('roc20') is not None else None)}% "
            f"出来高比={_fmt_num(row.get('volume_ratio20'))} "
            f"ブレイク強度={_fmt_num(row.get('breakout_strength20') * 100 if row.get('breakout_strength20') is not None else None)}%"
        )
        lines.append(f"上昇シナリオ: {bull}")
        lines.append(f"下落シナリオ: {bear}")
        lines.append(
            "目安: "
            f"エントリー={_safe_level_text(key_levels.get('entry_idea'))} / "
            f"利確={_safe_level_text(key_levels.get('takeprofit_idea'))} / "
            f"損切り={_safe_level_text(key_levels.get('stop_idea'))}"
        )
        lines.append(f"注意イベント: {' / '.join(events) if events else '特記事項なし'}")
        lines.append(f"自信度: {confidence}")
        if _is_tech_fallback(llm):
            lines.append("備考: LLMフォールバック結果")

    lines.append(disclaimer)
    body = "\n".join(lines).strip()
    return split_messages(body, max_chars=max_chars, max_parts=max_parts)
