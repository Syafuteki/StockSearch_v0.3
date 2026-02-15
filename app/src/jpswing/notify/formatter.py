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
        name = str(row.get("name") or "")
        rank = int(row.get("rank"))
        llm = llm_map.get(code, {})
        bull = " / ".join(llm.get("thesis_bull", [])[:2]) if llm else "未取得"
        bear = " / ".join(llm.get("thesis_bear", [])[:2]) if llm else "未取得"
        key_levels = llm.get("key_levels", {})
        events = llm.get("event_risks", [])
        confidence = llm.get("confidence_0_100", "N/A")

        lines.append(f"【{rank}】{code} {name}".strip())
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
            f"エントリー={key_levels.get('entry_idea', '未取得')} / "
            f"利確={key_levels.get('takeprofit_idea', '未取得')} / "
            f"損切り={key_levels.get('stop_idea', '未取得')}"
        )
        lines.append(f"注意イベント: {' / '.join(events) if events else '特記事項なし'}")
        lines.append(f"自信度: {confidence}")

    lines.append(disclaimer)
    body = "\n".join(lines).strip()
    return split_messages(body, max_chars=max_chars, max_parts=max_parts)
