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


def _join_blocks(parts: list[str]) -> str:
    return "\n".join([p for p in parts if str(p or "").strip()]).strip()


def split_messages_by_symbol_blocks(
    *,
    header_lines: list[str],
    symbol_blocks: list[str],
    footer_lines: list[str],
    max_chars: int,
    max_parts: int,
) -> list[str]:
    if max_parts <= 0:
        return []

    header_text = _join_blocks(header_lines)
    footer_text = _join_blocks(footer_lines)
    messages: list[str] = []
    current_blocks: list[str] = []
    footer_attached = False

    def _compose(blocks: list[str], *, include_header: bool, include_footer: bool) -> str:
        parts: list[str] = []
        if include_header and header_text:
            parts.append(header_text)
        parts.extend(blocks)
        if include_footer and footer_text:
            parts.append(footer_text)
        return _join_blocks(parts)

    def _flush(include_footer: bool = False) -> None:
        nonlocal current_blocks
        msg = _compose(
            current_blocks,
            include_header=(len(messages) == 0),
            include_footer=include_footer,
        )
        if msg:
            messages.append(msg)
        current_blocks = []

    for block in symbol_blocks:
        block = str(block or "").strip()
        if not block:
            continue

        candidate = _compose(
            current_blocks + [block],
            include_header=(len(messages) == 0),
            include_footer=False,
        )
        if len(candidate) <= max_chars:
            current_blocks.append(block)
            continue

        if current_blocks and len(messages) < max_parts - 1:
            _flush(include_footer=False)
            # Try the block again in a fresh message.
            candidate = _compose(
                [block],
                include_header=(len(messages) == 0),
                include_footer=False,
            )

        if len(candidate) <= max_chars:
            current_blocks.append(block)
            continue

        # Very long single-symbol block: fallback to line-based splitting only for this block.
        if len(messages) < max_parts:
            block_parts = split_messages(candidate, max_chars=max_chars, max_parts=max(1, max_parts - len(messages)))
            for part in block_parts:
                if len(messages) >= max_parts:
                    break
                messages.append(part)
        current_blocks = []
        if len(messages) >= max_parts:
            return messages[:max_parts]

    # Attach footer to the last message if possible.
    if current_blocks:
        with_footer = _compose(
            current_blocks,
            include_header=(len(messages) == 0),
            include_footer=True,
        )
        if len(with_footer) <= max_chars:
            messages.append(with_footer)
            current_blocks = []
            footer_attached = bool(footer_text)
        elif len(messages) < max_parts - 1:
            _flush(include_footer=False)
        else:
            # Last available slot: return current body without footer if footer doesn't fit.
            _flush(include_footer=False)
            current_blocks = []

    if footer_text and not footer_attached and len(messages) < max_parts:
        if messages:
            candidate = _join_blocks([messages[-1], footer_text])
            if len(candidate) <= max_chars:
                messages[-1] = candidate
            else:
                messages.append(footer_text if len(footer_text) <= max_chars else split_messages(footer_text, max_chars, 1)[0])
        else:
            header_only = header_text if len(header_text) <= max_chars else split_messages(header_text, max_chars, 1)[0]
            candidate = _join_blocks([header_only, footer_text])
            if len(candidate) <= max_chars:
                messages.append(candidate)
            else:
                messages.append(header_only)
                if len(messages) < max_parts:
                    messages.append(footer_text if len(footer_text) <= max_chars else split_messages(footer_text, max_chars, 1)[0])

    return [m for m in messages[:max_parts] if m]


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

    header_lines = [title]
    header_lines.append(
        "注目イベント: "
        f"{_tag_label('earnings', '決算')}={event_summary.get('earnings', 0)}件 "
        f"{_tag_label('margin_alert', '信用規制')}={event_summary.get('margin_alert', 0)}件 "
        f"{_tag_label('short_sale', '空売り')}={event_summary.get('short_sale_report', 0)}件"
    )
    if run_type == "close" and signal_changes:
        ins = ",".join(signal_changes.get("in", [])) or "なし"
        outs = ",".join(signal_changes.get("out", [])) or "なし"
        header_lines.append(f"シグナル変化: 新規IN={ins} / OUT={outs}")

    symbol_blocks: list[str] = []
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

        block_lines = [f"【{rank}】{display_code} {name}".strip()]
        block_lines.append(
            "テクニカル: "
            f"MA25={_fmt_num(row.get('ma25'))} "
            f"ROC20={_fmt_num(row.get('roc20') * 100 if row.get('roc20') is not None else None)}% "
            f"出来高比={_fmt_num(row.get('volume_ratio20'))} "
            f"ブレイク強度={_fmt_num(row.get('breakout_strength20') * 100 if row.get('breakout_strength20') is not None else None)}%"
        )
        block_lines.append(f"上昇シナリオ: {bull}")
        block_lines.append(f"下落シナリオ: {bear}")
        block_lines.append(
            "目安: "
            f"エントリー={_safe_level_text(key_levels.get('entry_idea'))} / "
            f"利確={_safe_level_text(key_levels.get('takeprofit_idea'))} / "
            f"損切り={_safe_level_text(key_levels.get('stop_idea'))}"
        )
        block_lines.append(f"注意イベント: {' / '.join(events) if events else '特記事項なし'}")
        block_lines.append(f"自信度: {confidence}")
        if _is_tech_fallback(llm):
            block_lines.append("備考: LLMフォールバック結果")
        symbol_blocks.append("\n".join(block_lines).strip())

    return split_messages_by_symbol_blocks(
        header_lines=header_lines,
        symbol_blocks=symbol_blocks,
        footer_lines=[disclaimer],
        max_chars=max_chars,
        max_parts=max_parts,
    )
