from __future__ import annotations

from typing import Any


def build_tag_lookup(tag_policy: dict[str, Any]) -> dict[str, dict[str, Any]]:
    tags = tag_policy.get("tags", {}) if isinstance(tag_policy, dict) else {}
    out: dict[str, dict[str, Any]] = {}
    for key, value in tags.items():
        if not isinstance(value, dict):
            continue
        out[str(key)] = {
            "emoji": str(value.get("emoji", "")),
            "label": str(value.get("label", key)),
            "severity": str(value.get("severity", "medium")),
        }
    return out


def map_tags_to_display(tags: list[str], tag_policy: dict[str, Any]) -> list[str]:
    lookup = build_tag_lookup(tag_policy)
    display: list[str] = []
    for tag in tags:
        item = lookup.get(tag)
        if item is None:
            display.append(tag)
            continue
        emoji = item.get("emoji", "")
        label = item.get("label", tag)
        display.append(f"{emoji}{label}".strip())
    return display

