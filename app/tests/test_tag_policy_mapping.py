from jpswing.intel.tag_policy import map_tags_to_display


def test_tag_policy_mapping_consistent_with_tech_style() -> None:
    tag_policy = {
        "tags": {
            "upward_revision": {"emoji": "✅", "label": "上方修正", "severity": "high"},
            "critical_risk": {"emoji": "☠️", "label": "重大リスク", "severity": "high"},
        }
    }
    mapped = map_tags_to_display(["upward_revision", "unknown_tag", "critical_risk"], tag_policy)
    assert mapped[0] == "✅上方修正"
    assert mapped[1] == "unknown_tag"
    assert mapped[2] == "☠️重大リスク"

