from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


PLACEHOLDER_TEXTS = {
    "",
    "-",
    "n/a",
    "na",
    "none",
    "null",
    "unknown",
    "tbd",
    "not available",
    "not_applicable",
}


def _is_placeholder(value: str) -> bool:
    return value.strip().lower() in PLACEHOLDER_TEXTS


class KeyLevels(BaseModel):
    entry_idea: str = Field(min_length=1)
    stop_idea: str = Field(min_length=1)
    takeprofit_idea: str = Field(min_length=1)

    @field_validator("entry_idea", "stop_idea", "takeprofit_idea")
    @classmethod
    def _validate_key_level_text(cls, value: str) -> str:
        text = value.strip()
        if _is_placeholder(text):
            raise ValueError("placeholder key level is not allowed")
        return text


class LlmTop10Item(BaseModel):
    code: str = Field(min_length=1)
    top10_rank: int = Field(ge=1, le=10)
    thesis_bull: list[str] = Field(min_length=1)
    thesis_bear: list[str] = Field(min_length=1)
    key_levels: KeyLevels
    event_risks: list[str] = Field(default_factory=list)
    confidence_0_100: int = Field(ge=0, le=100)
    data_gaps: list[str] = Field(default_factory=list)
    rule_suggestion: str | None = None

    @field_validator("thesis_bull", "thesis_bear")
    @classmethod
    def _validate_thesis_list(cls, value: list[str]) -> list[str]:
        cleaned = [v.strip() for v in value if isinstance(v, str) and v.strip()]
        if not cleaned:
            raise ValueError("thesis must not be empty")
        if any(_is_placeholder(v) for v in cleaned):
            raise ValueError("placeholder thesis text is not allowed")
        return cleaned


class LlmTop10Response(BaseModel):
    top10: list[LlmTop10Item] = Field(min_length=1, max_length=10)
