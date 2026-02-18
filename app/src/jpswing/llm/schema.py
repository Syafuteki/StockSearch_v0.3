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
    "\u672a\u53d6\u5f97",
    "\u306a\u3057",
    "\u7121\u3057",
    "\u8a72\u5f53\u306a\u3057",
    "\u63d0\u6848\u306a\u3057",
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


def _normalize_text_optional(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or _is_placeholder(text):
        return None
    return text


def _normalize_text_list(value: list[str] | None) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = _normalize_text_optional(item)
        if not text:
            continue
        out.append(text)
    return out


class SingleCandidateKeyLevels(BaseModel):
    entry_idea: str | None = None
    stop_idea: str | None = None
    takeprofit_idea: str | None = None

    @field_validator("entry_idea", "stop_idea", "takeprofit_idea", mode="before")
    @classmethod
    def _coerce_text(cls, value: object) -> str | None:
        if value is None:
            return None
        return _normalize_text_optional(str(value))


class LlmSingleCandidateResponse(BaseModel):
    thesis_bull: list[str] = Field(default_factory=list)
    thesis_bear: list[str] = Field(default_factory=list)
    key_levels: SingleCandidateKeyLevels = Field(default_factory=SingleCandidateKeyLevels)
    event_risks: list[str] = Field(default_factory=list)
    confidence_0_100: int | None = Field(default=None, ge=0, le=100)
    data_gaps: list[str] = Field(default_factory=list)
    rule_suggestion: str | None = None

    @field_validator("thesis_bull", "thesis_bear", "event_risks", "data_gaps", mode="before")
    @classmethod
    def _normalize_list_fields(cls, value: object) -> list[str]:
        if isinstance(value, list):
            return _normalize_text_list(value)
        if value is None:
            return []
        text = _normalize_text_optional(str(value))
        return [text] if text else []

    @field_validator("rule_suggestion", mode="before")
    @classmethod
    def _normalize_rule_suggestion(cls, value: object) -> str | None:
        if value is None:
            return None
        return _normalize_text_optional(str(value))
