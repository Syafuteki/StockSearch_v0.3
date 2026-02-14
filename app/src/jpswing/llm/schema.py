from __future__ import annotations

from pydantic import BaseModel, Field


class KeyLevels(BaseModel):
    entry_idea: str = Field(min_length=1)
    stop_idea: str = Field(min_length=1)
    takeprofit_idea: str = Field(min_length=1)


class LlmTop10Item(BaseModel):
    code: str = Field(min_length=1)
    top10_rank: int = Field(ge=1, le=10)
    thesis_bull: list[str] = Field(default_factory=list)
    thesis_bear: list[str] = Field(default_factory=list)
    key_levels: KeyLevels
    event_risks: list[str] = Field(default_factory=list)
    confidence_0_100: int = Field(ge=0, le=100)
    data_gaps: list[str] = Field(default_factory=list)
    rule_suggestion: str | None = None


class LlmTop10Response(BaseModel):
    top10: list[LlmTop10Item] = Field(min_length=1, max_length=10)

