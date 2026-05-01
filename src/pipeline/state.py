from __future__ import annotations

from typing import Any, TypedDict


class PipelineState(TypedDict, total=False):
    """Graph state: only JSON-serializable values where possible (no DataFrames)."""

    csv_path: str
    user_hints: str
    sample_rows: int
    profile: dict[str, Any]
    cleaning_plan: dict[str, Any]
    cleaned_csv_path: str
    validation: dict[str, Any]
    explanation: str
    errors: list[str]
