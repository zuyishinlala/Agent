from __future__ import annotations

from typing import Any, TypedDict


class PipelineState(TypedDict, total=False):
    """Graph state: only JSON-serializable values where possible (no DataFrames)."""

    csv_path: str
    user_hints: str
    sample_rows: int
    quality_pass_threshold: float
    max_clean_retries: int
    clean_retry_count: int
    quality_feedback: str
    profile: dict[str, Any]
    cleaning_plan: dict[str, Any]
    cleaned_csv_path: str
    validation: dict[str, Any]
    issues_detected: int
    cleaning_stats: dict[str, Any]
    quality_score: float
    quality_pass: bool
    quality_breakdown: dict[str, Any]
    explanation: str
    errors: list[str]
