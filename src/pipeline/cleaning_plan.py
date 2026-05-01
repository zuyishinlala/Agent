from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field


class CleaningPlan(BaseModel):
    """Declarative plan produced by the cleaning agent; executor runs only these steps."""

    sentinel_tokens: list[str] = Field(
        default_factory=lambda: ["ERROR", "UNKNOWN"],
        description="Cell values to treat as null after trim.",
    )
    replace_empty_strings: bool = Field(
        default=True,
        description="Treat empty/whitespace-only strings as null.",
    )
    strip_whitespace: bool = Field(
        default=True,
        description="Strip leading/trailing whitespace on string columns before other steps.",
    )
    numeric_columns: list[str] = Field(
        default_factory=list,
        description="Column names to coerce with pandas to_numeric after sentinel handling.",
    )
    date_columns: list[str] = Field(
        default_factory=list,
        description="Column names to parse as datetime (ISO-like strings).",
    )
    drop_rows_missing_in_columns: list[str] = Field(
        default_factory=list,
        description="Drop rows with null in any of these columns after cleaning.",
    )
    notes: str = Field(
        default="",
        description="Short human-readable summary of intent (logged only).",
    )


def _strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_string_dtype(out[col]) or out[col].dtype == object:
            out[col] = out[col].map(
                lambda x: x.strip() if isinstance(x, str) else x,
            )
    return out


def apply_cleaning_plan(df: pd.DataFrame, plan: CleaningPlan) -> pd.DataFrame:
    """Apply `CleaningPlan` deterministically (no code execution from the model)."""
    out = df.copy()
    tokens = list(plan.sentinel_tokens)

    if plan.strip_whitespace:
        out = _strip_strings(out)

    if tokens:
        token_set = set(tokens)
        if plan.replace_empty_strings:
            token_set.add("")
        out = out.mask(out.isin(token_set), pd.NA)

    if plan.replace_empty_strings:
        for col in out.columns:
            if out[col].dtype == object or pd.api.types.is_string_dtype(out[col]):
                out[col] = out[col].map(lambda x: pd.NA if isinstance(x, str) and not x.strip() else x)

    for col in plan.numeric_columns:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    for col in plan.date_columns:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")

    if plan.drop_rows_missing_in_columns:
        cols = [c for c in plan.drop_rows_missing_in_columns if c in out.columns]
        if cols:
            out = out.dropna(subset=cols, how="any")

    return out


def load_csv(csv_path: str | Path) -> pd.DataFrame:
    return pd.read_csv(csv_path)


def write_csv(df: pd.DataFrame, path: str | Path) -> str:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)
    return str(p.resolve())
