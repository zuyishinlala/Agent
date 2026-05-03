from __future__ import annotations

import re
from pathlib import Path


def sanitize_planner_prompt_id_for_filename(prompt_id: str) -> str:
    """Filesystem-safe segment derived from cleaning_planner_prompt_id."""
    s = (prompt_id or "").strip() or "default"
    s = re.sub(r"[^a-zA-Z0-9_-]+", "_", s)
    s = s.strip("_") or "variant"
    return s[:80]


def planner_prompt_filename_suffix(prompt_id: str) -> str:
    """
    Suffix before file extension for non-default planner prompts.

    default -> '' (keeps legacy ``{stem}_cleaned.csv`` / ``{stem}_report.md``).
    other   -> '__{sanitized}' e.g. ``dirty_cafe_sales_cleaned__duplicate_focus.csv``.
    """
    pid = (prompt_id or "default").strip() or "default"
    if pid == "default":
        return ""
    return f"__{sanitize_planner_prompt_id_for_filename(pid)}"


def cleaned_csv_filename(stem: str, prompt_id: str) -> str:
    return f"{stem}_cleaned{planner_prompt_filename_suffix(prompt_id)}.csv"


def report_md_filename(stem: str, prompt_id: str) -> str:
    return f"{stem}_report{planner_prompt_filename_suffix(prompt_id)}.md"


def default_cleaned_csv_path(stem: str, prompt_id: str, *, artifacts_dir: Path | None = None) -> Path:
    base = artifacts_dir or Path("artifacts")
    return base / cleaned_csv_filename(stem, prompt_id)


def default_report_md_path(stem: str, prompt_id: str, *, artifacts_dir: Path | None = None) -> Path:
    base = artifacts_dir or Path("artifacts")
    return base / report_md_filename(stem, prompt_id)
