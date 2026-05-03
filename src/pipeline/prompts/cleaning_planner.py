from __future__ import annotations

# Named variants for cleaning planner system prompts (harness / A-B).
# Human and structured-output schema are unchanged in nodes.

_CLEANING_PLANNER_BASE = (
    "Given a JSON profile of a CSV dataset, output a CleaningPlan with ONLY allowed fields: "
    "sentinel_tokens, replace_empty_strings, strip_whitespace, numeric_columns, date_columns, "
    "drop_rows_missing_in_columns, notes. "
    "Choose sentinel_tokens that mark invalid/missing categorical or numeric data "
    "(e.g. ERROR, UNKNOWN). "
    "List numeric_columns for columns that should be floats/ints. "
    "List date_columns for date-like columns. "
    "drop_rows_missing_in_columns should include critical identifiers if obvious "
    "(e.g. primary key / transaction id). "
    "Do not invent column names: use only names from the profile."
)

CLEANING_PLANNER_PROMPTS: dict[str, str] = {
    "default": (
        "You are a data cleaning planner. "
        + _CLEANING_PLANNER_BASE
    ),
    "duplicate_focus": (
        "You are a data cleaning planner focused on identifier quality. "
        + _CLEANING_PLANNER_BASE
        + " "
        "Prioritize plans that reduce duplicate-ID risk: infer likely ID columns from the "
        "profile column names and samples, include them in drop_rows_missing_in_columns when "
        "missing values would corrupt uniqueness, and choose sentinel_tokens that normalize "
        "dirty placeholders without collapsing distinct IDs."
    ),
}


def cleaning_planner_prompt_ids() -> tuple[str, ...]:
    return tuple(sorted(CLEANING_PLANNER_PROMPTS.keys()))


def cleaning_planner_system_prompt(prompt_id: str) -> str:
    if prompt_id not in CLEANING_PLANNER_PROMPTS:
        known = ", ".join(cleaning_planner_prompt_ids())
        raise ValueError(f"Unknown cleaning_planner_prompt_id={prompt_id!r}; known: {known}")
    return CLEANING_PLANNER_PROMPTS[prompt_id]
