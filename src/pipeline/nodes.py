from __future__ import annotations

import json
import os
from pathlib import Path

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI

from pipeline.cleaning_plan import CleaningPlan, apply_cleaning_plan, load_csv, write_csv
from pipeline.llm_text import extract_message_text
from pipeline.profile import build_profile
from pipeline.state import PipelineState
from pipeline.validation import run_validation, validation_summary_text


def _model_name() -> str:
    return os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview")


def _llm() -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(
        model=_model_name(),
        temperature=0,
    )


def node_load(state: PipelineState) -> dict:
    csv_path = state["csv_path"]
    sample_rows = int(state.get("sample_rows", 15))
    try:
        profile = build_profile(csv_path, sample_rows=sample_rows)
        return {"profile": profile, "errors": []}
    except Exception as e:  # noqa: BLE001 — surface to graph consumer
        return {"errors": state.get("errors", []) + [f"load_failed: {e!s}"]}


def node_cleaning_agent(state: PipelineState) -> dict:
    if state.get("errors"):
        return {}
    profile = state["profile"]
    hints = state.get("user_hints", "")
    try:
        llm = _llm().with_structured_output(CleaningPlan)
    except Exception as e:  # noqa: BLE001 — missing API key validates at init
        return {"errors": state.get("errors", []) + [f"llm_init_failed: {e!s}"]}

    sys = SystemMessage(
        content=(
            "You are a data cleaning planner. Given a JSON profile of a CSV dataset, "
            "output a CleaningPlan with ONLY allowed fields: sentinel_tokens, "
            "replace_empty_strings, strip_whitespace, numeric_columns, date_columns, "
            "drop_rows_missing_in_columns, notes. "
            "Choose sentinel_tokens that mark invalid/missing categorical or numeric data "
            "(e.g. ERROR, UNKNOWN). "
            "List numeric_columns for columns that should be floats/ints. "
            "List date_columns for date-like columns. "
            "drop_rows_missing_in_columns should include critical identifiers if obvious "
            "(e.g. primary key / transaction id). "
            "Do not invent column names: use only names from the profile."
        ),
    )
    human = HumanMessage(
        content=(
            f"User hints:\n{hints or '(none)'}\n\n"
            f"Profile JSON:\n{json.dumps(profile, indent=2)[:120_000]}"
        ),
    )
    try:
        plan: CleaningPlan = llm.invoke([sys, human])
        return {"cleaning_plan": plan.model_dump()}
    except Exception as e:  # noqa: BLE001
        return {"errors": state.get("errors", []) + [f"cleaning_agent_failed: {e!s}"]}


def node_apply_cleaning(state: PipelineState) -> dict:
    if state.get("errors"):
        return {}
    csv_path = state["csv_path"]
    plan_dict = state.get("cleaning_plan") or {}
    try:
        plan = CleaningPlan.model_validate(plan_dict)
    except Exception as e:  # noqa: BLE001
        return {"errors": state.get("errors", []) + [f"invalid_cleaning_plan: {e!s}"]}

    df = load_csv(csv_path)
    cleaned = apply_cleaning_plan(df, plan)
    stem = Path(csv_path).stem
    out_dir = Path("artifacts")
    out_path = out_dir / f"{stem}_cleaned.csv"
    cleaned_csv_path = write_csv(cleaned, out_path)
    return {"cleaned_csv_path": cleaned_csv_path}


def node_validate(state: PipelineState) -> dict:
    if state.get("errors"):
        return {}
    path = state.get("cleaned_csv_path")
    if not path:
        return {"errors": state.get("errors", []) + ["missing_cleaned_csv_path"]}
    profile = state.get("profile", {})
    df = load_csv(path)

    id_columns: list[str] = []
    for col in profile.get("columns", []):
        name = col.get("name", "")
        n = name.lower()
        if "transaction id" in n or n.endswith("_id") or n == "id":
            id_columns.append(name)
    if not id_columns:
        cols = [c["name"] for c in profile.get("columns", []) if "id" in c["name"].lower()]
        id_columns = cols[:1]

    consistency = None
    col_names = {c["name"] for c in profile.get("columns", [])}
    if {"Quantity", "Price Per Unit", "Total Spent"}.issubset(col_names):
        consistency = ("Quantity", "Price Per Unit", "Total Spent")

    v = run_validation(df, id_columns=id_columns or None, consistency_total=consistency)
    return {"validation": v}


def node_explain(state: PipelineState) -> dict:
    if state.get("errors"):
        return {}
    profile = state.get("profile", {})
    validation = state.get("validation", {})
    plan = state.get("cleaning_plan", {})

    try:
        llm = _llm()
    except Exception as e:  # noqa: BLE001
        return {
            "errors": state.get("errors", []) + [f"llm_init_failed: {e!s}"],
            "explanation": "",
        }

    sys = SystemMessage(
        content=(
            "You are a validation analyst. Write a clear Markdown report for engineers. "
            "Use `##` section headings in this order: "
            "## Data quality issues; ## What cleaning did; ## Validation results; "
            "## Risks and next steps. "
            "Where helpful, use a short Markdown table for null counts (column vs count). "
            "Be concise and factual; use only numbers and facts present in the input. "
            "Do not wrap the answer in JSON or code fences around the whole document."
        ),
    )
    human = HumanMessage(
        content=(
            f"Cleaning plan:\n{json.dumps(plan, indent=2)[:40_000]}\n\n"
            f"Profile summary (row_count={profile.get('row_count')}):\n"
            f"{json.dumps(profile.get('columns', []), indent=2)[:40_000]}\n\n"
            f"Validation:\n{validation_summary_text(validation)}\n\n"
            f"Full validation JSON:\n{json.dumps(validation, indent=2)[:40_000]}"
        ),
    )
    try:
        resp = llm.invoke([sys, human])
        return {"explanation": extract_message_text(resp)}
    except Exception as e:  # noqa: BLE001
        return {
            "errors": state.get("errors", []) + [f"explain_failed: {e!s}"],
            "explanation": "",
        }
