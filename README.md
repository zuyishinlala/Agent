# Data Pipeline Agent

A LangGraph + Gemini pipeline for cleaning, validating, scoring, and reporting data quality for CSV datasets.

## Overview

This project implements a CSV data-quality pipeline with the following stages:

1. **Cleaning Plan Generation (Gemini)**  
   Proposes a declarative `CleaningPlan`: sentinel tokens, optional empty-string handling, whitespace stripping, numeric and date columns, and rows to drop when critical fields are missing.

2. **Deterministic Cleaning Executor**  
   Applies the plan with pandas (no model-generated code): sentinel and empty normalization, coercion, then optional `drop_duplicates` on inferred ID-like columns. Writes `artifacts/<stem>_cleaned.csv`.

3. **Validation**  
   Rule-based checks on the cleaned file: per-column null counts and duplicate-ID rows when ID-like column names are inferred from the profile.

4. **Quality Scoring**  
   A deterministic score from **0–100** from validation (null and duplicate-ID penalties). If the score is below `--quality-pass-threshold` and retries remain, the graph **re-runs the cleaning agent** with structured feedback; otherwise it proceeds to the report.

5. **Explanation Report (Gemini)**  
   Produces Markdown narrative sections. The report file also includes a deterministic **Summary** (built in Python) plus the model explanation; message text uses the API `.text` field so block metadata is not dumped into the file.

## Setup

```bash
cd /path/to/this-repo
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

Create a `.env` file in the repo root and set `GOOGLE_API_KEY` for Gemini.

## Run

```bash
pipeline-run --csv dirty_cafe_sales.csv --hints "Retail cafe transactions; IDs must be unique."
pipeline-run --csv dirty_cafe_sales.csv --quality-pass-threshold 85 --max-quality-retries 1
```

- `--quality-pass-threshold`: **0–100** (default `70`).
- `--max-quality-retries`: maximum **extra** cleaning passes after a failing score (default `2`).
- `--hints`: optional natural language for the planner (domain context, ID rules, common dirty tokens). Must still map to allowed `CleaningPlan` fields.
- `--sample-rows`: how many non-null sample values per column go into the profile for the LLM (default `15`).
- `--report`: path to write the Markdown report (default `artifacts/<csv-stem>_report.md`).

Run `pipeline-run -h` or `pipeline-run --help` for the full argparse usage (provided by the standard library).

**Outputs**

- `artifacts/<name>_cleaned.csv`
- `artifacts/<name>_report.md`

Each report **Summary** includes **volume reduction** (input row count → final cleaned row count, e.g. `10,000 → 3,089`), **missing/invalid values standardized** (sentinel or empty cells normalized to null; not statistical imputation), **duplicates removed** (when ID columns are inferred), and **quality score** with threshold and retries. Full null counts and duplicate-ID counts are in the validation JSON fed to the explanation step.

Optional environment variable: `GEMINI_MODEL` (default `gemini-3-flash-preview`).

## Tests

```bash
python -m pytest
```

Unit tests do not call the Gemini API.

## Layout

- `src/pipeline/` — graph, nodes, profiling, `CleaningPlan` + executor, validation, quality scoring
- `tests/` — pytest

**Graph (Mermaid):** run `pipeline-graph-mermaid` (after `pip install -e .`) to print the compiled LangGraph as Mermaid text from `get_graph().draw_mermaid()`; paste the output into [mermaid.live](https://mermaid.live) to visualize.
