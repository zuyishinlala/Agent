# Cleaning planner prompt harness

This document describes how to **batch-compare** cleaning planner system prompt variants (`default`, `duplicate_focus`, `retention_focus`, etc.) and how to read the outputs.  
Implementation lives in `src/pipeline/harness_planner.py`, `src/pipeline/prompts/cleaning_planner.py`, and `src/pipeline/compare_harness_jsonl.py`.

## How this differs from `pipeline-run`

| | `pipeline-run` | Harness |
|---|----------------|---------|
| **Goal** | Run the full pipeline once and write cleaned CSV + Markdown report | Run the full pipeline many times for **prompt × (optional multiple) CSV** and record **metrics in JSONL** |
| **Artifacts** | Same cleaned paths + report | Same cleaned outputs per run, plus **`--output` JSONL** |
| **Report `.md`** | Written by `pipeline-run` after the graph returns | **Not** written by the harness CLI; the graph still runs the explain node, but there is no harness step that persists a report file (use `pipeline-run` if you need one) |

The harness still invokes the **full LangGraph** (validation, quality, explain), so it still consumes Gemini quota. **Default `pytest` does not** run the harness.

## Prerequisites

- `pip install -e ".[dev]"` (or at least `pip install -e .`) with your venv activated.
- **`GOOGLE_API_KEY`** in a `.env` file at the repo root.
- Optional: `GEMINI_MODEL`.

## CLI: `pipeline-planner-harness`

For every **`--csv`** × every **`--variants`** entry (or `all`) × `--repeats`, **append one JSON line** to the JSONL file.

```bash
pipeline-planner-harness \
  --csv files/dirty_cafe_sales.csv \
  --csv files/listings.csv \
  --variants all \
  --output artifacts/planner_harness.jsonl \
  --quality-pass-threshold 85
```

Common flags:

- `--variants`: `all` or a comma-separated list of ids (**no spaces**).
- `--output`: path to a **file** (JSONL; opens in **append** mode).
- `--quality-pass-threshold`, `--max-quality-retries`, `--hints`, `--sample-rows`: same semantics as `pipeline-run`.

**Console output**

- **stderr**: before each run, `[harness] cleaning_planner_prompt_id=... csv=... repeat=...`; when finished, `Appended JSONL: ...`.
- **stdout**: one line per run: **`prompt_id<TAB>score<TAB>cleaned_csv_path`** (easy to pipe).

**JSONL fields (subset)**

- `variant_id` / `cleaning_planner_prompt_id`: the same cleaning planner prompt key.
- `quality_score`, `quality_pass`, `quality_breakdown`, `clean_retry_count`, `errors`.
- `rows_in`, `rows_out`, `cleaned_csv_path`.

Non-`default` prompts write **`artifacts/<stem>_cleaned__<id>.csv`** so runs do not overwrite each other’s cleaned outputs.

## Bash: `scripts/harness_files.sh`

Adds one `--csv` for each **`files/*.csv`**, then runs `pipeline-planner-harness`. Scripts should use **LF** line endings (see `.gitattributes` for `*.sh`).

```bash
./scripts/harness_files.sh
./scripts/harness_files.sh artifacts/my_run.jsonl

VARIANTS=default,retention_focus THRESHOLD=85 ./scripts/harness_files.sh
HINTS="Treat ID columns strictly." ./scripts/harness_files.sh
```

- First argument: JSONL path (default `artifacts/planner_harness_files.jsonl`).
- **Line continuation**: if you use `\`, it must be the **last character** on the line (no spaces after it before the newline).

## Comparing runs: `pipeline-compare-harness-jsonl`

Loads the JSONL with pandas and prints:

1. **Quality score** pivot (dataset × variant).
2. **rows_out** pivot with **`rows_in`** as the first column (original row count before cleaning).
3. **Winner** per dataset: variant with the **highest** score (ties: first max column in column order).

```bash
pipeline-compare-harness-jsonl artifacts/planner_harness_files.jsonl
```

If any row has a non-empty `errors` list, a warning is printed on **stderr** (scores may be placeholders; do not rank those runs blindly).

## Where prompts are defined

`CLEANING_PLANNER_PROMPTS` in `src/pipeline/prompts/cleaning_planner.py`.  
For a single pipeline run with a specific prompt:

```bash
pipeline-run --csv files/dirty_cafe_sales.csv --cleaning-planner-prompt duplicate_focus
```

## Operational notes

- **Cost**: `--variants all` multiplied by several large CSVs triggers many API calls.
- **JSONL is append-only**: reusing the same `--output` appends new lines; use a fresh filename or truncate the file for a clean experiment.
- **Encoding**: CSV reads use UTF-8. If you see `load_failed` with a codec error, convert the file to UTF-8 and retry.
