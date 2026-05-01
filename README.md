# Data pipeline agent (LangGraph + Gemini)

CSV pipeline with LangGraph:

1. **Cleaning agent (Gemini)** — proposes a declarative `CleaningPlan` (sentinels, dtypes, required columns).
2. **Validate** — rule-based checks (nulls, duplicate IDs when column names match heuristics).
3. **Quality score (0–100)** — deterministic penalties; compare to `--quality-pass-threshold` (0–100). If below threshold (and retries remain), the graph **re-runs the cleaning agent** with feedback; otherwise it continues to the report.
4. **Explanation (Gemini)** — Markdown report including quality score and retries.

## Setup

```bash
cd <this-repo>
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
# Edit .env and set GOOGLE_API_KEY
```

## Run

```bash
pipeline-run --csv dirty_cafe_sales.csv --hints "Retail cafe transactions; IDs must be unique."
pipeline-run --csv dirty_cafe_sales.csv --quality-pass-threshold 85 --max-quality-retries 1
```

`--quality-pass-threshold` must be between **0** and **100** (default `70`). `--max-quality-retries` is how many **extra** cleaning passes are allowed after a failed score (default `2`).

Outputs:

- `artifacts/<name>_cleaned.csv`
- `artifacts/<name>_report.md` (Markdown from the model; text is taken from the message’s `.text` field so Gemini block metadata is not dumped into the file)

Each run’s report **Summary** lists **issues detected**, **missing filled** (sentinel/empty cells normalized to null; not imputation), **duplicates removed** (when ID columns are inferred), and **quality score**.

Optional: `GEMINI_MODEL` (default `gemini-3-flash-preview`).

## Tests

```bash
pytest
```

Unit tests do not call the Gemini API.

## Layout

- `src/pipeline/` — graph, nodes, profiling, cleaning, validation, **quality** scoring
- `tests/` — pytest
