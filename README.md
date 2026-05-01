# Data pipeline agent (LangGraph + Gemini)

Two-step pipeline over CSV files:

1. **Cleaning agent (Gemini)** — proposes a declarative `CleaningPlan` (sentinels, dtypes, required columns).
2. **Validation + explanation (Gemini)** — runs deterministic checks, then narrates results.

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
```

Outputs:

- `artifacts/<name>_cleaned.csv`
- `artifacts/<name>_report.md` (Markdown from the model; text is taken from the message’s `.text` field so Gemini block metadata is not dumped into the file)

Optional: `GEMINI_MODEL` (default `gemini-3-flash-preview`).

## Tests

```bash
pytest
```

Unit tests do not call the Gemini API.

## Layout

- `src/pipeline/` — graph, nodes, profiling, cleaning executor, validation
- `tests/` — pytest
