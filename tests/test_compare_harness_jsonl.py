from __future__ import annotations

import json
from pathlib import Path

from pipeline.compare_harness_jsonl import load_harness_jsonl, main


def test_load_harness_jsonl_pivot_ready(tmp_path: Path) -> None:
    lines = [
        {
            "csv_path": "files/a.csv",
            "variant_id": "default",
            "quality_score": 80.0,
            "rows_in": 100,
            "rows_out": 100,
            "errors": [],
        },
        {
            "csv_path": "files/a.csv",
            "variant_id": "duplicate_focus",
            "quality_score": 85.0,
            "rows_in": 100,
            "rows_out": 90,
            "errors": [],
        },
    ]
    p = tmp_path / "h.jsonl"
    p.write_text("\n".join(json.dumps(x) for x in lines) + "\n", encoding="utf-8")
    df = load_harness_jsonl(p)
    assert len(df) == 2
    assert list(df["dataset"]) == ["a.csv", "a.csv"]
    piv = df.pivot_table(index="dataset", columns="variant_id", values="quality_score", aggfunc="first")
    assert piv.loc["a.csv", "duplicate_focus"] == 85.0
    rows_in = df.groupby("dataset")["rows_in"].first()
    rows_out = df.pivot_table(index="dataset", columns="variant_id", values="rows_out", aggfunc="first")
    disp = rows_out.copy()
    disp.insert(0, "rows_in", rows_in)
    assert "rows_in" in disp.columns and disp.loc["a.csv", "rows_in"] == 100


def test_main_missing_file() -> None:
    assert main([str(Path("/nonexistent/nope.jsonl"))]) == 1
