"""
Load planner harness JSONL and print pandas pivots comparing variants per dataset.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


def load_harness_jsonl(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(path)
    df = pd.read_json(path, lines=True)
    if df.empty:
        return df
    df["dataset"] = df["csv_path"].astype(str).map(lambda p: Path(p).name)
    return df


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare planner harness JSONL runs (pandas pivots on stdout).",
    )
    parser.add_argument(
        "jsonl",
        type=Path,
        nargs="?",
        default=Path("artifacts/planner_harness_files.jsonl"),
        help="Path to JSONL from pipeline-planner-harness (default: artifacts/planner_harness_files.jsonl).",
    )
    args = parser.parse_args(argv)

    try:
        df = load_harness_jsonl(args.jsonl)
    except FileNotFoundError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    if df.empty:
        print("error: no rows in JSONL", file=sys.stderr)
        return 1

    err_mask = df["errors"].map(lambda x: bool(x) if isinstance(x, list) else bool(x))
    if err_mask.any():
        bad = df.loc[err_mask, ["dataset", "variant_id", "errors"]]
        print("Warning: some runs have errors (scores may be placeholders):\n", bad, "\n", file=sys.stderr)

    score = df.pivot_table(
        index="dataset",
        columns="variant_id",
        values="quality_score",
        aggfunc="first",
    ).sort_index()
    rows_out = df.pivot_table(
        index="dataset",
        columns="variant_id",
        values="rows_out",
        aggfunc="first",
    ).sort_index()
    rows_in = df.groupby("dataset")["rows_in"].first()
    rows_out_display = rows_out.copy()
    rows_out_display.insert(0, "rows_in", rows_in)

    best_by_score = score.idxmax(axis=1)
    summary = pd.DataFrame({"best_quality_score_variant": best_by_score})
    summary["max_quality_score"] = score.max(axis=1)
    summary["rows_in"] = rows_in

    def _show(title: str, frame: pd.DataFrame) -> None:
        print(f"\n## {title}\n")
        print(frame.to_string(float_format=lambda x: f"{x:.2f}" if pd.notna(x) else ""))

    _show("Quality score by dataset × variant_id", score)
    _show(
        "rows_out by dataset × variant_id (rows_in = original row count before cleaning)",
        rows_out_display,
    )
    _show("Winner by max quality_score (+ ties: first max column order)", summary)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
