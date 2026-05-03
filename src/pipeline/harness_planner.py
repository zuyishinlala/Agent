"""
Run the full pipeline for each (cleaning planner prompt variant × CSV) and append JSONL rows.

Requires GOOGLE_API_KEY and network access to Gemini. Not invoked by default pytest.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from pipeline.graph import run_pipeline
from pipeline.prompts import cleaning_planner_prompt_ids, cleaning_planner_system_prompt


def _parse_variants(arg: str) -> list[str]:
    s = arg.strip()
    if s.lower() == "all":
        return list(cleaning_planner_prompt_ids())
    parts = [p.strip() for p in s.split(",") if p.strip()]
    for p in parts:
        cleaning_planner_system_prompt(p)  # validate early
    return parts


def _row(
    *,
    variant_id: str,
    csv_path: str,
    repeat_index: int,
    state: dict,
) -> dict:
    plan = state.get("cleaning_plan") or {}
    bd = state.get("quality_breakdown") or {}
    cs = state.get("cleaning_stats") or {}
    prof = state.get("profile") or {}
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "variant_id": variant_id,
        "csv_path": csv_path,
        "repeat_index": repeat_index,
        "quality_score": state.get("quality_score"),
        "quality_pass": state.get("quality_pass"),
        "clean_retry_count": state.get("clean_retry_count", 0),
        "errors": state.get("errors") or [],
        "cleaning_plan_keys": sorted(plan.keys()) if isinstance(plan, dict) else [],
        "quality_hard_fail": bd.get("hard_duplicate") if isinstance(bd, dict) else None,
        "quality_breakdown": bd if isinstance(bd, dict) else {},
        "rows_in": prof.get("row_count"),
        "rows_out": cs.get("rows_after_cleaning"),
        "missing_filled": cs.get("missing_filled"),
        "duplicates_removed": cs.get("duplicates_removed"),
        "cleaning_planner_prompt_id": variant_id,
        "cleaned_csv_path": state.get("cleaned_csv_path"),
    }


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Cleaning planner prompt harness: run pipeline per variant × CSV, JSONL out.",
    )
    parser.add_argument(
        "--csv",
        action="append",
        dest="csvs",
        required=True,
        help="Input CSV path (repeat --csv for multiple fixtures).",
    )
    parser.add_argument(
        "--variants",
        default="all",
        help='Comma-separated prompt ids, or "all" (default: all registered variants).',
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Append one JSON object per line (JSONL).",
    )
    parser.add_argument("--hints", default="", help="Same as pipeline-run --hints.")
    parser.add_argument("--sample-rows", type=int, default=15)
    parser.add_argument(
        "--quality-pass-threshold",
        type=float,
        default=70.0,
        help="0–100, same as pipeline-run.",
    )
    parser.add_argument("--max-quality-retries", type=int, default=2)
    parser.add_argument(
        "--repeats",
        type=int,
        default=1,
        help="Runs per (variant, csv); use >1 to sample LLM variance.",
    )
    args = parser.parse_args(argv)

    if args.repeats < 1:
        print("error: --repeats must be >= 1", file=sys.stderr)
        return 1
    if not 0.0 <= args.quality_pass_threshold <= 100.0:
        print("error: --quality-pass-threshold must be 0..100", file=sys.stderr)
        return 1

    for p in args.csvs:
        if not Path(p).is_file():
            print(f"error: file not found: {p}", file=sys.stderr)
            return 1

    try:
        variants = _parse_variants(args.variants)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    out_path: Path = args.output
    out_path.parent.mkdir(parents=True, exist_ok=True)

    for variant_id in variants:
        for csv_path in args.csvs:
            for r in range(args.repeats):
                print(
                    f"[harness] cleaning_planner_prompt_id={variant_id} "
                    f"csv={csv_path!s} repeat={r}",
                    file=sys.stderr,
                    flush=True,
                )
                state = run_pipeline(
                    csv_path,
                    cleaning_planner_prompt_id=variant_id,
                    user_hints=args.hints,
                    sample_rows=args.sample_rows,
                    quality_pass_threshold=args.quality_pass_threshold,
                    max_clean_retries=args.max_quality_retries,
                )
                line = json.dumps(
                    _row(variant_id=variant_id, csv_path=csv_path, repeat_index=r, state=state),
                    ensure_ascii=False,
                )
                with out_path.open("a", encoding="utf-8") as f:
                    f.write(line + "\n")
                q = state.get("quality_score")
                score_s = "n/a" if q is None else str(q)
                out_csv = state.get("cleaned_csv_path") or "n/a"
                print(f"{variant_id}\t{score_s}\t{out_csv}", flush=True)

    print(f"Appended JSONL: {out_path.resolve()}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
