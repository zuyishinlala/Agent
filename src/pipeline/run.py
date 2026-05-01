from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv


def _quality_threshold_0_100(value: str) -> float:
    v = float(value)
    if not 0.0 <= v <= 100.0:
        raise argparse.ArgumentTypeError("quality-pass-threshold must be between 0 and 100")
    return v


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Run LangGraph CSV cleaning + validation pipeline.")
    parser.add_argument(
        "--csv",
        required=True,
        type=Path,
        help="Path to input CSV file.",
    )
    parser.add_argument(
        "--hints",
        default="",
        help="Optional domain hints for the cleaning agent.",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=15,
        help="Rows to include in profile sample per column.",
    )
    parser.add_argument(
        "--quality-pass-threshold",
        type=_quality_threshold_0_100,
        default=70.0,
        metavar="N",
        help="Minimum quality score (0-100) to pass without another cleaning pass.",
    )
    parser.add_argument(
        "--max-quality-retries",
        type=int,
        default=2,
        help="Maximum extra cleaning passes after quality score is below threshold (>= 0).",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Write explanation Markdown to this path (default: artifacts/<stem>_report.md).",
    )
    args = parser.parse_args(argv)

    if not args.csv.is_file():
        print(f"error: file not found: {args.csv}", file=sys.stderr)
        return 1

    if args.max_quality_retries < 0:
        print("error: --max-quality-retries must be >= 0", file=sys.stderr)
        return 1

    from pipeline.graph import run_pipeline

    state = run_pipeline(
        str(args.csv.resolve()),
        user_hints=args.hints,
        sample_rows=args.sample_rows,
        quality_pass_threshold=args.quality_pass_threshold,
        max_clean_retries=args.max_quality_retries,
    )

    if state.get("errors"):
        for e in state["errors"]:
            print(e, file=sys.stderr)
        return 2

    report_path = args.report
    if report_path is None:
        report_path = Path("artifacts") / f"{args.csv.stem}_report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    explanation = state.get("explanation", "")
    summary_lines = ""
    cs = state.get("cleaning_stats") or {}
    if "missing_filled" in cs:
        summary_lines += (
            f"- Missing filled (sentinels/empties normalized to null): {cs.get('missing_filled')}\n"
        )
    if "duplicates_removed" in cs:
        summary_lines += f"- Duplicates removed: {cs.get('duplicates_removed')}\n"

    q_meta = ""
    if state.get("quality_score") is not None:
        q_meta = (
            f"\n**Quality score:** {state.get('quality_score')} / 100 "
            f"(threshold {state.get('quality_pass_threshold', 70)}, "
            f"pass={state.get('quality_pass')}, retries={state.get('clean_retry_count', 0)})\n\n"
            f"```json\n{json.dumps(state.get('quality_breakdown', {}), indent=2)}\n```\n\n"
        )
    body = (
        f"# Pipeline report\n\n"
        f"Cleaned CSV: `{state.get('cleaned_csv_path', '')}`\n\n"
        f"## Summary\n{summary_lines}\n"
        f"{q_meta}"
        f"{explanation}\n"
    )
    report_path.write_text(body, encoding="utf-8")
    print(f"Wrote report: {report_path.resolve()}")
    print(f"Cleaned CSV: {state.get('cleaned_csv_path', '')}")
    cs = state.get("cleaning_stats") or {}
    if "missing_filled" in cs:
        print(f"Missing filled (normalized): {cs.get('missing_filled')}")
    if "duplicates_removed" in cs:
        print(f"Duplicates removed: {cs.get('duplicates_removed')}")
    if state.get("quality_score") is not None:
        print(
            f"Quality score: {state.get('quality_score')} "
            f"(pass={state.get('quality_pass')}, retries={state.get('clean_retry_count', 0)})",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
