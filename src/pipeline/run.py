from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv


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
        "--report",
        type=Path,
        default=None,
        help="Write explanation Markdown to this path (default: artifacts/<stem>_report.md).",
    )
    args = parser.parse_args(argv)

    if not args.csv.is_file():
        print(f"error: file not found: {args.csv}", file=sys.stderr)
        return 1

    from pipeline.graph import run_pipeline

    state = run_pipeline(
        str(args.csv.resolve()),
        user_hints=args.hints,
        sample_rows=args.sample_rows,
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
    body = f"# Pipeline report\n\nCleaned CSV: `{state.get('cleaned_csv_path', '')}`\n\n{explanation}\n"
    report_path.write_text(body, encoding="utf-8")
    print(f"Wrote report: {report_path.resolve()}")
    print(f"Cleaned CSV: {state.get('cleaned_csv_path', '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
