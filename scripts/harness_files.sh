#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT="${1:-artifacts/planner_harness_files.jsonl}"
VARIANTS="${VARIANTS:-all}"
THRESHOLD="${THRESHOLD:-85}"

shopt -s nullglob
csvs=(files/*.csv)
if [[ ${#csvs[@]} -eq 0 ]]; then
  echo "error: no CSV files matching files/*.csv" >&2
  exit 1
fi

args=(pipeline-planner-harness --variants "$VARIANTS" --output "$OUT" --quality-pass-threshold "$THRESHOLD")
if [[ -n "${HINTS:-}" ]]; then
  args+=(--hints "$HINTS")
fi
for f in "${csvs[@]}"; do
  if [[ -f "$f" ]]; then
    args+=(--csv "$f")
  fi
done

exec "${args[@]}"
