from __future__ import annotations

from typing import Any

import pandas as pd


def run_validation(
    df: pd.DataFrame,
    *,
    id_columns: list[str] | None = None,
    consistency_total: tuple[str, str, str] | None = None,
) -> dict[str, Any]:
    """
    Deterministic checks. `consistency_total` is (qty_col, price_col, total_col) if all exist.
    """
    out: dict[str, Any] = {
        "row_count": int(len(df)),
        "column_count": int(df.shape[1]),
        "null_counts": {c: int(df[c].isna().sum()) for c in df.columns},
        "duplicate_id_rows": None,
        "consistency": None,
    }

    id_columns = id_columns or []
    id_cols_present = [c for c in id_columns if c in df.columns]
    if id_cols_present:
        dup = int(df.duplicated(subset=id_cols_present, keep=False).sum())
        out["duplicate_id_rows"] = dup

    if consistency_total:
        q, p, t = consistency_total
        if q in df.columns and p in df.columns and t in df.columns:
            expected = df[q].astype(float) * df[p].astype(float)
            actual = df[t].astype(float)
            mask = df[q].notna() & df[p].notna() & df[t].notna()
            if mask.any():
                diff = (expected[mask] - actual[mask]).abs()
                tol = 0.05
                mism = int((diff > tol).sum())
                out["consistency"] = {
                    "checked_rows": int(mask.sum()),
                    "mismatch_beyond_tolerance": mism,
                    "tolerance": tol,
                }

    return out


def infer_id_columns_from_profile(profile: dict[str, Any]) -> list[str]:
    """Heuristic ID columns from a profile dict (same rules as the validate node)."""
    id_columns: list[str] = []
    for col in profile.get("columns", []):
        name = col.get("name", "")
        n = name.lower()
        if "transaction id" in n or n.endswith("_id") or n == "id":
            id_columns.append(name)
    if not id_columns:
        cols = [c["name"] for c in profile.get("columns", []) if "id" in c["name"].lower()]
        id_columns = cols[:1]
    return id_columns


def issues_detected_count(validation: dict[str, Any]) -> int:
    """
    Rough issue tally: one per column with any nulls, plus one if duplicate ID rows exist.
    """
    null_counts = validation.get("null_counts") or {}
    n = sum(1 for _col, cnt in null_counts.items() if int(cnt) > 0)
    dup = validation.get("duplicate_id_rows")
    if dup is not None and int(dup) > 0:
        n += 1
    return int(n)


def validation_summary_text(v: dict[str, Any]) -> str:
    """Compact string for LLM context."""
    parts = [
        f"rows={v['row_count']} cols={v['column_count']}",
        f"null_counts={v['null_counts']}",
    ]
    if v.get("duplicate_id_rows") is not None:
        parts.append(f"duplicate_id_rows={v['duplicate_id_rows']}")
    if v.get("consistency"):
        parts.append(f"consistency={v['consistency']}")
    return "\n".join(parts)
