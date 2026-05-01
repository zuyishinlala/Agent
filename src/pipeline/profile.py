from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def build_profile(
    csv_path: str | Path,
    *,
    sample_rows: int = 15,
    max_uniques: int = 12,
) -> dict[str, Any]:
    """Compact dataset profile for LLM prompts (no full file contents)."""
    path = Path(csv_path)
    if not path.is_file():
        raise FileNotFoundError(str(path))

    df = pd.read_csv(path)
    n = len(df)
    columns: list[dict[str, Any]] = []
    for col in df.columns:
        s = df[col]
        null_pct = float(s.isna().mean()) if n else 0.0
        dtype = str(s.dtype)
        sample = s.dropna().head(sample_rows).astype(str).tolist()
        nunique = int(s.nunique(dropna=True))
        if nunique <= 200:
            vc = s.astype(str).value_counts().head(max_uniques)
            top_values = vc.index.astype(str).tolist()
        else:
            top_values = ["<many distinct values>"]
        columns.append(
            {
                "name": col,
                "dtype": dtype,
                "null_fraction": round(null_pct, 4),
                "sample_values": sample[:sample_rows],
                "distinct_approx": int(s.nunique(dropna=True)),
                "value_preview": top_values,
            }
        )

    head_df = df.head(sample_rows)
    return {
        "path": str(path.resolve()),
        "row_count": int(n),
        "column_count": int(df.shape[1]),
        "columns": columns,
        "head_csv": head_df.to_csv(index=False),
    }
