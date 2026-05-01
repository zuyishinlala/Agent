from __future__ import annotations

import pandas as pd

from pipeline.cleaning_plan import CleaningPlan


def count_missing_filled_normalizations(
    before: pd.DataFrame,
    after: pd.DataFrame,
    plan: CleaningPlan,
) -> int:
    """
    Count cells where dirty sentinels or empty strings became null (not imputation).

    Scans string-like columns and any numeric_columns from the plan: before strip,
    value matches sentinel tokens or empty/nan string; after is null.
    """
    tokens = {str(t).strip() for t in plan.sentinel_tokens}
    cols = set(before.columns) & set(after.columns)
    cols.update(plan.numeric_columns)
    hits = 0
    for col in cols:
        if col not in before.columns or col not in after.columns:
            continue
        b = before[col]
        a = after[col]
        s = b.astype(str).str.strip()
        bad = s.isin(tokens)
        if plan.replace_empty_strings:
            bad = bad | (s == "") | (s.str.lower() == "nan")
        hits += int((bad & a.isna()).sum())
    return hits
