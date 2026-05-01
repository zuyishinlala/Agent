from __future__ import annotations

import pandas as pd

from pipeline.cleaning_plan import CleaningPlan, apply_cleaning_plan


def test_apply_sentinels_and_numeric():
    df = pd.DataFrame(
        {
            "id": ["a", "b", "c"],
            "qty": ["1", "ERROR", "3"],
            "total": ["10.0", "2.0", "UNKNOWN"],
        }
    )
    plan = CleaningPlan(
        sentinel_tokens=["ERROR", "UNKNOWN"],
        numeric_columns=["qty"],
        drop_rows_missing_in_columns=["qty"],
    )
    out = apply_cleaning_plan(df, plan)
    assert len(out) == 2
    assert out["qty"].iloc[0] == 1.0
    assert int(out["total"].isna().sum()) >= 1


def test_strip_and_drop_required():
    df = pd.DataFrame({"x": ["  a  ", "b"], "y": [1, 2]})
    plan = CleaningPlan(
        strip_whitespace=True,
        drop_rows_missing_in_columns=["x"],
        sentinel_tokens=[],
        replace_empty_strings=True,
    )
    out = apply_cleaning_plan(df, plan)
    assert out["x"].tolist() == ["a", "b"]
