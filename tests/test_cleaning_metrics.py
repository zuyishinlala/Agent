from __future__ import annotations

import pandas as pd

from pipeline.cleaning_metrics import count_missing_filled_normalizations
from pipeline.cleaning_plan import CleaningPlan, apply_cleaning_plan


def test_count_missing_filled_sentinels_to_na():
    df = pd.DataFrame({"a": ["ERROR", "ok"], "b": [1, 2]})
    plan = CleaningPlan(sentinel_tokens=["ERROR"], replace_empty_strings=True)
    after = apply_cleaning_plan(df, plan)
    n = count_missing_filled_normalizations(df, after, plan)
    assert n == 1


def test_count_missing_filled_empty_string():
    df = pd.DataFrame({"a": ["  ", "x"], "b": [1, 2]})
    plan = CleaningPlan(sentinel_tokens=[], replace_empty_strings=True)
    after = apply_cleaning_plan(df, plan)
    n = count_missing_filled_normalizations(df, after, plan)
    assert n == 1
