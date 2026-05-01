from __future__ import annotations

import pandas as pd

from pipeline.validation import issues_detected_count, run_validation


def test_duplicates_and_consistency():
    df = pd.DataFrame(
        {
            "Transaction ID": ["t1", "t1", "t2"],
            "Quantity": [2.0, 1.0, 3.0],
            "Price Per Unit": [2.0, 2.0, 4.0],
            "Total Spent": [4.0, 3.0, 12.0],
        }
    )
    v = run_validation(
        df,
        id_columns=["Transaction ID"],
        consistency_total=("Quantity", "Price Per Unit", "Total Spent"),
    )
    assert v["duplicate_id_rows"] == 2
    assert v["consistency"] is not None
    assert v["consistency"]["mismatch_beyond_tolerance"] >= 1


def test_issues_detected_count_nulls_and_dup():
    df = pd.DataFrame({"id": [1, 2], "x": [None, 1]})
    v = run_validation(df, id_columns=["id"])
    assert issues_detected_count(v) == 1

    df2 = pd.DataFrame({"id": [1, 1], "x": [1, 2]})
    v2 = run_validation(df2, id_columns=["id"])
    assert issues_detected_count(v2) == 1
