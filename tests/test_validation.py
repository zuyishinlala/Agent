from __future__ import annotations

import pandas as pd

from pipeline.validation import run_validation


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
