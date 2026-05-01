from __future__ import annotations

import pandas as pd

from pipeline.quality import compute_quality_score
from pipeline.validation import run_validation


def test_quality_perfect_score():
    df = pd.DataFrame({"id": [1, 2], "x": [1.0, 2.0]})
    v = run_validation(df, id_columns=["id"])
    out = compute_quality_score(v)
    assert out["score"] == 100.0
    assert not out["hard_fail"]


def test_quality_duplicate_penalty_and_hard_fail():
    df = pd.DataFrame({"id": [1, 1], "x": [1.0, 2.0]})
    v = run_validation(df, id_columns=["id"])
    out = compute_quality_score(v)
    assert out["hard_fail"]
    assert out["score"] < 100.0


def test_quality_high_nulls_no_hard_fail():
    df = pd.DataFrame({"id": [1, 2], "x": [None, None]})
    v = run_validation(df, id_columns=["id"])
    out = compute_quality_score(v)
    assert not out["hard_fail"]
    assert out["score"] < 100.0
    assert out["breakdown"]["penalty_null"] > 0
