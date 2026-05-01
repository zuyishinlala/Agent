from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.profile import build_profile


def test_build_profile(tmp_path: Path):
    p = tmp_path / "t.csv"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(p, index=False)
    prof = build_profile(p, sample_rows=5)
    assert prof["row_count"] == 2
    assert prof["column_count"] == 2
    assert any(c["name"] == "a" for c in prof["columns"])
