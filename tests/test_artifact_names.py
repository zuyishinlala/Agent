from __future__ import annotations

from pathlib import Path

from pipeline.artifact_names import (
    cleaned_csv_filename,
    default_cleaned_csv_path,
    default_report_md_path,
    planner_prompt_filename_suffix,
    report_md_filename,
    sanitize_planner_prompt_id_for_filename,
)


def test_default_suffix_empty():
    assert planner_prompt_filename_suffix("default") == ""
    assert planner_prompt_filename_suffix("") == ""
    assert cleaned_csv_filename("foo", "default") == "foo_cleaned.csv"
    assert report_md_filename("foo", "default") == "foo_report.md"


def test_non_default_suffix():
    assert planner_prompt_filename_suffix("duplicate_focus") == "__duplicate_focus"
    assert cleaned_csv_filename("dirty_cafe_sales", "duplicate_focus") == (
        "dirty_cafe_sales_cleaned__duplicate_focus.csv"
    )
    assert report_md_filename("dirty_cafe_sales", "duplicate_focus") == (
        "dirty_cafe_sales_report__duplicate_focus.md"
    )


def test_sanitize_replaces_unsafe_chars():
    assert sanitize_planner_prompt_id_for_filename("a/b c") == "a_b_c"


def test_default_paths_under_artifacts():
    p = default_cleaned_csv_path("x", "default", artifacts_dir=Path("artifacts"))
    assert p == Path("artifacts/x_cleaned.csv")
    r = default_report_md_path("x", "duplicate_focus", artifacts_dir=Path("out"))
    assert r == Path("out/x_report__duplicate_focus.md")
