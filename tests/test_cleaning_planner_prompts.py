from __future__ import annotations

import pytest

from pipeline.prompts import (
    cleaning_planner_prompt_ids,
    cleaning_planner_system_prompt,
)


def test_cleaning_planner_prompt_ids_include_default():
    ids = cleaning_planner_prompt_ids()
    assert "default" in ids
    assert "duplicate_focus" in ids


def test_cleaning_planner_system_prompt_default_substring():
    text = cleaning_planner_system_prompt("default")
    assert "data cleaning planner" in text.lower()
    assert "CleaningPlan" in text or "cleaningplan" in text.lower()


def test_unknown_cleaning_planner_prompt_raises():
    with pytest.raises(ValueError, match="Unknown cleaning_planner_prompt_id"):
        cleaning_planner_system_prompt("not_a_real_variant")
