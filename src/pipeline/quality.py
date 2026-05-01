from __future__ import annotations

from typing import Any


def compute_quality_score(validation: dict[str, Any]) -> dict[str, Any]:
    """
    Deterministic 0-100 score from `run_validation` output.

    Penalties: average null rate per column, duplicate key rows.
    Hard fail: duplicate IDs only (blocks pass regardless of score).
    """
    n = max(int(validation.get("row_count", 0)), 1)
    null_counts = validation.get("null_counts") or {}

    if null_counts:
        fracs = [int(null_counts[c]) / n for c in null_counts]
        r_null = sum(fracs) / len(fracs)
    else:
        r_null = 0.0

    penalty_null = min(40.0, r_null * 100.0)

    dup = validation.get("duplicate_id_rows")
    hard_duplicate = dup is not None and int(dup) > 0
    penalty_dup = min(50.0, (int(dup) / n) * 200.0) if hard_duplicate else 0.0

    score = 100.0 - penalty_null - penalty_dup
    score = max(0.0, min(100.0, score))

    hard_fail = hard_duplicate

    return {
        "score": round(score, 2),
        "hard_fail": hard_fail,
        "breakdown": {
            "null_rate_avg": round(r_null, 4),
            "penalty_null": round(penalty_null, 2),
            "penalty_dup": round(penalty_dup, 2),
            "duplicate_id_rows": dup,
            "hard_duplicate": hard_duplicate,
        },
    }


def quality_feedback_text(
    *,
    quality_score: float,
    quality_pass: bool,
    threshold: float,
    breakdown: dict[str, Any],
    validation_summary: str,
) -> str:
    lines = [
        f"Quality score={quality_score} (pass threshold={threshold}, pass={quality_pass}).",
        f"Breakdown: {breakdown}",
        f"Validation summary: {validation_summary}",
        "Propose a stricter CleaningPlan if possible (more sentinels, stricter drop rules, or additional numeric columns).",
    ]
    return "\n".join(lines)
