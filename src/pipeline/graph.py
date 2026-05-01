from __future__ import annotations

from langgraph.graph import END, StateGraph

from pipeline.nodes import (
    node_apply_cleaning,
    node_cleaning_agent,
    node_explain,
    node_increment_retry,
    node_load,
    node_quality_score,
    node_validate,
)
from pipeline.state import PipelineState


def route_after_quality(state: PipelineState) -> str:
    if state.get("errors"):
        print("[quality] next step: write report (pipeline errors).", flush=True)
        return "explain"
    if state.get("quality_pass"):
        print("[quality] next step: write report (score meets threshold).", flush=True)
        return "explain"
    max_r = int(state.get("max_clean_retries", 2))
    if int(state.get("clean_retry_count", 0)) >= max_r:
        print(
            "[quality] next step: write report (score below threshold; "
            f"no more cleaning retries, limit was {max_r}).",
            flush=True,
        )
        return "explain"
    print("[quality] next step: run another cleaning pass.", flush=True)
    return "increment_retry"


def build_graph():
    g = StateGraph(PipelineState)
    g.add_node("load", node_load)
    g.add_node("cleaning_agent", node_cleaning_agent)
    g.add_node("apply_cleaning", node_apply_cleaning)
    g.add_node("validate", node_validate)
    g.add_node("quality_score", node_quality_score)
    g.add_node("increment_retry", node_increment_retry)
    g.add_node("explain", node_explain)

    g.set_entry_point("load")
    g.add_edge("load", "cleaning_agent")
    g.add_edge("cleaning_agent", "apply_cleaning")
    g.add_edge("apply_cleaning", "validate")
    g.add_edge("validate", "quality_score")
    g.add_conditional_edges(
        "quality_score",
        route_after_quality,
        {
            "explain": "explain",
            "increment_retry": "increment_retry",
        },
    )
    g.add_edge("increment_retry", "cleaning_agent")
    g.add_edge("explain", END)
    return g.compile()


def run_pipeline(
    csv_path: str,
    *,
    user_hints: str = "",
    sample_rows: int = 15,
    quality_pass_threshold: float = 70.0,
    max_clean_retries: int = 2,
) -> PipelineState:
    from dotenv import load_dotenv

    load_dotenv()
    app = build_graph()
    initial: PipelineState = {
        "csv_path": csv_path,
        "user_hints": user_hints,
        "sample_rows": sample_rows,
        "quality_pass_threshold": float(quality_pass_threshold),
        "max_clean_retries": int(max_clean_retries),
        "clean_retry_count": 0,
        "quality_feedback": "",
        "errors": [],
    }
    result = app.invoke(initial)
    return result  # type: ignore[return-value]
