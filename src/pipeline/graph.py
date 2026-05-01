from __future__ import annotations

from langgraph.graph import END, StateGraph

from pipeline.nodes import (
    node_apply_cleaning,
    node_cleaning_agent,
    node_explain,
    node_load,
    node_validate,
)
from pipeline.state import PipelineState


def build_graph():
    g = StateGraph(PipelineState)
    g.add_node("load", node_load)
    g.add_node("cleaning_agent", node_cleaning_agent)
    g.add_node("apply_cleaning", node_apply_cleaning)
    g.add_node("validate", node_validate)
    g.add_node("explain", node_explain)

    g.set_entry_point("load")
    g.add_edge("load", "cleaning_agent")
    g.add_edge("cleaning_agent", "apply_cleaning")
    g.add_edge("apply_cleaning", "validate")
    g.add_edge("validate", "explain")
    g.add_edge("explain", END)
    return g.compile()


def run_pipeline(
    csv_path: str,
    *,
    user_hints: str = "",
    sample_rows: int = 15,
) -> PipelineState:
    from dotenv import load_dotenv

    load_dotenv()
    app = build_graph()
    initial: PipelineState = {
        "csv_path": csv_path,
        "user_hints": user_hints,
        "sample_rows": sample_rows,
        "errors": [],
    }
    result = app.invoke(initial)
    return result  # type: ignore[return-value]
