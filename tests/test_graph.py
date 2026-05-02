from __future__ import annotations

from pipeline.graph import graph_mermaid


def test_graph_mermaid_contains_core_nodes():
    text = graph_mermaid()
    assert "cleaning_agent" in text
    assert "quality_score" in text
    assert "explain_agent" in text
