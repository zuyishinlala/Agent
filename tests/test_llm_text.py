from __future__ import annotations

from langchain_core.messages import AIMessage

from pipeline.llm_text import extract_message_text


def test_extract_prefers_message_text_property():
    m = AIMessage(content=[{"type": "text", "text": "## Hello", "extras": {"signature": "x" * 100}}])
    out = extract_message_text(m)
    assert out == "## Hello"
    assert "signature" not in out


def test_extract_plain_string_content():
    m = AIMessage(content="plain")
    assert extract_message_text(m) == "plain"


def test_extract_list_blocks_fallback():
    class FakeMsg:
        content = [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]
        text = ""

    out = extract_message_text(FakeMsg())
    assert "a" in out and "b" in out
