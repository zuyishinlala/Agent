from __future__ import annotations

from typing import Any


def extract_message_text(message: Any) -> str:
    """
    Turn a chat model `AIMessage` (or similar) into plain text for files and logs.

    Gemini (and others) may put structured blocks in `content`; `str(content)` then
    leaks JSON and internal fields. Prefer `BaseMessage.text`, which concatenates
    user-facing text parts only.
    """
    text = getattr(message, "text", None)
    if isinstance(text, str) and text.strip():
        return text.strip()

    content = getattr(message, "content", "")
    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                typ = block.get("type")
                chunk = block.get("text")
                if isinstance(chunk, str) and typ in (None, "text"):
                    parts.append(chunk)
        return "\n\n".join(p for p in parts if p).strip()

    return str(content).strip()
