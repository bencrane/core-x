"""Forced-tool Anthropic Messages client for the map ``/ask`` TRANSLATE step.

ONE deterministic call: ``tool_choice`` pins the ``emit_filter`` tool so the model MUST
return a constrained ``{title, filters}`` object — never prose, never SQL. Reuses the
httpx/header shape of the managed-agents client (``x-api-key`` + ``anthropic-version``)
but hits ``/v1/messages`` with the standard ``ANTHROPIC_API_KEY`` (NOT the managed-agents
key) and no managed-agents beta header. The decoder system block is sent with
``cache_control`` so it is prompt-cached across calls.
"""
from __future__ import annotations

from typing import Any

import httpx

from .. import config

BASE_URL = "https://api.anthropic.com"
API_VERSION = "2023-06-01"


class AnthropicMessagesError(Exception):
    def __init__(self, *, status_code: int | None, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


def _api_key_or_raise() -> str:
    key = config.anthropic_api_key()
    if not key:
        raise AnthropicMessagesError(status_code=None, message="ANTHROPIC_API_KEY is not configured")
    return key


def _headers() -> dict[str, str]:
    return {
        "x-api-key": _api_key_or_raise(),
        "anthropic-version": API_VERSION,
        "content-type": "application/json",
    }


def _extract_tool_input(payload: dict[str, Any]) -> dict[str, Any]:
    """Pull the forced ``emit_filter`` tool_use input ``{title, filters}`` out of the
    Messages response. tool_choice guarantees a tool_use block; this still validates it."""
    for block in payload.get("content", []):
        if block.get("type") == "tool_use" and block.get("name") == "emit_filter":
            return block.get("input") or {}
    raise AnthropicMessagesError(status_code=None, message="no emit_filter tool_use block in response")


async def emit_filter(*, model: str, system_blocks: list[dict], tool: dict, user_text: str,
                      timeout: float = 15.0, retries: int = 1) -> dict[str, Any]:
    """One forced-tool Messages round-trip → the ``{title, filters}`` object. Retries
    once on a transport error or 5xx; raises ``AnthropicMessagesError`` on exhaustion."""
    body = {
        "model": model,
        "max_tokens": 512,
        "system": system_blocks,
        "tools": [tool],
        "tool_choice": {"type": "tool", "name": "emit_filter"},
        "messages": [{"role": "user", "content": user_text}],
    }
    err: str | None = None
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=timeout) as client:
        for _ in range(retries + 1):
            try:
                resp = await client.post("/v1/messages", headers=_headers(), json=body)
                if resp.status_code < 400:
                    return _extract_tool_input(resp.json())
                err = f"HTTP {resp.status_code}: {resp.text[:500]}"
                if resp.status_code < 500:
                    break  # a 4xx (bad model/key/schema) won't fix on retry
            except httpx.HTTPError as exc:
                err = f"transport error: {exc}"
    raise AnthropicMessagesError(status_code=None, message=f"emit_filter failed: {err}")
