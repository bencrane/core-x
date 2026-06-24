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


def _extract_tool_input(payload: dict[str, Any], tool_name: str) -> dict[str, Any]:
    """Pull the forced ``tool_name`` tool_use input out of the Messages response.
    tool_choice guarantees a tool_use block; this still validates it."""
    for block in payload.get("content", []):
        if block.get("type") == "tool_use" and block.get("name") == tool_name:
            return block.get("input") or {}
    raise AnthropicMessagesError(status_code=None, message=f"no {tool_name} tool_use block in response")


async def force_tool(*, model: str, system_blocks: list[dict], tool: dict, user_text: str,
                     tool_name: str | None = None, max_tokens: int = 512,
                     timeout: float = 15.0, retries: int = 1) -> dict[str, Any]:
    """One forced-tool Messages round-trip → the tool's ``input`` object. ``tool_choice``
    pins ``tool_name`` (defaults to ``tool["name"]``) so the model MUST return that tool's
    constrained input — never prose. Retries once on a transport error or 5xx; raises
    ``AnthropicMessagesError`` on exhaustion."""
    name = tool_name or tool.get("name")
    if not name:
        raise AnthropicMessagesError(status_code=None, message="force_tool requires a tool name")
    body = {
        "model": model,
        "max_tokens": max_tokens,
        "system": system_blocks,
        "tools": [tool],
        "tool_choice": {"type": "tool", "name": name},
        "messages": [{"role": "user", "content": user_text}],
    }
    err: str | None = None
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=timeout) as client:
        for _ in range(retries + 1):
            try:
                resp = await client.post("/v1/messages", headers=_headers(), json=body)
                if resp.status_code < 400:
                    return _extract_tool_input(resp.json(), name)
                err = f"HTTP {resp.status_code}: {resp.text[:500]}"
                if resp.status_code < 500:
                    break  # a 4xx (bad model/key/schema) won't fix on retry
            except httpx.HTTPError as exc:
                err = f"transport error: {exc}"
    raise AnthropicMessagesError(status_code=None, message=f"{name} failed: {err}")


async def emit_filter(*, model: str, system_blocks: list[dict], tool: dict, user_text: str,
                      timeout: float = 15.0, retries: int = 1) -> dict[str, Any]:
    """One forced-tool Messages round-trip → the map ``{title, filters}`` object. Thin
    wrapper over :func:`force_tool` pinning the ``emit_filter`` tool (the map /ask path)."""
    return await force_tool(model=model, system_blocks=system_blocks, tool=tool,
                            user_text=user_text, tool_name="emit_filter",
                            timeout=timeout, retries=retries)
