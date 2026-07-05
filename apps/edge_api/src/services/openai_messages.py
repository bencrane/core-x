"""Forced-tool OpenAI Chat Completions client for the map ``/ask`` TRANSLATE step.

Interface twin of ``anthropic_messages`` (same ``force_tool``/``emit_filter`` signatures,
same error shape) so the router swaps by import. ONE deterministic call: ``tool_choice``
pins the function so the model MUST return the constrained arguments object — never
prose, never SQL. The Anthropic tool shape ``{name, description, input_schema}`` is
converted to the OpenAI function shape ``{name, description, parameters}`` here; system
blocks are concatenated to one system message (``cache_control`` has no OpenAI analog —
OpenAI prompt-caches automatically). Uses ``OPENAI_API_KEY``.
"""
from __future__ import annotations

import json
from typing import Any

import httpx

from .. import config

BASE_URL = "https://api.openai.com"


class OpenAIMessagesError(Exception):
    def __init__(self, *, status_code: int | None, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


def _api_key_or_raise() -> str:
    key = config.openai_api_key()
    if not key:
        raise OpenAIMessagesError(status_code=None, message="OPENAI_API_KEY is not configured")
    return key


def _headers() -> dict[str, str]:
    return {
        "authorization": f"Bearer {_api_key_or_raise()}",
        "content-type": "application/json",
    }


def _system_text(system_blocks: list[dict]) -> str:
    """Flatten Anthropic-shaped system blocks (``{"type": "text", "text": ...}``) into
    the single system-message string Chat Completions expects."""
    return "\n\n".join(b.get("text", "") for b in system_blocks if b.get("type") == "text")


def _extract_tool_arguments(payload: dict[str, Any], tool_name: str) -> dict[str, Any]:
    """Pull the forced function call's arguments out of the Chat Completions response.
    tool_choice guarantees a tool call; this still validates name + JSON shape."""
    choices = payload.get("choices") or []
    calls = ((choices[0].get("message") or {}).get("tool_calls") or []) if choices else []
    for call in calls:
        fn = call.get("function") or {}
        if fn.get("name") == tool_name:
            try:
                args = json.loads(fn.get("arguments") or "{}")
            except json.JSONDecodeError as exc:
                raise OpenAIMessagesError(
                    status_code=None, message=f"{tool_name} arguments are not valid JSON: {exc}")
            if not isinstance(args, dict):
                raise OpenAIMessagesError(
                    status_code=None, message=f"{tool_name} arguments are not an object")
            return args
    raise OpenAIMessagesError(status_code=None, message=f"no {tool_name} tool call in response")


async def force_tool(*, model: str, system_blocks: list[dict], tool: dict, user_text: str,
                     tool_name: str | None = None, max_tokens: int = 512,
                     timeout: float = 15.0, retries: int = 1) -> dict[str, Any]:
    """One forced-tool Chat Completions round-trip → the function's arguments object.
    ``tool_choice`` pins ``tool_name`` (defaults to ``tool["name"]``) so the model MUST
    return that function's constrained arguments — never prose. Retries once on a
    transport error or 5xx; raises ``OpenAIMessagesError`` on exhaustion."""
    name = tool_name or tool.get("name")
    if not name:
        raise OpenAIMessagesError(status_code=None, message="force_tool requires a tool name")
    body = {
        "model": model,
        "max_completion_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": _system_text(system_blocks)},
            {"role": "user", "content": user_text},
        ],
        "tools": [{
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool.get("description", ""),
                "parameters": tool.get("input_schema") or tool.get("parameters") or {},
            },
        }],
        "tool_choice": {"type": "function", "function": {"name": name}},
    }
    err: str | None = None
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=timeout) as client:
        for _ in range(retries + 1):
            try:
                resp = await client.post("/v1/chat/completions", headers=_headers(), json=body)
                if resp.status_code < 400:
                    return _extract_tool_arguments(resp.json(), name)
                err = f"HTTP {resp.status_code}: {resp.text[:500]}"
                if resp.status_code < 500:
                    break  # a 4xx (bad model/key/schema) won't fix on retry
            except httpx.HTTPError as exc:
                err = f"transport error: {exc}"
    raise OpenAIMessagesError(status_code=None, message=f"{name} failed: {err}")


async def emit_filter(*, model: str, system_blocks: list[dict], tool: dict, user_text: str,
                      timeout: float = 15.0, retries: int = 1) -> dict[str, Any]:
    """One forced-tool round-trip → the map ``{title, filters}`` object. Thin wrapper
    over :func:`force_tool` pinning the ``emit_filter`` tool (the map /ask path)."""
    return await force_tool(model=model, system_blocks=system_blocks, tool=tool,
                            user_text=user_text, tool_name="emit_filter",
                            timeout=timeout, retries=retries)
