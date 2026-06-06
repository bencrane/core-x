"""Bearer-token middleware for the mounted MCP ASGI apps (Anthropic-facing).

Copied verbatim from hq-x ``app/mcp/bearer_auth.py`` — the gate that authenticates
managed-agent MCP traffic at the transport boundary. Used in Phase 1+ to wrap each
FastMCP ``http_app()`` before mounting (``/mcp/trigger`` etc.).

Why ASGI middleware (not a FastAPI dependency): FastMCP mounts as a Starlette
sub-app, so it bypasses the parent FastAPI's dependency-injection. Wrapping the
inner ASGI app in a Bearer check is the cleanest way to enforce auth at the MCP
transport boundary without touching every individual tool.

Behavior:
  * If ``bearer_token`` is None, the middleware is a no-op (dev/CI).
  * Otherwise every request must present ``Authorization: Bearer <token>`` where
    <token> compares equal under ``secrets.compare_digest``.
  * Failure responses are JSON envelopes matching the rest of the API:
    ``{"detail": {"error": "...", "message": "..."}}``.

The bearer value is ``DMAAS_MCP_BEARER_TOKEN`` — the SAME value the agent's
Anthropic vault credential injects (scoped by mcp_server_url), so keeping the value
identical across the move means the vault credential keeps authenticating once the
mount URL is repointed.
"""
from __future__ import annotations

import json
import secrets
from collections.abc import Awaitable, Callable
from typing import Any

ASGISend = Callable[[dict[str, Any]], Awaitable[None]]
ASGIReceive = Callable[[], Awaitable[dict[str, Any]]]


def _json_response(send: ASGISend, status: int, body: dict[str, Any]) -> Awaitable[None]:
    payload = json.dumps(body).encode()

    async def _send_pair() -> None:
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"www-authenticate", b'Bearer realm="edge-mcp"'),
                ],
            }
        )
        await send({"type": "http.response.body", "body": payload, "more_body": False})

    return _send_pair()


def bearer_token_app(inner: Any, *, bearer_token: str | None) -> Any:
    """Return an ASGI app that validates ``Authorization: Bearer <token>`` against
    ``bearer_token`` before delegating to ``inner``. When ``bearer_token`` is None,
    returns ``inner`` unchanged."""
    if bearer_token is None:
        return inner

    expected = bearer_token.encode()

    async def app(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        # Only HTTP traffic carries an Authorization header. Lifespan events pass
        # through so FastMCP's session-manager startup still runs.
        if scope["type"] != "http":
            return await inner(scope, receive, send)

        headers = dict(scope.get("headers") or [])
        auth = headers.get(b"authorization", b"")
        if not auth:
            return await _json_response(
                send,
                401,
                {
                    "detail": {
                        "error": "missing_bearer_token",
                        "message": "Authorization: Bearer <token> required",
                    }
                },
            )
        if not auth.lower().startswith(b"bearer "):
            return await _json_response(
                send,
                401,
                {
                    "detail": {
                        "error": "bad_authorization_scheme",
                        "message": "expected 'Bearer <token>'",
                    }
                },
            )
        provided = auth[len(b"bearer ") :].strip()
        if not secrets.compare_digest(provided, expected):
            return await _json_response(
                send,
                401,
                {"detail": {"error": "invalid_bearer_token", "message": "token mismatch"}},
            )

        return await inner(scope, receive, send)

    # Forward FastMCP's lifespan attribute so main.py can chain it.
    if hasattr(inner, "lifespan"):
        app.lifespan = inner.lifespan  # type: ignore[attr-defined]
    return app
