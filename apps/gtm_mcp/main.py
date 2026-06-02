"""gtm-mcp server entrypoint.

Initializes the MCP server with the **correct** FastMCP class —
``mcp.server.fastmcp.FastMCP`` (NOT ``mcp.server.fastapi``) — which handles the
SSE transport natively, mounts the tool modules from ``src/tools/``, and serves
the public ``/sse`` endpoint.

Run (locally and on Render, from the repo root):

    python -m apps.gtm_mcp.main

Render binds the process to ``$PORT`` on ``0.0.0.0``; ``mcp.sse_app()`` is the
native SSE Starlette app, exposing GET ``/sse`` (event stream) + POST
``/messages/`` (client→server). The MCP routes are gated by a bearer token
(``HQX_MCP_BEARER_TOKEN``); a lightweight ``/healthz`` (and ``/``) route stays
open for liveness probes and connectivity checks — outside the MCP surface.
"""

from __future__ import annotations

import hmac
import os

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse
from starlette.routing import Route

from .src.tools import audience, dmaas

# Disable the SDK's DNS-rebinding protection. It defaults on for localhost and
# rejects any non-localhost Host header with `421 Invalid Host header` — which
# breaks a PUBLIC endpoint (every request arrives with the Render/edge host). The
# protection guards browser-reachable *localhost* dev servers from DNS-rebinding;
# it does not apply to a public HTTPS service whose host is already enforced by
# Render's routing edge, and whose clients are non-browser MCP agents.
_SECURITY = TransportSecuritySettings(enable_dns_rebinding_protection=False)

# The unified gateway. One server instance; tool modules mount onto it.
mcp = FastMCP("gtm-mcp", transport_security=_SECURITY)
audience.register(mcp)
dmaas.register(mcp)


async def _info(request):  # noqa: ANN001 — Starlette endpoint
    """Non-MCP liveness + capability probe."""
    tools = await mcp.list_tools()  # public FastMCP API (stable across SDK versions)
    return JSONResponse(
        {
            "service": "gtm-mcp",
            "status": "ok",
            "transport": "sse",
            "endpoints": {"sse": "/sse", "messages": "/messages/"},
            "tools": sorted(t.name for t in tools),
        }
    )


class _BearerAuth:
    """Pure-ASGI bearer-token gate over the MCP transport routes (/sse, /messages).

    Pure ASGI on purpose — Starlette's ``BaseHTTPMiddleware`` buffers the response
    body and would break the long-lived SSE stream. Enforces
    ``Authorization: Bearer <HQX_MCP_BEARER_TOKEN>`` on the MCP endpoints; /healthz
    and / stay open for liveness probes. When the token is unset (local dev) it
    warns and allows; production (Render) sets it, so enforcement is live there.
    Constant-time comparison avoids token-timing leaks."""

    def __init__(self, app, token: str | None):
        self.app = app
        self._expected = f"Bearer {token}".encode() if token else None

    @staticmethod
    def _protected(path: str) -> bool:
        return path == "/sse" or path.startswith("/messages")

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and self._expected and self._protected(scope.get("path", "")):
            provided = dict(scope.get("headers") or []).get(b"authorization", b"")
            if not hmac.compare_digest(provided, self._expected):
                await JSONResponse(
                    {"error": "unauthorized"},
                    status_code=401,
                    headers={"WWW-Authenticate": "Bearer"},
                )(scope, receive, send)
                return
        await self.app(scope, receive, send)


# Native SSE Starlette app (GET /sse + POST /messages/), plus open ops routes.
_sse_app = mcp.sse_app()
_sse_app.router.routes.append(Route("/healthz", _info, methods=["GET"]))
_sse_app.router.routes.append(Route("/", _info, methods=["GET"]))

# Public ASGI entrypoint — bearer-gated MCP transport (token mirrors
# DMAAS_MCP_BEARER_TOKEN per directive); /healthz stays open for liveness.
_TOKEN = os.environ.get("HQX_MCP_BEARER_TOKEN")
if not _TOKEN:
    print("WARNING: HQX_MCP_BEARER_TOKEN unset — MCP endpoints (/sse, /messages) are UNAUTHENTICATED.")
app = _BearerAuth(_sse_app, _TOKEN)


def main() -> None:
    import uvicorn

    port = int(os.environ.get("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
