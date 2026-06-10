"""gtm-mcp server entrypoint.

Initializes the MCP server with the **correct** FastMCP class —
``mcp.server.fastmcp.FastMCP`` (NOT ``mcp.server.fastapi``) — mounts the tool
modules from ``src/tools/``, and serves the MCP protocol over BOTH HTTP transports
from one process:

  • Streamable HTTP at ``/mcp`` — REQUIRED by the Anthropic managed-agent connector
    (reference: "the server must support the MCP protocol's streamable HTTP
    transport") and the transport every sibling MCP server in the GTM agent uses.
  • SSE at ``/sse`` (+ ``/messages/``) — the directive's named endpoint, and what
    the MCP Inspector / SSE clients use.

Run (locally and on Render, from the repo root):

    python -m apps.gtm_mcp.main

Render binds the process to ``$PORT`` on ``0.0.0.0``. The MCP routes (``/mcp``,
``/sse``, ``/messages/``) are gated by a bearer token (``HQX_MCP_BEARER_TOKEN``);
a lightweight ``/healthz`` (and ``/``) route stays open for liveness probes —
outside the MCP surface.
"""

from __future__ import annotations

import hmac
import os

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from .src.tools import audience, catalog, corex, dmaas, federal, govcon, hydration, ops, parallel, provider360

# Disable the SDK's DNS-rebinding protection. It defaults on for localhost and
# rejects any non-localhost Host header with `421 Invalid Host header` — which
# breaks a PUBLIC endpoint (every request arrives with the Render/edge host). The
# protection guards browser-reachable *localhost* dev servers from DNS-rebinding;
# it does not apply to a public HTTPS service whose host is already enforced by
# Render's routing edge, and whose clients are non-browser MCP agents.
_SECURITY = TransportSecuritySettings(enable_dns_rebinding_protection=False)

# The unified gateway. One server instance; tool modules mount onto it.
#
# stateless_http=True: the Streamable HTTP transport holds NO in-memory session
# state. The default (stateful) pins each client to an in-memory mcp-session-id,
# which is lost whenever the instance restarts — every deploy, platform restart,
# or scale event then makes in-flight agent calls fail with `404 Not Found`
# ("session terminated"). A tools-only gateway needs no resumable streams or
# server-initiated messages, so stateless is correct AND restart-resilient (it
# also makes the service safe to run on >1 instance later).
mcp = FastMCP("gtm-mcp", stateless_http=True, transport_security=_SECURITY)
audience.register(mcp)
catalog.register(mcp)
dmaas.register(mcp)
ops.register(mcp)  # Directive 18: hq-x ops.* upsert + Postgres schema introspection
hydration.register(mcp)  # Directive 20-Final: launch Waterfall ICP contact hydration (Trigger trigger)
corex.register(mcp)  # GTM control surface: initiative→campaign→lead→send (corex schema)
parallel.register(mcp)  # Directive 24: Parallel.ai enrich / deep_research / web_search + refresh_catalog
provider360.register(mcp)  # entity-360 targeting: independent platforms / acquisition groups / dual-pole (e2b479c)
govcon.register(mcp)  # hybrid filter→ANN semantic search over govcon_scope_vectors_90day (Tier B)
federal.register(mcp)  # FEDERAL group: deterministic map/chart aggregations + entity resolution over entity_profile_gold (separate from CRM audience tools)


async def _info(request):  # noqa: ANN001 — Starlette endpoint
    """Non-MCP liveness + capability probe."""
    tools = await mcp.list_tools()  # public FastMCP API (stable across SDK versions)
    return JSONResponse(
        {
            "service": "gtm-mcp",
            "status": "ok",
            "transports": {"streamable_http": "/mcp", "sse": "/sse", "messages": "/messages/"},
            "tools": sorted(t.name for t in tools),
        }
    )


class _BearerAuth:
    """Pure-ASGI bearer-token gate over the MCP transport routes (/mcp, /sse, /messages).

    Pure ASGI on purpose — Starlette's ``BaseHTTPMiddleware`` buffers the response
    body and would break the long-lived SSE stream. Enforces
    ``Authorization: Bearer <HQX_MCP_BEARER_TOKEN>`` on the MCP endpoints; /healthz
    and / stay open for liveness probes. When the token is unset (local dev) it
    warns and allows; production (Render) sets it, so enforcement is live there.
    Lifespan/websocket scopes pass straight through. Constant-time comparison
    avoids token-timing leaks."""

    def __init__(self, app, token: str | None):
        self.app = app
        self._expected = f"Bearer {token}".encode() if token else None

    @staticmethod
    def _protected(path: str) -> bool:
        return path.startswith(("/mcp", "/sse", "/messages"))

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


# Build both native transports over the one FastMCP instance and serve them from a
# single Starlette app. streamable_http_app() also lazily creates the session
# manager, which must be driven by the app lifespan (the SSE transport needs none).
_sse_app = mcp.sse_app()                # /sse  + /messages/
_http_app = mcp.streamable_http_app()   # /mcp  (+ creates mcp.session_manager)

_app = Starlette(
    routes=[*_sse_app.routes, *_http_app.routes],
    lifespan=lambda _scope: mcp.session_manager.run(),  # drive the Streamable HTTP manager
)
_app.router.routes.append(Route("/healthz", _info, methods=["GET"]))
_app.router.routes.append(Route("/", _info, methods=["GET"]))

# Public ASGI entrypoint — bearer-gated MCP transports (token mirrors
# DMAAS_MCP_BEARER_TOKEN per directive); /healthz stays open for liveness.
_TOKEN = os.environ.get("HQX_MCP_BEARER_TOKEN")
if not _TOKEN:
    print("WARNING: HQX_MCP_BEARER_TOKEN unset — MCP endpoints (/mcp, /sse, /messages) are UNAUTHENTICATED.")
app = _BearerAuth(_app, _TOKEN)


def main() -> None:
    import logging

    import uvicorn

    logging.basicConfig(level=logging.INFO)

    # Warm the dataset registry at startup: list the active sink once so the
    # catalog is in memory before the first query, and a listing/credential
    # failure surfaces at boot rather than mid-request.
    from .src import database

    database.get_registry()
    print(f"gtm-mcp: discovered {len(database.dataset_names())} datasets in the active sink.")

    # Warm the shared DuckDB connection so the hq-x Postgres attach (Directive 18) is
    # attempted at boot — an attach/credential failure surfaces here, not mid-request.
    database.get_connection()
    print(f"gtm-mcp: hq-x Postgres attached: {database.hqx_attached()}")

    port = int(os.environ.get("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
