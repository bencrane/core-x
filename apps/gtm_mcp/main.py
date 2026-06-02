"""gtm-mcp server entrypoint.

Initializes the MCP server with the **correct** FastMCP class —
``mcp.server.fastmcp.FastMCP`` (NOT ``mcp.server.fastapi``) — which handles the
SSE transport natively, mounts the tool modules from ``src/tools/``, and serves
the public ``/sse`` endpoint.

Run (locally and on Render, from the repo root):

    python -m apps.gtm_mcp.main

Render binds the process to ``$PORT`` on ``0.0.0.0``; ``mcp.sse_app()`` is the
native SSE Starlette app, exposing GET ``/sse`` (event stream) + POST
``/messages/`` (client→server). A lightweight ``/healthz`` (and ``/``) route is
added for liveness probes and connectivity checks — outside the MCP surface.
"""

from __future__ import annotations

import os

from mcp.server.fastmcp import FastMCP
from starlette.responses import JSONResponse
from starlette.routing import Route

from .src.tools import audience, dmaas

# The unified gateway. One server instance; tool modules mount onto it.
mcp = FastMCP("gtm-mcp")
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


# Native SSE ASGI app (GET /sse + POST /messages/), plus the ops routes.
app = mcp.sse_app()
app.router.routes.append(Route("/healthz", _info, methods=["GET"]))
app.router.routes.append(Route("/", _info, methods=["GET"]))


def main() -> None:
    import uvicorn

    port = int(os.environ.get("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
