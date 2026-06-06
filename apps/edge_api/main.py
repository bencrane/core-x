"""edge_api entrypoint — the public Anthropic Managed-Agents edge for core-x.

Strangler-fig extraction of the agent-facing surface out of the hq-x monolith:
the MCP mounts the gtm-agent calls (trigger / lob), the agent-runs streaming
proxy the platform-api BFF drives, and (optionally) the post-payment pipeline
seam. Each lands in its own phase.

  * Phase 0 — authenticated chassis (/, /healthz, /v1/_authcheck).
  * Phase 1 — `/mcp/trigger/` mounted (this file). Also resolves the original
    "TRIGGER_SECRET_KEY not configured on the server" error: the key now lives
    in core-x/prd, which this service reads.

Two auth boundaries:
  * MCP mounts <- Anthropic's managed-agents platform: bearer = DMAAS_MCP_BEARER_TOKEN,
    injected by the agent's Anthropic vault (scoped by mcp_server_url). ASGI gate:
    ``src/mcp_bearer.py``.
  * agent-runs + pipeline <- platform-api BFF / Trigger.dev: bearer =
    EDGE_API_SERVICE_TOKEN, constant-time compared (``src/service_token.py``).

Run locally and on the deployed (public) service from the repo root:

    doppler run -p core-x -c prd -- python -m apps.edge_api.main

Secrets come from Doppler ``core-x/prd`` — the same config the hq-x service reads,
so edge_api operates on the identical ``business.*`` Postgres rows. Compute
relocation, not data migration.
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse

from .src import config
from .src.mcp.trigger import mcp as trigger_mcp
from .src.mcp_bearer import bearer_token_app
from .src.service_token import require_service_token

log = logging.getLogger("edge_api")


# ── MCP mounts (Anthropic-facing) ────────────────────────────────────────────
# Each FastMCP server is exposed as an ASGI sub-app and wrapped in the shared
# transport-bearer check (DMAAS_MCP_BEARER_TOKEN — the SAME value the agent's
# Anthropic vault injects, scoped by mcp_server_url; keeping the value identical
# across the move means the vault credential keeps authenticating once the mount
# URL is repointed). Captured at import so the lifespan can chain each inner app.
_mcp_bearer = os.environ.get("DMAAS_MCP_BEARER_TOKEN")
_trigger_mcp_inner = trigger_mcp.http_app(path="/")
_trigger_mcp_app = bearer_token_app(_trigger_mcp_inner, bearer_token=_mcp_bearer)


@asynccontextmanager
async def lifespan(app_: FastAPI):
    # Fail-loud-but-not-fatal warnings so a misconfigured deploy is obvious in
    # the logs rather than silently open.
    if config.service_token() is None:
        log.warning(
            "EDGE_API_SERVICE_TOKEN unset -- service-token routes are UNAUTHENTICATED "
            "(local dev only). Set it in core-x/prd for every deployed environment."
        )
    if _mcp_bearer is None:
        log.warning(
            "DMAAS_MCP_BEARER_TOKEN unset -- /mcp/* mounts are UNAUTHENTICATED "
            "(local dev only). Set it in core-x/prd for every deployed environment."
        )
    # Chain every mounted FastMCP sub-app's lifespan so its session manager
    # starts/stops with the parent app.
    async with _trigger_mcp_inner.lifespan(app_):
        log.info("edge_api: boot ok (mounts: trigger)")
        yield


app = FastAPI(title="edge_api", version="0.2.0", lifespan=lifespan)

# Mount the MCP servers. Managed agents authenticate via
# Authorization: Bearer <DMAAS_MCP_BEARER_TOKEN>; the wrapper rejects
# unauthorized requests at the ASGI boundary before FastMCP sees them.
# NOTE: register the agent's mcp_servers[].url + vault credential WITH the
# trailing slash (.../mcp/trigger/). Starlette mounts 307-redirect the slash-less
# form to an insecure URL the managed-agents platform blocks.
app.mount("/mcp/trigger", _trigger_mcp_app)  # Trigger.dev task control


def _info() -> dict:
    return {
        "service": "edge_api",
        "status": "ok",
        "phase": "1-trigger",
        "mounts": {
            "mcp": ["trigger"],   # Phase 2 adds "lob"
            "agent_runs": False,  # Phase 3
            "pipeline": False,    # Phase 4
        },
    }


@app.get("/")
def root() -> dict:
    return _info()


@app.get("/healthz")
def healthz() -> JSONResponse:
    """Liveness. Open (no token) for platform probes. 200 while the process is up;
    later phases extend this with a DB-pool reachability check."""
    return JSONResponse(_info(), status_code=200)


@app.get("/v1/_authcheck", dependencies=[Depends(require_service_token)])
def authcheck() -> dict:
    """Diagnostic: proves the EDGE_API_SERVICE_TOKEN gate end-to-end. 200 only with a
    valid Bearer (401 otherwise). No functionality — remove once real routes land."""
    return {"ok": True, "gate": "service_token"}


def main() -> None:
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host=config.host(), port=config.port())


if __name__ == "__main__":
    main()
