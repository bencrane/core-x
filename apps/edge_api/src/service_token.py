"""Service-token gate for the BFF / Trigger-facing surface (agent-runs + pipeline).

A FastAPI dependency that validates ``Authorization: Bearer <EDGE_API_SERVICE_TOKEN>``
in constant time. Mirrors catalyst_api's ``require_operator``: when the token is
unset (local dev) it allows; every deployed environment sets it, so enforcement is
live there. Distinct from the MCP transport bearer (``src/mcp_bearer.py``), which
gates the Anthropic-facing MCP mounts with ``DMAAS_MCP_BEARER_TOKEN``.
"""
from __future__ import annotations

import hmac

from fastapi import Header, HTTPException

from . import config


def require_service_token(authorization: str | None = Header(default=None)) -> None:
    """Constant-time ``Bearer`` check against ``EDGE_API_SERVICE_TOKEN``. Unset
    token (local dev) allows; production sets it, so enforcement is live."""
    expected = config.service_token()
    if expected is None:
        return
    provided = ""
    if authorization and authorization.lower().startswith("bearer "):
        provided = authorization[len("bearer ") :].strip()
    if not hmac.compare_digest(provided, expected):
        raise HTTPException(
            status_code=401,
            detail="unauthorized",
            headers={"WWW-Authenticate": "Bearer"},
        )
