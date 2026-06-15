"""Trigger-secret gate for the ``/internal/*`` surface (Trigger.dev → edge_api).

A FastAPI dependency validating ``Authorization: Bearer <TRIGGER_SHARED_SECRET>``
in constant time — the same secret the Trigger.dev tasks present via the
``callHqx`` helper (src/trigger/lib/hqx-client.ts). Mirrors ``require_service_token``:
unset secret (local dev) allows; every deployed environment sets it, so enforcement
is live there. Distinct from ``require_service_token`` (EDGE_API_SERVICE_TOKEN, the
BFF-facing ``/api/v1`` surface) — the two rotate independently.
"""
from __future__ import annotations

import hmac

from fastapi import Header, HTTPException

from . import config


def require_trigger_secret(authorization: str | None = Header(default=None)) -> None:
    """Constant-time ``Bearer`` check against ``TRIGGER_SHARED_SECRET``. Unset
    token (local dev) allows; production sets it, so enforcement is live."""
    expected = config.trigger_shared_secret()
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
