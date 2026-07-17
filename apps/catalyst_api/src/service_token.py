"""Service-token gate for the market/query routers relocated from edge_api.

Same dependency name the routers import (``require_service_token``) so the
router files carry over verbatim — but it validates against catalyst_api's
existing operator bearer (``CATALYST_API_TOKEN``), the single auth boundary of
this service. Constant-time compare; unset token (local dev) allows, every
deployed env sets it (fail-closed at boot via ``config.auth_required``).
"""
from __future__ import annotations

import hmac

from fastapi import Header, HTTPException

from . import config


def require_service_token(authorization: str | None = Header(default=None)) -> None:
    expected = config.operator_token()
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
