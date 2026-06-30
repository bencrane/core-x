"""Close.com REST read client — the booking path's contact/activity read-back.

Used by ``/internal/cal/book`` to resolve a completed custom activity (its field values) and the
prospect contact (attendee name/email) when minting a cal.com booking. READ-ONLY: GET only, never
writes back to Close. Auth is HTTP Basic with the API key as the username and an empty password
(developer.close.com/api/overview/api-key-authentication). Distinct from ``close_webhook_secret()``
(the INBOUND signature gate) — this is the OUTBOUND API credential (``CLOSE_API_KEY``).
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

from .. import config

logger = logging.getLogger(__name__)

_TIMEOUT_SEC = 20.0


class CloseApiError(Exception):
    """A Close REST call returned a non-2xx, or the API key is unconfigured."""

    def __init__(self, message: str, *, status_code: int | None = None, body: Any = None):
        self.status_code = status_code
        self.body = body
        super().__init__(message)


def _auth() -> httpx.BasicAuth:
    key = config.close_api_key()
    if not key:
        raise CloseApiError("CLOSE_API_KEY not configured")
    return httpx.BasicAuth(key, "")


async def _get(path: str) -> dict[str, Any]:
    url = f"{config.close_api_base()}{path}"
    async with httpx.AsyncClient(timeout=_TIMEOUT_SEC) as client:
        r = await client.get(url, auth=_auth())
    if r.status_code >= 400:
        try:
            err: Any = r.json()
        except Exception:  # noqa: BLE001
            err = r.text
        raise CloseApiError(
            f"Close GET {path} -> {r.status_code}", status_code=r.status_code, body=err
        )
    return r.json() if r.content else {}


async def get_custom_activity(activity_id: str) -> dict[str, Any]:
    """Fetch a custom activity by id — the authoritative field values (``custom.*`` keys)."""
    return await _get(f"/activity/custom/{activity_id}/")


async def get_contact(contact_id: str) -> dict[str, Any]:
    return await _get(f"/contact/{contact_id}/")


async def get_lead(lead_id: str) -> dict[str, Any]:
    return await _get(f"/lead/{lead_id}/")


def primary_email(contact: dict[str, Any]) -> str | None:
    """First email on a Close contact, or None."""
    emails = contact.get("emails") if isinstance(contact, dict) else None
    if isinstance(emails, list):
        for e in emails:
            if isinstance(e, dict) and e.get("email"):
                return str(e["email"])
    return None
