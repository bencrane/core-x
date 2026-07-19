"""cal.com booking → LeadMagic company firmographics (direct, edge-side, best-effort).

The THIRD data lane on a new booking (siblings: deep research + Parallel enrichment).
One ``POST /company-search`` to LeadMagic for the prospect's domain — the paragraph
description, employee range, and headline firmographics that flesh out the profile page.
The profile transform reads three sources: research report + enrichment columns + this.

COMPANY-GRAIN, fetch-once: rows key on ``domain`` in ``corex.company_leadmagic``; an
existing row for the domain skips the call entirely (operator ruling 2026-07-19 — repeat
bookings never re-fetch or overwrite). The response payload lands VERBATIM (jsonb) — the
raw is the source of truth; nothing the vendor returns is discarded.

Best-effort throughout: no key, no domain, HTTP failure, or timeout → logged, webhook
unaffected. Not-found still lands a row (status='not_found') so we don't re-ask on every
booking for a firm LeadMagic doesn't know.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

LM_URL = os.environ.get("LEADMAGIC_COMPANY_URL", "https://api.leadmagic.io/company-search")
HTTP_TIMEOUT_S = 8.0

DDL = """
CREATE TABLE IF NOT EXISTS corex.company_leadmagic (
    domain      text PRIMARY KEY,
    payload     jsonb,
    status      text NOT NULL,
    http_status integer,
    fetched_at  timestamptz NOT NULL DEFAULT now()
);
"""


async def fetch_and_store(conn, *, domain: str | None, company_name: str | None) -> dict[str, Any]:
    """Fetch LeadMagic company firmographics for ``domain`` and upsert into
    ``corex.company_leadmagic``. Skips when a row already exists (company-grain, fetch-once).
    Never raises; the caller owns commit."""
    key = os.environ.get("LEADMAGIC_API_KEY")
    if not key:
        return {"fetched": False, "reason": "LEADMAGIC_API_KEY unset"}
    if not domain:
        return {"fetched": False, "reason": "no domain"}
    domain = domain.strip().lower()

    async with conn.cursor() as cur:
        await cur.execute("SELECT status FROM corex.company_leadmagic WHERE domain = %s", (domain,))
        row = await cur.fetchone()
    if row:
        return {"fetched": False, "reason": "exists", "status": row[0]}

    status = "error"
    http_status: int | None = None
    payload_json: str | None = None
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
            resp = await client.post(
                LM_URL,
                headers={"X-API-Key": key, "Content-Type": "application/json"},
                json={"company_domain": domain},
            )
        http_status = resp.status_code
        if resp.status_code == 200:
            payload_json = resp.text
            # LeadMagic returns 200 + {message} for no-hits — 'found' requires identity fields.
            try:
                body = resp.json()
                has_identity = bool(body.get("companyName") or body.get("description"))
            except Exception:  # noqa: BLE001
                has_identity = False
            status = "found" if has_identity else "not_found"
        elif resp.status_code == 404:
            payload_json = resp.text
            status = "not_found"
        else:
            payload_json = resp.text[:2000]
            status = "error"
            logger.warning("leadmagic %s for %s: %s", resp.status_code, domain, resp.text[:200])
    except Exception as exc:  # noqa: BLE001 — best-effort lane; the webhook must not care
        logger.warning("leadmagic fetch failed for %s: %s", domain, exc)
        return {"fetched": False, "reason": f"exception: {str(exc)[:200]}"}

    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO corex.company_leadmagic (domain, payload, status, http_status)
                VALUES (%s, %s::jsonb, %s, %s)
                ON CONFLICT (domain) DO UPDATE
                  SET payload = EXCLUDED.payload, status = EXCLUDED.status,
                      http_status = EXCLUDED.http_status, fetched_at = now()
                """,
                (domain, payload_json, status, http_status),
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("leadmagic upsert failed for %s: %s", domain, exc)
        return {"fetched": True, "stored": False, "status": status}
    return {"fetched": True, "stored": True, "status": status}
