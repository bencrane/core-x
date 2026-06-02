"""Push-ingestion webhook — Clay company-industry payloads → HQX Postgres.

A single proxy-authed POST endpoint that inserts one payload row into
``public.company_industry_payloads`` on the HQX Supabase. Built because Clay pushes
to an HTTP endpoint (it cannot write Postgres directly) and we want ``raw_payload``
delivered as a NATIVE JSON OBJECT — which this accepts and stores verbatim as jsonb.

Unlike the rest of the fleet (spawned by the Universal Dispatcher), this is an
EXTERNAL-push receiver, so it exposes its own endpoint. It is proxy-authenticated
(``requires_proxy_auth=True``) — callers send ``Modal-Key`` / ``Modal-Secret`` headers,
exactly like the dispatcher. No new secret: it reuses ``hqx-postgres`` for the DB.

    POST  https://<workspace>--clay-industries.modal.run
    Headers: Modal-Key: <key>   Modal-Secret: <secret>   Content-Type: application/json
    Body:    {"normalized_domain": "acme.com",
              "source_platform": "clay-equipment-finance",
              "raw_payload": { ...the industries object... }}
    →  201 {"ok": true, "id": <bigint>, "normalized_domain": "acme.com"}

    modal deploy pipelines/gtm/clay_industries_endpoint.py
"""

from __future__ import annotations

import os

import modal
from pydantic import BaseModel, field_validator

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "fastapi[standard]>=0.115",
    "pydantic>=2",
    "psycopg[binary]>=3.2",
)
app = modal.App("gtm-clay-ingest", image=image)

TARGET_TABLE = "public.company_industry_payloads"


class IndustryPayload(BaseModel):
    normalized_domain: str
    source_platform: str = "clay-equipment-finance"
    raw_payload: dict  # native JSON object → stored as jsonb

    @field_validator("raw_payload", mode="before")
    @classmethod
    def _coerce(cls, v):
        # The caller should send a JSON object (the norm). Defensively also accept a
        # JSON string and parse it, so serialization quirks can never block ingestion.
        if isinstance(v, str):
            import json
            return json.loads(v)
        return v


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60)
@modal.fastapi_endpoint(method="POST", requires_proxy_auth=True, label="clay-industries")
def ingest(payload: IndustryPayload):
    """Insert one row into HQX ``public.company_industry_payloads``; return the new id.

    ``raw_payload`` is bound straight into a jsonb column via psycopg's ``Jsonb`` —
    no stringification, the object is stored as-is."""
    import psycopg
    from fastapi.responses import JSONResponse
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        return JSONResponse(status_code=500, content={"ok": False, "error": "HQX_DB_URL_POOLED not set in hqx-postgres secret"})

    dom = (payload.normalized_domain or "").strip()
    if not dom:
        return JSONResponse(status_code=422, content={"ok": False, "error": "normalized_domain is required"})

    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {TARGET_TABLE} (normalized_domain, source_platform, raw_payload) "
                "VALUES (%s, %s, %s) RETURNING id",
                (dom, (payload.source_platform or "").strip() or "clay-equipment-finance",
                 Jsonb(payload.raw_payload)),
            )
            new_id = cur.fetchone()[0]
            conn.commit()
        return JSONResponse(status_code=201, content={"ok": True, "id": new_id, "normalized_domain": dom})
    except Exception as exc:  # noqa: BLE001 — surface the DB error to the caller
        return JSONResponse(status_code=500, content={"ok": False, "error": str(exc)})
