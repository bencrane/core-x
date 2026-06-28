"""business.documenso_documents — the per-document SoR sourced from the Documenso envelope PAYLOAD.

Two write paths, mirroring the two moments we receive a payload for a document:
  • ``stamp(...)``            at Confirm & Originate (instantiation) — INSERT the row from the live
                              envelope payload, resolving the variant + amount FROM the template.
  • ``sync_from_webhook(...)`` on each Documenso webhook — advance status, append the event, and
                              (when a fresh payload is supplied) refresh recipients/fields/raw.

Reads serve the payment page (``get_amount_by_document_id``) and the opportunity view
(``get_by_opportunity``). The caller owns commit/rollback.
"""
from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


async def stamp(
    conn,
    *,
    envelope_id: str,
    document_id: int | None,
    external_id: str | None,
    opportunity_id: str,
    documenso_template_id: str,
    payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """INSERT (or refresh) the document row from the instantiated envelope payload.

    The amount + variant are resolved FROM the template (``documenso_templates`` ->
    ``global_input_content_variants``), so ``amount_cents`` is the variant's ``system_fee_cents``,
    FROZEN here at stamp time — the printed value and the charged value are the same row. Idempotent on
    ``envelope_id`` (re-confirm mints a NEW envelope → a new row). Returns the row, or ``None`` when the
    template id is unknown (no row inserted — INSERT...SELECT finds no template)."""
    env = payload or {}
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO business.documenso_documents AS d
                (envelope_id, document_id, external_id, opportunity_id, documenso_template_id,
                 global_input_content_variant_id, amount_cents, recipients, fields, raw, status)
            SELECT %(envelope_id)s, %(document_id)s, %(external_id)s, %(opportunity_id)s::uuid,
                   %(template_id)s, dt.global_input_content_variant_id,
                   (v.params->>'system_fee_cents')::int,
                   %(recipients)s, %(fields)s, %(raw)s, %(status)s
              FROM business.documenso_templates dt
         LEFT JOIN business.global_input_content_variants v
                ON v.id = dt.global_input_content_variant_id
             WHERE dt.documenso_template_id = %(template_id)s
            ON CONFLICT (envelope_id) DO UPDATE SET
                document_id                     = EXCLUDED.document_id,
                external_id                     = COALESCE(EXCLUDED.external_id, d.external_id),
                recipients                      = EXCLUDED.recipients,
                fields                          = EXCLUDED.fields,
                raw                             = EXCLUDED.raw,
                status                          = COALESCE(EXCLUDED.status, d.status),
                global_input_content_variant_id = EXCLUDED.global_input_content_variant_id,
                amount_cents                    = EXCLUDED.amount_cents,
                updated_at                      = now()
            RETURNING envelope_id, document_id, opportunity_id::text AS opportunity_id,
                      documenso_template_id,
                      global_input_content_variant_id::text AS global_input_content_variant_id,
                      amount_cents, currency, status
            """,
            {
                "envelope_id": envelope_id,
                "document_id": document_id,
                "external_id": external_id,
                "opportunity_id": opportunity_id,
                "template_id": documenso_template_id,
                "recipients": Jsonb(env.get("recipients") or []),
                "fields": Jsonb(env.get("fields") or []),
                "raw": Jsonb(env),
                "status": env.get("status"),
            },
        )
        return await cur.fetchone()


async def sync_from_webhook(
    conn,
    *,
    match: str,
    status: str | None,
    event: dict[str, Any] | None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Advance a document on a Documenso webhook, matched by ``external_id`` OR ``envelope_id``.

    Always updates ``status`` and appends the raw ``event`` to the audit trail; when a fresh envelope
    ``payload`` is supplied (a successful GET /envelope re-read) it also refreshes recipients/fields/
    raw from the authoritative source. Returns the row, or ``None`` when no document matches (the event
    belongs to another lane — caller treats it as ignored)."""
    sets = [
        "status = COALESCE(%(status)s, status)",
        "events = events || %(event)s",
        "updated_at = now()",
    ]
    params: dict[str, Any] = {
        "match": match,
        "status": status,
        "event": Jsonb([event] if event else []),
    }
    if payload is not None:
        sets += ["recipients = %(recipients)s", "fields = %(fields)s", "raw = %(raw)s"]
        params["recipients"] = Jsonb(payload.get("recipients") or [])
        params["fields"] = Jsonb(payload.get("fields") or [])
        params["raw"] = Jsonb(payload)

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            UPDATE business.documenso_documents SET
                {", ".join(sets)}
            WHERE external_id = %(match)s OR envelope_id = %(match)s
            RETURNING envelope_id, opportunity_id::text AS opportunity_id, status
            """,
            params,
        )
        return await cur.fetchone()


async def get_amount_by_document_id(conn, document_id: int) -> dict[str, Any] | None:
    """The payment page's amount source: the variant price frozen on the document, keyed by the numeric
    Documenso document id. Returns ``None`` when no document matches."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT envelope_id, document_id, opportunity_id::text AS opportunity_id,
                   amount_cents, currency, status
              FROM business.documenso_documents
             WHERE document_id = %s
            """,
            (document_id,),
        )
        return await cur.fetchone()


async def get_by_opportunity(conn, opportunity_id: str) -> list[dict[str, Any]]:
    """All documents instantiated for an opportunity, newest first (the opportunity document view)."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT envelope_id, document_id, external_id, documenso_template_id,
                   global_input_content_variant_id::text AS global_input_content_variant_id,
                   amount_cents, currency, status, recipients, created_at, updated_at
              FROM business.documenso_documents
             WHERE opportunity_id = %s::uuid
             ORDER BY created_at DESC
            """,
            (opportunity_id,),
        )
        return await cur.fetchall()
