"""business.documenso_documents — the per-document SoR sourced from the Documenso envelope PAYLOAD.

Two write paths, mirroring the two moments we receive a payload for a document:
  • ``stamp(...)``            at Confirm & Originate (instantiation) — INSERT the row from the live
                              envelope payload, resolving the variant + amount FROM the template.
  • ``sync_from_webhook(...)`` on each Documenso webhook — advance status, append the event, and
                              (when a fresh payload is supplied) refresh recipients/fields/raw.

Reads serve the payment page (``get_amount_by_opportunity_and_document_id``, pair-gated) and the
opportunity view (``get_by_opportunity``). Signed-PDF capture uses ``get_by_match`` (probe) +
``set_signed_pdf`` (single-row write). The caller owns commit/rollback.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

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


async def get_by_match(conn, match: str) -> dict[str, Any] | None:
    """Resolve the CANONICAL document row from a webhook handle — matched by the stamped ``external_id``
    (the draft id) OR the prefixed ``envelope_id``. Returns the row's own ``envelope_id`` (the unique,
    prefixed handle) so callers download/store/write by the canonical key regardless of the id SHAPE the
    webhook carried (Documenso may send a numeric document id, which never matches the prefixed column).
    Most-recent row wins (a re-confirm reuses ``external_id`` across envelopes). ``None`` when no match.
    Used as the signed-PDF capture probe (skip when ``signed_pdf_url`` is already set)."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT envelope_id, document_id, opportunity_id::text AS opportunity_id,
                   status, signed_pdf_url, signed_pdf_r2_key
              FROM business.documenso_documents
             WHERE external_id = %(match)s OR envelope_id = %(match)s
             ORDER BY created_at DESC
             LIMIT 1
            """,
            {"match": match},
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

    Updates ``status`` and appends the raw ``event`` to the audit trail; when a fresh envelope
    ``payload`` is supplied (a successful GET /envelope re-read) it also refreshes recipients/fields/
    raw from the authoritative source. Returns the row, or ``None`` when no document matches (the event
    belongs to another lane — caller treats it as ignored). The sealed-PDF write is intentionally NOT
    here — it goes through ``set_signed_pdf`` (a single-row write by the unique envelope id) so the
    capture never rides this ``external_id``-or-``envelope_id`` UPDATE, which can span >1 row when a
    re-confirm reuses the draft's external id."""
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


async def set_signed_pdf(
    conn, *, envelope_id: str, signed_pdf_url: str, signed_pdf_r2_key: str
) -> None:
    """Persist the captured sealed-PDF pointers on a SINGLE row, keyed by the unique ``envelope_id``.
    Separate from ``sync_from_webhook`` so the capture write can never span multiple rows (a re-confirm
    reuses the draft's ``external_id`` across envelopes)."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE business.documenso_documents
               SET signed_pdf_url = %(url)s, signed_pdf_r2_key = %(key)s, updated_at = now()
             WHERE envelope_id = %(envelope_id)s
            """,
            {"url": signed_pdf_url, "key": signed_pdf_r2_key, "envelope_id": envelope_id},
        )


async def get_amount_by_opportunity_and_document_id(
    conn, opportunity_id: str, document_id: int
) -> dict[str, Any] | None:
    """The BFF payment page's amount source — keyed by the (opportunity_id, document_id) PAIR.

    The pair IS the access capability: the document_id is sequential/enumerable, so a public read
    keyed by id alone would leak every charge amount; requiring BOTH columns to match the SAME row
    gates the read (a wrong opportunity uuid + a guessed id = no row). Returns ``None`` when the pair
    does not match a row (404 at the route), or when ``opportunity_id`` is not a well-formed UUID — the
    guard mirrors the other reads so a garbage handle returns a clean 404, never a 500 that also aborts
    the pooled transaction on the ``::uuid`` cast."""
    try:
        UUID(str(opportunity_id))
    except (ValueError, TypeError, AttributeError):
        return None
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT envelope_id, document_id, opportunity_id::text AS opportunity_id,
                   amount_cents, currency, status
              FROM business.documenso_documents
             WHERE document_id = %(document_id)s
               AND opportunity_id = %(opportunity_id)s::uuid
            """,
            {"document_id": document_id, "opportunity_id": opportunity_id},
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
