"""business.ao_engagement_mandates writes + the opportunity read this pathway needs — psycopg async.

Standalone: its OWN single-opportunity read (the proposal pathway has none) and its OWN ledger upsert.
The caller owns commit/rollback (mirrors the cal/queries convention)."""
from __future__ import annotations

import secrets
from typing import Any

from psycopg.rows import dict_row


def _mint_id() -> str:
    return f"mand_{secrets.token_hex(12)}"


async def read_opportunity_for_doc(conn, opportunity_id: str) -> dict[str, Any] | None:
    """The values the agreement binds: company (Participant) + signer (name/title). Read-only."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT o.id::text AS opportunity_id,
                   acc.name    AS company_name,
                   c.first_name, c.last_name, c.title, c.email
              FROM business.opportunities o
              JOIN business.accounts  acc ON acc.id = o.account_id
         LEFT JOIN business.contacts  c   ON c.id   = o.contact_id
             WHERE o.id = %s::uuid
            """,
            (opportunity_id,),
        )
        return await cur.fetchone()


async def upsert_mandate(
    conn,
    *,
    opportunity_id: str,
    package_key: str,
    term_fee_cents: int,
    duration_months: int,
    slug: str,
    style: str,
    status: str,
    pdf_r2_key: str | None = None,
    pdf_url: str | None = None,
    pdf_bytes: int | None = None,
    field_values: dict[str, Any] | None = None,
    trigger_run_id: str | None = None,
    error: str | None = None,
    documenso_envelope_id: str | None = None,
    documenso_document_id: int | None = None,
    participant_signing_token: str | None = None,
    provider_signing_token: str | None = None,
) -> dict[str, Any]:
    """Idempotent upsert keyed on ``opportunity_id`` (one live mandate per opportunity). The id is
    minted on insert and PRESERVED on conflict. Does NOT commit. Returns the row."""
    from psycopg.types.json import Jsonb

    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            INSERT INTO business.ao_engagement_mandates AS m
                (id, opportunity_id, package_key, term_fee_cents, duration_months, document_slug,
                 style, status, pdf_r2_key, pdf_url, pdf_bytes, field_values, trigger_run_id, error,
                 documenso_envelope_id, documenso_document_id, participant_signing_token,
                 provider_signing_token)
            VALUES
                (%(id)s, %(opportunity_id)s::uuid, %(package_key)s, %(term_fee_cents)s, %(duration_months)s,
                 %(slug)s, %(style)s, %(status)s, %(pdf_r2_key)s, %(pdf_url)s, %(pdf_bytes)s,
                 %(field_values)s, %(trigger_run_id)s, %(error)s,
                 %(documenso_envelope_id)s, %(documenso_document_id)s, %(participant_signing_token)s,
                 %(provider_signing_token)s)
            ON CONFLICT (opportunity_id) DO UPDATE SET
                package_key     = EXCLUDED.package_key,
                term_fee_cents  = EXCLUDED.term_fee_cents,
                duration_months = EXCLUDED.duration_months,
                document_slug   = EXCLUDED.document_slug,
                style           = EXCLUDED.style,
                status          = EXCLUDED.status,
                pdf_r2_key      = COALESCE(EXCLUDED.pdf_r2_key, m.pdf_r2_key),
                pdf_url         = COALESCE(EXCLUDED.pdf_url, m.pdf_url),
                pdf_bytes       = COALESCE(EXCLUDED.pdf_bytes, m.pdf_bytes),
                field_values    = CASE WHEN EXCLUDED.field_values = '{}'::jsonb
                                       THEN m.field_values ELSE EXCLUDED.field_values END,
                trigger_run_id  = COALESCE(EXCLUDED.trigger_run_id, m.trigger_run_id),
                error           = EXCLUDED.error,
                documenso_envelope_id     = COALESCE(EXCLUDED.documenso_envelope_id, m.documenso_envelope_id),
                documenso_document_id     = COALESCE(EXCLUDED.documenso_document_id, m.documenso_document_id),
                participant_signing_token = COALESCE(EXCLUDED.participant_signing_token, m.participant_signing_token),
                provider_signing_token    = COALESCE(EXCLUDED.provider_signing_token, m.provider_signing_token),
                updated_at      = now()
            RETURNING id, opportunity_id::text AS opportunity_id, package_key, term_fee_cents,
                      duration_months, document_slug, style, status, pdf_r2_key, pdf_url, pdf_bytes,
                      trigger_run_id, error, documenso_envelope_id, documenso_document_id,
                      participant_signing_token, provider_signing_token
            """,
            {
                "id": _mint_id(),
                "opportunity_id": opportunity_id,
                "package_key": package_key,
                "term_fee_cents": term_fee_cents,
                "duration_months": duration_months,
                "slug": slug,
                "style": style,
                "status": status,
                "pdf_r2_key": pdf_r2_key,
                "pdf_url": pdf_url,
                "pdf_bytes": pdf_bytes,
                "field_values": Jsonb(field_values or {}),
                "trigger_run_id": trigger_run_id,
                "error": error,
                "documenso_envelope_id": documenso_envelope_id,
                "documenso_document_id": documenso_document_id,
                "participant_signing_token": participant_signing_token,
                "provider_signing_token": provider_signing_token,
            },
        )
        return await cur.fetchone()


async def get_by_opportunity(conn, opportunity_id: str) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT id, opportunity_id::text AS opportunity_id, package_key, term_fee_cents,
                   duration_months, document_slug, style, status, pdf_r2_key, pdf_bytes,
                   trigger_run_id, error, documenso_envelope_id, documenso_document_id,
                   participant_signing_token, provider_signing_token, created_at, updated_at
              FROM business.ao_engagement_mandates
             WHERE opportunity_id = %s::uuid
            """,
            (opportunity_id,),
        )
        return await cur.fetchone()


async def update_documenso_status_by_envelope(
    conn,
    *,
    envelope_id: str,
    documenso_status: str,
) -> dict[str, Any] | None:
    """UPDATE the documenso_status by envelope_id (webhook-driven). Does NOT commit."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE business.ao_engagement_mandates SET
                documenso_status = %(documenso_status)s,
                updated_at       = now()
            WHERE documenso_envelope_id = %(envelope_id)s
            RETURNING id, opportunity_id::text AS opportunity_id, package_key, term_fee_cents,
                      duration_months, document_slug, style, status, pdf_r2_key, pdf_bytes,
                      trigger_run_id, error, documenso_envelope_id, documenso_document_id,
                      participant_signing_token, provider_signing_token, documenso_status, created_at, updated_at
            """,
            {
                "envelope_id": envelope_id,
                "documenso_status": documenso_status,
            },
        )
        return await cur.fetchone()
