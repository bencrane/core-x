"""business.ao_engagement_mandates writes + the opportunity read this pathway needs — psycopg async.

Each row is one DEAL (one rendered document + its Documenso document); an opportunity may have MANY.
The deal is INSERTed at Stage (status='pending', the intent), then UPDATEd by id as it renders. The
caller owns commit/rollback (mirrors the cal/queries convention)."""
from __future__ import annotations

import secrets
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

# Columns returned to callers (the BFF/UI read these). Tokens stay NULL until a later "send" step.
_COLS = (
    "id, opportunity_id::text AS opportunity_id, package_key, term_fee_cents, duration_months, "
    "document_slug, style, status, pdf_r2_key, pdf_bytes, trigger_run_id, error, "
    "documenso_envelope_id, documenso_document_id, participant_signing_token, "
    "provider_signing_token, created_at, updated_at"
)


def _mint_id() -> str:
    return f"mand_{secrets.token_hex(12)}"


async def read_opportunity_for_doc(conn, opportunity_id: str) -> dict[str, Any] | None:
    """The values the agreement binds: company (Participant) + signer (name/title/email). Read-only."""
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


async def insert_mandate(
    conn,
    *,
    opportunity_id: str,
    package_key: str,
    term_fee_cents: int,
    duration_months: int,
    slug: str,
    style: str,
    status: str = "pending",
    trigger_run_id: str | None = None,
) -> dict[str, Any]:
    """INSERT a new deal row (the Stage intent). Mints the id. Does NOT commit. Returns the row."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            INSERT INTO business.ao_engagement_mandates
                (id, opportunity_id, package_key, term_fee_cents, duration_months, document_slug,
                 style, status, trigger_run_id)
            VALUES
                (%(id)s, %(opportunity_id)s::uuid, %(package_key)s, %(term_fee_cents)s,
                 %(duration_months)s, %(slug)s, %(style)s, %(status)s, %(trigger_run_id)s)
            RETURNING {_COLS}
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
                "trigger_run_id": trigger_run_id,
            },
        )
        return await cur.fetchone()


async def update_mandate(
    conn,
    *,
    mandate_id: str,
    status: str,
    pdf_r2_key: str | None = None,
    pdf_url: str | None = None,
    pdf_bytes: int | None = None,
    field_values: dict[str, Any] | None = None,
    documenso_envelope_id: str | None = None,
    documenso_document_id: int | None = None,
    error: str | None = None,
    trigger_run_id: str | None = None,
) -> dict[str, Any] | None:
    """UPDATE a deal BY ID as it renders. Artifact/Documenso fields are COALESCE'd (a later partial
    update never wipes them); ``status`` + ``error`` are set outright. Does NOT commit."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            UPDATE business.ao_engagement_mandates SET
                status                = %(status)s,
                pdf_r2_key            = COALESCE(%(pdf_r2_key)s, pdf_r2_key),
                pdf_url               = COALESCE(%(pdf_url)s, pdf_url),
                pdf_bytes             = COALESCE(%(pdf_bytes)s, pdf_bytes),
                field_values          = COALESCE(%(field_values)s, field_values),
                documenso_envelope_id = COALESCE(%(documenso_envelope_id)s, documenso_envelope_id),
                documenso_document_id = COALESCE(%(documenso_document_id)s, documenso_document_id),
                trigger_run_id        = COALESCE(%(trigger_run_id)s, trigger_run_id),
                error                 = %(error)s,
                updated_at            = now()
            WHERE id = %(id)s
            RETURNING {_COLS}
            """,
            {
                "id": mandate_id,
                "status": status,
                "pdf_r2_key": pdf_r2_key,
                "pdf_url": pdf_url,
                "pdf_bytes": pdf_bytes,
                "field_values": Jsonb(field_values) if field_values is not None else None,
                "documenso_envelope_id": documenso_envelope_id,
                "documenso_document_id": documenso_document_id,
                "trigger_run_id": trigger_run_id,
                "error": error,
            },
        )
        return await cur.fetchone()


async def get_by_id(conn, mandate_id: str) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLS} FROM business.ao_engagement_mandates WHERE id = %s", (mandate_id,)
        )
        return await cur.fetchone()


async def get_latest_by_opportunity(conn, opportunity_id: str) -> dict[str, Any] | None:
    """The most recent deal for an opportunity (the UI's per-row status source)."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLS} FROM business.ao_engagement_mandates "
            "WHERE opportunity_id = %s::uuid ORDER BY created_at DESC LIMIT 1",
            (opportunity_id,),
        )
        return await cur.fetchone()
