"""business.engagement_proposals persistence — psycopg async, append-then-advance.

Status only moves FORWARD along draft→sent→opened→signed→completed (idempotent against
duplicate webhook deliveries); rejected/voided are terminal side-exits. The row is the
authoritative record — never advanced by the client-side embed callback, only by the
Documenso webhook.
"""
from __future__ import annotations

import datetime as _dt
import secrets
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .models import Proposal, ProposalCreate

# Monotonic forward chain. A webhook for a status at/below the current rank is a no-op.
_RANK: dict[str, int] = {"draft": 0, "sent": 1, "opened": 2, "signed": 3, "completed": 4}
_STATUS_TS_COL: dict[str, str] = {
    "sent": "sent_at", "opened": "opened_at", "signed": "signed_at", "completed": "completed_at",
}
_TERMINAL = {"rejected", "voided"}

# COALESCE the pricing config so proposals minted before these columns existed read cleanly
# (duration/cadence/schedule default in-query; the Proposal model never sees a NULL).
_SELECT_COLS = (
    "ref, template_id, client_name, client_signer_name, client_title, client_email, "
    "effective_date, monthly_fee_cents, "
    "COALESCE(duration_months, 6) AS duration_months, "
    "COALESCE(billing_cadence, 'upfront_in_full') AS billing_cadence, "
    "COALESCE(success_fee_schedule, '[]'::jsonb) AS success_fee_schedule, "
    "quarterly_total_cents, rs_signer_name, status, "
    "documenso_envelope_id, documenso_client_token, signed_pdf_url, field_values, created_by, "
    "created_at, sent_at, opened_at, signed_at, completed_at"
)


def new_ref() -> str:
    """An unguessable capability token — the proposal's primary key, URL slug, and credential."""
    return "rs_" + secrets.token_urlsafe(16)


def _to_proposal(row: dict[str, Any]) -> Proposal:
    return Proposal(**row)


async def insert_proposal(
    conn,
    *,
    ref: str,
    body: ProposalCreate,
    template_id: str,
    effective_date: _dt.date,
    monthly_fee_cents: int,
    duration_months: int,
    billing_cadence: str,
    success_fee_schedule: list[dict[str, str]],
    quarterly_total_cents: int,
    rs_signer_name: str,
    field_values: dict[str, Any],
) -> Proposal:
    sql = f"""
        INSERT INTO business.engagement_proposals
            (ref, template_id, client_name, client_signer_name, client_title, client_email,
             effective_date, monthly_fee_cents, duration_months, billing_cadence,
             success_fee_schedule, quarterly_total_cents, rs_signer_name,
             status, field_values, created_by)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'draft',%s,%s)
        RETURNING {_SELECT_COLS}
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            sql,
            (
                ref, template_id, body.client_name, body.client_signer_name, body.client_title,
                body.client_email, effective_date, monthly_fee_cents, duration_months,
                billing_cadence, Jsonb(success_fee_schedule), quarterly_total_cents,
                rs_signer_name, Jsonb(field_values), body.created_by,
            ),
        )
        row = await cur.fetchone()
    await conn.commit()
    return _to_proposal(row)


async def get_by_ref(conn, ref: str) -> Proposal | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(f"SELECT {_SELECT_COLS} FROM business.engagement_proposals WHERE ref = %s", (ref,))
        row = await cur.fetchone()
    return _to_proposal(row) if row else None


async def get_by_envelope(conn, envelope_id: str) -> Proposal | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_SELECT_COLS} FROM business.engagement_proposals WHERE documenso_envelope_id = %s",
            (envelope_id,),
        )
        row = await cur.fetchone()
    return _to_proposal(row) if row else None


async def list_recent(conn, limit: int = 100) -> list[Proposal]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_SELECT_COLS} FROM business.engagement_proposals ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        rows = await cur.fetchall()
    return [_to_proposal(r) for r in rows]


async def attach_envelope(conn, ref: str, envelope_id: str, client_token: str | None) -> bool:
    """Bind the created Documenso envelope to the proposal and move draft → sent.

    Binds ONLY when no envelope is attached yet (``documenso_envelope_id IS NULL``), so a
    re-provision can never silently rebind a proposal that already has a (possibly signed)
    envelope. Returns whether a row was bound.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE business.engagement_proposals
               SET documenso_envelope_id = %s,
                   documenso_client_token = %s,
                   status = CASE WHEN status = 'draft' THEN 'sent' ELSE status END,
                   sent_at = COALESCE(sent_at, now()),
                   updated_at = now()
             WHERE ref = %s AND documenso_envelope_id IS NULL
            """,
            (envelope_id, client_token, ref),
        )
        bound = cur.rowcount == 1
    await conn.commit()
    return bound


# SQL rank expression mirroring _RANK — lets the UPDATE itself enforce forward-only motion.
_RANK_CASE = (
    "CASE status WHEN 'draft' THEN 0 WHEN 'sent' THEN 1 WHEN 'opened' THEN 2 "
    "WHEN 'signed' THEN 3 WHEN 'completed' THEN 4 ELSE -1 END"
)


async def advance_status(
    conn, *, envelope_id: str, status: str, signed_pdf_url: str | None = None,
) -> bool:
    """Apply a webhook-driven transition ATOMICALLY and idempotently.

    The monotonic/terminal guard lives IN the UPDATE's WHERE predicate, so concurrent or
    out-of-order Documenso deliveries cannot regress state — there is no read-then-write race.
    Forward-chain statuses apply only when strictly ahead of the current rank; terminal statuses
    (rejected/voided) apply unless already terminal or completed. Returns whether a row changed.
    """
    sets = ["status = %s", "updated_at = now()"]
    set_params: list[Any] = [status]
    ts_col = _STATUS_TS_COL.get(status)
    if ts_col:
        sets.append(f"{ts_col} = COALESCE({ts_col}, now())")
    if signed_pdf_url is not None:
        sets.append("signed_pdf_url = %s")
        set_params.append(signed_pdf_url)

    if status in _TERMINAL:
        guard, guard_params = "status NOT IN ('rejected','voided','completed')", []
    else:
        guard, guard_params = f"%s > {_RANK_CASE}", [_RANK.get(status, -1)]

    sql = (
        f"UPDATE business.engagement_proposals SET {', '.join(sets)} "
        f"WHERE documenso_envelope_id = %s AND {guard}"
    )
    async with conn.cursor() as cur:
        await cur.execute(sql, set_params + [envelope_id] + guard_params)
        changed = cur.rowcount == 1
    await conn.commit()
    return changed
