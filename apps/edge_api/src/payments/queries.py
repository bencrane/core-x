"""business.engagement_payments persistence + the append-only engagement_events ledger.

Payment state lives on the ``engagement_proposals`` row (added by ``sql/engagement_payments.sql``)
and is ISOLATED from the proposals/signing read path: these functions select only the payment
columns, so if the payment migration has not been applied the signing flow is unaffected (the
payment routes fail loudly instead). ``payment_status`` advances monotonically and only via the
Stripe webhook — never by the browser confirm result.
"""
from __future__ import annotations

import datetime as _dt
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .models import PAYMENT_RANK

_PAY_COLS = (
    "stripe_customer_id, stripe_payment_intent_id, payment_status, amount_charged_cents, "
    "payment_currency, payment_initiated_at, paid_at"
)


async def get_payment_by_ref(conn, ref: str) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_PAY_COLS} FROM business.engagement_proposals WHERE ref = %s", (ref,)
        )
        return await cur.fetchone()


async def get_status_by_ref(conn, ref: str) -> str:
    """Defensive single-value read for the public proposal projection — returns 'none' when the row
    or the payment columns are absent. The caller owns rollback on its own connection."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT payment_status FROM business.engagement_proposals WHERE ref = %s", (ref,)
        )
        row = await cur.fetchone()
    return row[0] if row and row[0] else "none"


async def ref_for_intent(conn, intent_id: str) -> str | None:
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT ref FROM business.engagement_proposals WHERE stripe_payment_intent_id = %s",
            (intent_id,),
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def attach_payment_intent(
    conn, ref: str, *, customer_id: str, intent_id: str, amount_cents: int, currency: str, status: str,
) -> None:
    """Bind a freshly created PaymentIntent to the proposal. Never overwrites a row that already
    reached 'succeeded' (idempotent guard against a double-create race). COMMITS."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE business.engagement_proposals
               SET stripe_customer_id       = COALESCE(stripe_customer_id, %s),
                   stripe_payment_intent_id = %s,
                   amount_charged_cents     = %s,
                   payment_currency         = %s,
                   payment_status           = %s,
                   payment_initiated_at     = COALESCE(payment_initiated_at, now()),
                   updated_at               = now()
             WHERE ref = %s AND payment_status <> 'succeeded'
            """,
            (customer_id, intent_id, amount_cents, currency, status, ref),
        )
    await conn.commit()


# SQL rank mirroring PAYMENT_RANK so the UPDATE itself enforces forward-only motion.
_PAY_RANK_CASE = (
    "CASE payment_status WHEN 'none' THEN 0 WHEN 'requires_payment' THEN 1 "
    "WHEN 'processing' THEN 2 WHEN 'succeeded' THEN 3 ELSE 0 END"
)


async def advance_payment_status(
    conn, *, intent_id: str, status: str, paid_at: _dt.datetime | None = None,
    amount_cents: int | None = None,
) -> bool:
    """Apply a webhook-driven payment transition by PaymentIntent id, atomically + idempotently.

    'succeeded' is terminal-highest: once set, neither 'processing' nor a later 'failed' regress it.
    'failed'/'canceled' apply only when not already succeeded. Forward states apply only when
    strictly ahead of the current rank. Returns whether a row changed. COMMITS.
    """
    sets = ["payment_status = %s", "updated_at = now()"]
    params: list[Any] = [status]
    if status == "succeeded":
        sets.append("paid_at = COALESCE(%s, now())")
        params.append(paid_at)
    if amount_cents is not None:
        sets.append("amount_charged_cents = %s")
        params.append(amount_cents)

    if status in ("failed", "canceled"):
        guard = "payment_status <> 'succeeded'"
        guard_params: list[Any] = []
    else:
        guard = f"%s > {_PAY_RANK_CASE}"
        guard_params = [PAYMENT_RANK.get(status, 0)]

    sql = (
        f"UPDATE business.engagement_proposals SET {', '.join(sets)} "
        f"WHERE stripe_payment_intent_id = %s AND {guard}"
    )
    async with conn.cursor() as cur:
        await cur.execute(sql, params + [intent_id] + guard_params)
        changed = cur.rowcount == 1
    await conn.commit()
    return changed


async def insert_event(
    conn, *, ref: str, source: str, event_type: str, idempotency_key: str | None,
    payload: dict[str, Any],
) -> bool:
    """Append one immutable lifecycle event. Idempotent on (source, idempotency_key): a redelivered
    webhook is a no-op. Returns True if this is the first time the event was seen. COMMITS."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO business.engagement_events (ref, source, event_type, idempotency_key, payload)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
            RETURNING id
            """,
            (ref, source, event_type, idempotency_key, Jsonb(payload)),
        )
        inserted = await cur.fetchone() is not None
    await conn.commit()
    return inserted
