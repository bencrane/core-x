"""business.document_payments reads/writes + the inputs the mint needs — psycopg async.

Keyed by the Documenso ``document_id`` (the unique pin); ``opportunity_id`` (8-char handle) carries
the pair. The fee + customer are resolved by joining the opportunity ON ITS 8-CHAR HANDLE
(``opportunities.opportunity_id``), so the route works off the link's pair without the row UUID.
"""
from __future__ import annotations

import json
from typing import Any


async def get_payment(conn, document_id: str) -> dict | None:
    """The document payment record for a document, or None if no intent has been minted yet."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT document_id, opportunity_id, amount_cents, currency,
                   stripe_customer_id, stripe_payment_intent_id, payment_status, paid_at, rail
              FROM business.document_payments
             WHERE document_id = %s
            """,
            (document_id,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return {
        "document_id": row[0],
        "opportunity_id": row[1],
        "amount_cents": row[2],
        "currency": row[3],
        "stripe_customer_id": row[4],
        "stripe_payment_intent_id": row[5],
        "payment_status": row[6],
        "paid_at": row[7],
        "rail": row[8],
    }


async def get_fee_and_contact(conn, opportunity_id: str) -> dict | None:
    """Resolve, by the 8-char opportunity handle: the per-deal ``field_values`` (carries ``fee_amount``)
    and the contact (email/name) used as the Stripe customer. None when the handle is unknown."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT osc.field_values,
                   c.email,
                   NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)), '')
              FROM business.opportunities o
              JOIN business.opportunity_specific_content osc ON osc.opportunity_id = o.id
              LEFT JOIN business.contacts c ON c.id = o.contact_id
             WHERE o.opportunity_id = %s
             LIMIT 1
            """,
            (opportunity_id,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return {"field_values": row[0] or {}, "recipient_email": row[1], "recipient_name": row[2]}


async def get_agreement_fee_and_contact(conn, agreement_handle: str) -> dict | None:
    """Resolve, by the public ``agreement_handle``: the agreement's raw ``field_values`` (operator
    overrides, keyed by template field LABEL), the bound template's prefill-config ``field_settings``
    (label → defaults — the other half of the generate-time merge), and the signatory contact. The
    caller merges via ``deals.originate.resolve_field_values`` so the resolved fee equals what generate
    stamped (and locked) into the signed document. None when the handle is unknown."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT a.field_values,
                   COALESCE(pfc.field_settings, '{}'::jsonb),
                   c.email,
                   NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)), '')
              FROM business.agreements a
              LEFT JOIN business.documenso_template_document_prefill_configs pfc
                     ON pfc.template_documenso_id::text = a.template_documenso_id
              LEFT JOIN business.contacts c ON c.id = a.signatory_contact_id
             WHERE a.agreement_handle = %s
             LIMIT 1
            """,
            (agreement_handle,),
        )
        row = await cur.fetchone()
    if not row:
        return None
    return {
        "field_values": row[0] or {},
        "field_settings": row[1] or {},
        "recipient_email": row[2],
        "recipient_name": row[3],
    }


async def get_stripe_mode_selection(conn) -> str | None:
    """The operator's selected Stripe mode ('test'|'live') from operator_settings, or None to fall
    back to the STRIPE_MODE env. SINGLE-OPERATOR PLATFORM: Stripe mode is global (one Stripe account)
    and the prospect-facing mint has no operator session + no opportunity→operator link, so the (one)
    operator_settings row carries the platform-wide selection. Latest non-null row wins."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT stripe_mode
              FROM public.operator_settings
             WHERE stripe_mode IS NOT NULL
             ORDER BY updated_at DESC
             LIMIT 1
            """
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def upsert_intent(
    conn,
    *,
    document_id: str,
    opportunity_id: str,
    amount_cents: int,
    currency: str,
    customer_id: str,
    intent_id: str,
    status: str,
) -> None:
    """Persist the minted intent on the pair's payment record (idempotent upsert by document_id).
    COMMITS. A terminal ``succeeded`` status is NEVER downgraded by a re-mint (defense-in-depth — the
    webhook is the only writer of ``succeeded``/``paid_at``)."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO business.document_payments
                 (document_id, opportunity_id, amount_cents, currency,
                  stripe_customer_id, stripe_payment_intent_id, payment_status)
            VALUES (%(document_id)s, %(opportunity_id)s, %(amount_cents)s, %(currency)s,
                    %(customer_id)s, %(intent_id)s, %(status)s)
            ON CONFLICT (document_id) DO UPDATE
               SET opportunity_id           = EXCLUDED.opportunity_id,
                   amount_cents             = EXCLUDED.amount_cents,
                   currency                 = EXCLUDED.currency,
                   stripe_customer_id       = EXCLUDED.stripe_customer_id,
                   stripe_payment_intent_id = EXCLUDED.stripe_payment_intent_id,
                   payment_status           = CASE
                       WHEN business.document_payments.payment_status = 'succeeded'
                       THEN business.document_payments.payment_status
                       ELSE EXCLUDED.payment_status END,
                   updated_at               = now()
            """,
            {
                "document_id": document_id,
                "opportunity_id": opportunity_id,
                "amount_cents": amount_cents,
                "currency": currency,
                "customer_id": customer_id,
                "intent_id": intent_id,
                "status": status,
            },
        )
    await conn.commit()


async def record_event_if_new(
    conn,
    *,
    stripe_event_id: str,
    event_type: str | None,
    document_id: str | None,
    opportunity_id: str | None,
    payload: Any,
) -> bool:
    """Append the raw Stripe event to the audit ledger. Returns False if it was already recorded
    (UNIQUE ``stripe_event_id`` → ON CONFLICT DO NOTHING) — the webhook idempotency guard. Does NOT
    commit; the caller commits together with the status advance."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO business.document_payment_events
                 (stripe_event_id, event_type, document_id, opportunity_id, payload)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (stripe_event_id) DO NOTHING
            RETURNING id
            """,
            (stripe_event_id, event_type, document_id, opportunity_id, json.dumps(payload)),
        )
        row = await cur.fetchone()
    return row is not None


# SQL rank mirroring the document payment_status lifecycle so the UPDATE itself enforces forward-only
# motion (parity with payments.advance_payment_status). Distinct out-of-order deliveries — e.g. a
# delayed ``processing`` arriving after ``succeeded`` — cannot regress the row.
_DOC_PAY_RANK_CASE = (
    "CASE payment_status WHEN 'none' THEN 0 WHEN 'requires_payment' THEN 1 "
    "WHEN 'processing' THEN 2 WHEN 'succeeded' THEN 3 ELSE 0 END"
)
_DOC_PAY_RANK = {"none": 0, "requires_payment": 1, "processing": 2, "succeeded": 3}


async def advance_status(
    conn,
    *,
    document_id: str,
    status: str,
    paid: bool,
    intent_id: str | None = None,
    rail: str | None = None,
) -> None:
    """Advance the payment record from a Stripe webhook, MONOTONICALLY. ``paid_at`` and ``rail`` are
    set ONCE (COALESCE keeps the first value). Forward states (``processing``/``succeeded``) apply only
    when strictly ahead of the current rank; ``failed``/``canceled`` apply only when not already
    succeeded — so an out-of-order redelivery cannot regress a terminal row. Does NOT commit; the
    caller commits together with the event-ledger insert."""
    # ``rail`` is set-once (COALESCE(rail, new)): ACH stamps 'us_bank_account' on its ``processing``
    # event and the later ``succeeded`` keeps it; a card intent jumps straight to ``succeeded`` with
    # rail still NULL, so it is stamped 'card'. The webhook supplies the candidate from the event type.
    if status in ("failed", "canceled"):
        guard = "payment_status <> 'succeeded'"
    else:
        guard = f"%(rank)s > {_DOC_PAY_RANK_CASE}"
    sql = f"""
        UPDATE business.document_payments
           SET payment_status           = %(status)s,
               paid_at                   = CASE WHEN %(paid)s
                                                THEN COALESCE(paid_at, now())
                                                ELSE paid_at END,
               stripe_payment_intent_id  = COALESCE(%(intent_id)s, stripe_payment_intent_id),
               rail                      = COALESCE(rail, %(rail)s),
               updated_at                = now()
         WHERE document_id = %(document_id)s AND {guard}
    """
    async with conn.cursor() as cur:
        await cur.execute(
            sql,
            {
                "status": status,
                "paid": paid,
                "intent_id": intent_id,
                "rail": rail,
                "document_id": document_id,
                "rank": _DOC_PAY_RANK.get(status, 0),
            },
        )
