"""business.documenso_webhook_events — RAW Documenso webhook capture (append-only, psycopg async).

The full webhook body is stored verbatim in ``payload`` (the system of record). The scalar columns
are best-effort lookup extracts only — never re-derive truth from them, re-read ``payload``.
"""
from __future__ import annotations

import json
from typing import Any


async def insert_event(
    conn,
    *,
    event: str | None,
    envelope_id: str | None,
    external_id: str | None,
    payload: Any,
) -> str:
    """Append one raw webhook delivery. Returns the row id. COMMITS."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO business.documenso_webhook_events (event, envelope_id, external_id, payload)
            VALUES (%s, %s, %s, %s::jsonb)
            RETURNING id::text
            """,
            (event, envelope_id, external_id, json.dumps(payload)),
        )
        row = await cur.fetchone()
    await conn.commit()
    return row[0]


# Documenso webhook event names that mean "the document is fully executed" — the terminal,
# all-signers-done state. For the single-signer engagement flow, DOCUMENT_COMPLETED IS the done
# signal. Verified against REAL landed events (business.documenso_webhook_events, 2026-06-17):
# the webhook stores the event verbatim as UPPERCASE_UNDERSCORE (DOCUMENT_SENT / DOCUMENT_OPENED /
# DOCUMENT_SIGNED / DOCUMENT_COMPLETED) — NOT the lowercase-dotted form. Truth derives from the
# presence of a terminal-event ROW, never from a projection.
_TERMINAL_EVENTS = ("DOCUMENT_COMPLETED",)

# PROVIDER-side signing domains. A SIGNED recipient whose email domain is NONE of these is the
# COUNTERPARTY (the prospect) — and a counterparty signature is what flips ``signed``, INDEPENDENT of
# whether the provider has countersigned. Keyed on the email DOMAIN (not the booked-contact address)
# so a DIFFERENT person at the counterparty's company can be the one who signs and the gate still
# trips. SAFETY: this set MUST list every domain a provider-side signer can use — a domain missing
# here would let a prospect reach the payment step before they themselves signed (fail-open).
_PROVIDER_SIGNING_DOMAINS = (
    "substrate.build",
    "rarestructure.com",
    "activeoperators.com",
    "governmentcontracted.com",
    # The template Provider slot carries its placeholder email (provider@example.com) through to the
    # derived document — create_document_from_template leaves fixed-email recipients untouched — so the
    # provider's own signature arrives from example.com. IETF-reserved, so it can never be a prospect.
    "example.com",
    # Operator mailbox domain — covers a provider slot bound via "Add Myself" with the real address.
    "engineereddemand.com",
)


async def get_agreement_signatory_email(conn, agreement_handle: str) -> str | None:
    """The agreement's SIGNATORY contact email by its public ``agreement_handle`` — the email bound to
    the Participant recipient at generate (``externalId = agreement_handle``). The sign-token read
    tries this FIRST (the /sign/{agreement_handle} flow), then falls back to the deal-handle lookup
    below (the older deal-keyed links). None when the handle/signatory is unknown."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT c.email
              FROM business.agreements a
              JOIN business.contacts c ON c.id = a.signatory_contact_id
             WHERE a.agreement_handle = %s
             LIMIT 1
            """,
            (agreement_handle,),
        )
        row = await cur.fetchone()
    return row[0] if row and row[0] else None


async def get_deal_signatory_email(conn, deal_handle: str) -> str | None:
    """The deal's SIGNATORY contact email by its public ``deal_handle`` — the email bound to the
    client recipient at originate. The sign-token read uses it to tell the client's token from the
    originator's on a two-signer document. ONE signatory in practice (templates carry a single
    prospect slot); the deterministic ORDER BY matches ``deals.queries.get_deal_originate_inputs`` so
    this resolves the SAME signatory the originate bound. Hard-keyed on the deal_contacts junction,
    NO opportunity fallback. None when the handle/signatory is unknown."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT c.email
              FROM business.deals d
              JOIN business.deal_contacts dc ON dc.deal_id = d.id AND dc.is_signatory
              JOIN business.contacts c ON c.id = dc.contact_id
             WHERE d.deal_handle = %s
             ORDER BY dc.created_at, dc.contact_id
             LIMIT 1
            """,
            (deal_handle,),
        )
        row = await cur.fetchone()
    return row[0] if row and row[0] else None


async def read_sign_state(
    conn, *, opportunity_id: str, document_id: str, signer: str = "client"
) -> dict[str, Any]:
    """Project signing state for an ``(opportunity_id, document_id)`` PAIR at read time from the raw
    webhook rows — FULLY OFFLINE (no projection table, no live Documenso call).

    Security model — the pair MUST be valid (the document belongs to the opportunity), so a guessed
    numeric ``document_id`` with a wrong/missing opportunity handle returns nothing:
      * ``external_id`` (the captured column) = the opportunity's public 8-char handle stamped on the
        envelope at originate (the access capability — 8 hex = 32 bits).
      * ``envelope_id`` (the captured column) = Documenso's NUMERIC document id (e.g. ``"1462137"``),
        the same value the webhook payload carries as ``payload.id`` and the SPA link carries as the
        ``document_id`` segment.
    Verified against REAL landed rows 2026-06-17: the pair
    (``external_id='7bbf1081-…', envelope_id='1462137'``) carries a ``DOCUMENT_COMPLETED`` row.

    Returns ``{signed, latest_event, status, received_at}``:
      * ``signed``       — the REQUESTED ``signer`` has signed (recipient-scoped):
                             - ``signer='client'`` (default) — a captured payload shows a recipient with
                               ``signingStatus='SIGNED'`` (or a non-null ``signedAt``) whose email DOMAIN
                               is NOT a provider domain (``_PROVIDER_SIGNING_DOMAINS``): the prospect.
                             - ``signer='originator'`` — the same, but whose DOMAIN IS a provider domain:
                               you (the countersigner). This is what the operator's ``?signer=originator``
                               signing link polls so it advances only once YOU sign — not when the
                               prospect did (the bug this fixes).
                           A terminal ``DOCUMENT_COMPLETED`` row satisfies EITHER (both parties signed).
      * ``latest_event`` — the most recent event name seen (by received_at), or None if no rows.
      * ``status``       — the ``payload->payload->>status`` of the latest row (PENDING/COMPLETED/…),
                           the envelope-level Documenso status carried verbatim in the raw body.
    """
    empty = {"signed": False, "latest_event": None, "status": None, "received_at": None}
    if not opportunity_id or not document_id:
        return empty
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT
              bool_or(
                event = ANY(%(terminal)s)
                OR EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements(
                    COALESCE(payload->'payload'->'Recipient', payload->'payload'->'recipients', '[]'::jsonb)
                  ) AS r
                  WHERE COALESCE(r->>'email', '') <> ''
                    AND (r->>'signingStatus' = 'SIGNED' OR (r->>'signedAt') IS NOT NULL)
                    AND CASE WHEN %(originator)s
                             THEN lower(split_part(r->>'email', '@', 2)) = ANY(%(provider_domains)s)
                             ELSE lower(split_part(r->>'email', '@', 2)) <> ALL(%(provider_domains)s)
                        END
                )
              )                                                          AS signed,
              (array_agg(event ORDER BY received_at DESC))[1]            AS latest_event,
              (array_agg(payload->'payload'->>'status' ORDER BY received_at DESC))[1] AS status,
              max(received_at)                                           AS received_at
            FROM business.documenso_webhook_events
            WHERE external_id = %(opportunity_id)s
              AND envelope_id = %(document_id)s
            """,
            {
                "terminal": list(_TERMINAL_EVENTS),
                "provider_domains": [d.lower() for d in _PROVIDER_SIGNING_DOMAINS],
                "originator": signer == "originator",
                "opportunity_id": opportunity_id,
                "document_id": document_id,
            },
        )
        row = await cur.fetchone()
    if row is None or row[3] is None:
        return empty
    return {
        "signed": bool(row[0]),
        "latest_event": row[1],
        "status": row[2],
        "received_at": row[3].isoformat() if row[3] else None,
    }
