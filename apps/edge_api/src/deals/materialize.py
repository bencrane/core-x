"""Booking → CRM projection: materialize one ``business.deal`` (+ its ``account``,
``contact``, and the ``deal_contacts`` signatory link) from a normalized
``corex.bookings`` row.

This is the going-forward PRODUCER that replaces the booking → opportunity projection
(``opportunities/materialize.py``, retired). The cal webhook fires the ``deal-materialize``
Trigger.dev task on a new booking; that task calls the ``/internal/deals/materialize``
route, which calls this. DocRaptor render layers onto the SAME task later (phase 2).

GRAIN — one deal per ACCOUNT, not per booking. The opportunity model created one row
per booking (``opportunities.source_booking_id`` unique); the deal model collapses every
booking on the same company onto ONE deal (``uq_deals_account``):
  • the FIRST booking for an account CREATES the deal — ``source_booking_id`` is stamped
    to that booking (once, never moved) and ``last_booking_id`` tracks the latest;
  • each SUBSEQUENT booking on the same account ADVANCES ``last_booking_id`` to the newer
    booking and NEVER regresses the deal's ``status`` or its originating booking.
The booker is attached to the deal as a signatory in ``business.deal_contacts``.

SCHEMA OWNERSHIP: ``business.accounts`` / ``business.contacts`` / ``business.deals`` are
upstream-owned (hq-x's migration tool) — edge_api WRITES them (as it already reads them)
but does NOT define them. The upserts pin to the live keys; if that schema drifts the
upsert fails loudly rather than duplicating. The keys this module depends on:
  • accounts      UNIQUE (domain)                  WHERE deleted_at IS NULL AND domain IS NOT NULL
  • contacts      UNIQUE (account_id, lower(email)) WHERE deleted_at IS NULL AND email IS NOT NULL
  • contacts      UNIQUE (account_id)              WHERE is_primary AND deleted_at IS NULL  (≤1 primary/acct)
  • deals         UNIQUE (account_id)              (uq_deals_account — one deal per account)
  • deals.status  CHECK IN (draft, originated, sent, signed, paid, void); DEFAULT 'draft'
  • deals.deal_handle GENERATED ALWAYS AS LEFT(id,8) STORED — derived by the DB, never written here
  • deal_contacts PRIMARY KEY (deal_id, contact_id)

IDEMPOTENCY: keyed end-to-end on the booking + the account/email natural keys. A booking
carrying a domain collapses structurally (domain → account → ``uq_deals_account``), so the
task's retries and cal's duplicate deliveries never duplicate. A booking with NO domain has
no natural account key; the backstop reuses the deal already created FOR THIS booking
(``deals.source_booking_id``) — and its signatory contact — so a key-less retry never spawns
a second account/deal/contact. Safe to call repeatedly.

TRANSACTION: writes only; the CALLER owns commit/rollback (mirrors cal/queries.py), so
account + contact + deal + deal_contacts move atomically.
"""
from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row

# corex.bookings columns this projection sources from (resolved by the stable ical_uid).
_BOOKING_SELECT = (
    "booking_id::text AS booking_id, first_name, last_name, email, company_name, domain, title"
)


def _full_name(first: str | None, last: str | None) -> str | None:
    name = " ".join(p for p in (first, last) if p and p.strip()).strip()
    return name or None


async def materialize_deal_for_booking(conn, *, ical_uid: str) -> dict[str, Any]:
    """Project the booking identified by ``ical_uid`` into account → contact → deal
    (+ a deal_contacts signatory link). Idempotent. Does NOT commit. Returns a structured
    result the Trigger run surfaces (action + ids + deal_handle + status)."""
    async with conn.cursor(row_factory=dict_row) as cur:
        # ── 0) Resolve the booking (the source of every field) ───────────────────────────
        await cur.execute(
            f"SELECT {_BOOKING_SELECT} FROM corex.bookings WHERE ical_uid = %s",
            (ical_uid,),
        )
        bk = await cur.fetchone()
        if bk is None:
            return {"action": "skipped_no_booking", "ical_uid": ical_uid}

        booking_id = bk["booking_id"]
        domain = (bk["domain"] or "").strip().lower() or None
        email = (bk["email"] or "").strip() or None
        company = (bk["company_name"] or "").strip() or None
        first, last, title = bk["first_name"], bk["last_name"], bk["title"]
        full_name = _full_name(first, last)

        # ── 1) Idempotency backstop: a deal already CREATED by this booking ──────────────
        # Reused only to resolve account/contact when the booking carries no natural dedupe
        # key (domain/email) — so a key-less retry never spawns duplicates. Keyed on the
        # ORIGINATING booking (deals.source_booking_id, stamped once at create); the deal's
        # signatory is the earliest deal_contacts row.
        await cur.execute(
            """
            SELECT d.account_id::text AS account_id, dc.contact_id::text AS contact_id
              FROM business.deals d
              LEFT JOIN LATERAL (
                    SELECT contact_id FROM business.deal_contacts
                     WHERE deal_id = d.id
                     ORDER BY is_signatory DESC, created_at
                     LIMIT 1
              ) dc ON true
             WHERE d.source_booking_id = %s::uuid
             LIMIT 1
            """,
            (booking_id,),
        )
        existing = await cur.fetchone()

        # ── 2) Account (account = company). NOT NULL name; dedupe by domain when present. ──
        account_name = company or domain or full_name or "Unknown Account"
        if domain:
            await cur.execute(
                """
                INSERT INTO business.accounts AS a (name, domain)
                VALUES (%(name)s, %(domain)s)
                ON CONFLICT (domain) WHERE deleted_at IS NULL AND domain IS NOT NULL
                DO UPDATE SET name = COALESCE(a.name, EXCLUDED.name), updated_at = now()
                RETURNING id::text AS id
                """,
                {"name": account_name, "domain": domain},
            )
            account_id = (await cur.fetchone())["id"]
        elif existing and existing["account_id"]:
            account_id = existing["account_id"]
        else:
            await cur.execute(
                "INSERT INTO business.accounts (name) VALUES (%s) RETURNING id::text AS id",
                (account_name,),
            )
            account_id = (await cur.fetchone())["id"]

        # ── 3) Contact (contact = person). FK account_id NOT NULL; dedupe by email/account. ──
        # is_primary: claim primary only if the account has none yet (respects the
        # ≤1-primary-per-account partial unique without ever colliding).
        contact_id: str | None = None
        if email:
            await cur.execute(
                """
                INSERT INTO business.contacts AS c
                    (account_id, first_name, last_name, email, title, is_primary)
                VALUES (
                    %(account_id)s::uuid, %(first)s, %(last)s, %(email)s, %(title)s,
                    NOT EXISTS (SELECT 1 FROM business.contacts x
                                WHERE x.account_id = %(account_id)s::uuid
                                  AND x.is_primary AND x.deleted_at IS NULL)
                )
                ON CONFLICT (account_id, lower(email)) WHERE deleted_at IS NULL AND email IS NOT NULL
                DO UPDATE SET
                    first_name = COALESCE(c.first_name, EXCLUDED.first_name),
                    last_name  = COALESCE(c.last_name,  EXCLUDED.last_name),
                    title      = COALESCE(c.title,       EXCLUDED.title),
                    updated_at = now()
                RETURNING id::text AS id
                """,
                {"account_id": account_id, "first": first, "last": last, "email": email, "title": title},
            )
            contact_id = (await cur.fetchone())["id"]
        elif existing and existing["contact_id"]:
            contact_id = existing["contact_id"]
        elif full_name:
            # Named booker but no email → no dedupe key; insert once (the backstop above guards retries).
            await cur.execute(
                """
                INSERT INTO business.contacts AS c (account_id, first_name, last_name, title, is_primary)
                VALUES (
                    %(account_id)s::uuid, %(first)s, %(last)s, %(title)s,
                    NOT EXISTS (SELECT 1 FROM business.contacts x
                                WHERE x.account_id = %(account_id)s::uuid
                                  AND x.is_primary AND x.deleted_at IS NULL)
                )
                RETURNING id::text AS id
                """,
                {"account_id": account_id, "first": first, "last": last, "title": title},
            )
            contact_id = (await cur.fetchone())["id"]

        # ── 4) Deal — one per ACCOUNT (uq_deals_account). On create, stamp source+last booking
        # and let status default to 'draft'. On conflict, ADVANCE last_booking_id to this newer
        # booking, fill the company identity if missing, and NEVER regress status or the
        # originating booking (source_booking_id is untouched on update). ────────────────────
        await cur.execute(
            """
            INSERT INTO business.deals AS d
                (account_id, source_booking_id, last_booking_id, company_name, company_domain)
            VALUES (%(account_id)s::uuid, %(booking_id)s::uuid, %(booking_id)s::uuid,
                    %(company)s, %(domain)s)
            ON CONFLICT (account_id) DO UPDATE SET
                last_booking_id = EXCLUDED.last_booking_id,
                company_name    = COALESCE(d.company_name,   EXCLUDED.company_name),
                company_domain  = COALESCE(d.company_domain, EXCLUDED.company_domain),
                updated_at      = now()
            RETURNING id::text AS id, deal_handle, status, (xmax = 0) AS inserted
            """,
            {"account_id": account_id, "booking_id": booking_id, "company": company, "domain": domain},
        )
        deal = await cur.fetchone()
        deal_id = deal["id"]

        # ── 5) Attach the booker to the deal as a signatory. is_signatory defaults true (all
        # deal contacts sign until toggled off); DO NOTHING on conflict preserves an operator's
        # later toggle and keeps re-materialization idempotent. ──────────────────────────────
        if contact_id:
            await cur.execute(
                """
                INSERT INTO business.deal_contacts (deal_id, contact_id, is_signatory)
                VALUES (%s::uuid, %s::uuid, true)
                ON CONFLICT (deal_id, contact_id) DO NOTHING
                """,
                (deal_id, contact_id),
            )

    return {
        "action": "created" if deal["inserted"] else "updated",
        "ical_uid": ical_uid,
        "booking_id": booking_id,
        "account_id": account_id,
        "contact_id": contact_id,
        "deal_id": deal_id,
        "deal_handle": deal["deal_handle"],
        "status": deal["status"],
    }
