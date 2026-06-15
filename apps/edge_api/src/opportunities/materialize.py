"""Booking → CRM projection: materialize one ``business.opportunity`` (+ its
``account`` and ``contact``) from a normalized ``corex.bookings`` row.

This is the PRODUCER the read surface (``opportunities/queries.py``) has always
implied ("one opportunity per booking, materialized at booking time") but that
nothing wrote. The cal webhook fires the ``opportunity-materialize`` Trigger.dev
task on a new booking; that task calls the ``/internal/opportunities/materialize``
route, which calls this. DocRaptor render layers onto the SAME task later (phase 2).

SCHEMA OWNERSHIP: ``business.accounts`` / ``business.contacts`` /
``business.opportunities`` are owned by hq-x's own migration tool — edge_api WRITES
them (as it already reads them) but does NOT define them. The upserts below pin to
the live partial-unique keys; if that schema drifts, the upsert fails loudly rather
than duplicating. The keys this module depends on:
  • accounts       UNIQUE (domain)                 WHERE deleted_at IS NULL AND domain IS NOT NULL
  • contacts       UNIQUE (account_id, lower(email)) WHERE deleted_at IS NULL AND email IS NOT NULL
  • contacts       UNIQUE (account_id)              WHERE is_primary AND deleted_at IS NULL  (≤1 primary/acct)
  • opportunities  UNIQUE (source_booking_id)       WHERE source_booking_id IS NOT NULL      (1 opp/booking)
  • opportunities.source_booking_id → corex.bookings.booking_id (FK)

IDEMPOTENCY: keyed end-to-end on the booking. The opportunity upsert collapses on
``source_booking_id`` (one per booking); accounts/contacts collapse on their domain/
email keys. For bookings missing a domain or email (no natural dedupe key), an
already-materialized opportunity is reused so a retry never spawns a second account/
contact. Safe to call repeatedly — the Trigger task retries on the default policy.

TRANSACTION: writes only; the CALLER owns commit/rollback (mirrors cal/queries.py),
so account+contact+opportunity move atomically.
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


async def materialize_for_booking(conn, *, ical_uid: str) -> dict[str, Any]:
    """Project the booking identified by ``ical_uid`` into account → contact →
    opportunity. Idempotent. Does NOT commit. Returns a structured result the
    Trigger run surfaces (action + the three ids + opportunity status)."""
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

        # ── 1) Idempotency backstop: an opportunity already materialized for this booking ──
        # Reused only to resolve account/contact when the booking carries no natural dedupe
        # key (domain/email) — so a retry of a key-less booking never spawns duplicates.
        await cur.execute(
            "SELECT account_id::text AS account_id, contact_id::text AS contact_id "
            "FROM business.opportunities WHERE source_booking_id = %s::uuid",
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
            # Named booker but no email → no dedupe key; insert once (backstop above guards retries).
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

        # ── 4) Opportunity — one per booking. status left to default 'open' on insert and
        # NEVER regressed on conflict (preserves any operator advancement). ───────────────
        opp_name = company or full_name or domain
        await cur.execute(
            """
            INSERT INTO business.opportunities AS o (account_id, contact_id, source_booking_id, name)
            VALUES (%(account_id)s::uuid, %(contact_id)s::uuid, %(booking_id)s::uuid, %(name)s)
            ON CONFLICT (source_booking_id) WHERE source_booking_id IS NOT NULL
            DO UPDATE SET
                account_id = EXCLUDED.account_id,
                contact_id = COALESCE(EXCLUDED.contact_id, o.contact_id),
                name       = COALESCE(o.name, EXCLUDED.name),
                updated_at = now()
            RETURNING id::text AS id, status, (xmax = 0) AS inserted
            """,
            {"account_id": account_id, "contact_id": contact_id, "booking_id": booking_id, "name": opp_name},
        )
        opp = await cur.fetchone()

    return {
        "action": "created" if opp["inserted"] else "updated",
        "ical_uid": ical_uid,
        "booking_id": booking_id,
        "account_id": account_id,
        "contact_id": contact_id,
        "opportunity_id": opp["id"],
        "status": opp["status"],
    }
