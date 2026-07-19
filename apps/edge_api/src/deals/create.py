"""Operator → CRM projection: mint one ``business.deal`` (+ its ``account``, ``contact``,
and the ``deal_contacts`` signatory link) from an OPERATOR payload — the manual parallel
to the booking producer (``deals/materialize.py``).

WHY A PARALLEL PRODUCER — the booking lane assumes a Cal.com ``BOOKING_CREATED`` upstream.
This lane serves the no-call path (deal authored in Settings → document originated → sign
link emailed after), so deals exist without any ``corex.bookings`` row: ``source_booking_id``
/ ``last_booking_id`` stay NULL (both nullable in-DB by design).

PARITY WITH THE BOOKING PRODUCER — same grain, same keys, same upsert semantics:
  • one deal per ACCOUNT (``uq_deals_account``) — creating a deal for a company that already
    has one UPDATES that deal (fills missing company identity, never regresses status) and
    the response says so (``action: updated``);
  • accounts dedupe on domain (``UNIQUE (domain)``), contacts on (account_id, lower(email));
  • person fields are OPTIONAL (ontology ruling 2026-07-19: the signatory lives on the
    AGREEMENT — ``business.agreements.signatory_contact_id`` — and agreement generation is
    the enforcement point); when given, the person attaches as a plain deal contact
    (``deal_contacts``, is_signatory=false, DO NOTHING on conflict).

IDEMPOTENCY — keyed on the natural keys only (domain → account → uq_deals_account; email →
contact). A payload WITHOUT a domain has no account dedupe key: each call inserts a fresh
account (there is no booking backstop here). Acceptable for a synchronous operator action —
the operator sees the created deal immediately.

TRANSACTION: writes only; the CALLER owns commit/rollback (mirrors materialize.py), so
account + contact + deal + deal_contacts move atomically.
"""
from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row


async def create_deal_manual(
    conn,
    *,
    company_name: str,
    domain: str | None,
    first_name: str | None = None,
    last_name: str | None = None,
    email: str | None = None,
    title: str | None = None,
) -> dict[str, Any]:
    """Project the operator payload into account → deal (+ optionally a contact). Idempotent
    on the natural keys. Does NOT commit. Ontology ruling 2026-07-19: the signatory lives on
    the AGREEMENT — person fields are optional here; when ``email`` is absent the contact and
    deal_contacts steps are skipped entirely and ``contact_id`` returns None."""
    domain = (domain or "").strip().lower() or None
    email = (email or "").strip() or None
    company = company_name.strip()

    async with conn.cursor(row_factory=dict_row) as cur:
        # ── 1) Account (account = company). NOT NULL name; dedupe by domain when present. ──
        if domain:
            await cur.execute(
                """
                INSERT INTO business.accounts AS a (name, domain)
                VALUES (%(name)s, %(domain)s)
                ON CONFLICT (domain) WHERE deleted_at IS NULL AND domain IS NOT NULL
                DO UPDATE SET name = COALESCE(a.name, EXCLUDED.name), updated_at = now()
                RETURNING id::text AS id
                """,
                {"name": company, "domain": domain},
            )
        else:
            await cur.execute(
                "INSERT INTO business.accounts (name) VALUES (%s) RETURNING id::text AS id",
                (company,),
            )
        account_id = (await cur.fetchone())["id"]

        # ── 2) Contact (optional person). Dedupe by (account_id, lower(email)); claim
        # primary only if the account has none yet (≤1-primary partial unique). Skipped
        # entirely when no email — signatory identity is Agreement-stage data. ───────────────
        contact_id = None
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
                {"account_id": account_id, "first": first_name, "last": last_name,
                 "email": email, "title": title},
            )
            contact_id = (await cur.fetchone())["id"]

        # ── 3) Deal — one per ACCOUNT (uq_deals_account); no booking columns on this lane.
        # On conflict, fill the company identity if missing and NEVER regress status. ─────────
        await cur.execute(
            """
            INSERT INTO business.deals AS d (account_id, company_name, company_domain)
            VALUES (%(account_id)s::uuid, %(company)s, %(domain)s)
            ON CONFLICT (account_id) DO UPDATE SET
                company_name   = COALESCE(d.company_name,   EXCLUDED.company_name),
                company_domain = COALESCE(d.company_domain, EXCLUDED.company_domain),
                updated_at     = now()
            RETURNING id::text AS id, deal_handle, status, (xmax = 0) AS inserted
            """,
            {"account_id": account_id, "company": company, "domain": domain},
        )
        deal = await cur.fetchone()

        # ── 4) Attach the person as a deal contact when given (NOT a signatory — that role
        # is an Agreement fact now; DO NOTHING preserves an operator's later toggle). ────────
        if contact_id:
            await cur.execute(
                """
                INSERT INTO business.deal_contacts (deal_id, contact_id, is_signatory)
                VALUES (%s::uuid, %s::uuid, false)
                ON CONFLICT (deal_id, contact_id) DO NOTHING
                """,
                (deal["id"], contact_id),
            )

    return {
        "action": "created" if deal["inserted"] else "updated",
        "account_id": account_id,
        "contact_id": contact_id,
        "deal_id": deal["id"],
        "deal_handle": deal["deal_handle"],
        "status": deal["status"],
    }
