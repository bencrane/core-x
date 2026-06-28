"""business.deals read access — psycopg async, read-only.

The operator Applications / Research list. One deal per account (``uq_deals_account``);
this lists deals joined to their company (``deals.company_name``/``company_domain``), the
account's primary contact (person), and to ``corex.bookings`` (via ``last_booking_id``) for
the most-recent booked date. Mutations (the booking->deal upsert) land with the producer later.

The deal's own ``deal_handle`` (LEFT(id,8)) is the public list/detail key; ``last_booking_id``
is exposed so the Application detail page can resolve handle -> booking -> company profile.
"""
from __future__ import annotations

from psycopg.rows import dict_row

from .models import Deal

# id/last_booking_id are uuid in-DB; cast to text so the model carries plain strings.
_SELECT_COLS = (
    "d.id::text AS deal_id, d.deal_handle, d.status, d.created_at, "
    "d.company_name, d.company_domain AS domain, "
    "c.first_name, c.last_name, c.email, c.title, "
    "d.last_booking_id::text AS last_booking_id, bk.booked_at"
)


async def list_recent(conn, limit: int = 100) -> list[Deal]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""SELECT {_SELECT_COLS}
                  FROM business.deals d
             LEFT JOIN business.contacts c
                    ON c.account_id = d.account_id AND c.is_primary AND c.deleted_at IS NULL
             LEFT JOIN corex.bookings bk ON bk.booking_id = d.last_booking_id
              ORDER BY d.created_at DESC
                 LIMIT %s""",
            (limit,),
        )
        rows = await cur.fetchall()
    return [Deal(**r) for r in rows]
