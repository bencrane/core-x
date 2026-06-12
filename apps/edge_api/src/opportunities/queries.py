"""business.opportunities read access — psycopg async, read-only.

The Pipeline tab's data source. One opportunity per booking (materialized at booking
time); this module lists them joined to their account (company) + primary contact
(person), and to corex.bookings for the booked date. Mutations land with the
origination surface later.
"""
from __future__ import annotations

from psycopg.rows import dict_row

from .models import Opportunity

# id/source_booking_id are uuid in-DB; cast to text so the model carries plain strings.
_SELECT_COLS = (
    "o.id::text AS opportunity_id, o.status, o.created_at, "
    "acc.name AS company_name, acc.domain AS domain, "
    "c.first_name, c.last_name, c.email, c.title, "
    "o.source_booking_id::text AS source_booking_id, bk.booked_at"
)


async def list_recent(conn, limit: int = 100) -> list[Opportunity]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""SELECT {_SELECT_COLS}
                  FROM business.opportunities o
                  JOIN business.accounts acc ON acc.id = o.account_id
             LEFT JOIN business.contacts c   ON c.id = o.contact_id
             LEFT JOIN corex.bookings bk     ON bk.booking_id = o.source_booking_id
              ORDER BY o.created_at DESC
                 LIMIT %s""",
            (limit,),
        )
        rows = await cur.fetchall()
    return [Opportunity(**r) for r in rows]
