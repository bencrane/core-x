"""corex.bookings read access — psycopg async, read-only.

The Pipeline tab's data source. Bookings are appended by the cal.com webhook
consumer (one row per ``cal_event_uid``); this module only lists them. Mutations
(status advance, enrichment binding) land with the origination surface later.
"""
from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row

from .models import Booking, CompanyProfile

# booking_id is uuid in-DB; cast to text so the model carries a plain string.
_SELECT_COLS = (
    "booking_id::text AS booking_id, cal_event_uid, first_name, last_name, email, "
    "company_name, domain, title, status, start_time, created_at"
)

# Full row for the booking-profile page (everything the cal booking gives us).
_SELECT_DETAIL_COLS = (
    "booking_id::text AS booking_id, ical_uid, cal_event_uid, cal_booking_id, event_type_id, "
    "first_name, last_name, email, company_name, domain, title, status, "
    "start_time, end_time, booked_at, created_at, updated_at"
)


def _to_booking(row: dict[str, Any]) -> Booking:
    return Booking(**row)


async def list_recent(conn, limit: int = 100) -> list[Booking]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_SELECT_COLS} FROM corex.bookings ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        rows = await cur.fetchall()
    return [_to_booking(r) for r in rows]


async def get_by_id(conn, booking_id: str) -> Booking | None:
    """One booking by its (our) ``booking_id`` uuid. Returns None when not found."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_SELECT_DETAIL_COLS} FROM corex.bookings WHERE booking_id = %s::uuid",
            (booking_id,),
        )
        row = await cur.fetchone()
    return _to_booking(row) if row else None


# business.company_profiles — the domain-keyed dossier payload (company-level). The booking
# resolves to its profile by domain (lowercased); jsonb tag arrays come back as native lists.
_PROFILE_COLS = (
    "domain, company, hq, headcount, est_revenue_range, overview, "
    "focus, industries, geographies, source"
)


async def get_profile_by_domain(conn, domain: str) -> CompanyProfile | None:
    """One company dossier by domain. Returns None when the company is not yet enriched."""
    norm = (domain or "").strip().lower()
    if not norm:
        return None
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_PROFILE_COLS} FROM business.company_profiles WHERE domain = %s",
            (norm,),
        )
        row = await cur.fetchone()
    return CompanyProfile(**row) if row else None
