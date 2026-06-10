"""business.company_profile_snapshots persistence — psycopg async, APPEND-ONLY.

Insert-only: every Save Profile appends one immutable row (never an update), so a domain
accumulates a timestamped history. The Dossier read resolves the LATEST row by domain. jsonb
columns (focus/industries/geographies/verified) round-trip as native lists/dicts.
"""
from __future__ import annotations

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .models import CompanyProfileSnapshot, CompanyProfileSnapshotCreate

_SELECT_COLS = (
    "id, domain, company, signer_name, title, email, hq, headcount, est_revenue_range, "
    "overview, focus, industries, geographies, verified, saved_by, created_at"
)


def _norm_domain(domain: str) -> str:
    return (domain or "").strip().lower()


async def insert_snapshot(
    conn, *, domain: str, body: CompanyProfileSnapshotCreate
) -> CompanyProfileSnapshot:
    """Append one immutable snapshot for ``domain``. Returns the persisted row."""
    sql = f"""
        INSERT INTO business.company_profile_snapshots
            (domain, company, signer_name, title, email, hq, headcount, est_revenue_range,
             overview, focus, industries, geographies, verified, saved_by)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING {_SELECT_COLS}
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            sql,
            (
                _norm_domain(domain), body.company, body.signer_name, body.title, body.email,
                body.hq, body.headcount, body.est_revenue_range, body.overview,
                Jsonb(body.focus), Jsonb(body.industries), Jsonb(body.geographies),
                Jsonb(body.verified), body.saved_by,
            ),
        )
        row = await cur.fetchone()
    await conn.commit()
    return CompanyProfileSnapshot.from_row(row)


async def get_latest_by_domain(conn, domain: str) -> CompanyProfileSnapshot | None:
    """The most recent snapshot for a domain, or None when the operator has never saved one."""
    norm = _norm_domain(domain)
    if not norm:
        return None
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_SELECT_COLS} FROM business.company_profile_snapshots "
            f"WHERE domain = %s ORDER BY created_at DESC, id DESC LIMIT 1",
            (norm,),
        )
        row = await cur.fetchone()
    return CompanyProfileSnapshot.from_row(row) if row else None
