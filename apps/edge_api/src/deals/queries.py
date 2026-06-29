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
from psycopg.types.json import Jsonb

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


async def get_deal_with_details(conn, handle: str) -> dict | None:
    """The deal + its (optional 1:1) deal_details, resolved by public ``deal_handle``. LEFT JOIN so a
    deal with no deal_details row yet still returns (empty contacts/content). None if no such deal."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT d.id::text              AS deal_id,
                   d.deal_handle,
                   d.company_name,
                   d.company_domain,
                   d.organization_id::text AS organization_id,
                   COALESCE(dd.contacts, '[]'::jsonb) AS contacts,
                   COALESCE(dd.content,  '{}'::jsonb) AS content,
                   dd.default_template_uuid::text     AS default_template_uuid,
                   COALESCE(dd.template_origin, 'default') AS template_origin
              FROM business.deals d
         LEFT JOIN business.deal_details dd ON dd.deal_id = d.id
             WHERE d.deal_handle = %s
             LIMIT 1
            """,
            (handle,),
        )
        return await cur.fetchone()


async def list_org_templates(conn, organization_id: str | None) -> list[dict]:
    """Active Documenso templates for the deal's org — the editor's template dropdown. Each row
    carries the template UUID (matches deal_details.default_template_uuid) + external id + name."""
    if not organization_id:
        return []
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT dt.id::text AS template_uuid, dt.documenso_template_id, dt.name, dt.is_default
              FROM business.documenso_templates dt
             WHERE dt.organization_id = %s::uuid AND dt.status = 'active'
             ORDER BY dt.is_default DESC, dt.name
            """,
            (organization_id,),
        )
        return await cur.fetchall()


async def upsert_details(conn, *, deal_id: str, contacts, content,
                         default_template_uuid: str | None) -> None:
    """Upsert the deal's deal_details (1:1 on deal_id). ``template_origin`` is DERIVED: 'default' when
    the chosen template IS the deal-org's is_default (or none is chosen), else 'operator' (a manual
    override). Commits."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT dt.id::text AS uuid
              FROM business.documenso_templates dt
              JOIN business.deals d ON d.organization_id = dt.organization_id
             WHERE d.id = %s::uuid AND dt.is_default
             LIMIT 1
            """,
            (deal_id,),
        )
        row = await cur.fetchone()
        org_default = row["uuid"] if row else None
        origin = "default" if (default_template_uuid is None or default_template_uuid == org_default) else "operator"
        await cur.execute(
            """
            INSERT INTO business.deal_details
                (deal_id, content, contacts, default_template_uuid, template_origin)
            VALUES (%(deal_id)s::uuid, %(content)s, %(contacts)s, %(tmpl)s::uuid, %(origin)s)
            ON CONFLICT (deal_id) DO UPDATE SET
                content               = EXCLUDED.content,
                contacts              = EXCLUDED.contacts,
                default_template_uuid = EXCLUDED.default_template_uuid,
                template_origin       = EXCLUDED.template_origin,
                updated_at            = now()
            """,
            {"deal_id": deal_id, "content": Jsonb(content), "contacts": Jsonb(contacts),
             "tmpl": default_template_uuid, "origin": origin},
        )
    await conn.commit()
