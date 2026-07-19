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
    "d.last_booking_id::text AS last_booking_id, bk.booked_at, "
    "ag.status AS latest_agreement_status"
)


async def list_recent(conn, limit: int = 100) -> list[Deal]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""SELECT {_SELECT_COLS}
                  FROM business.deals d
             LEFT JOIN business.contacts c
                    ON c.account_id = d.account_id AND c.is_primary AND c.deleted_at IS NULL
             LEFT JOIN corex.bookings bk ON bk.booking_id = d.last_booking_id
             LEFT JOIN LATERAL (
                       SELECT a.status FROM business.agreements a
                        WHERE a.deal_id = d.id
                        ORDER BY a.created_at DESC LIMIT 1
                  ) ag ON true
              ORDER BY d.created_at DESC
                 LIMIT %s""",
            (limit,),
        )
        rows = await cur.fetchall()
    return [Deal(**r) for r in rows]


async def get_deal_with_details(conn, handle: str) -> dict | None:
    """The deal + its ACTIVE document config (business.deal_document_configs), by public ``deal_handle``.
    LEFT JOIN so a deal with no config row yet still returns (empty field_values, no template).
    ``template_origin`` is DERIVED: 'default' when no template is attached OR the attached template is the
    operator's MIRROR default (business.documenso_template_defaults), else 'operator'. None if no such deal."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT d.id::text              AS deal_id,
                   d.deal_handle,
                   d.company_name,
                   d.company_domain,
                   d.organization_id::text AS organization_id,
                   d.account_id::text      AS account_id,
                   COALESCE(cfg.field_values, '{}'::jsonb) AS field_values,
                   cfg.template_documenso_id,
                   CASE
                       WHEN cfg.template_documenso_id IS NULL            THEN 'default'
                       WHEN cfg.template_documenso_id = def.documenso_id THEN 'default'
                       ELSE 'operator'
                   END AS template_origin
              FROM business.deals d
         LEFT JOIN business.deal_document_configs cfg
                ON cfg.deal_id = d.id AND cfg.status = 'active'
         LEFT JOIN business.documenso_template_defaults def
                ON def.is_default
             WHERE d.deal_handle = %s
             LIMIT 1
            """,
            (handle,),
        )
        return await cur.fetchone()


async def get_deal_contacts(conn, deal_id: str) -> list[dict]:
    """The deal's contacts (deal_contacts junction JOINed to the person), signatories first. The
    person fields (full_name/email/title) are read-only here — business.contacts is their source."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT dc.contact_id::text AS contact_id,
                   NULLIF(btrim(concat_ws(' ', c.first_name, c.last_name)), '') AS full_name,
                   c.email, c.title, dc.is_signatory
              FROM business.deal_contacts dc
              JOIN business.contacts c ON c.id = dc.contact_id
             WHERE dc.deal_id = %s::uuid
             ORDER BY dc.is_signatory DESC, c.first_name NULLS LAST, c.last_name NULLS LAST
            """,
            (deal_id,),
        )
        return await cur.fetchall()


async def get_available_contacts(conn, account_id: str | None, deal_id: str) -> list[dict]:
    """The eligible pool — account contacts NOT yet on the deal — for the editor's add-contact picker."""
    if not account_id:
        return []
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT c.id::text AS contact_id,
                   NULLIF(btrim(concat_ws(' ', c.first_name, c.last_name)), '') AS full_name,
                   c.email, c.title
              FROM business.contacts c
             WHERE c.account_id = %s::uuid AND c.deleted_at IS NULL
               AND NOT EXISTS (SELECT 1 FROM business.deal_contacts dc
                                WHERE dc.deal_id = %s::uuid AND dc.contact_id = c.id)
             ORDER BY c.first_name NULLS LAST, c.last_name NULLS LAST
            """,
            (account_id, deal_id),
        )
        return await cur.fetchall()


async def list_org_templates(conn, organization_id: str | None) -> list[dict]:
    """The selectable Documenso templates for the deal editor's dropdown — read off the MIRROR
    (business.documenso_envelopes, type='template', non-deleted), each flagged with the operator's
    mirror default (business.documenso_template_defaults). ``organization_id`` is accepted for call
    compatibility but the mirror is NOT org-scoped. Each row carries documenso_id (the attach key,
    matching deal_document_configs.template_documenso_id) + name (the envelope title) + is_default."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT e.documenso_id,
                   e.title                       AS name,
                   COALESCE(d.is_default, false)  AS is_default
              FROM business.documenso_envelopes e
              LEFT JOIN business.documenso_template_defaults d
                     ON d.documenso_id = e.documenso_id AND d.is_default
             WHERE e.type = 'template' AND e.deleted_at IS NULL
             ORDER BY e.synced_at DESC
            """
        )
        return await cur.fetchall()


async def upsert_document_config(conn, *, deal_id: str, contacts, field_values,
                                 template_documenso_id: int | None) -> None:
    """Persist the deal's ACTIVE document config (attached MIRROR template + per-field prefill
    ``field_values``) into business.deal_document_configs, and reconcile the deal_contacts junction.

    APPEND-ONLY, one active row per deal: editing values on the SAME template UPDATES the active row in
    place; attaching a DIFFERENT template ARCHIVES the active row and INSERTS a new active one (its
    field_values are per-template, keyed by that template's labels). The partial unique index
    ``deal_document_configs_one_active_per_deal_uidx`` pins one active per deal; archive-before-insert
    keeps it from being transiently violated. ``field_values`` holds operator OVERRIDES only (template
    defaults + prospect facts resolve at read/originate). Person identity (business.contacts) is NOT
    edited here. ``contacts`` = [{contact_id, is_signatory}]. Commits."""
    async with conn.cursor(row_factory=dict_row) as cur:
        # Serialize concurrent writers for THIS deal: lock the parent deal row so simultaneous
        # template-switch PUTs can't race the append-only archive→insert into a unique_violation on
        # deal_document_configs_one_active_per_deal_uidx (the loser would otherwise surface a 500). The
        # lock is held for the whole transaction (released at conn.commit() below).
        await cur.execute("SELECT 1 FROM business.deals WHERE id = %s::uuid FOR UPDATE", (deal_id,))
        await cur.execute(
            "SELECT id, template_documenso_id FROM business.deal_document_configs "
            "WHERE deal_id = %s::uuid AND status = 'active' LIMIT 1",
            (deal_id,),
        )
        active = await cur.fetchone()
        if active is not None and active["template_documenso_id"] == template_documenso_id:
            # Same template → update the active config's values in place.
            await cur.execute(
                "UPDATE business.deal_document_configs SET field_values = %(fv)s, updated_at = now() "
                "WHERE id = %(id)s",
                {"fv": Jsonb(field_values), "id": active["id"]},
            )
        else:
            # New or changed template → archive the prior active, insert a fresh active config.
            if active is not None:
                await cur.execute(
                    "UPDATE business.deal_document_configs SET status = 'archived', updated_at = now() "
                    "WHERE id = %(id)s",
                    {"id": active["id"]},
                )
            await cur.execute(
                """
                INSERT INTO business.deal_document_configs
                    (deal_id, template_documenso_id, field_values, status)
                VALUES (%(deal_id)s::uuid, %(tmpl)s, %(fv)s, 'active')
                """,
                {"deal_id": deal_id, "tmpl": template_documenso_id, "fv": Jsonb(field_values)},
            )
        # Reconcile the deal_contacts junction to the desired set: drop removed, upsert kept (is_signatory).
        desired = [c["contact_id"] for c in contacts if c.get("contact_id")]
        await cur.execute(
            "DELETE FROM business.deal_contacts WHERE deal_id = %s::uuid AND NOT (contact_id = ANY(%s::uuid[]))",
            (deal_id, desired),
        )
        for c in contacts:
            cid = c.get("contact_id")
            if not cid:
                continue
            await cur.execute(
                """
                INSERT INTO business.deal_contacts (deal_id, contact_id, is_signatory)
                VALUES (%s::uuid, %s::uuid, %s)
                ON CONFLICT (deal_id, contact_id) DO UPDATE SET is_signatory = EXCLUDED.is_signatory
                """,
                (deal_id, cid, bool(c.get("is_signatory", True))),
            )
    await conn.commit()


# ── Originate inputs (read-only) ─────────────────────────────────────────────────────────────────
# Everything the originate RESOLVER (deals/originate.py) needs to mint a prefilled document — sourced
# from the NEW world only (deal_document_configs + the prefill config + the verbatim mirror), never the
# legacy documenso_templates. The signatory is the ONE deal_contacts row WHERE is_signatory (one prospect
# slot per template); the deterministic ORDER BY matches the sign-token client-email resolver so both
# pick the SAME signatory.
async def get_deal_originate_inputs(conn, handle: str) -> dict | None:
    """Resolve, by public ``deal_handle``: the externalId (``deal_handle``); the attached template's
    numeric id (the deal's ACTIVE ``deal_document_configs.template_documenso_id``); the deal's per-field
    OVERRIDES (``deal_document_configs.field_values``); the operator PREFILL CONFIG for that template
    (``documenso_template_document_prefill_configs.field_settings`` — per-label default + read_only); the
    template's verbatim envelope (``documenso_envelopes.documenso_response`` — its fields[]/recipients[],
    for the prospect-recipient derivation); and the SIGNATORY contact's email + name. ``None`` when the
    handle is unknown; per-field nulls (no attached template / no signatory / no email) are the caller's
    422 to raise. Read-only."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT d.deal_handle,
                   cfg.template_documenso_id,
                   c.email                                                     AS recipient_email,
                   NULLIF(TRIM(CONCAT_WS(' ', c.first_name, c.last_name)), '') AS recipient_name,
                   COALESCE(cfg.field_values, '{}'::jsonb)                     AS field_values,
                   COALESCE(pc.field_settings, '{}'::jsonb)                    AS field_settings,
                   env.documenso_response                                      AS template_response
              FROM business.deals d
         LEFT JOIN business.deal_document_configs cfg
                ON cfg.deal_id = d.id AND cfg.status = 'active'
         LEFT JOIN business.documenso_template_document_prefill_configs pc
                ON pc.template_documenso_id = cfg.template_documenso_id
         LEFT JOIN business.documenso_envelopes env
                ON env.documenso_id = cfg.template_documenso_id
               AND env.type = 'template' AND env.deleted_at IS NULL
         LEFT JOIN LATERAL (
                   SELECT dc.contact_id
                     FROM business.deal_contacts dc
                    WHERE dc.deal_id = d.id AND dc.is_signatory
                    ORDER BY dc.created_at, dc.contact_id
                    LIMIT 1
              ) sig ON true
         LEFT JOIN business.contacts c ON c.id = sig.contact_id
             WHERE d.deal_handle = %s
             LIMIT 1
            """,
            (handle,),
        )
        return await cur.fetchone()
