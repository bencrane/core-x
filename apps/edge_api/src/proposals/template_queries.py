"""business.proposal_content_configs persistence — psycopg async.

The authoring registry: drafts the operator edits, published templates the Proposals intake
picker reads, and the slug→template resolution the real proposal-create path uses. The markdown
SOURCE lives here; rendered PDFs never do.
"""
from __future__ import annotations

import re
import secrets
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

_SELECT_COLS = (
    "id, slug, name, status, markdown, apply_brand, token_manifest, monthly_fee_cents, "
    "duration_months, billing_cadence, success_fee_schedule, exec_summary, organization_id, "
    "created_by, created_at, updated_at, published_at"
)
# Same columns, ``pt.``-qualified — for the org-scoped list, where a JOIN to
# business.organizations makes the bare names (id/slug/name/status) ambiguous.
_SELECT_COLS_PT = ", ".join(f"pt.{c.strip()}" for c in _SELECT_COLS.split(","))

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def new_template_id() -> str:
    return "tpl_" + secrets.token_urlsafe(12)


def slugify(name: str) -> str:
    """Operator label → stable URL-safe selector ("Standard Engagement" → "standard-engagement")."""
    return _SLUG_RE.sub("-", name.strip().lower()).strip("-") or "template"


async def create_draft(
    conn,
    *,
    id: str,
    markdown: str,
    apply_brand: bool,
    name: str | None,
    token_manifest: list[str],
    created_by: str | None,
) -> dict[str, Any]:
    sql = f"""
        INSERT INTO business.proposal_content_configs
            (id, status, markdown, apply_brand, token_manifest, name, created_by)
        VALUES (%s, 'draft', %s, %s, %s, %s, %s)
        RETURNING {_SELECT_COLS}
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, (id, markdown, apply_brand, Jsonb(token_manifest), name, created_by))
        row = await cur.fetchone()
    await conn.commit()
    return row


async def update_draft(
    conn,
    id: str,
    *,
    markdown: str | None = None,
    apply_brand: bool | None = None,
    name: str | None = None,
    token_manifest: list[str] | None = None,
) -> dict[str, Any] | None:
    sets: list[str] = ["updated_at = now()"]
    params: list[Any] = []
    if markdown is not None:
        sets.append("markdown = %s")
        params.append(markdown)
    if apply_brand is not None:
        sets.append("apply_brand = %s")
        params.append(apply_brand)
    if name is not None:
        sets.append("name = %s")
        params.append(name)
    if token_manifest is not None:
        sets.append("token_manifest = %s")
        params.append(Jsonb(token_manifest))

    sql = (
        f"UPDATE business.proposal_content_configs SET {', '.join(sets)} "
        f"WHERE id = %s RETURNING {_SELECT_COLS}"
    )
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params + [id])
        row = await cur.fetchone()
    await conn.commit()
    return row


async def get(conn, id: str) -> dict[str, Any] | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_SELECT_COLS} FROM business.proposal_content_configs WHERE id = %s", (id,)
        )
        return await cur.fetchone()


async def get_published_by_slug(conn, slug: str) -> dict[str, Any] | None:
    """Resolve a published template by its selector — the real proposal-create path's lookup."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_SELECT_COLS} FROM business.proposal_content_configs "
            f"WHERE slug = %s AND status = 'published'",
            (slug,),
        )
        return await cur.fetchone()


async def list_all(
    conn,
    *,
    published_only: bool = False,
    org_domain: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Templates newest-first. ``org_domain`` scopes to the issuing org whose
    ``organizations.metadata->>'domain'`` matches (the engagement picker is per
    operator-org: an ``@activeoperators.com`` operator sees only Active Operators
    templates). Unattributed templates (NULL ``organization_id``) are excluded when
    a domain is supplied — the JOIN drops them."""
    clauses: list[str] = []
    params: list[Any] = []
    join = ""
    if published_only:
        clauses.append("pt.status = 'published'")
    if org_domain:
        join = "JOIN business.organizations o ON o.id = pt.organization_id"
        clauses.append("lower(o.metadata->>'domain') = lower(%s)")
        params.append(org_domain)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_SELECT_COLS_PT} FROM business.proposal_content_configs pt {join} {where} "
            f"ORDER BY pt.updated_at DESC LIMIT %s",
            params + [limit],
        )
        return await cur.fetchall()


async def publish(
    conn, id: str, *, name: str, slug: str, monthly_fee_cents: int | None
) -> dict[str, Any] | None:
    """Promote a template to ``published`` with its name + selector. The caller maps a unique-slug
    collision (another template already owns this slug) to a 409."""
    sql = f"""
        UPDATE business.proposal_content_configs
           SET status = 'published', name = %s, slug = %s, monthly_fee_cents = %s,
               published_at = COALESCE(published_at, now()), updated_at = now()
         WHERE id = %s
        RETURNING {_SELECT_COLS}
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, (name, slug, monthly_fee_cents, id))
        row = await cur.fetchone()
    await conn.commit()
    return row
