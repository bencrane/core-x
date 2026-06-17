"""public.operator_settings reads/writes — psycopg async.

One row per operator, keyed by the Supabase ``auth_user_id`` (the JWT sub the BFF validates upstream
and asserts here; edge_api TRUSTS it, never re-validates — the same trust model as agent_runs). Lives
in the SAME Postgres as ``business.*``; this module is edge_api's sole access path to the table. The
table is fully-qualified (``public.operator_settings``) so the search_path is irrelevant.
"""
from __future__ import annotations

from .models import DEFAULT_DIRECT_TO_DOCUMENSO_LANE, DEFAULT_RENDER_MODE


async def get_settings(conn, auth_user_id: str) -> dict:
    """The operator's effective settings. Returns the resolved DEFAULTS (with ``updated_at=None``)
    when no row exists yet, so the BFF always gets a usable pathway and never re-encodes the defaults
    itself. Read-only (no commit); the pool resets the connection on return."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT render_mode, direct_to_documenso_lane, stripe_mode, updated_at
              FROM public.operator_settings
             WHERE auth_user_id = %(auth_user_id)s::uuid
            """,
            {"auth_user_id": auth_user_id},
        )
        row = await cur.fetchone()
    if not row:
        return {
            "render_mode": DEFAULT_RENDER_MODE,
            "direct_to_documenso_lane": DEFAULT_DIRECT_TO_DOCUMENSO_LANE,
            "stripe_mode": None,
            "updated_at": None,
        }
    return {
        "render_mode": row[0],
        "direct_to_documenso_lane": row[1],
        "stripe_mode": row[2],
        "updated_at": row[3],
    }


async def upsert_settings(
    conn,
    *,
    auth_user_id: str,
    render_mode: str | None,
    direct_to_documenso_lane: str | None,
    stripe_mode: str | None,
) -> dict:
    """Merge-upsert the operator's settings and return the resulting effective row.

    A NULL field preserves the existing value on UPDATE and falls to the DB default on INSERT — the
    ``DO UPDATE`` references the BOUND PARAMS directly (NOT ``EXCLUDED``, whose values already had the
    INSERT-branch default applied), so an omitted field is a no-op rather than a reset to default.
    ``stripe_mode`` is nullable (no DB default) — NULL means "follow the STRIPE_MODE env"; an omitted
    field preserves the existing value. Idempotent: re-sending the same body yields the same row.
    """
    params = {
        "auth_user_id": auth_user_id,
        "render_mode": render_mode,
        "direct_to_documenso_lane": direct_to_documenso_lane,
        "stripe_mode": stripe_mode,
        "default_render_mode": DEFAULT_RENDER_MODE,
        "default_lane": DEFAULT_DIRECT_TO_DOCUMENSO_LANE,
    }
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO public.operator_settings
                 (auth_user_id, render_mode, direct_to_documenso_lane, stripe_mode)
            VALUES (
                %(auth_user_id)s::uuid,
                COALESCE(%(render_mode)s, %(default_render_mode)s),
                COALESCE(%(direct_to_documenso_lane)s, %(default_lane)s),
                %(stripe_mode)s
            )
            ON CONFLICT (auth_user_id) DO UPDATE
               SET render_mode = COALESCE(%(render_mode)s, public.operator_settings.render_mode),
                   direct_to_documenso_lane = COALESCE(
                       %(direct_to_documenso_lane)s,
                       public.operator_settings.direct_to_documenso_lane
                   ),
                   stripe_mode = COALESCE(%(stripe_mode)s, public.operator_settings.stripe_mode),
                   updated_at = now()
            RETURNING render_mode, direct_to_documenso_lane, stripe_mode, updated_at
            """,
            params,
        )
        row = await cur.fetchone()
    await conn.commit()
    return {
        "render_mode": row[0],
        "direct_to_documenso_lane": row[1],
        "stripe_mode": row[2],
        "updated_at": row[3],
    }
