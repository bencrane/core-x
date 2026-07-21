"""Content-source registry — ``business.global_input_content`` reads.

Each row names WHERE the render+push lane pulls content from: ``brand`` (content root) + ``path``
(brand-relative ``<family>/<archetype>/<version>`` for repo-html, or a ``global_agreement_content``
slug for db-markdown) + ``source_kind``. The renderer (``push.py``) resolves a row into HTML.

DDL: ``apps/edge_api/sql/global_input_content.sql``. Read-only here — rows are seeded by the DDL.
"""
from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row

_COLS = "id, brand, path, name, source_kind, status, created_at, updated_at"


async def get_by_path(conn, path: str) -> dict[str, Any] | None:
    """The registry row for a brand-relative ``path`` (UNIQUE), or None."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLS} FROM business.global_input_content WHERE path = %s",
            (path,),
        )
        return await cur.fetchone()


async def get_by_id(conn, row_id: str) -> dict[str, Any] | None:
    """The registry row by its uuid id, or None."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLS} FROM business.global_input_content WHERE id = %s::uuid",
            (row_id,),
        )
        return await cur.fetchone()


async def list_active(conn) -> list[dict[str, Any]]:
    """Every active registry row, newest first — the operator's 'choose where to pull from' menu."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLS} FROM business.global_input_content "
            "WHERE status = 'active' ORDER BY created_at DESC"
        )
        return list(await cur.fetchall())
