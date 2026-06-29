"""business.documenso_template_document_prefill_configs reads/writes + template value-field reads.

Two data sources, ONE owner each:

* TEMPLATE VALUE FIELDS are read off the verbatim envelope MIRROR
  (``business.documenso_envelopes``, ``type='template'``). The mirror's ``documenso_response`` is the
  FULL Documenso envelope stored exactly as the API returns it; the value-bearing fields are the TEXT/
  NUMBER fields that carry a non-null ``fieldMeta.label``. Read-only here.

* PREFILL CONFIG is the operator-owned editor state in
  ``business.documenso_template_document_prefill_configs`` (one row per template, keyed by
  ``template_documenso_id``). ``field_settings`` is keyed by field LABEL; each value is an ARBITRARY
  object stored verbatim (Phase 1 sets ``default_document_field_value`` + ``read_only``; Phase 2 will
  add ``source`` — both pass through untouched). This module is the SOLE writer of that table; the
  webhook projector / resync NEVER touch it.
"""
from __future__ import annotations

from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


async def get_template_value_fields(conn, documenso_id: int) -> list[dict]:
    """The value-bearing fields of a mirrored TEMPLATE, in field order.

    Reads ``documenso_response->'fields'`` off the verbatim mirror row for ``documenso_id`` (a
    ``type='template'`` envelope) and keeps only TEXT/NUMBER fields that carry a non-null
    ``fieldMeta.label``. ``read_only``/``required`` are passed through verbatim from ``fieldMeta``
    (booleans, no transform). Read-only; touches ONLY business.documenso_envelopes.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT
                fld->'fieldMeta'->>'label'              AS label,
                fld->>'type'                            AS type,
                (fld->'fieldMeta'->>'required')::bool   AS required,
                (fld->'fieldMeta'->>'readOnly')::bool   AS read_only,
                (fld->>'recipientId')::bigint           AS recipient_id
            FROM business.documenso_envelopes env
            CROSS JOIN LATERAL jsonb_array_elements(
                COALESCE(env.documenso_response->'fields', '[]'::jsonb)
            ) AS fld
            WHERE env.documenso_id = %(documenso_id)s
              AND env.type = 'template'
              AND env.deleted_at IS NULL
              AND fld->>'type' IN ('TEXT', 'NUMBER')
              AND fld->'fieldMeta'->>'label' IS NOT NULL
            """,
            {"documenso_id": documenso_id},
        )
        return await cur.fetchall()


async def get_prefill_config(conn, documenso_id: int) -> dict:
    """The saved ``field_settings`` for ``documenso_id``, or ``{}`` when no config row exists yet.

    Read-only (no commit); touches ONLY business.documenso_template_document_prefill_configs.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT field_settings
              FROM business.documenso_template_document_prefill_configs
             WHERE template_documenso_id = %(documenso_id)s
            """,
            {"documenso_id": documenso_id},
        )
        row = await cur.fetchone()
    if not row or row[0] is None:
        return {}
    return row[0]


async def upsert_prefill_config(
    conn, documenso_id: int, field_settings: dict[str, Any]
) -> dict:
    """UPSERT the operator-owned prefill config for ``documenso_id`` and return the saved
    ``field_settings`` (stored verbatim — arbitrary per-label objects pass through). One row per
    template, keyed on the ``template_documenso_id`` UNIQUE. COMMITS. Sole writer of this table.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO business.documenso_template_document_prefill_configs
                 (template_documenso_id, field_settings)
            VALUES (%(documenso_id)s, %(field_settings)s)
            ON CONFLICT (template_documenso_id) DO UPDATE
               SET field_settings = EXCLUDED.field_settings,
                   updated_at      = now()
            RETURNING field_settings
            """,
            {
                "documenso_id": documenso_id,
                "field_settings": Jsonb(field_settings),
            },
        )
        row = await cur.fetchone()
    await conn.commit()
    return row[0]
