"""Operational Postgres tools — the gateway's structured write + introspect surface
over the attached hq-x control-plane database (Directive 18 §2).

database.py ATTACHes hq-x to the shared DuckDB connection as the ``hqx`` catalog, so
an agent can JOIN Lance R2 datasets against live ``hqx.ops.*`` state inside
``execute_audience_query`` (the read/join path). This module adds the two EXPLICIT,
STRUCTURED operational tools the directive mandates — never raw, unchecked mutation:

  • ``save_campaign_audience`` — upsert a campaign-audience tracking row into
    ``ops.campaign_audiences``. The sole write path into hq-x.
  • ``list_postgres_tables`` — cheap "look around": the table names (+ column counts)
    in a schema, no column detail. The progressive-disclosure entry point.
  • ``get_postgres_schema`` — drill into one table's columns (or, heavy, a whole
    schema's) via DuckDB's ``information_schema``.

Introspection is two-level by design: an agent calls ``list_postgres_tables`` to see
what exists, then ``get_postgres_schema(schema, table)`` for just the table(s) it
needs — instead of dumping every column of every table (``ops`` alone is ~44 tables /
hundreds of columns) in one oversized result.

WHY psycopg FOR THE WRITE (not the DuckDB attach). The DuckDB postgres writer has no
clean ``ON CONFLICT`` upsert and only coarse transaction control. The established
control-plane convention (pipelines/*) writes ``ops.*`` state with psycopg:
ensure-schema DDL + a native ``INSERT … ON CONFLICT DO UPDATE`` inside one
transaction. We mirror it exactly. The DuckDB attach owns the read/join path; psycopg
owns the structured write path — the same split materialize_blitz.py uses (ATTACH
READ_ONLY to project, psycopg to record run state). The two connections are
independent, so the write never depends on the DuckDB attach being live.

SAFETY (Directive 18 §4). The only write is this PARAMETERIZED upsert inside an
explicit ``conn.transaction()`` boundary; ``source_query`` and ``parameters`` are
stored as DATA, never executed. Arbitrary ``DROP`` / ``ALTER`` / structural mutation
has no path here, and the other DB entry point — ``execute_audience_query`` — is
read-only-gated in ``database.query`` (``assert_read_only``)."""

from __future__ import annotations

import re
from typing import Any

from .. import database

# ── ops.campaign_audiences DDL ───────────────────────────────────────────────
# Mirrors the house ops.*_runs idiom (GENERATED IDENTITY PK, jsonb payload,
# timestamptz audit cols, helpful secondary indexes). Applied idempotently before
# each write so the tracking table self-bootstraps — no out-of-band migration. The
# (campaign_id, audience_name) uniqueness is the upsert conflict target: re-saving
# an audience for a campaign updates the definition in place.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.campaign_audiences (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    campaign_id    text        NOT NULL,
    audience_name  text        NOT NULL,
    source_query   text        NOT NULL,
    parameters     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (campaign_id, audience_name)
);

CREATE INDEX IF NOT EXISTS campaign_audiences_campaign_id_idx ON ops.campaign_audiences (campaign_id);
CREATE INDEX IF NOT EXISTS campaign_audiences_updated_at_idx  ON ops.campaign_audiences (updated_at DESC);
"""

# Upsert on the natural key. ``(xmax = 0)`` is the canonical Postgres tell for
# insert-vs-update from a single ON CONFLICT statement: a freshly inserted row has
# xmax 0, an updated one carries the locking xid.
_UPSERT_SQL = """
INSERT INTO ops.campaign_audiences (campaign_id, audience_name, source_query, parameters)
VALUES (%s, %s, %s, %s)
ON CONFLICT (campaign_id, audience_name) DO UPDATE
   SET source_query = EXCLUDED.source_query,
       parameters   = EXCLUDED.parameters,
       updated_at    = now()
RETURNING id, campaign_id, audience_name, created_at, updated_at, (xmax = 0) AS inserted;
"""

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _iso(value: Any) -> Any:
    """timestamptz → ISO string for the JSON tool channel; pass non-datetimes through."""
    return value.isoformat() if hasattr(value, "isoformat") else value


def save_campaign_audience(
    campaign_id: str,
    audience_name: str,
    source_query: str,
    parameters: dict | None = None,
) -> dict[str, Any]:
    """Persist (insert or update) a campaign-audience tracking row in the hq-x
    control plane (``ops.campaign_audiences``). This is the structured, audited way
    to register the audience segment that backs a campaign — no raw SQL mutation.

    Args:
      campaign_id: the campaign this audience belongs to (your campaign key).
      audience_name: a name for the segment, unique within the campaign. Re-saving
        the same (campaign_id, audience_name) UPDATES the stored definition in place.
      source_query: the ANSI SQL (as you'd pass to ``execute_audience_query``) that
        resolves the audience rows. Stored verbatim as a definition — it is recorded,
        NOT executed by this tool.
      parameters: optional JSON object of segment parameters (filters, thresholds,
        provenance) stored as ``jsonb``.

    The write is a parameterized upsert inside one transaction (Directive 18 §4).
    Returns ``{"status": "ok", "table", "operation": "inserted"|"updated", "row":
    {id, campaign_id, audience_name, created_at, updated_at}}``.
    """
    cid = (campaign_id or "").strip()
    aname = (audience_name or "").strip()
    squery = (source_query or "").strip()
    if not cid or not aname or not squery:
        raise ValueError("campaign_id, audience_name, and source_query are required")
    params = parameters if parameters is not None else {}
    if not isinstance(params, dict):
        raise ValueError("parameters must be a JSON object (dict)")

    dsn = database.hqx_dsn()
    if not dsn:
        raise RuntimeError(
            "HQX_DB_URL_POOLED is not set — cannot reach hq-x to save the audience."
        )

    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb

    # Explicit transaction boundary: ensure-schema DDL + the upsert commit atomically,
    # or roll back together (Directive 18 §4). Own psycopg connection — independent of
    # the DuckDB attach.
    with psycopg.connect(dsn) as conn:
        with conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(OPS_DDL)
                cur.execute(_UPSERT_SQL, (cid, aname, squery, Jsonb(params)))
                row = cur.fetchone()

    return {
        "status": "ok",
        "table": "hqx.ops.campaign_audiences",
        "operation": "inserted" if row["inserted"] else "updated",
        "row": {
            "id": row["id"],
            "campaign_id": row["campaign_id"],
            "audience_name": row["audience_name"],
            "created_at": _iso(row["created_at"]),
            "updated_at": _iso(row["updated_at"]),
        },
    }


_SYSTEM_SCHEMAS = "('pg_catalog', 'information_schema')"  # never interesting to a GTM agent


def _sql_str(value: str) -> str:
    """Single-quoted SQL string literal (validated identifiers; escaped as belt-and-suspenders)."""
    return "'" + value.replace("'", "''") + "'"


def _not_attached(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        **payload,
        "attached": False,
        "detail": "hq-x is not attached (HQX_DB_URL_POOLED unset or attach failed).",
    }


def list_postgres_tables(schema_name: str = "ops") -> dict[str, Any]:
    """Look around the attached hq-x Postgres CHEAPLY: list the table NAMES (with a
    column count each) in a schema — no column detail. **Start here**, then call
    ``get_postgres_schema(schema_name, table_name)`` to pull the columns of only the
    table(s) you actually need, instead of dumping every column of every table.

    ``schema_name`` defaults to ``'ops'`` (the operational state: ``campaign_audiences``
    and the ``*_runs`` ledgers). ``available_schemas`` is returned too, so you can
    pivot to another schema without guessing.

    Returns ``{"database": "hqx", "attached", "schema", "available_schemas": [...],
    "table_count", "tables": [{"name", "column_count"}]}`` (tables sorted by name).
    When hq-x is not attached, returns ``attached=false`` with a ``detail`` message.
    """
    schema = (schema_name or "").strip()
    if not _IDENT.fullmatch(schema):
        raise ValueError("schema_name must be a bare SQL identifier, e.g. 'ops'")

    database.get_connection()  # ensure the hqx attach has been attempted
    if not database.hqx_attached():
        return _not_attached(
            {"database": "hqx", "schema": schema, "available_schemas": [], "table_count": 0, "tables": []}
        )

    available = [
        r["table_schema"]
        for r in database.query(
            "SELECT DISTINCT table_schema FROM information_schema.tables "
            f"WHERE table_catalog = 'hqx' AND table_schema NOT IN {_SYSTEM_SCHEMAS} "
            "ORDER BY table_schema"
        )["rows"]
    ]
    rows = database.query(
        "SELECT table_name, count(*) AS column_count FROM information_schema.columns "
        f"WHERE table_catalog = 'hqx' AND table_schema = {_sql_str(schema)} "
        "GROUP BY table_name ORDER BY table_name"
    )["rows"]
    tables = [{"name": r["table_name"], "column_count": int(r["column_count"])} for r in rows]
    return {
        "database": "hqx",
        "attached": True,
        "schema": schema,
        "available_schemas": available,
        "table_count": len(tables),
        "tables": tables,
    }


def get_postgres_schema(schema_name: str, table_name: str | None = None) -> dict[str, Any]:
    """Describe the columns of attached hq-x Postgres tables, via DuckDB's
    ``information_schema``. Reference the results in ``execute_audience_query`` as
    ``hqx.<schema>.<table>`` (e.g. ``FROM companies c LEFT JOIN hqx.ops.exclusions e``).

    Pass ``table_name`` to get just THAT table's columns — the targeted drill-down,
    and the preferred path: call ``list_postgres_tables`` first to find the table, then
    this with the name. Returns ``{"database", "attached", "schema", "table", "found",
    "column_count", "columns": [{"name", "type"}]}``.

    Omit ``table_name`` to get EVERY table in the schema with all columns. This is the
    heavy form (e.g. ``ops`` is ~44 tables / hundreds of columns) — prefer the
    targeted form above unless you truly need the whole schema. Returns ``{"database",
    "attached", "schema", "table_count", "tables": [{"name", "columns": [...]}]}``.

    When hq-x is not attached, returns ``attached=false`` with a ``detail`` message.
    """
    schema = (schema_name or "").strip()
    if not _IDENT.fullmatch(schema):
        raise ValueError("schema_name must be a bare SQL identifier, e.g. 'ops'")
    table = (table_name or "").strip() or None
    if table is not None and not _IDENT.fullmatch(table):
        raise ValueError("table_name, when given, must be a bare SQL identifier")

    database.get_connection()  # ensure the hqx attach has been attempted
    if not database.hqx_attached():
        base = {"database": "hqx", "schema": schema}
        return _not_attached(
            {**base, "table": table, "found": False, "column_count": 0, "columns": []}
            if table is not None
            else {**base, "table_count": 0, "tables": []}
        )

    where = f"table_catalog = 'hqx' AND table_schema = {_sql_str(schema)}"
    if table is not None:
        where += f" AND table_name = {_sql_str(table)}"
    rows = database.query(
        "SELECT table_name, column_name, data_type FROM information_schema.columns "
        f"WHERE {where} ORDER BY table_name, ordinal_position"
    )["rows"]

    tables: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        tables.setdefault(r["table_name"], []).append(
            {"name": r["column_name"], "type": r["data_type"]}
        )

    if table is not None:
        cols = tables.get(table, [])
        return {
            "database": "hqx",
            "attached": True,
            "schema": schema,
            "table": table,
            "found": bool(cols),
            "column_count": len(cols),
            "columns": cols,
        }
    return {
        "database": "hqx",
        "attached": True,
        "schema": schema,
        "table_count": len(tables),
        "tables": [{"name": name, "columns": cols} for name, cols in sorted(tables.items())],
    }


def register(mcp) -> None:
    """Mount the operational Postgres tools onto the FastMCP server. Each function's
    signature + docstring becomes the tool's input schema + agent-facing contract."""
    for fn in (save_campaign_audience, list_postgres_tables, get_postgres_schema):
        mcp.add_tool(fn)
