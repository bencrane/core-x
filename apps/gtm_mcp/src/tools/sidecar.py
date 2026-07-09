"""sidecar — arbitrary read-only SQL over the query-sidecar artifact (bundle cycle).

The query-sidecar is a warm DuckDB endpoint serving the GTM analytical substrate
(~708M rows, 42 sorted tables) in milliseconds-to-seconds. These tools are the
console agent's FAST lane for analytical questions — entities, awards,
transactions-by-recipient, expiring contracts, teaming, capability lookalikes —
and should be preferred over `execute_audience_query` (DuckDB-over-registered-
Lance: no index pushdown) and over any direct Lance scan for questions the
artifact answers.

Navigation map (table catalog, grains, sort keys, patterns):
docs/reference/QUERY_SIDECAR_AGENT_GUIDE.md. Self-serve introspection:
`sidecar_tables()` then `sidecar_sql("DESCRIBE <table>")`.

Not here: full canonical row detail beyond txn_rows' 16 columns, non-GTM domains
(EPA/CMS/MSHA/...), live-freshness reads — those stay on the Lance lanes. The
artifact is the last rebuild's snapshot (stamp returned on every call).

Config: QUERY_SIDECAR_URL + QUERY_SIDECAR_TOKEN env vars (Render service env /
doppler core-x/prd). Tools degrade with a clear error when unset.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

_TIMEOUT_S = 125  # endpoint's own query timeout is 120s


def _call(path: str, payload: dict | None = None) -> dict[str, Any]:
    url = os.environ.get("QUERY_SIDECAR_URL")
    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if not (url and token):
        return {"error": "QUERY_SIDECAR_URL / QUERY_SIDECAR_TOKEN unset on this gateway"}
    req = urllib.request.Request(
        url.rstrip("/") + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"content-type": "application/json", "authorization": f"Bearer {token}"},
        method="POST" if payload is not None else "GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:  # noqa: S310 — config-controlled host
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read()).get("detail", "")
        except Exception:  # noqa: BLE001
            detail = ""
        return {"error": f"sidecar HTTP {exc.code}: {detail}"[:500]}
    except Exception as exc:  # noqa: BLE001
        return {"error": f"sidecar unreachable: {exc}"[:300]}


def register(mcp) -> None:  # noqa: ANN001 — FastMCP instance
    @mcp.tool()
    def sidecar_sql(sql: str, limit: int = 1000) -> dict:
        """Run ONE read-only SQL statement (SELECT/WITH/DESCRIBE/SHOW) against the
        query-sidecar — the FAST lane for GTM analytical questions (~708M rows,
        sorted DuckDB tables, ms-class when filtering on each table's sort key).

        PREFER THIS over execute_audience_query / Lance scans for: entities,
        awards, transactions-by-recipient, expiring contracts, teaming,
        capability lookalikes, people/POC lookups. Join key is `uei` almost
        everywhere. Sort keys that make filters fast: gtm_txn_events_slim
        (uei, action_date) · usaspending_fpds_prime_award_state
        (current_end_date) · txn_rows (action_date) · inferred-code tables
        (code_type, code — filter by code FIRST). Column traps: events_slim uses
        `obligation`/`psc_code`; subaward_canonical_slim's `subaward_amount` is
        VARCHAR (use subaward_amount_num). Full map:
        docs/reference/QUERY_SIDECAR_AGENT_GUIDE.md.

        Args:
            sql: one SELECT/WITH/DESCRIBE/SHOW statement (no semicolons).
            limit: row cap (default 1000, max 50000).

        Returns {columns, rows, row_count, truncated, elapsed_ms, artifact} —
        `artifact` is the snapshot stamp (this is the last rebuild, not live).
        """
        return _call("/api/v1/sql", {"sql": sql, "limit": limit})

    @mcp.tool()
    def sidecar_tables() -> dict:
        """List the query-sidecar's tables with grain source, tier, sort key,
        pinned Lance version, and row count. Use before composing SQL; then
        sidecar_sql("DESCRIBE <table>") for columns."""
        return _call("/api/v1/tables")
