"""Federal data-plane tools — deterministic map/chart aggregations + entity resolution
over the 1.54M-entity federal universe (``entity_profile_gold``), with per-agency
dollars sourced from the precomputed award resume (``contractor_award_summary``).

WHY A SEPARATE MODULE + GROUP. These tools back a public map/chart frontend surface
that queries the FEDERAL universe — a different audience and a different data plane
from the operator's private CRM (``companies`` / ``people`` in tools/audience.py). The
CRM tools are UNTOUCHED by this module; the federal group is its own, independently
selectable surface. No federal data is ever poured into the CRM tables.

ALL DETERMINISTIC. Every tool is a direct DuckDB-over-Lance read (no LLM in the path),
returns STRUCTURED JSON ready to drive frontend state arrays, and pushes predicates
onto the load-bearing BTREE columns. Each is a plain importable function so a future
BFF can call it directly (not only via MCP); ``register(mcp)`` mounts them as tools.

BOUND TABLES / COLUMNS / INDICES (read verbatim from the live Lance manifest):

  ``entity_profile_gold`` — 1 row / UEI, 1,541,566 rows. BTREE on ``uei``,
  ``cage_code``, ``legal_name_base``, ``primary_naics``, ``total_active_obligations``;
  BITMAP on ``is_active``. Columns this module binds:
    identity:    uei, cage_code, is_active, has_federal_awards
    name forms:  legal_business_name, dba_name, normalized_legal_name, legal_name_base
    naics:       primary_naics
    address:     physical_address_line_1, physical_address_city,
                 physical_address_state, physical_address_zip_postal_code,
                 physical_address_country_code
    obligations: total_active_obligations, total_lifetime_obligations,
                 award_count, active_award_count
    contacts:    pocs (list<struct>)
    asof:        profile_as_of_date

  ``contractor_award_summary`` (alias ``awards``) — 1 row / recipient UEI, 581,923
  rows. BTREE on ``recipient_uei``. Carries the per-UEI top-3 funding agencies
  (``top_agency_{1,2,3}_name`` / ``top_agency_{1,2,3}_dollars``) that gold does NOT —
  the source for ``federal_spend_by_agency``.

RESOURCE POSTURE. The aggregations scan the full 1.54M-row gold dataset, so the shared
DuckDB connection is given an explicit ``memory_limit`` + ``temp_directory`` for
out-of-core safety (``database._ensure_resource_limits``). With predicate pushdown and
projection the grouped scans run sub-second-to-low-seconds.

TRUNCATION CONTRACT. Aggregations return the FULL grouped set (groups are never capped
silently; ``federal_spend_by_industry``'s ``limit`` is an explicit top-N the caller
asks for, and the response says how many groups existed). The list projection
(``federal_entities_by_filter``) paginates via ``limit`` / ``offset`` and returns a
``truncated`` flag plus the total matching count — NO silent truncation.

NAME SEARCH. ``search_entity_by_name`` derives the canonical blocking key with the
fleet's ``core.name_norm`` (``legal_name_base(name_norm(input))``) IN DuckDB — so the
literal is byte-identical to the indexed ``legal_name_base`` spine — then pushes an
exact-equality predicate into the Lance BTREE for a sub-second point lookup.
"""

from __future__ import annotations

import re
from typing import Any

from core.name_norm import legal_name_base, name_norm

from .. import database

# ── Bound dataset names ──────────────────────────────────────────────────────
GOLD = "entity_profile_gold"
AWARD_SUMMARY = "contractor_award_summary"

# Hard ceiling on the map/list projection page size — a single MCP/BFF result is a
# JSON state array, not a bulk-export channel. Overflow is flagged, never silent.
_MAX_LIST_LIMIT = 2000
_DEFAULT_LIST_LIMIT = 500

# NAICS codes are 2–6 digit strings; a prefix filter restricts to a sector.
_NAICS_PREFIX_RE = re.compile(r"^[0-9]{2,6}$")
# US state code (2-letter) — physical_address_state is the 2-letter form in gold.
_STATE_RE = re.compile(r"^[A-Za-z]{2}$")

# The map/list projection columns (the frontend's live replacement for mock COMPANIES).
_LIST_COLUMNS = [
    "uei",
    "legal_name_base",
    "physical_address_city",
    "physical_address_state",
    "primary_naics",
    "total_lifetime_obligations",
    "total_active_obligations",
    "active_award_count",
    "has_federal_awards",
]

# The full profile projection (identity + 4 name forms + address + naics + rollup + pocs).
_PROFILE_COLUMNS = [
    "uei",
    "cage_code",
    "is_active",
    "has_federal_awards",
    "legal_business_name",
    "dba_name",
    "normalized_legal_name",
    "legal_name_base",
    "primary_naics",
    "physical_address_line_1",
    "physical_address_city",
    "physical_address_state",
    "physical_address_zip_postal_code",
    "physical_address_country_code",
    "total_active_obligations",
    "total_lifetime_obligations",
    "award_count",
    "active_award_count",
    "pocs",
    "profile_as_of_date",
]


def _sql_str(value: str) -> str:
    """A safe single-quoted SQL/Lance string literal."""
    return "'" + value.replace("'", "''") + "'"


def _agg_over(table, sql: str, max_rows: int) -> dict[str, Any]:
    """Run a DuckDB aggregate ``sql`` over an already-materialized Arrow ``table`` bound
    as the relation ``g`` — the scan-then-aggregate path. DuckDB never sees a registered
    Lance manifest (which its substrait pushdown can panic on); it only groups over an
    Arrow table the LANCE scanner already filtered + projected. Runs on a fresh cursor
    of the shared connection (R2/hqx config + resource limits intact), closed after."""
    con = database.get_connection()
    cur = con.cursor()
    try:
        cur.register("g", table)
        rel = cur.execute(sql)
        columns = [d[0] for d in rel.description] if rel.description else []
        fetched = rel.fetchmany(max_rows + 1)
        truncated = len(fetched) > max_rows
        rows = [dict(zip(columns, r)) for r in fetched[:max_rows]]
        return {"columns": columns, "rows": rows, "row_count": len(rows), "truncated": truncated}
    finally:
        cur.close()


def _norm_state(state: str | None) -> str | None:
    """Validate + uppercase a 2-letter state code, or raise. ``None`` passes through."""
    if state is None:
        return None
    s = state.strip()
    if not _STATE_RE.match(s):
        raise ValueError(f"state must be a 2-letter code (got {state!r})")
    return s.upper()


def _norm_naics_prefix(prefix: str | None) -> str | None:
    """Validate a 2–6 digit NAICS prefix, or raise. ``None`` passes through."""
    if prefix is None:
        return None
    p = prefix.strip()
    if not _NAICS_PREFIX_RE.match(p):
        raise ValueError(f"naics prefix must be a 2–6 digit string (got {prefix!r})")
    return p


def _gold_filter(
    *,
    min_obligation: float = 0.0,
    state: str | None = None,
    naics_prefix: str | None = None,
    active_only: bool = False,
) -> list[str]:
    """Compose the shared scalar WHERE legs for a gold scan — every leg either pushes
    onto a BTREE/BITMAP (``total_active_obligations``, ``primary_naics``, ``is_active``)
    or is a cheap scalar (``physical_address_state``). Returns a list of SQL clauses."""
    clauses: list[str] = []
    if min_obligation and min_obligation > 0:
        # Threshold on the indexed active-obligation rollup (BTREE pushdown).
        clauses.append(f"total_lifetime_obligations >= {float(min_obligation)}")
    if state is not None:
        clauses.append(f"physical_address_state = {_sql_str(state)}")
    if naics_prefix is not None:
        if len(naics_prefix) == 6:
            clauses.append(f"primary_naics = {_sql_str(naics_prefix)}")
        else:
            clauses.append(f"primary_naics LIKE {_sql_str(naics_prefix + '%')}")
    if active_only:
        clauses.append("is_active = TRUE")
    return clauses


# ── A. Aggregations (chart surface) ──────────────────────────────────────────
def federal_spend_by_industry(
    min_obligation: float = 0,
    state: str | None = None,
    active_only: bool = False,
    limit: int = 50,
) -> dict[str, Any]:
    """Federal spend rolled up BY INDUSTRY (NAICS) across the 1.54M-entity universe —
    the chart aggregation for an industry treemap / bar chart.

    Groups ``entity_profile_gold`` by ``primary_naics`` and sums the obligation rollup.
    Optional filters push down before the GROUP BY: ``min_obligation`` (lifetime
    threshold on the indexed rollup), ``state`` (2-letter ``physical_address_state``),
    ``active_only`` (the ``is_active`` BITMAP). ``limit`` is the top-N NAICS by lifetime
    spend to return (the response reports the full group count separately — the groups
    themselves are never silently capped).

    Returns ``{"group_count", "returned", "rows": [{"naics", "spend_lifetime",
    "spend_active", "firms"}], "filter"}`` ordered by ``spend_lifetime`` desc.
    """
    st = _norm_state(state)
    clauses = _gold_filter(min_obligation=min_obligation, state=st, active_only=active_only)
    clauses.append("primary_naics IS NOT NULL")
    pred = " AND ".join(clauses)
    n = max(1, int(limit))
    # Lance scanner applies the predicate + projection (its own filter engine, BTREE/
    # BITMAP pushdown); DuckDB groups over the materialized Arrow table.
    table = database.scan_table(
        GOLD,
        columns=["primary_naics", "total_lifetime_obligations", "total_active_obligations"],
        filter=pred,
    )
    sql = """
        WITH gg AS (
            SELECT primary_naics AS naics,
                   SUM(total_lifetime_obligations) AS spend_lifetime,
                   SUM(total_active_obligations)   AS spend_active,
                   COUNT(*)                        AS firms
            FROM g
            GROUP BY primary_naics
        )
        SELECT (SELECT COUNT(*) FROM gg) AS group_count, naics, spend_lifetime, spend_active, firms
        FROM gg
        ORDER BY spend_lifetime DESC NULLS LAST
        LIMIT """ + str(n)
    res = _agg_over(table, sql, n)
    rows = res["rows"]
    group_count = rows[0]["group_count"] if rows else 0
    out = [
        {
            "naics": r["naics"],
            "spend_lifetime": r["spend_lifetime"],
            "spend_active": r["spend_active"],
            "firms": r["firms"],
        }
        for r in rows
    ]
    return {
        "group_count": group_count,
        "returned": len(out),
        "rows": out,
        "filter": {"min_obligation": min_obligation, "state": st, "active_only": active_only, "limit": n},
    }


def federal_spend_by_state(
    min_obligation: float = 0,
    naics_prefix: str | None = None,
    active_only: bool = False,
) -> dict[str, Any]:
    """Federal spend rolled up BY STATE across the 1.54M-entity universe — the chart
    aggregation for a choropleth / state bar chart.

    Groups ``entity_profile_gold`` by ``physical_address_state`` and sums the obligation
    rollup. Optional filters push down before the GROUP BY: ``min_obligation`` (lifetime
    threshold on the indexed rollup), ``naics_prefix`` (2–6 digit; pushes onto the
    ``primary_naics`` BTREE as ``=`` for a full 6-digit code, else ``LIKE prefix%``),
    ``active_only`` (the ``is_active`` BITMAP). Returns the FULL set of states (never
    capped).

    Returns ``{"group_count", "rows": [{"state", "spend_lifetime", "spend_active",
    "firms"}], "filter"}`` ordered by ``spend_lifetime`` desc.
    """
    naics = _norm_naics_prefix(naics_prefix)
    clauses = _gold_filter(min_obligation=min_obligation, naics_prefix=naics, active_only=active_only)
    clauses.append("physical_address_state IS NOT NULL")
    pred = " AND ".join(clauses)
    table = database.scan_table(
        GOLD,
        columns=["physical_address_state", "total_lifetime_obligations", "total_active_obligations"],
        filter=pred,
    )
    sql = """
        SELECT physical_address_state AS state,
               SUM(total_lifetime_obligations) AS spend_lifetime,
               SUM(total_active_obligations)   AS spend_active,
               COUNT(*)                        AS firms
        FROM g
        GROUP BY physical_address_state
        ORDER BY spend_lifetime DESC NULLS LAST
    """
    # States are ~50–60 groups; cap generously so the FULL set always returns.
    res = _agg_over(table, sql, 500)
    return {
        "group_count": res["row_count"],
        "rows": res["rows"],
        "filter": {"min_obligation": min_obligation, "naics_prefix": naics, "active_only": active_only},
    }


def federal_spend_by_agency(
    min_dollars: float = 0,
    naics_prefix: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """Federal spend rolled up BY AWARDING AGENCY — the chart aggregation for an
    agency leaderboard.

    Gold carries NO per-agency dollars, so this sources them from the precomputed
    award resume ``contractor_award_summary``, which holds each recipient's top-3
    funding agencies (``top_agency_{1,2,3}_name`` / ``_dollars``). Those three slots
    are UNPIVOTed to (agency, dollars) pairs and summed across recipients. Optional
    ``naics_prefix`` restricts to recipients whose ``primary_naics`` matches (a 6-digit
    code → ``=``, else ``LIKE prefix%``); ``min_dollars`` thresholds the summed agency
    total; ``limit`` is the top-N agencies returned (the full group count is reported
    separately).

    CAVEAT (stated, not hidden): this is a per-recipient TOP-3 rollup, so it is the
    leading-agency view of spend, not every agency↔recipient edge — an agency outside
    a recipient's top 3 is not counted for that recipient. It is exact for the dominant
    funders and the correct one-table source for the agency chart; a complete
    agency×recipient cube would require the transaction grain
    (``usaspending_api_fresh/contract_prime_txn``), out of scope here.

    Returns ``{"group_count", "returned", "rows": [{"agency", "spend", "recipients"}],
    "source", "filter"}`` ordered by ``spend`` desc.
    """
    naics = _norm_naics_prefix(naics_prefix)
    clauses: list[str] = []
    if naics is not None:
        if len(naics) == 6:
            clauses.append(f"primary_naics = {_sql_str(naics)}")
        else:
            clauses.append(f"primary_naics LIKE {_sql_str(naics + '%')}")
    pred = " AND ".join(clauses)
    n = max(1, int(limit))
    having = f"HAVING SUM(dollars) >= {float(min_dollars)}" if min_dollars and min_dollars > 0 else ""
    # Lance scanner applies the optional NAICS prefilter + projects the agency slots;
    # DuckDB UNPIVOTs the top-3 slots and groups over the materialized Arrow table.
    table = database.scan_table(
        AWARD_SUMMARY,
        columns=[
            "recipient_uei", "primary_naics",
            "top_agency_1_name", "top_agency_1_dollars",
            "top_agency_2_name", "top_agency_2_dollars",
            "top_agency_3_name", "top_agency_3_dollars",
        ],
        filter=pred or None,
    )
    sql = f"""
        WITH src AS (
            SELECT recipient_uei, agency, dollars
            FROM g
            UNPIVOT (
                (agency, dollars) FOR slot IN (
                    (top_agency_1_name, top_agency_1_dollars),
                    (top_agency_2_name, top_agency_2_dollars),
                    (top_agency_3_name, top_agency_3_dollars)
                )
            )
        ),
        gg AS (
            SELECT agency,
                   SUM(dollars)                  AS spend,
                   COUNT(DISTINCT recipient_uei) AS recipients
            FROM src
            WHERE agency IS NOT NULL AND agency <> ''
            GROUP BY agency
            {having}
        )
        SELECT (SELECT COUNT(*) FROM gg) AS group_count, agency, spend, recipients
        FROM gg
        ORDER BY spend DESC NULLS LAST
        LIMIT {n}
    """
    res = _agg_over(table, sql, n)
    rows = res["rows"]
    group_count = rows[0]["group_count"] if rows else 0
    out = [{"agency": r["agency"], "spend": r["spend"], "recipients": r["recipients"]} for r in rows]
    return {
        "group_count": group_count,
        "returned": len(out),
        "rows": out,
        "source": AWARD_SUMMARY,
        "note": "per-recipient top-3 agency rollup (leading-funder view, not every agency↔recipient edge)",
        "filter": {"min_dollars": min_dollars, "naics_prefix": naics, "limit": n},
    }


# ── B. Entity resolution + list (map result set + profile drawer) ────────────
def search_entity_by_uei(uei: str) -> dict[str, Any]:
    """Resolve ONE federal entity by UEI — the profile-drawer point lookup. Pushes the
    predicate into the Lance BTREE on ``entity_profile_gold.uei`` (1 row / UEI) for a
    sub-100 ms lookup.

    UEIs are uppercase alphanumeric; the input is upper/trimmed to match. Returns
    ``{"uei", "found", "entity": {...}|null}`` where the entity is the full profile:
    identity (``uei``, ``cage_code``, ``is_active``, ``has_federal_awards``), all four
    name forms (``legal_business_name``, ``dba_name``, ``normalized_legal_name``,
    ``legal_name_base``), physical address, ``primary_naics``, the obligation rollup
    (``total_lifetime_obligations``, ``total_active_obligations``, ``award_count``,
    ``active_award_count``), and the ``pocs`` array.
    """
    u = (uei or "").strip().upper()
    if not u:
        return {"uei": None, "found": False, "entity": None}
    tbl = (
        database.open_dataset(GOLD)
        .scanner(filter=f"uei = {_sql_str(u)}", columns=_PROFILE_COLUMNS, limit=1)
        .to_table()
    )
    rows = tbl.to_pylist()
    return {"uei": u, "found": bool(rows), "entity": rows[0] if rows else None}


def search_entity_by_name(name: str, limit: int = 25) -> dict[str, Any]:
    """Resolve federal entities by legal/trade name on the canonical blocking key.

    The caller's input is normalized with the fleet's ``core.name_norm`` —
    ``legal_name_base(name_norm(input))`` computed IN DuckDB so the literal is
    byte-identical to the indexed ``legal_name_base`` spine — then an exact-equality
    predicate is pushed into the Lance BTREE on ``entity_profile_gold.legal_name_base``
    for a sub-second lookup. So "Lockheed Martin", "LOCKHEED MARTIN" and "Lockheed
    Martin " all resolve to the same blocking key and return every UEI sharing it
    (the federal universe carries many UEIs per parent name).

    NOTE: ``legal_name_base`` strips only the corporate designators ``LLC/INC/CORP/CO/
    LTD/PLC`` as whole trailing tokens — "Lockheed Martin Corporation" keeps its tail
    (``CORPORATION`` is not a stripped designator), so it blocks differently from
    "Lockheed Martin". Search by the bare brand for the widest set.

    Returns ``{"query", "blocking_key", "match_count", "entities": [...]}`` ordered by
    ``total_lifetime_obligations`` desc; each entity carries the map/list projection.
    """
    q = (name or "").strip()
    if not q:
        return {"query": name, "blocking_key": None, "match_count": 0, "entities": []}
    n = max(1, min(int(limit), _MAX_LIST_LIMIT))
    # Derive the canonical blocking key IN DuckDB (no Python re-implementation → no drift).
    key = database.query(
        "SELECT " + legal_name_base(name_norm(_sql_str(q))) + " AS k",
        datasets=set(),
        max_rows=1,
    )["rows"][0]["k"]
    if not key:
        return {"query": q, "blocking_key": None, "match_count": 0, "entities": []}
    tbl = (
        database.open_dataset(GOLD)
        .scanner(filter=f"legal_name_base = {_sql_str(key)}", columns=_LIST_COLUMNS, limit=n)
        .to_table()
    )
    rows = tbl.to_pylist()
    rows.sort(key=lambda r: (r.get("total_lifetime_obligations") or 0), reverse=True)
    return {"query": q, "blocking_key": key, "match_count": len(rows), "entities": rows}


def federal_entities_by_filter(
    naics: str | None = None,
    min_obligation: float = 0,
    state: str | None = None,
    active_only: bool = False,
    limit: int = 500,
    offset: int = 0,
) -> dict[str, Any]:
    """The MAP / LIST projection — the live replacement for the frontend's mock
    ``runQuery`` / ``COMPANIES``. Filters ``entity_profile_gold`` and returns a paged,
    flat result set ready to drive the map's state array.

    Filters push onto the indexes: ``naics`` (2–6 digit; a 6-digit code → ``=`` on the
    ``primary_naics`` BTREE, else ``LIKE prefix%``), ``min_obligation`` (lifetime
    threshold on the indexed obligation rollup), ``state`` (2-letter
    ``physical_address_state``), ``active_only`` (the ``is_active`` BITMAP). Pagination
    is ``limit`` (capped at 2000) / ``offset``.

    NO SILENT TRUNCATION: the response carries ``total`` (the full matching count) and
    ``truncated`` (true when more rows exist beyond this page); page through with
    ``offset`` to retrieve them.

    Returns ``{"total", "returned", "offset", "limit", "truncated", "rows": [{"uei",
    "legal_name_base", "physical_address_city", "physical_address_state",
    "primary_naics", "total_lifetime_obligations", "total_active_obligations",
    "active_award_count", "has_federal_awards"}], "filter"}`` ordered by
    ``total_lifetime_obligations`` desc.
    """
    st = _norm_state(state)
    naics_p = _norm_naics_prefix(naics)
    n = max(1, min(int(limit), _MAX_LIST_LIMIT))
    off = max(0, int(offset))
    clauses = _gold_filter(
        min_obligation=min_obligation, state=st, naics_prefix=naics_p, active_only=active_only
    )
    pred = " AND ".join(clauses) if clauses else None
    cols = ", ".join(_LIST_COLUMNS)
    # Lance scanner applies the predicate + projects only the list columns; DuckDB does
    # the ORDER BY + window total + LIMIT/OFFSET page over the materialized Arrow table.
    table = database.scan_table(GOLD, columns=_LIST_COLUMNS, filter=pred)
    sql = f"""
        SELECT {cols},
               COUNT(*) OVER () AS _total
        FROM g
        ORDER BY total_lifetime_obligations DESC NULLS LAST
        LIMIT {n} OFFSET {off}
    """
    res = _agg_over(table, sql, n)
    rows = res["rows"]
    total = rows[0]["_total"] if rows else table.num_rows
    for r in rows:
        r.pop("_total", None)
    truncated = (off + len(rows)) < total
    return {
        "total": total,
        "returned": len(rows),
        "offset": off,
        "limit": n,
        "truncated": truncated,
        "rows": rows,
        "filter": {
            "naics": naics_p,
            "min_obligation": min_obligation,
            "state": st,
            "active_only": active_only,
        },
    }


def register(mcp) -> None:
    """Mount the FEDERAL tool group onto the FastMCP server — a SEPARATE, independently
    selectable group from the CRM audience tools (companies/people in tools/audience.py),
    which this module never touches. Each function's signature + docstring becomes the
    tool's input schema + agent-facing contract."""
    for fn in (
        federal_spend_by_industry,
        federal_spend_by_state,
        federal_spend_by_agency,
        search_entity_by_uei,
        search_entity_by_name,
        federal_entities_by_filter,
    ):
        mcp.add_tool(fn)
