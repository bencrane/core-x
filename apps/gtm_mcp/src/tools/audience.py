"""Audience tools — semantic point-lookups (Lance index pushdown) and dynamic
audience querying (raw DuckDB SQL) over the Gen-3 R2 sink.

Two access shapes, one sink:

  • Point-lookups push their predicate straight into ``lance.dataset(...).scanner(
    filter=...)`` so the load-bearing ``BTREE`` answers in sub-100 ms — no full
    scan, no DuckDB round-trip. Indexed anchors: ``companies.normalized_domain``,
    ``people.normalized_domain`` / ``people.company_id``,
    ``awards.recipient_uei``.

  • ``search_company_by_name`` matches on the canonical blocking key. Per the
    directive, ``core.name_norm`` builds a *DuckDB SQL* expression (not a Python
    string), so name-matching runs in DuckDB with ``name_norm`` applied as a SQL
    literal on both sides of the predicate — guaranteeing byte-identical
    normalization to every spine in ``pipelines/``.

  • ``execute_audience_query`` is the general escape hatch: arbitrary ANSI SQL
    over the full runtime-discovered Lance plane (``companies`` / ``people`` /
    ``awards`` and ~100 more — see ``list_datasets``), plus raw ``s3://`` Parquet,
    for cross-layer segment building. Only the datasets a query references are
    attached to DuckDB (JIT), so the plane stays sub-second.

Tool docstrings below are the agent-facing contracts — the MCP client shows them
verbatim, so they describe inputs, the matching semantics, and the shape returned.
"""

from __future__ import annotations

import re
from typing import Any

from core.name_norm import name_norm

from .. import database
from . import lookup_cache

# Per-lookup row ceiling. A normalized_domain is NOT unique (e.g. jpmorgan.com
# resolves to several source-platform rows), so a domain lookup legitimately
# returns a small set; this caps pathological fan-out.
_LOOKUP_LIMIT = 50

# Domain anchor normalization — mirrors pipelines/gtm/companies_people_bulk.py
# `_normalized_domain` EXACTLY so a caller's raw input collapses to the same
# stored anchor the BTREE is built on. lower/trim → strip scheme → strip leading
# www. → strip path/query → strip trailing dots → empty becomes None.
_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")


def _normalize_domain(raw: str) -> str | None:
    d = (raw or "").strip().lower()
    d = _SCHEME.sub("", d)
    d = _WWW.sub("", d)
    d = d.split("/", 1)[0]
    d = d.rstrip(".")
    return d or None


def _sql_str(value: str) -> str:
    """A safe single-quoted SQL/Lance string literal."""
    return "'" + value.replace("'", "''") + "'"


# Documented agent-facing column contracts — the EXACT projected sets the point-lookup
# docstrings promise (and the benchmark's COMPANY_DOC/PEOPLE_DOC golden keys). Passed as
# the Lance scanner ``columns=`` so only these are read/deserialized from R2 instead of the
# full wide row; the wide undocumented filler never crosses the object-store boundary. These
# lists ARE the contract — adding a documented column means editing both the docstring and
# this list. Order matches the docstrings.
_COMPANY_COLUMNS = [
    "company_id", "company_name", "normalized_domain",
    "company_linkedin_url", "source_platform",
]
_PEOPLE_COLUMNS = [
    "person_id", "company_id", "normalized_domain", "full_name",
    "first_name", "last_name", "title", "person_linkedin_url", "source_platform",
]


# ── Cache key normalizers — collapse each lookup's arg to the SAME stored anchor the
# function itself normalizes to, so casing/whitespace variants share one cache entry and the
# cached result is byte-identical to the uncached one. Mirrors _normalize_domain / the UEI
# upper-trim below; an empty/None arg keys deterministically (its early-return result is the
# same empty-shape dict, so caching it stays transparent).
def _domain_key(domain: str) -> str:
    return _normalize_domain(domain) or ""


def _uei_key(recipient_uei: str) -> str:
    return (recipient_uei or "").strip().upper()


# ── Semantic point-lookups (Lance BTREE pushdown, sub-100 ms) ────────────────
@lookup_cache.memoize(ttl_s=lookup_cache.COMPANY_TTL_S, key_fn=_domain_key)
def search_company_by_domain(domain: str) -> dict[str, Any]:
    """Look up companies by web domain. Pushes the predicate into the Lance
    BTREE on ``companies.normalized_domain`` for a sub-100 ms point-lookup.

    The input is normalized to the stored anchor (scheme / ``www.`` / path /
    casing stripped), so ``https://www.JPMorgan.com/about`` and ``jpmorgan.com``
    resolve identically. A domain is not unique (multiple source platforms may
    carry the same company), so this returns every matching company row
    (capped at 50).

    Returns ``{"normalized_domain", "match_count", "companies": [...]}``; each
    company has ``company_id, company_name, normalized_domain,
    company_linkedin_url, source_platform``.
    """
    norm = _normalize_domain(domain)
    if not norm:
        return {"normalized_domain": None, "match_count": 0, "companies": []}
    tbl = (
        database.open_dataset("companies")
        .scanner(
            filter=f"normalized_domain = {_sql_str(norm)}",
            columns=_COMPANY_COLUMNS,
            limit=_LOOKUP_LIMIT,
        )
        .to_table()
    )
    rows = tbl.to_pylist()
    return {"normalized_domain": norm, "match_count": len(rows), "companies": rows}


@lookup_cache.memoize(ttl_s=lookup_cache.PEOPLE_TTL_S, key_fn=_domain_key)
def search_people_by_domain(domain: str) -> dict[str, Any]:
    """Look up people (contacts) by their company's web domain. Pushes the
    predicate into the Lance BTREE on ``people.normalized_domain`` (denormalized
    from the person's company) for a sub-100 ms point-lookup.

    Input normalization matches ``search_company_by_domain``. Returns
    ``{"normalized_domain", "match_count", "people": [...]}``; each person has
    ``person_id, contact_id, company_id, normalized_domain, full_name, first_name,
    last_name, title, person_linkedin_url, source_platform`` (capped at 50).
    ``person_id`` mirrors ``contact_id`` (both are the person's id; ``contact_id``
    is retained for backward compatibility).
    """
    norm = _normalize_domain(domain)
    if not norm:
        return {"normalized_domain": None, "match_count": 0, "people": []}
    tbl = (
        database.open_dataset("people")
        .scanner(
            filter=f"normalized_domain = {_sql_str(norm)}",
            columns=_PEOPLE_COLUMNS,
            limit=_LOOKUP_LIMIT,
        )
        .to_table()
    )
    rows = tbl.to_pylist()
    # Back-compat: surface `contact_id` mirroring `person_id` (there is no physical
    # contact_id column to read — the response value is the person_id).
    for r in rows:
        r["contact_id"] = r.get("person_id")
    return {"normalized_domain": norm, "match_count": len(rows), "people": rows}


@lookup_cache.memoize(ttl_s=lookup_cache.AWARDS_TTL_S, key_fn=_uei_key)
def lookup_awards_by_uei(recipient_uei: str) -> dict[str, Any]:
    """Fetch the precomputed federal-spend resume for one recipient UEI. Pushes
    the predicate into the Lance BTREE on ``awards.recipient_uei``
    (contractor_award_summary, one row per UEI) for a sub-100 ms point-lookup.

    UEIs are uppercase alphanumeric; the input is upper/trimmed to match. Returns
    ``{"recipient_uei", "found", "award_summary": {...}|null}`` where the summary
    carries lifetime/active/closed prime + subaward dollars and counts, the
    combined total, date span, dollar buckets (contract/grant/other), and the
    top-3 funding agencies.
    """
    uei = (recipient_uei or "").strip().upper()
    if not uei:
        return {"recipient_uei": None, "found": False, "award_summary": None}
    tbl = (
        database.open_dataset("awards")
        .scanner(filter=f"recipient_uei = {_sql_str(uei)}", limit=1)
        .to_table()
    )
    rows = tbl.to_pylist()
    return {
        "recipient_uei": uei,
        "found": bool(rows),
        "award_summary": rows[0] if rows else None,
    }


# ── Name matching (canonical blocking key, applied in DuckDB) ────────────────
def search_company_by_name(name: str) -> dict[str, Any]:
    """Look up companies by legal/trade name on the canonical cross-spine
    blocking key. Both the stored ``company_name`` and the caller's input are run
    through ``core.name_norm`` (UPPER → ``&``→AND → dash-split → strip punctuation
    → collapse whitespace) so "Acme, Inc." matches "ACME INC" — the same
    normalization every resolution spine in ``pipelines/`` uses.

    Note: companies carry a BTREE on ``normalized_domain``, not on the name, so
    this is a normalized full scan of a small dimension (not an index lookup);
    prefer ``search_company_by_domain`` when a domain is known. Returns
    ``{"query_name", "match_count", "companies": [...]}`` (capped at 50).
    """
    if not (name or "").strip():
        return {"query_name": name, "match_count": 0, "companies": []}
    # name_norm builds a DuckDB SQL expression. Apply it to the column AND to the
    # input wrapped as a SQL string literal — never compare raw Python strings.
    predicate = f"{name_norm('company_name')} = {name_norm(_sql_str(name.strip()))}"
    sql = f"""
        SELECT company_id, company_name, normalized_domain,
               company_linkedin_url, source_platform
        FROM companies
        WHERE {predicate}
        LIMIT {_LOOKUP_LIMIT}
    """
    # Only the companies relation is needed — bind it explicitly (JIT) rather than
    # the whole plane, so this stays a one-manifest open.
    result = database.query(sql, datasets={"companies"}, max_rows=_LOOKUP_LIMIT)
    return {
        "query_name": name,
        "match_count": result["row_count"],
        "companies": result["rows"],
    }


# ── Dynamic audience querying (raw DuckDB SQL) ───────────────────────────────
def execute_audience_query(sql: str) -> dict[str, Any]:
    """Run arbitrary read-only ANSI SQL over the full Gen-3 Lance plane to build
    audience segments. The datasets are named relations — reference them by name;
    every committed dataset in the sink is available (the registry is discovered
    at runtime, not a fixed list). Call ``list_datasets`` for the names (with column
    counts), then ``describe_dataset(name)`` for a dataset's columns. Operational
    Postgres state is reachable too: ``list_postgres_tables`` / ``get_postgres_schema``
    and ``hqx.<schema>.<table>`` joins (see those tools).

    Naming: a flat dataset is a bare identifier (``companies``, ``people``,
    ``firmographics_blitz``); ``awards`` is an alias for ``contractor_award_summary``.
    A dataset nested under a source namespace is named by its path and MUST be
    double-quoted in SQL, e.g. ``FROM "usaspending/award_search"``,
    ``"fmcsa/carrier"``.

    Only the datasets your query references are attached to DuckDB for the call —
    a two-table join opens two Lance manifests, not the ~100-dataset plane — so
    cross-layer joins stay fast. Raw transport Parquet is also reachable via
    ``read_parquet('s3://data-sink/...')``. Cross-layer joins are the point — e.g.
    companies ⋈ awards on a domain→UEI bridge to segment contractors by spend.
    The result is capped at 1000 rows (``truncated`` flags overflow). Returns
    ``{"columns", "rows", "row_count", "truncated"}``.
    """
    # The performance gate: resolve which registered datasets the SQL names, and
    # bind ONLY those (never the whole catalog) before handing the SQL to DuckDB.
    return database.query(sql, datasets=database.referenced_datasets(sql))


def register(mcp) -> None:
    """Mount the audience tools onto the FastMCP server. Each function's
    signature + docstring becomes the tool's input schema + agent-facing contract."""
    for fn in (
        search_company_by_domain,
        search_people_by_domain,
        lookup_awards_by_uei,
        search_company_by_name,
        execute_audience_query,
    ):
        mcp.add_tool(fn)
