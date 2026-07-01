"""Batched multi-id lookups — ONE `IN (…)` Lance scan per audience batch.

WHY. An audience workflow resolves MANY ids at once: a list of recipient UEIs, a
list of company domains. Calling the single-id point-lookups in ``audience.py`` /
``sam_entities.py`` N times is N separate Lance scanner invocations — N BTREE index
probes = **N R2 round-trips**. The prior retrieval-opt cycle's finding: the cost of
a lookup is native scan + R2 object-store I/O *per call*, so the single biggest real
lever is collapsing N point-lookups into ONE scan with an ``<col> IN (<lits>)``
predicate over the same BTREE-indexed column. One round-trip amortizes the I/O across
the whole audience.

THE LOAD-BEARING INVARIANT: **exactly ONE ``.scanner()`` call per batched tool,
regardless of N.** Each tool normalizes + de-dups its id list, caps it at
``_IN_LIST_CAP`` unique ids, builds ONE ``IN (…)`` predicate, issues ONE
``database.open_dataset(name).scanner(filter=…, columns=…, limit=…).to_table()
.to_pylist()``, and groups the rows by the requested id in Python — never a per-id
loop. An empty / whitespace-only batch issues NO scan and returns the empty shape.

These mirror ``audience.py`` / ``sam_entities.py`` conventions exactly: the domain
tools reuse ``audience._normalize_domain`` (so a caller's raw input collapses to the
same stored anchor the BTREE is built on) and ``audience._sql_str`` for literal
escaping; the projected tools reuse the documented column contracts and a
``_present``-style schema-intersect so an undocumented wide column never crosses the
R2 boundary and a documented column absent from a prod partition never makes the
scanner raise. Awards is returned WHOLE-ROW by contract (no projection).

Tool docstrings below are the agent-facing contracts — the MCP client shows them
verbatim, so they describe inputs, the one-scan/IN-pushdown semantics, and the shape
returned.
"""

from __future__ import annotations

from typing import Any

from .. import database
from . import audience, sam_entities

# Hard ceiling on the IN-list: a batch larger than this is SLICED to the cap (never
# rejected, never raised) — the predicate grammar and a single scan stay bounded no
# matter how large the caller's audience. The directive's "sane bound (e.g. 1000)".
_IN_LIST_CAP = 1000

# Per-scan row ceiling. A batched scan can legitimately return many rows: up to the
# cap for the unique-per-id datasets (awards/sam), and several rows per domain for the
# non-unique domain datasets (companies/people). Size the limit so the whole capped
# batch's matches deserialize in the one scan — generous headroom over the id cap.
_SCAN_LIMIT = 50_000

# Documented agent-facing column contracts for the PROJECTED datasets — the EXACT sets
# the tools promise (and the benchmark's COMPANY_DOC / PEOPLE_DOC golden keys). Passed
# as the Lance scanner ``columns=`` so only these deserialize from R2; the wide
# undocumented filler never crosses the object-store boundary. Reused from audience.py /
# sam_entities.py so there is ONE source of truth per contract. Awards is whole-row
# (no projection) by contract.
_COMPANY_DOC = audience._COMPANY_COLUMNS
_PEOPLE_DOC = audience._PEOPLE_COLUMNS
_SAM_ENTITY_DOC = sam_entities.SAM_ENTITY_DOC


def _present(name: str, columns: list[str]) -> list[str]:
    """Intersect a documented column list with the dataset's actual schema, preserving
    documented order. Prod partitions vary in width, so a documented column absent from
    the live schema must never make the scanner raise — project only what exists.
    (Same shape as ``sam_entities._present``.)"""
    have = set(database.open_dataset(name).schema.names)
    return [c for c in columns if c in have]


def _in_predicate(column: str, literals: list[str]) -> str:
    """Build a single ``<column> IN ('a','b',…)`` Lance scanner predicate from already
    -normalized, de-duped, capped string ids. Each literal is escaped via
    ``audience._sql_str`` (single-quoted, embedded-quote-safe)."""
    joined = ", ".join(audience._sql_str(lit) for lit in literals)
    return f"{column} IN ({joined})"


def _dedup_capped(ids: list[str]) -> list[str]:
    """De-dup preserving first-seen order, then slice to ``_IN_LIST_CAP``. The caller
    has already normalized each id (and dropped falsy ones)."""
    seen: set[str] = set()
    out: list[str] = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            out.append(i)
        if len(out) >= _IN_LIST_CAP:
            break
    return out


def _norm_uei(value: str) -> str:
    """Collapse a raw UEI to the stored anchor: trim + uppercase (the BTREE anchor).
    Mirrors ``audience.lookup_awards_by_uei`` / ``sam_entities._normalize``."""
    return (value or "").strip().upper()


# ── awards (whole-row by contract) ───────────────────────────────────────────
def lookup_awards_by_ueis(recipient_ueis: list[str]) -> dict[str, Any]:
    """Batch-fetch the precomputed federal-spend resume for MANY recipient UEIs in ONE
    Lance scan. Issues a single ``recipient_uei IN (…)`` predicate over the BTREE on
    ``awards`` (contractor_award_summary, one row per UEI) — one index range-scan / one
    R2 round-trip for the whole audience, instead of N point-lookups.

    Each UEI is upper/trimmed to the stored anchor and the list is de-duped, so
    ``["  uei000 ", "UEI000"]`` resolves once. The batch is capped at 1000 unique ids
    (an over-cap list is sliced, never rejected). Award summaries are returned WHOLE-ROW
    by contract (no projection). Returns
    ``{"requested": <#unique requested>, "found": <#resolved>, "by_uei": {uei: summary_row}}``;
    a requested UEI with no award row is simply absent from ``by_uei`` (never an error).
    An empty / whitespace-only batch returns ``{"requested":0,"found":0,"by_uei":{}}`` and
    issues NO scan.
    """
    ids = _dedup_capped([u for u in (_norm_uei(x) for x in (recipient_ueis or [])) if u])
    requested = len(ids)
    if not ids:
        return {"requested": 0, "found": 0, "by_uei": {}}
    rows = (
        database.open_dataset("awards")
        .scanner(filter=_in_predicate("recipient_uei", ids), limit=_SCAN_LIMIT)
        .to_table()
        .to_pylist()
    )
    by_uei: dict[str, Any] = {}
    for row in rows:
        key = row.get("recipient_uei")
        if key is not None and key not in by_uei:
            by_uei[key] = row  # one summary row per UEI (dataset is one-row-per-UEI)
    return {"requested": requested, "found": len(by_uei), "by_uei": by_uei}


# ── sam entities (projected) ─────────────────────────────────────────────────
def lookup_sam_entities_by_ueis(ueis: list[str]) -> dict[str, Any]:
    """Batch-fetch SAM.gov entity profiles for MANY UEIs in ONE Lance scan. Issues a
    single ``uei IN (…)`` predicate over the BTREE on ``sam_master_entities``, then
    projects the wide entity row down to the documented profile — one index range-scan /
    one R2 round-trip, undocumented wide columns never deserialized.

    Each UEI is upper/trimmed and the list is de-duped + capped at 1000 unique ids.
    Returns ``{"requested","found","by_uei": {uei: entity_row}}`` where each entity
    carries the documented ``SAM_ENTITY_DOC`` columns (``uei, cage_code, primary_naics,
    legal_business_name`` … intersected with the live schema). A requested UEI with no
    entity is absent from ``by_uei``. An empty / whitespace-only batch returns
    ``{"requested":0,"found":0,"by_uei":{}}`` and issues NO scan.
    """
    ids = _dedup_capped([u for u in (_norm_uei(x) for x in (ueis or [])) if u])
    requested = len(ids)
    if not ids:
        return {"requested": 0, "found": 0, "by_uei": {}}
    cols = _present("sam_master_entities", _SAM_ENTITY_DOC)
    rows = (
        database.open_dataset("sam_master_entities")
        .scanner(filter=_in_predicate("uei", ids), columns=cols, limit=_SCAN_LIMIT)
        .to_table()
        .to_pylist()
    )
    by_uei: dict[str, Any] = {}
    for row in rows:
        key = row.get("uei")
        if key is not None and key not in by_uei:
            by_uei[key] = row  # one entity per UEI
    return {"requested": requested, "found": len(by_uei), "by_uei": by_uei}


# ── companies by domains (projected; list grouping — domain not unique) ──────
def search_companies_by_domains(domains: list[str]) -> dict[str, Any]:
    """Batch-look-up companies for MANY web domains in ONE Lance scan. Each input domain
    is normalized to the stored anchor (scheme / ``www.`` / path / casing stripped, via
    the same logic the company spine builds the BTREE on), the list is de-duped + capped
    at 1000, and a single ``normalized_domain IN (…)`` predicate is pushed into the BTREE
    on ``companies`` — one index range-scan / one R2 round-trip for the whole batch.

    A domain is NOT unique (multiple source-platform rows may carry the same company), so
    results group into LISTS. Each company row is projected to the documented company
    columns (``company_id, company_name, normalized_domain, company_linkedin_url,
    source_platform``). Returns ``{"requested","match_count","by_domain": {dom: [rows]}}``
    where ``match_count`` is the TOTAL found-row count and a domain with no match is absent
    from ``by_domain``. An empty / whitespace-only batch returns
    ``{"requested":0,"match_count":0,"by_domain":{}}`` and issues NO scan.
    """
    return _search_by_domains(domains, "companies", _COMPANY_DOC)


# ── people by domains (projected; list grouping) ─────────────────────────────
def search_people_by_domains(domains: list[str]) -> dict[str, Any]:
    """Batch-look-up people (contacts) for MANY company web domains in ONE Lance scan.
    Same shape as ``search_companies_by_domains`` over the ``people`` dataset (BTREE on
    ``people.normalized_domain``, denormalized from the person's company): normalize +
    de-dup + cap the domains, push ONE ``normalized_domain IN (…)`` predicate, project
    each row to the documented people columns (``person_id, contact_id, company_id,
    normalized_domain, full_name, first_name, last_name, title, person_linkedin_url,
    source_platform``; ``person_id`` mirrors ``contact_id``, retained for backward
    compatibility), and group into LISTS.

    Returns ``{"requested","match_count","by_domain": {dom: [rows]}}`` where
    ``match_count`` is the TOTAL found-row count. An empty / whitespace-only batch returns
    ``{"requested":0,"match_count":0,"by_domain":{}}`` and issues NO scan.
    """
    return _search_by_domains(domains, "people", _PEOPLE_DOC)


def _search_by_domains(domains: list[str], dataset: str, doc: list[str]) -> dict[str, Any]:
    """Shared batched-by-domain path for the company / people tools: normalize each input
    domain via ``audience._normalize_domain``, de-dup + cap, issue ONE
    ``normalized_domain IN (…)`` scan with the documented projection, and group rows into
    per-domain LISTS (domain is not unique). ``match_count`` is the total found-row count."""
    normalized = [d for d in (audience._normalize_domain(x) for x in (domains or [])) if d]
    ids = _dedup_capped(normalized)
    requested = len(ids)
    if not ids:
        return {"requested": 0, "match_count": 0, "by_domain": {}}
    cols = _present(dataset, doc)
    rows = (
        database.open_dataset(dataset)
        .scanner(filter=_in_predicate("normalized_domain", ids), columns=cols, limit=_SCAN_LIMIT)
        .to_table()
        .to_pylist()
    )
    by_domain: dict[str, list[Any]] = {}
    for row in rows:
        key = row.get("normalized_domain")
        if key is not None:
            by_domain.setdefault(key, []).append(row)
    match_count = sum(len(v) for v in by_domain.values())
    return {"requested": requested, "match_count": match_count, "by_domain": by_domain}


def register(mcp) -> None:
    """Mount the batched multi-id lookup tools onto the FastMCP server. Each function's
    signature + docstring becomes the tool's input schema + agent-facing contract."""
    for fn in (
        lookup_awards_by_ueis,
        lookup_sam_entities_by_ueis,
        search_companies_by_domains,
        search_people_by_domains,
    ):
        mcp.add_tool(fn)
