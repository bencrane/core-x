"""SAM.gov entity tools — typed BTREE point-lookups over the Gen-3 R2 sink.

The ``sam_master_entities`` dataset is WIDE (68 columns) and carries hard ``BTREE``
scalar indices on ``uei``, ``primary_naics``, and ``cage_code``; its companion
``sam_master_contacts`` carries a ``BTREE`` on ``uei``. An agent that wants a SAM
entity by one of those keys would otherwise hand-write ``execute_audience_query``
SQL — the full DuckDB-over-Lance path (registry scan → JIT register → DuckDB plan)
for what is a single indexed point-lookup.

These four read-only tools push the predicate straight into the Lance scanner so the
``BTREE`` answers in sub-100 ms — no full scan, no DuckDB round-trip — AND project the
wide 68-column row down to a documented entity profile (``SAM_ENTITY_DOC``) so the
undocumented wide columns never cross the R2 object-store boundary. This mirrors
``audience.py``'s ``lookup_awards_by_uei`` / ``search_company_by_domain`` shape exactly.

  • UEI / CAGE lookups are exact point-lookups (one entity per id).
  • NAICS is hierarchical, so ``search_sam_entities_by_naics`` does a PREFIX match
    (``primary_naics LIKE 'naics%'``) — a 4-digit input fans out to every 6-digit
    industry beneath it.

Tool docstrings below are the agent-facing contracts — the MCP client shows them
verbatim, so they describe inputs, the BTREE-pushdown semantics, and the shape returned.
"""

from __future__ import annotations

from typing import Any

from .. import database

# Per-lookup row ceiling for the fan-out tools (NAICS prefix, contacts). A NAICS
# prefix legitimately matches many entities and a UEI may carry several POCs; this
# caps pathological fan-out. Mirrors audience._LOOKUP_LIMIT.
_LOOKUP_LIMIT = 50


def _sql_str(value: str) -> str:
    """A safe single-quoted SQL/Lance string literal."""
    return "'" + value.replace("'", "''") + "'"


def _normalize(value: str) -> str:
    """Collapse a raw id to the stored anchor: trim + uppercase. UEI / CAGE / NAICS
    are stored uppercase, so this is what the BTREE is keyed on."""
    return (value or "").strip().upper()


# Documented agent-facing column contracts — the EXACT projected sets the lookups
# promise. Passed as the Lance scanner ``columns=`` so only these are read/deserialized
# from R2 instead of the full wide row; the wide undocumented filler never crosses the
# object-store boundary. These lists ARE the contract — adding a documented column means
# editing both the docstring and this list.
SAM_ENTITY_DOC = [
    "uei", "cage_code", "primary_naics", "legal_business_name", "dba_name", "entity_url",
    "physical_address_city", "physical_address_province_or_state",
    "physical_address_country_code", "state_of_incorporation", "entity_structure",
    "registration_expiration_date", "sam_extract_code", "is_active",
]
SAM_CONTACT_DOC = [
    "uei", "poc_type", "first_name", "last_name", "title", "city", "state_or_province",
]


def _present(name: str, columns: list[str]) -> list[str]:
    """Intersect a documented column list with the dataset's actual schema, preserving
    documented order. Prod partitions vary in width, so a documented column absent from
    the live schema must never make the scanner raise — project only what exists."""
    have = set(database.open_dataset(name).schema.names)
    return [c for c in columns if c in have]


# ── lookup_sam_entity_by_uei (BTREE on uei) ──────────────────────────────────
def lookup_sam_entity_by_uei(uei: str) -> dict[str, Any]:
    """Fetch one SAM.gov entity profile by its UEI (Unique Entity Identifier). Pushes
    the predicate into the Lance BTREE on ``sam_master_entities.uei`` for a sub-100 ms
    point-lookup, then projects the wide 68-column row down to the documented profile.

    UEIs are 12-char uppercase alphanumeric; the input is upper/trimmed to match the
    stored anchor (so ``  uei0000000aaa `` resolves identically). Returns
    ``{"uei", "found", "entity": {...}|null}`` where the entity carries
    ``uei, cage_code, primary_naics, legal_business_name, dba_name, entity_url,
    physical_address_city, physical_address_province_or_state,
    physical_address_country_code, state_of_incorporation, entity_structure,
    registration_expiration_date, sam_extract_code, is_active``. Unknown / empty id →
    ``found=False, entity=None`` (never a raise).
    """
    norm = _normalize(uei)
    if not norm:
        return {"uei": None, "found": False, "entity": None}
    cols = _present("sam_master_entities", SAM_ENTITY_DOC)
    rows = (
        database.open_dataset("sam_master_entities")
        .scanner(filter=f"uei = {_sql_str(norm)}", columns=cols, limit=1)
        .to_table()
        .to_pylist()
    )
    return {"uei": norm, "found": bool(rows), "entity": rows[0] if rows else None}


# ── lookup_sam_entity_by_cage (BTREE on cage_code) ───────────────────────────
def lookup_sam_entity_by_cage(cage_code: str) -> dict[str, Any]:
    """Fetch one SAM.gov entity profile by its CAGE code (Commercial And Government
    Entity code). Pushes the predicate into the Lance BTREE on
    ``sam_master_entities.cage_code`` for a sub-100 ms point-lookup, then projects to
    the documented profile.

    CAGE codes are 5-char uppercase alphanumeric; the input is upper/trimmed to match.
    Returns ``{"cage_code", "found", "entity": {...}|null}`` with the same projected
    entity columns as ``lookup_sam_entity_by_uei``. Unknown / empty code →
    ``found=False, entity=None`` (never a raise).
    """
    norm = _normalize(cage_code)
    if not norm:
        return {"cage_code": None, "found": False, "entity": None}
    cols = _present("sam_master_entities", SAM_ENTITY_DOC)
    rows = (
        database.open_dataset("sam_master_entities")
        .scanner(filter=f"cage_code = {_sql_str(norm)}", columns=cols, limit=1)
        .to_table()
        .to_pylist()
    )
    return {"cage_code": norm, "found": bool(rows), "entity": rows[0] if rows else None}


# ── search_sam_entities_by_naics (BTREE on primary_naics, PREFIX) ────────────
def search_sam_entities_by_naics(naics: str) -> dict[str, Any]:
    """Find SAM.gov entities by primary NAICS code. NAICS is hierarchical, so this is a
    PREFIX match: a 4-digit input (e.g. ``5415`` — Computer Systems Design) fans out to
    every 6-digit industry beneath it (``541511``, ``541512``, ...). Pushes the
    ``primary_naics LIKE 'naics%'`` predicate into the Lance BTREE on
    ``sam_master_entities.primary_naics`` for a sub-100 ms index range-scan, then projects
    each row to the documented profile.

    The input is trimmed; an exact 6-digit code works as a degenerate prefix. Returns
    ``{"primary_naics", "match_count", "entities": [{...}]}`` (capped at 50) with the same
    projected entity columns as ``lookup_sam_entity_by_uei``. No match → ``match_count=0,
    entities=[]``.
    """
    prefix = (naics or "").strip()
    if not prefix:
        return {"primary_naics": prefix, "match_count": 0, "entities": []}
    cols = _present("sam_master_entities", SAM_ENTITY_DOC)
    rows = (
        database.open_dataset("sam_master_entities")
        .scanner(
            filter=f"primary_naics LIKE {_sql_str(prefix + '%')}",
            columns=cols,
            limit=_LOOKUP_LIMIT,
        )
        .to_table()
        .to_pylist()
    )
    return {"primary_naics": prefix, "match_count": len(rows), "entities": rows}


# ── lookup_sam_contacts_by_uei (BTREE on uei, sam_master_contacts) ───────────
def lookup_sam_contacts_by_uei(uei: str) -> dict[str, Any]:
    """Fetch the SAM.gov points-of-contact (POCs) for one entity by its UEI. Pushes the
    predicate into the Lance BTREE on ``sam_master_contacts.uei`` for a sub-100 ms
    point-lookup, then projects each POC to the documented contact shape. A single entity
    typically carries several POCs (government-business, electronic-business, etc.), so
    this returns a list.

    The input is upper/trimmed to match the stored anchor. Returns
    ``{"uei", "match_count", "contacts": [{...}]}`` (capped at 50) where each contact
    carries ``uei, poc_type, first_name, last_name, title, city, state_or_province``.
    Unknown / empty id → ``match_count=0, contacts=[]`` (never a raise).
    """
    norm = _normalize(uei)
    if not norm:
        return {"uei": None, "match_count": 0, "contacts": []}
    cols = _present("sam_master_contacts", SAM_CONTACT_DOC)
    rows = (
        database.open_dataset("sam_master_contacts")
        .scanner(filter=f"uei = {_sql_str(norm)}", columns=cols, limit=_LOOKUP_LIMIT)
        .to_table()
        .to_pylist()
    )
    return {"uei": norm, "match_count": len(rows), "contacts": rows}


def register(mcp) -> None:
    """Mount the sam.gov entity tools onto the FastMCP server. Each function's signature +
    docstring becomes the tool's input schema + agent-facing contract."""
    for fn in (
        lookup_sam_entity_by_uei,
        lookup_sam_entity_by_cage,
        search_sam_entities_by_naics,
        lookup_sam_contacts_by_uei,
    ):
        mcp.add_tool(fn)
