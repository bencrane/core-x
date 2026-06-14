"""GovCon capability query legs — §3 of the canonical build plan, on a phase schedule.

Plan: ``docs/plans/GOVCON_SCOPE_PROCESSING_AND_GTM_QUERY_BUILD_PLAN.md`` (§2 artifact map names
this module; §3 defines the legs). THIS FILE SHIPS EXACTLY THE PHASE-1 LEG:

  HARD-PREDICATE CONJUNCTION (§3 item 1, live at Phase 1). "Companies whose covered awards
  need A AND B AND C" — deterministic, citeable, polarity-correct predicates over
  ``govcon_award_requirements_90day`` (requirement_type / value_norm AND-combinations),
  aggregated up the grain ladder:

      requirement rows → resource_id → manifest ``award_keys[]`` EXPLODE → award grain
      (``contract_prime_txn`` collapsed to award) → ``recipient_uei`` company grain.

  The exploded manifest grain is CANONICAL (plan §1: 35,002 awards, 4.1× the inline scalar
  key); the requirement row's inline ``contract_award_unique_key`` is a convenience floor and
  is NEVER used as the join spine here (anti-pattern #5). Conjunction is enforced at award
  grain in DuckDB over Lance-scanner-materialized Arrow tables (the ``federal.py``
  scan-then-aggregate pattern — DuckDB never sees a registered Lance manifest).

EVIDENCE (§3 item 5, non-negotiable). Every company carries its matched awards and every
award its matched requirement rows: ``requirement_id``, ``source_chunk_ids`` (the
chunk→resource→blob CAS citation chain) and ``evidence_quote`` where present. Rows from
MARKED resources (non-empty chunk-level ``content_marking``) are written with NULL
``evidence_quote``/``requirement_detail`` at Phase-1 write time (the write-side gate); this
module re-NULLs both fields whenever ``marked_resource`` is true — defense in depth, verbatim
marked text never leaves a serving response — and surfaces the row with ``marked: true``.

COVERAGE HONESTY (plan §1 + anti-pattern #11, mandatory on every response). Only ~0.96% of
the 1,119,355 trailing-90d prime awards have readable solicitation text; "no match" means NO
DOCUMENT, not no need. Every response carries the caveat plus the measured per-query
denominators (per-leg row counts, exploded award counts, txn resolution rate).

READ PATHS. ``govcon_award_requirements_90day`` (frozen schema —
``pipelines/sam_gov/govcon_gtm_schemas.py``), ``sam_opps_attachment_manifest_90day_winners``
(resource_id → award_keys[], BTREE on resource_id), ``usaspending_api_fresh/contract_prime_txn``
(BTREE on contract_award_unique_key). All reads go through ``database.open_dataset`` (warm
handle cache) + Lance scanner pushdown; the chunk sinks are never touched.

PHASE SCHEDULE — activation points for the later legs (plan §2/§3; NOT built here):
  * Phase 2 — grain-ladder leg over ``govcon_award_capability_profiles``
    (``capability_tags`` / ``scope_summary`` / clearance rollups; the degraded north-star
    gate query "companies tagged electrical_systems under awards requiring SECRET + CMMC L2").
  * Phase 4 — sub pivot: award set → prime UEIs → ``govcon_sub_targeting_90day`` →
    ``sam_pocs`` join at query time.
  * Phase 5 — fuzzy-X ANN leg over both chunk sinks (k≈1000, ``prefilter=True``,
    max-of-top-m per-award scoring; resource_id IN-lists derived in DuckDB, never
    award-attribute filters on chunks).
"""

from __future__ import annotations

import re
from typing import Any

from .. import database

# ── Bound dataset names (registry keys) ──────────────────────────────────────
REQUIREMENTS = "govcon_award_requirements_90day"
MANIFEST = "sam_opps_attachment_manifest_90day_winners"
PRIME_TXN = "usaspending_api_fresh/contract_prime_txn"

# Plan Phase-1 requirement_type enum (frozen schema column comment; plan Phase-1 table).
REQUIREMENT_TYPES = frozenset({
    "certification", "clearance", "labor_category", "standard_compliance", "license",
    "equipment_capability", "past_performance", "deliverable", "insurance_bonding",
    "staffing_constraint", "vehicle_constraint",
})

# Coverage-honesty contract (plan §1 canonical numbers, probed 2026-06-12; anti-pattern #11).
COVERAGE_CAVEAT = (
    "Coverage honesty: only ~0.96% of the 1,119,355 trailing-90d prime awards have readable "
    "solicitation text in this corpus (35,002 text-covered awards at the canonical exploded-"
    "manifest grain; 10,729 at the inline-key floor). A company or award absent from these "
    "results means NO DOCUMENT WAS READ for it — absence of text is not absence of need. "
    "'No match' = no document, not no need."
)
_COVERAGE = {
    "caveat": COVERAGE_CAVEAT,
    "text_covered_awards_ceiling": 35_002,   # exploded manifest grain — canonical
    "inline_key_awards_floor": 10_729,       # never mixed with the ceiling
    "trailing_90d_prime_awards": 1_119_355,
    "coverage_ceiling_pct": 0.96,
    "denominators_probed": "2026-06-12 (plan §1)",
}

# NAICS codes are 2–6 digit strings; a prefix restricts to a sector (federal.py convention).
_NAICS_RE = re.compile(r"^[0-9]{2,6}$")

_MAX_LEGS = 8
_MAX_COMPANIES = 200
_DEFAULT_COMPANIES = 50
_MAX_EVIDENCE_PER_AWARD = 20
_DEFAULT_EVIDENCE_PER_AWARD = 5
_MAX_AWARD_SET = 5_000          # deterministic cap on the conjunction award set (lex order)
_PER_COMPANY_AWARDS = 10        # award context rows returned per company
_IN_BATCH = 1_000               # IN-list batch size for BTREE point scans
_FACET_MAX = 1_000

# Requirement-row projection for evidence (every name must exist on the frozen schema —
# pinned against pipelines/sam_gov/govcon_gtm_schemas.requirements_schema in tests).
_REQ_EVIDENCE_COLUMNS = [
    "requirement_id", "resource_id", "notice_id", "solicitation_number", "naics_code",
    "requirement_type", "requirement_value", "requirement_detail", "mandatory", "headcount",
    "clearance_level", "source_chunk_ids", "evidence_quote", "validated", "marked_resource",
    "extractor", "extractor_version", "confidence",
]

# Award-context projection from the raw prime txn feed (all string-typed at source; cast at
# query time). contract_transaction_unique_key is projected ONLY as the deterministic
# collapse tiebreak and is dropped from the response.
_TXN_COLUMNS = [
    "contract_award_unique_key", "contract_transaction_unique_key", "recipient_uei",
    "recipient_name", "recipient_parent_uei", "naics_code", "awarding_agency_name",
    "type_of_set_aside", "product_or_service_code", "period_of_performance_start_date",
    "period_of_performance_current_end_date", "current_total_value_of_award",
    "total_dollars_obligated", "action_date", "last_modified_date",
]
_AWARD_CONTEXT_COLUMNS = [c for c in _TXN_COLUMNS if c != "contract_transaction_unique_key"]


def _sql_str(value: str) -> str:
    """Single-quoted, escaped literal for a Lance scanner predicate / DuckDB SQL."""
    return "'" + value.replace("'", "''") + "'"


def _norm_naics(naics: str | None) -> str | None:
    if naics is None:
        return None
    p = naics.strip()
    if not _NAICS_RE.match(p):
        raise ValueError(f"naics must be a 2–6 digit NAICS code/prefix string (got {naics!r})")
    return p


def _naics_clause(naics: str | None) -> str | None:
    """6 digits → BTREE-pushable equality; shorter → prefix LIKE (federal.py convention)."""
    if naics is None:
        return None
    if len(naics) == 6:
        return f"naics_code = {_sql_str(naics)}"
    return f"naics_code LIKE {_sql_str(naics + '%')}"


_LEG_KEYS = {"requirement_type", "value", "clearance_level", "mandatory"}


def _normalize_legs(requirements: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Validate the conjunction legs. Each leg is one hard predicate over the requirements
    sink; the conjunction requires EVERY leg witnessed on the same award (across any of its
    covering documents — conjunctions are cross-chunk and cross-document by construction)."""
    if not requirements or not isinstance(requirements, list):
        raise ValueError("requirements must be a non-empty list of predicate legs")
    if len(requirements) > _MAX_LEGS:
        raise ValueError(f"at most {_MAX_LEGS} requirement legs per query (got {len(requirements)})")
    legs: list[dict[str, Any]] = []
    for ix, raw in enumerate(requirements):
        if not isinstance(raw, dict):
            raise ValueError(f"leg {ix} must be an object, got {type(raw).__name__}")
        unknown = set(raw) - _LEG_KEYS
        if unknown:
            raise ValueError(
                f"leg {ix} has unknown key(s) {sorted(unknown)}; allowed: {sorted(_LEG_KEYS)}")
        rtype = (raw.get("requirement_type") or "").strip()
        if rtype not in REQUIREMENT_TYPES:
            raise ValueError(
                f"leg {ix} requirement_type {rtype!r} is not in the Phase-1 enum: "
                f"{sorted(REQUIREMENT_TYPES)}")
        leg: dict[str, Any] = {"requirement_type": rtype}
        value = raw.get("value")
        if value is not None:
            v = str(value).strip()
            if not v:
                raise ValueError(f"leg {ix} value must be a non-empty string when given")
            leg["value"] = v
        clearance = raw.get("clearance_level")
        if clearance is not None:
            c = str(clearance).strip()
            if not c:
                raise ValueError(f"leg {ix} clearance_level must be a non-empty string when given")
            leg["clearance_level"] = c
        mandatory = raw.get("mandatory")
        if mandatory is not None:
            if not isinstance(mandatory, bool):
                raise ValueError(f"leg {ix} mandatory must be a boolean when given")
            leg["mandatory"] = mandatory
        legs.append(leg)
    return legs


def _leg_filter(leg: dict[str, Any], naics: str | None) -> str:
    """Lance scanner predicate for one leg. ``validated = true`` is HARD-CODED — the row flag
    is the trust boundary (plan Phase 2 gate: consumers filter WHERE validated, no toggle)."""
    clauses = ["validated = true", f"requirement_type = {_sql_str(leg['requirement_type'])}"]
    if "value" in leg:
        clauses.append(f"requirement_value = {_sql_str(leg['value'])}")
    if "clearance_level" in leg:
        clauses.append(f"clearance_level = {_sql_str(leg['clearance_level'])}")
    if "mandatory" in leg:
        clauses.append(f"mandatory = {'true' if leg['mandatory'] else 'false'}")
    nc = _naics_clause(naics)
    if nc:
        clauses.append(nc)
    return " AND ".join(clauses)


def _scan_in(name: str, key_col: str, keys: list[str], columns: list[str],
             extra_filter: str | None = None):
    """Batched IN-list point scans pushed onto the named dataset's BTREE — never one giant
    predicate. Returns one concatenated Arrow table (empty-schema table when keys is empty)."""
    import pyarrow as pa

    ds = database.open_dataset(name)
    parts = []
    for i in range(0, len(keys), _IN_BATCH):
        chunk = keys[i:i + _IN_BATCH]
        pred = f"{key_col} IN (" + ", ".join(_sql_str(k) for k in chunk) + ")"
        if extra_filter:
            pred = f"({extra_filter}) AND {pred}"
        parts.append(ds.scanner(columns=columns, filter=pred).to_table())
    if not parts:
        return ds.scanner(columns=columns, limit=0).to_table()
    return pa.concat_tables(parts)


def _duck(tables: dict[str, Any], sql: str, max_rows: int) -> list[dict[str, Any]]:
    """Run DuckDB ``sql`` over already-materialized Arrow tables registered under their dict
    names — the scan-then-aggregate path (federal.py ``_agg_over``): DuckDB only ever sees
    Arrow tables the Lance scanner already filtered + projected, never a Lance manifest."""
    con = database.get_connection()
    cur = con.cursor()
    try:
        for tname, tbl in tables.items():
            cur.register(tname, tbl)
        rel = cur.execute(sql)
        columns = [d[0] for d in rel.description] if rel.description else []
        return [dict(zip(columns, r)) for r in rel.fetchmany(max_rows)]
    finally:
        cur.close()


def _as_float(value: Any) -> float | None:
    """Best-effort cast of a raw-txn string dollar field. NULL/garbage → None, never a raise."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _redacted_evidence_row(row: dict[str, Any], leg_ix: int) -> dict[str, Any]:
    """One evidence item. DEFENSE IN DEPTH: rows from marked resources (chunk-level
    ``content_marking`` ≠ [] — the single egress enforcement signal) get ``evidence_quote``
    AND ``requirement_detail`` forced to None HERE even though Phase-1 writes them NULL —
    verbatim marked text never leaves a serving response (anti-pattern #10). Structured /
    enum fields stay. ``marked`` is the agent-facing flag."""
    marked = bool(row.get("marked_resource"))
    return {
        "leg_ix": leg_ix,
        "requirement_id": row.get("requirement_id"),
        "resource_id": row.get("resource_id"),
        "notice_id": row.get("notice_id"),
        "solicitation_number": row.get("solicitation_number"),
        "requirement_type": row.get("requirement_type"),
        "requirement_value": row.get("requirement_value"),
        "requirement_detail": None if marked else row.get("requirement_detail"),
        "mandatory": row.get("mandatory"),
        "headcount": row.get("headcount"),
        "clearance_level": row.get("clearance_level"),
        "source_chunk_ids": list(row.get("source_chunk_ids") or []),
        "evidence_quote": None if marked else row.get("evidence_quote"),
        "marked": marked,
        "extractor": row.get("extractor"),
        "extractor_version": row.get("extractor_version"),
        "confidence": row.get("confidence"),
    }


def govcon_companies_by_requirements(
    requirements: list[dict[str, Any]],
    naics: str | None = None,
    max_companies: int = _DEFAULT_COMPANIES,
    evidence_per_award: int = _DEFAULT_EVIDENCE_PER_AWARD,
) -> dict[str, Any]:
    """Companies whose covered awards demand EVERY listed hard requirement (conjunctive
    AND), with award context and verbatim document evidence — the GovCon Phase-1
    hard-predicate leg.

    Each ``requirements`` leg is one predicate over the extracted-requirements corpus
    (``govcon_award_requirements_90day``): ``{"requirement_type": <enum>, "value": <
    normalized requirement_value, optional>, "clearance_level": <optional>, "mandatory":
    <optional bool>}``. ``requirement_type`` ∈ certification · clearance · labor_category ·
    standard_compliance · license · equipment_capability · past_performance · deliverable ·
    insurance_bonding · staffing_constraint · vehicle_constraint. Example — "awards requiring
    TS/SCI + CMMC L2": ``[{"requirement_type": "clearance", "clearance_level": "TS_SCI"},
    {"requirement_type": "certification", "value": "CMMC_L2"}]``. Use
    ``govcon_requirement_facets`` to discover the live normalized values. Only
    ``validated = true`` rows are ever consulted (the row-level trust boundary).

    An award matches when ALL legs are witnessed across ANY of its covering documents
    (conjunctions are cross-document by construction). Matching runs up the canonical grain
    ladder: requirement rows → ``resource_id`` → manifest ``award_keys[]`` explode (never the
    inline scalar key) → award grain (prime txn collapsed by latest ``last_modified_date``)
    → ``recipient_uei`` company grain. ``naics`` (2–6 digit code/prefix) restricts to
    requirement rows whose NOTICE carries that NAICS.

    Returns ``{"status", "legs", "coverage", "measured", "company_count", "companies",
    "companies_truncated"}``. Each company: ``recipient_uei/name/parent_uei``,
    ``covered_award_count``, ``total_dollars_obligated_sum``, ``any_marked_evidence`` and up
    to 10 ``awards`` (award context + up to ``evidence_per_award`` evidence items). Each
    evidence item cites ``requirement_id`` + ``source_chunk_ids`` + ``evidence_quote``;
    rows from CUI-marked resources surface with ``marked: true`` and ``evidence_quote`` /
    ``requirement_detail`` null — redacted text is never served. ``coverage`` carries the
    mandatory honesty caveat: ~0.96% of trailing-90d prime awards have readable text, so
    "no match" means no document was read, NOT no need. Non-``ok`` statuses:
    ``dataset_unavailable``, ``requirements_sink_empty`` (Phase-1 extraction not yet run),
    ``no_matches``.
    """
    legs = _normalize_legs(requirements)
    naics_p = _norm_naics(naics)
    n_companies = max(1, min(int(max_companies), _MAX_COMPANIES))
    n_evidence = max(1, min(int(evidence_per_award), _MAX_EVIDENCE_PER_AWARD))
    legs_echo = [dict(leg, leg_ix=ix) for ix, leg in enumerate(legs)]

    # 0) Resolve the three datasets via the warm handle cache; absent → clear status.
    try:
        req_ds = database.open_dataset(REQUIREMENTS)
    except KeyError:
        return {"status": "dataset_unavailable", "detail": f"{REQUIREMENTS!r} is not registered "
                "in the active sink yet; call list_datasets to confirm.", "coverage": _COVERAGE}
    if req_ds.count_rows() == 0:
        return {
            "status": "requirements_sink_empty",
            "detail": (f"{REQUIREMENTS!r} exists (frozen schema) but holds 0 rows — the Phase-1 "
                       "regex extraction pass has not landed yet. No requirement is queryable."),
            "legs": legs_echo, "coverage": _COVERAGE,
        }

    # 1) Pass A — per-leg requirement hits, resource grain only (cheap projection; the full
    #    evidence rows are fetched in Pass B for matched resources only).
    leg_row_counts: list[int] = []
    leg_resources: list[set[str]] = []
    for leg in legs:
        tbl = req_ds.scanner(columns=["resource_id"], filter=_leg_filter(leg, naics_p)).to_table()
        rids = {r for r in tbl.column("resource_id").to_pylist() if r}
        leg_row_counts.append(tbl.num_rows)
        leg_resources.append(rids)
    measured: dict[str, Any] = {
        "leg_requirement_rows": leg_row_counts,
        "leg_resource_counts": [len(s) for s in leg_resources],
    }
    if any(not s for s in leg_resources):
        empty_ix = [ix for ix, s in enumerate(leg_resources) if not s]
        return {
            "status": "no_matches", "legs": legs_echo, "coverage": _COVERAGE,
            "measured": measured, "company_count": 0, "companies": [],
            "detail": f"leg(s) {empty_ix} matched zero validated requirement rows — no document "
                      "in the corpus witnesses them (see coverage caveat).",
        }

    # 2) Resource → award explode over the manifest (BTREE point scans), then award-grain
    #    conjunction in DuckDB: an award wins iff every leg is witnessed by ≥1 covering doc.
    union_resources = sorted(set.union(*leg_resources))
    try:
        m_tbl = _scan_in(MANIFEST, "resource_id", union_resources, ["resource_id", "award_keys"])
    except KeyError:
        return {"status": "dataset_unavailable", "detail": f"{MANIFEST!r} is not registered in "
                "the active sink.", "coverage": _COVERAGE}
    import pyarrow as pa
    hits_tbl = pa.table({
        "resource_id": [rid for ix, s in enumerate(leg_resources) for rid in sorted(s)],
        "leg_ix": [ix for ix, s in enumerate(leg_resources) for _ in range(len(s))],
    })
    pairs = _duck(
        {"m": m_tbl, "hits": hits_tbl},
        f"""
        WITH rmap AS (
            SELECT DISTINCT resource_id, UNNEST(award_keys) AS award_key
            FROM m WHERE award_keys IS NOT NULL
        ),
        al AS (
            SELECT DISTINCT r.award_key, h.leg_ix
            FROM rmap r JOIN hits h USING (resource_id)
        ),
        win AS (
            SELECT award_key FROM al
            GROUP BY award_key HAVING COUNT(DISTINCT leg_ix) = {len(legs)}
        )
        SELECT r.resource_id, r.award_key
        FROM rmap r JOIN win USING (award_key)
        ORDER BY award_key, resource_id
        """,
        max_rows=10_000_000,
    )
    matched_awards = sorted({p["award_key"] for p in pairs})
    measured["resources_in_union"] = len(union_resources)
    measured["awards_conjunction"] = len(matched_awards)
    if not matched_awards:
        return {
            "status": "no_matches", "legs": legs_echo, "coverage": _COVERAGE,
            "measured": measured, "company_count": 0, "companies": [],
            "detail": "every leg matched somewhere, but no single award is covered by "
                      "documents witnessing ALL legs (conjunction empty; see coverage caveat).",
        }
    awards_truncated = len(matched_awards) > _MAX_AWARD_SET
    if awards_truncated:  # deterministic cap: lexicographic award-key order
        matched_awards = matched_awards[:_MAX_AWARD_SET]
        keep = set(matched_awards)
        pairs = [p for p in pairs if p["award_key"] in keep]
    measured["awards_after_cap"] = len(matched_awards)
    measured["award_set_truncated"] = awards_truncated

    # 3) Award → prime txn (BTREE point scans), collapsed to award grain on the latest
    #    last_modified_date (deterministic tiebreak on contract_transaction_unique_key).
    try:
        txn_tbl = _scan_in(PRIME_TXN, "contract_award_unique_key", matched_awards, _TXN_COLUMNS)
    except KeyError:
        return {"status": "dataset_unavailable", "detail": f"{PRIME_TXN!r} is not registered in "
                "the active sink.", "coverage": _COVERAGE}
    award_cols = ", ".join(_AWARD_CONTEXT_COLUMNS)
    awards_rows = _duck(
        {"t": txn_tbl},
        f"""
        SELECT {award_cols} FROM (
            SELECT *, row_number() OVER (
                PARTITION BY contract_award_unique_key
                ORDER BY last_modified_date DESC NULLS LAST,
                         contract_transaction_unique_key DESC
            ) AS rn FROM t
        ) WHERE rn = 1
        ORDER BY contract_award_unique_key
        """,
        max_rows=_MAX_AWARD_SET + 1,
    )
    award_ctx = {r["contract_award_unique_key"]: r for r in awards_rows}
    measured["awards_resolved_in_txn"] = len(award_ctx)
    measured["award_resolution_rate"] = round(len(award_ctx) / len(matched_awards), 4)

    # 4) Pass B — evidence fetch: full requirement rows, restricted per leg to the resources
    #    that cover a matched award. Redaction applied per row (marked → quote/detail None).
    award_resources: dict[str, list[str]] = {}
    resource_awards: dict[str, list[str]] = {}
    for p in pairs:
        award_resources.setdefault(p["award_key"], []).append(p["resource_id"])
        resource_awards.setdefault(p["resource_id"], []).append(p["award_key"])
    ev_resources = sorted(resource_awards)
    evidence_by_resource: dict[str, list[dict[str, Any]]] = {}
    n_evidence_rows = 0
    for ix, leg in enumerate(legs):
        wanted = sorted(leg_resources[ix] & set(ev_resources))
        ev_tbl = _scan_in(REQUIREMENTS, "resource_id", wanted, _REQ_EVIDENCE_COLUMNS,
                          extra_filter=_leg_filter(leg, naics_p))
        for row in ev_tbl.to_pylist():
            item = _redacted_evidence_row(row, ix)
            evidence_by_resource.setdefault(row["resource_id"], []).append(item)
            n_evidence_rows += 1
    measured["evidence_rows"] = n_evidence_rows

    # 5) Award assembly: evidence over each award's covering resources, deterministic order,
    #    capped; then the recipient_uei company rollup.
    award_payload: dict[str, dict[str, Any]] = {}
    for ak in matched_awards:
        if ak not in award_ctx:
            continue  # unresolved in the txn window — counted in award_resolution_rate
        items: list[dict[str, Any]] = []
        for rid in sorted(set(award_resources.get(ak, ()))):
            items.extend(evidence_by_resource.get(rid, ()))
        items.sort(key=lambda e: (e["leg_ix"], e["requirement_type"], e["requirement_id"] or ""))
        award_payload[ak] = {
            **{c: award_ctx[ak].get(c) for c in _AWARD_CONTEXT_COLUMNS},
            "marked_award": any(e["marked"] for e in items),
            "evidence_count": len(items),
            "evidence_truncated": len(items) > n_evidence,
            "evidence": items[:n_evidence],
        }

    companies: dict[str, dict[str, Any]] = {}
    for ak, payload in award_payload.items():
        uei = payload.get("recipient_uei") or "∅"
        comp = companies.setdefault(uei, {
            "recipient_uei": payload.get("recipient_uei"),
            "recipient_name": payload.get("recipient_name"),
            "recipient_parent_uei": payload.get("recipient_parent_uei"),
            "covered_award_count": 0,
            "total_dollars_obligated_sum": 0.0,
            "any_marked_evidence": False,
            "awards": [],
        })
        comp["covered_award_count"] += 1
        comp["total_dollars_obligated_sum"] += _as_float(payload.get("total_dollars_obligated")) or 0.0
        comp["any_marked_evidence"] = comp["any_marked_evidence"] or payload["marked_award"]
        comp["awards"].append(payload)
    for comp in companies.values():
        comp["awards"].sort(
            key=lambda a: (-(_as_float(a.get("total_dollars_obligated")) or 0.0),
                           a["contract_award_unique_key"]))
        comp["awards_truncated"] = len(comp["awards"]) > _PER_COMPANY_AWARDS
        comp["awards"] = comp["awards"][:_PER_COMPANY_AWARDS]
    ordered = sorted(
        companies.values(),
        key=lambda c: (-c["covered_award_count"], -c["total_dollars_obligated_sum"],
                       c["recipient_uei"] or "∅"),
    )
    companies_truncated = len(ordered) > n_companies
    measured["company_count_total"] = len(ordered)

    return {
        "status": "ok",
        "legs": legs_echo,
        "coverage": _COVERAGE,
        "measured": measured,
        "company_count": min(len(ordered), n_companies),
        "companies": ordered[:n_companies],
        "companies_truncated": companies_truncated,
    }


def govcon_requirement_facets(
    requirement_type: str | None = None,
    naics: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """Discover the live normalized requirement vocabulary — the facet companion to
    ``govcon_companies_by_requirements``. Hard-predicate conjunctions match on EXACT
    normalized ``requirement_value`` / ``clearance_level`` strings; this tool enumerates
    what those values ARE, with row + document counts, so legs can be composed precisely.

    Groups ``validated = true`` rows of ``govcon_award_requirements_90day`` by
    ``(requirement_type, requirement_value, clearance_level)``. Optional
    ``requirement_type`` (Phase-1 enum) and ``naics`` (2–6 digit notice-NAICS code/prefix)
    restrict the scan. Returns ``{"status", "group_count", "returned", "rows": [
    {"requirement_type", "requirement_value", "clearance_level", "n_rows", "n_resources",
    "n_mandatory"}], "coverage"}`` ordered by ``n_rows`` desc — plus the mandatory coverage
    caveat (the vocabulary only spans the ~0.96% of awards with readable documents).
    """
    if requirement_type is not None:
        requirement_type = requirement_type.strip()
        if requirement_type not in REQUIREMENT_TYPES:
            raise ValueError(
                f"requirement_type {requirement_type!r} is not in the Phase-1 enum: "
                f"{sorted(REQUIREMENT_TYPES)}")
    naics_p = _norm_naics(naics)
    n = max(1, min(int(limit), _FACET_MAX))
    try:
        ds = database.open_dataset(REQUIREMENTS)
    except KeyError:
        return {"status": "dataset_unavailable", "detail": f"{REQUIREMENTS!r} is not registered "
                "in the active sink yet.", "coverage": _COVERAGE}
    if ds.count_rows() == 0:
        return {"status": "requirements_sink_empty",
                "detail": f"{REQUIREMENTS!r} holds 0 rows — Phase-1 extraction not yet landed.",
                "group_count": 0, "returned": 0, "rows": [], "coverage": _COVERAGE}
    clauses = ["validated = true"]
    if requirement_type:
        clauses.append(f"requirement_type = {_sql_str(requirement_type)}")
    nc = _naics_clause(naics_p)
    if nc:
        clauses.append(nc)
    tbl = ds.scanner(
        columns=["requirement_type", "requirement_value", "clearance_level", "mandatory",
                 "resource_id"],
        filter=" AND ".join(clauses),
    ).to_table()
    rows = _duck(
        {"r": tbl},
        f"""
        WITH gg AS (
            SELECT requirement_type, requirement_value, clearance_level,
                   COUNT(*) AS n_rows,
                   COUNT(DISTINCT resource_id) AS n_resources,
                   COUNT(*) FILTER (WHERE mandatory) AS n_mandatory
            FROM r
            GROUP BY requirement_type, requirement_value, clearance_level
        )
        SELECT (SELECT COUNT(*) FROM gg) AS group_count, *
        FROM gg
        ORDER BY n_rows DESC, requirement_type, requirement_value NULLS LAST
        LIMIT {n}
        """,
        max_rows=n,
    )
    group_count = rows[0]["group_count"] if rows else 0
    for r in rows:
        r.pop("group_count", None)
    return {
        "status": "ok",
        "group_count": group_count,
        "returned": len(rows),
        "rows": rows,
        "filter": {"requirement_type": requirement_type, "naics": naics_p, "limit": n},
        "coverage": _COVERAGE,
    }


def register(mcp) -> None:
    """Mount the GovCon CAPABILITY tool group (plan §3 legs, phase-scheduled). Phase 1 ships
    the hard-predicate conjunction + facet discovery ONLY. Later legs activate here:
    Phase 2 (profiles grain-ladder leg), Phase 4 (sub pivot via govcon_sub_targeting_90day +
    sam_pocs), Phase 5 (fuzzy-X ANN over the chunk sinks) — see the module docstring."""
    for fn in (govcon_companies_by_requirements, govcon_requirement_facets):
        mcp.add_tool(fn)
