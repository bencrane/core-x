"""provider360 — entity-360 GTM targeting tools over the provider_360 +
practice_group_360 serving layer (commit e2b479c).

These tools expose the deterministic, 1-row-per-NPI / 1-row-per-buyable-group
serving layer to the gtm-agent as semantic, parameterized tools. Every query is
driven FROM THE SELECTIVE SIDE so the load-bearing Lance scalar indices fire as
``ScalarIndexQuery`` pushdown rather than full scans (the pe_thesis_query.py
discipline):

  provider_360  (9,551,447 rows / 10 fragments)
    BTREE : npi, practice_zip5, last_name, smallest_practice_group_enrlmt_id
    BITMAP: entity_type_code, is_active, practice_state, primary_taxonomy_code,
            med_a1_provider_type, med_a1_latest_year, mips_final_score_year,
            is_independent_candidate
  practice_group_360  (253,740 rows / 1 fragment)
    BTREE : group_enrlmt_id, org_name
    BITMAP: group_state

PATTERN. The specialty (``primary_taxonomy_code``) + state (``practice_state``) +
``entity_type_code`` + ``is_active`` + ``med_a1_latest_year`` predicates are pushed
into the Lance scanner (bitmap intersection); the bounded Arrow cohort is then
sorted / thresholded / aggregated in DuckDB. Non-indexed measures (money, growth,
MIPS, group size) are applied to the *already-pruned* cohort, never to the 9.5M-row
base. Point lookups (``npi``, ``org_name``) are pure Lance BTREE — no DuckDB.

SCHEMA REALITY (vs. common directive shorthand — these tools speak the real schema):
  * specialty axis is ``primary_taxonomy_code`` (NUCC) — there is no
    ``provider_type_desc``. Friendly names resolve via ``SPECIALTY_TAXONOMY``.
  * "fan_in" is the dual-pole pair ``smallest_practice_group_size`` (solo = 1) /
    ``largest_practice_group_size`` (consolidator ≥ 50) — not a single column.
  * "2024 Medicare service $" is ``med_a1_latest_mdcr_pymt`` @
    ``med_a1_latest_year = 2024``; ``svc_money_complete`` is a build GATE, not a metric.
  * there is NO EIN/TIN anywhere in the substrate; the billing-org anchor for
    record matching is ``group_enrlmt_id`` / ``pecos_asct_cntl_id`` (PECOS).
  * YoY growth materialized is 2019→latest (``med_a1_pymt_growth_2019_latest_pct``);
    a discrete 2023→2024 delta was not built.
  * DME is the SUPPLIER side ($/claims), not "referring provider-years".

Every tool returns ``elapsed_ms`` (server-side query wall-time), ``index_path``
(the pushdown taken), ``result_count``, and ``results``. Read-only; mutates nothing.
"""
from __future__ import annotations

import os
import re
import threading
import time
from typing import Any

from .. import database

FEED_PROVIDER = "provider_360"
FEED_GROUP = "practice_group_360"
DEFAULT_SNAPSHOT = "2026-06"

# OOM guard. Each broad cohort `to_table()` materialization grows RSS by hundreds of MB
# (measured ~386-500 MB, partly unreclaimed); 6 concurrent broad cohorts peaked at
# ~2333 MB, past the 2048 MB Render Standard limit. Cap concurrent cohort scans so warm
# steady-state residency + concurrent scans stay under the box. Point lookups are NOT
# gated by this (they don't go through _scan), so they stay responsive under cohort load.
_COHORT_SEM = threading.Semaphore(int(os.environ.get("GTM_MAX_CONCURRENT_COHORT_SCANS", "2")))

_TAXONOMY_RE = re.compile(r"^[0-9A-Z]{10}$")
_STATE_RE = re.compile(r"^[A-Z]{2}$")
_RESULT_CAP = 500

# Friendly specialty name → NUCC primary_taxonomy_code (the BITMAP-indexed axis).
# Covers the common PE-target specialties; pass a raw 10-char NUCC code for anything
# not listed. Multi-specialty: comma-separate (e.g. "nephrology,cardiology").
SPECIALTY_TAXONOMY: dict[str, str] = {
    "dermatology": "207N00000X", "derm": "207N00000X",
    "gastroenterology": "207RG0100X", "gi": "207RG0100X",
    "nephrology": "207RN0300X",
    "cardiology": "207RC0000X", "cardiovascular disease": "207RC0000X",
    "ophthalmology": "207W00000X",
    "urology": "208800000X",
    "orthopedic surgery": "207X00000X", "orthopaedic surgery": "207X00000X",
    "orthopedics": "207X00000X", "ortho": "207X00000X",
    "podiatry": "213E00000X", "podiatrist": "213E00000X",
    "general surgery": "208600000X",
    "otolaryngology": "207Y00000X", "ent": "207Y00000X",
    "rheumatology": "207RR0500X",
    "pulmonology": "207RP1001X",
    "endocrinology": "207RE0101X",
    "hematology oncology": "207RH0003X", "oncology": "207RH0003X",
    "plastic surgery": "208200000X",
    "pain management": "208VP0014X",
    "allergy immunology": "207KA0200X",
}

# Surgical specialties used by find_clean_independent_practices' "surgical" shorthand.
SURGICAL_TAXONOMY = ["207X00000X", "208600000X", "208800000X", "207Y00000X", "208200000X"]


# ── snapshot resolution + dataset open ───────────────────────────────────────
def _latest_snapshot(feed: str) -> str:
    """Resolve the newest ``snapshot=YYYY-MM`` partition for ``feed`` from the registry.

    NO local pin: it reads ``database.get_registry()`` fresh every call (the registry is
    a cheap cached dict that self-refreshes on its own TTL), so a newly materialized
    month is picked up without a restart. (An earlier process-lifetime ``_snapshot_cache``
    pin here was the root of a month-rollover staleness bug — see
    docs/reference/GTM_MCP_RECALL_ARCHITECTURE_DIAGNOSTIC.md.) ``{FEED}_SNAPSHOT`` env
    pins explicitly; falls back to the known-good default."""
    env = os.environ.get(f"{feed.upper()}_SNAPSHOT")
    if env:
        return env
    prefix = f"{feed}/snapshot="
    snaps = sorted(
        k[len(prefix):].rstrip("/")
        for k in database.get_registry()
        if k.startswith(prefix)
    )
    return snaps[-1] if snaps else DEFAULT_SNAPSHOT


def _uri(feed: str) -> str:
    return f"{database.ACTIVE_URI}/{feed}/snapshot={_latest_snapshot(feed)}/"


def _open(feed: str):
    """Warm-cached open of the snapshot-partitioned dataset via the shared serving-tier
    handle cache (``database.open_dataset_uri``) — the scalar index stays resident across
    calls, so a point lookup drops from ~1.5 s (per-call index reload) to tens of ms
    co-located. TTL-bounded freshness + a publish-swap stability guard live in
    database.py; a new monthly snapshot is picked up via the TTL'd registry."""
    return database.open_dataset_uri(_uri(feed))


def _lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _resolve_specialties(specialty: str | None) -> list[str]:
    """Comma-separated friendly names and/or raw NUCC codes → list of NUCC codes.
    Raises with the known names when a token resolves to neither.

    The friendly-name map is consulted FIRST: several specialty names are exactly 10
    letters (NEPHROLOGY, CARDIOLOGY, …) and would otherwise satisfy the NUCC shape and
    be passed through as bogus codes → silent zero results. A token is treated as a raw
    NUCC code only if it matches the 10-char shape AND carries a digit (every real NUCC
    taxonomy code does; a pure-alpha word never is one)."""
    if not (specialty or "").strip():
        return []
    out: list[str] = []
    for tok in specialty.split(","):
        t = tok.strip()
        if not t:
            continue
        code = SPECIALTY_TAXONOMY.get(t.lower())
        if code:
            out.append(code)
            continue
        up = t.upper()
        if _TAXONOMY_RE.match(up) and any(ch.isdigit() for ch in up):
            out.append(up)
            continue
        raise ValueError(
            f"unknown specialty {t!r}; pass a 10-char NUCC taxonomy_code (must contain a "
            f"digit) or one of: {', '.join(sorted(set(SPECIALTY_TAXONOMY)))}"
        )
    return list(dict.fromkeys(out))


def _taxonomy_pred(codes: list[str]) -> str:
    if not codes:
        return ""
    if len(codes) == 1:
        return f"primary_taxonomy_code = {_lit(codes[0])}"
    return "primary_taxonomy_code IN (" + ",".join(_lit(c) for c in codes) + ")"


def _state(state: str | None) -> str | None:
    if not (state or "").strip():
        return None
    s = state.strip().upper()
    if not _STATE_RE.match(s):
        raise ValueError(f"state must be a 2-letter USPS code, got {state!r}")
    return s


def _jsonify(v: Any) -> Any:
    """Make a Lance/Arrow/Decimal value JSON-safe for the MCP channel."""
    if isinstance(v, dict):
        return {k: _jsonify(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonify(x) for x in v]
    import decimal
    if isinstance(v, decimal.Decimal):
        return float(v)
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


def _scan(feed: str, flt: str, columns: list[str]):
    """Lance prefilter pushdown → Arrow table (the bounded cohort). Gated by the cohort
    semaphore (OOM guard): concurrent broad materializations are capped so warm residency
    + in-flight scans stay under the instance memory limit."""
    with _COHORT_SEM:
        return _open(feed).scanner(columns=columns, filter=flt, prefilter=True).to_table()


def _sort_limit(tbl, order_sql: str, limit: int, where: str | None = None) -> list[dict]:
    """DuckDB sort/threshold over the (already index-pruned) Arrow cohort."""
    cur = database.get_connection().cursor()
    try:
        cur.register("c", tbl)
        w = f"WHERE {where} " if where else ""
        rel = cur.execute(f"SELECT * FROM c {w}ORDER BY {order_sql} LIMIT {int(limit)}")
        cols = [d[0] for d in rel.description]
        return [{c: _jsonify(v) for c, v in zip(cols, r)} for r in rel.fetchall()]
    finally:
        cur.unregister("c")
        cur.close()


def _scalar(tbl, sql: str) -> list[dict]:
    cur = database.get_connection().cursor()
    try:
        cur.register("c", tbl)
        rel = cur.execute(sql)
        cols = [d[0] for d in rel.description]
        return [{c: _jsonify(v) for c, v in zip(cols, r)} for r in rel.fetchall()]
    finally:
        cur.unregister("c")
        cur.close()


def _clamp(limit: int) -> int:
    return max(1, min(int(limit), _RESULT_CAP))


# ════════════════════════ provider_360 grain ══════════════════════════════════
def get_provider_360_profile(npi: str) -> dict[str, Any]:
    """Entity-360 point lookup: collapse the entire CMS/NPPES fan-out for ONE provider
    into a single indexed row — identity, 12-yr Medicare Part B/D economics, panel
    acuity/risk, MIPS quality, the Tier-1 service/drug/DME/Open-Payments rollups, and
    the dual-pole practice-affiliation graph (smallest = candidate independent practice,
    largest = system affiliation; ``enrollment_enrlmt_ids`` is the reassignment array).
    Pushes ``npi`` into the Lance BTREE — sub-150 ms, no scan. Returns
    ``{npi, found, elapsed_ms, index_path, profile}`` (profile is the full row, or null).
    """
    n = (npi or "").strip()
    if not n.isdigit() or len(n) != 10:
        raise ValueError(f"npi must be a 10-digit string, got {npi!r}")
    t0 = time.perf_counter()
    tbl = _open(FEED_PROVIDER).scanner(filter=f"npi = {_lit(n)}", limit=1, prefilter=True).to_table()
    rows = [{c: _jsonify(v) for c, v in row.items()} for row in tbl.to_pylist()]
    return {
        "npi": n,
        "found": bool(rows),
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
        "index_path": "BTREE(npi) point lookup",
        "profile": rows[0] if rows else None,
    }


def get_independent_platforms(
    specialty: str,
    state: str | None = None,
    min_size: int = 1,
    max_size: int = 1,
    rank_by: str = "medicare_2024",
    limit: int = 50,
) -> dict[str, Any]:
    """Independent-platform identification (the sweet spot): solo / small-practice
    physicians in a specialty, optionally a state, ranked by economics. ``min_size``/
    ``max_size`` bound the dual-pole ``smallest_practice_group_size`` (1 = pure solo PC;
    2-9 = micro-partnership). ``rank_by`` ∈ ``medicare_2024`` (``med_a1_latest_mdcr_pymt``
    @ latest year 2024), ``medicare_lifetime`` (``svc_total_medicare_paid_usd``),
    ``rx_cost``, ``rx_claims``. ``specialty`` is a friendly name or NUCC code(s),
    comma-separated for several. Pushes specialty + state + entity_type + is_active
    (+ med_a1_latest_year for the 2024 rank) into the Lance bitmaps. Returns the ranked
    cohort with identity + economics.
    """
    codes = _resolve_specialties(specialty)
    if not codes:
        raise ValueError("specialty is required (name or NUCC code)")
    st = _state(state)
    limit = _clamp(limit)
    rank = {
        "medicare_2024": "med_a1_latest_mdcr_pymt",
        "medicare_lifetime": "svc_total_medicare_paid_usd",
        "rx_cost": "rx_total_drug_cost_usd",
        "rx_claims": "rx_total_claims",
    }.get(rank_by)
    if not rank:
        raise ValueError("rank_by ∈ medicare_2024 | medicare_lifetime | rx_cost | rx_claims")

    preds = [_taxonomy_pred(codes), "entity_type_code = '1'", "is_active = true",
             f"smallest_practice_group_size >= {int(min_size)}",
             f"smallest_practice_group_size <= {int(max_size)}"]
    if st:
        preds.append(f"practice_state = {_lit(st)}")
    idx = "BITMAP(primary_taxonomy_code, entity_type_code, is_active" + (", practice_state" if st else "")
    if rank_by == "medicare_2024":
        preds.append("med_a1_latest_year = 2024")
        idx += ", med_a1_latest_year"
    idx += ") prefilter + residual(smallest_practice_group_size); DuckDB top-N"

    t0 = time.perf_counter()
    tbl = _scan(FEED_PROVIDER, " AND ".join(preds), [
        "npi", "provider_name", "credential", "primary_taxonomy_code", "med_a1_provider_type",
        "practice_state", "practice_city", "smallest_practice_group_size", "med_a1_latest_year",
        "med_a1_latest_mdcr_pymt", "svc_total_medicare_paid_usd", "rx_total_drug_cost_usd",
        "rx_total_claims", "mips_final_score", "op_total_payments_usd", "is_independent_candidate"])
    results = _sort_limit(tbl, f"{rank} DESC NULLS LAST", limit)
    return {
        "specialty": specialty, "taxonomy_codes": codes, "state": st,
        "size_band": [int(min_size), int(max_size)], "rank_by": rank_by,
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
        "index_path": idx, "cohort_size": tbl.num_rows,
        "result_count": len(results), "results": results,
    }


def find_heavy_prescribers(
    specialty: str,
    state: str | None = None,
    single_practitioner: bool = True,
    rank_by: str = "rx_claims",
    limit: int = 100,
) -> dict[str, Any]:
    """Heavy Part-D prescribers in a specialty, ranked by pure prescription volume
    (``rx_claims``) or drug cost (``rx_cost``). ``single_practitioner=True`` restricts to
    a solo-PC billing structure (``smallest_practice_group_size = 1``). Specialty + state
    + entity_type + is_active push into the bitmaps; ``has_rx`` gates to actual
    prescribers. Returns the ranked cohort with the top generic each.
    """
    codes = _resolve_specialties(specialty)
    if not codes:
        raise ValueError("specialty is required (name or NUCC code)")
    st = _state(state)
    limit = _clamp(limit)
    rank = {"rx_claims": "rx_total_claims", "rx_cost": "rx_total_drug_cost_usd"}.get(rank_by)
    if not rank:
        raise ValueError("rank_by ∈ rx_claims | rx_cost")
    preds = [_taxonomy_pred(codes), "entity_type_code = '1'", "is_active = true", "has_rx = true"]
    if single_practitioner:
        preds.append("smallest_practice_group_size = 1")
    if st:
        preds.append(f"practice_state = {_lit(st)}")
    t0 = time.perf_counter()
    tbl = _scan(FEED_PROVIDER, " AND ".join(preds), [
        "npi", "provider_name", "primary_taxonomy_code", "practice_state", "practice_city",
        "rx_total_claims", "rx_total_drug_cost_usd", "rx_top1_generic", "rx_distinct_generics",
        "smallest_practice_group_size"])
    results = _sort_limit(tbl, f"{rank} DESC NULLS LAST", limit)
    return {
        "specialty": specialty, "taxonomy_codes": codes, "state": st,
        "single_practitioner": single_practitioner, "rank_by": rank_by,
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
        "index_path": "BITMAP(primary_taxonomy_code, entity_type_code, is_active"
                      + (", practice_state" if st else "") + ") prefilter + residual(has_rx, size); DuckDB top-N",
        "cohort_size": tbl.num_rows, "result_count": len(results), "results": results,
    }


def find_growth_and_quality_triggers(
    specialty: str | None = None,
    state: str | None = None,
    min_growth_pct: float = 30.0,
    min_mips: float = 70.0,
    min_medicare_floor: float = 0.0,
    limit: int = 100,
) -> dict[str, Any]:
    """Outbound trigger vector — independent practices on a Medicare growth trajectory
    that also clear a MIPS quality bar. ``min_growth_pct`` filters
    ``med_a1_pymt_growth_2019_latest_pct``; ``min_mips`` filters ``mips_final_score``.
    Drives off ``is_independent_candidate`` + (optional) specialty/state bitmaps, then
    thresholds growth+MIPS on the cohort. NOTE: the materialized growth axis is
    2019→latest, NOT a discrete 2023→2024 YoY (that delta was not built) — surfaced in
    ``notes``. ``min_medicare_floor`` (on ``med_a1_latest_mdcr_pymt``, default 0) excludes
    tiny-2019-baseline noise: a near-zero baseline yields million-percent growth that is
    a denominator artifact, not a real trajectory — set e.g. 100000 for a meaningful
    floor. Ranked by growth desc.
    """
    codes = _resolve_specialties(specialty) if specialty else []
    st = _state(state)
    limit = _clamp(limit)
    preds = ["entity_type_code = '1'", "is_active = true", "is_independent_candidate = true"]
    if codes:
        preds.append(_taxonomy_pred(codes))
    if st:
        preds.append(f"practice_state = {_lit(st)}")
    t0 = time.perf_counter()
    tbl = _scan(FEED_PROVIDER, " AND ".join(preds), [
        "npi", "provider_name", "primary_taxonomy_code", "practice_state",
        "med_a1_pymt_growth_2019_latest_pct", "mips_final_score", "med_a1_latest_year",
        "med_a1_latest_mdcr_pymt"])
    where = (f"med_a1_pymt_growth_2019_latest_pct > {float(min_growth_pct)} "
             f"AND mips_final_score >= {float(min_mips)} "
             f"AND med_a1_latest_mdcr_pymt >= {float(min_medicare_floor)}")
    results = _sort_limit(tbl, "med_a1_pymt_growth_2019_latest_pct DESC NULLS LAST", limit, where=where)
    return {
        "specialty": specialty, "taxonomy_codes": codes, "state": st,
        "min_growth_pct": min_growth_pct, "min_mips": min_mips, "min_medicare_floor": min_medicare_floor,
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
        "index_path": "BITMAP(is_independent_candidate, entity_type_code, is_active"
                      + (", primary_taxonomy_code" if codes else "")
                      + (", practice_state" if st else "") + ") prefilter; DuckDB threshold+sort",
        "cohort_size": tbl.num_rows, "result_count": len(results), "results": results,
        "notes": "growth axis = med_a1_pymt_growth_2019_latest_pct (2019→latest); "
                 "a discrete 2023→2024 YoY column is NOT materialized in provider_360.",
    }


def find_clean_independent_practices(
    specialty: str = "surgical",
    state: str | None = None,
    min_medicare_2024: float = 1_000_000.0,
    max_manufacturer_payments: float = 5_000.0,
    limit: int = 100,
) -> dict[str, Any]:
    """Industry-money contrast — high-volume independent practices that bill big to
    Medicare but take little/no manufacturer money (the "non-compromised" segment).
    Keeps providers with ``med_a1_latest_mdcr_pymt > min_medicare_2024`` and
    ``op_total_payments_usd < max_manufacturer_payments`` (NULL Open-Payments counts as
    clean). ``specialty="surgical"`` expands to the surgical taxonomy set; or pass
    specific names/codes. Ranked by Medicare $ desc.
    """
    if specialty.strip().lower() == "surgical":
        codes = list(SURGICAL_TAXONOMY)
    else:
        codes = _resolve_specialties(specialty)
    if not codes:
        raise ValueError("specialty is required")
    st = _state(state)
    limit = _clamp(limit)
    preds = [_taxonomy_pred(codes), "entity_type_code = '1'", "is_active = true",
             "is_independent_candidate = true"]
    if st:
        preds.append(f"practice_state = {_lit(st)}")
    t0 = time.perf_counter()
    tbl = _scan(FEED_PROVIDER, " AND ".join(preds), [
        "npi", "provider_name", "primary_taxonomy_code", "practice_state",
        "med_a1_latest_mdcr_pymt", "med_a1_latest_year", "op_total_payments_usd",
        "op_distinct_manufacturers", "has_op"])
    where = (f"med_a1_latest_mdcr_pymt > {float(min_medicare_2024)} "
             f"AND (op_total_payments_usd IS NULL OR op_total_payments_usd < {float(max_manufacturer_payments)})")
    results = _sort_limit(tbl, "med_a1_latest_mdcr_pymt DESC NULLS LAST", limit, where=where)
    return {
        "specialty": specialty, "taxonomy_codes": codes, "state": st,
        "min_medicare_2024": min_medicare_2024, "max_manufacturer_payments": max_manufacturer_payments,
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
        "index_path": "BITMAP(primary_taxonomy_code, is_independent_candidate, entity_type_code, is_active"
                      + (", practice_state" if st else "") + ") prefilter; DuckDB threshold+sort",
        "cohort_size": tbl.num_rows, "result_count": len(results), "results": results,
    }


def find_dme_footprint(
    specialty: str = "orthopedic surgery,podiatry",
    state: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """DME footprint — independent providers in a specialty ranked by Durable Medical
    Equipment SUPPLIER economics (``dme_supplied_total_paid_usd``). Gated on
    ``is_dme_supplier``. NOTE: provider_360 carries the DME *supplier* side (paid $,
    claims, rental share) — NOT "referring provider-years"; that referral metric was not
    materialized (surfaced in ``notes``). Ranked by DME paid $ desc.
    """
    codes = _resolve_specialties(specialty)
    if not codes:
        raise ValueError("specialty is required")
    st = _state(state)
    limit = _clamp(limit)
    preds = [_taxonomy_pred(codes), "entity_type_code = '1'", "is_active = true",
             "is_independent_candidate = true", "is_dme_supplier = true"]
    if st:
        preds.append(f"practice_state = {_lit(st)}")
    t0 = time.perf_counter()
    tbl = _scan(FEED_PROVIDER, " AND ".join(preds), [
        "npi", "provider_name", "primary_taxonomy_code", "practice_state",
        "dme_supplied_total_paid_usd", "dme_supplied_claims", "dme_rental_share",
        "dme_distinct_hcpcs"])
    results = _sort_limit(tbl, "dme_supplied_total_paid_usd DESC NULLS LAST", limit)
    return {
        "specialty": specialty, "taxonomy_codes": codes, "state": st,
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
        "index_path": "BITMAP(primary_taxonomy_code, is_independent_candidate, entity_type_code, is_active"
                      + (", practice_state" if st else "") + ") prefilter + residual(is_dme_supplier); DuckDB top-N",
        "cohort_size": tbl.num_rows, "result_count": len(results), "results": results,
        "notes": "DME = supplier side (dme_supplied_*); 'referring provider-years' is not "
                 "a materialized metric in provider_360.",
    }


def get_dual_pole_leakage(
    state: str | None = None,
    specialty: str | None = None,
    max_secondary_group_size: int = 3,
    limit: int = 100,
) -> dict[str, Any]:
    """Dual-pole leakage — physicians who bill through their own solo PC
    (``smallest_practice_group_size = 1``) yet ALSO hold a minor, non-exclusive
    attachment to one or two small community groups (``practice_group_count >= 2`` and
    ``largest_practice_group_size`` between 2 and ``max_secondary_group_size``). Both
    poles + their org names are returned so the leakage is legible. Optional
    state/specialty bitmaps narrow it. Ranked by secondary group size.
    """
    codes = _resolve_specialties(specialty) if specialty else []
    st = _state(state)
    limit = _clamp(limit)
    preds = ["entity_type_code = '1'", "is_active = true", "practice_group_count >= 2",
             "smallest_practice_group_size = 1", "largest_practice_group_size >= 2",
             f"largest_practice_group_size <= {int(max_secondary_group_size)}"]
    if codes:
        preds.append(_taxonomy_pred(codes))
    if st:
        preds.append(f"practice_state = {_lit(st)}")
    t0 = time.perf_counter()
    tbl = _scan(FEED_PROVIDER, " AND ".join(preds), [
        "npi", "provider_name", "primary_taxonomy_code", "practice_state", "practice_group_count",
        "smallest_practice_group_enrlmt_id", "smallest_practice_group_size", "smallest_practice_org_name",
        "largest_practice_group_enrlmt_id", "largest_practice_group_size", "largest_practice_org_name"])
    results = _sort_limit(tbl, "largest_practice_group_size DESC, npi", limit)
    return {
        "state": st, "specialty": specialty, "taxonomy_codes": codes,
        "max_secondary_group_size": int(max_secondary_group_size),
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
        "index_path": "BITMAP(entity_type_code, is_active"
                      + (", primary_taxonomy_code" if codes else "")
                      + (", practice_state" if st else "") + ") prefilter + residual(dual-pole sizes); DuckDB top-N",
        "cohort_size": tbl.num_rows, "result_count": len(results), "results": results,
    }


# ════════════════════════ practice_group_360 grain ════════════════════════════
def find_acquisition_target_groups(
    specialty: str | None = None,
    state: str | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
    min_panel_risk: float | None = None,
    rank_by: str = "medicare",
    limit: int = 50,
) -> dict[str, Any]:
    """Buyable-group acquisition targets — the practice_group_360 grain (1 row per
    rcv-ENRLMT_ID group, member economics rolled up). Filter by ``specialty``
    (``top_specialty`` = modal member NUCC), ``state`` (``group_state`` BITMAP),
    ``min_size``/``max_size`` (``member_count``: 3-9 micro-partnership, 10-15 mid-market),
    and ``min_panel_risk`` (``avg_panel_risk_score`` — acuity). ``rank_by`` ∈ ``medicare``,
    ``rx``, ``risk``, ``mips``, ``op``, ``size``, ``independent``. Groups with member_count
    ≤ 15 are sub-consolidator by construction (no fan-in ≥ 50). NOTE: only ``group_state``
    is bitmap-indexed on this dataset — specialty/size are residual filters over 253K
    groups (still fast; ~270 ms cold). Ranked desc.
    """
    codes = _resolve_specialties(specialty) if specialty else []
    st = _state(state)
    limit = _clamp(limit)
    rank = {"medicare": "total_medicare_paid_usd", "rx": "total_rx_cost_usd",
            "risk": "avg_panel_risk_score", "mips": "avg_mips_score",
            "op": "total_op_payments_usd", "size": "member_count",
            "independent": "independent_member_count"}.get(rank_by)
    if not rank:
        raise ValueError("rank_by ∈ medicare | rx | risk | mips | op | size | independent")
    preds: list[str] = []
    if st:
        preds.append(f"group_state = {_lit(st)}")
    if codes:
        preds.append("top_specialty IN (" + ",".join(_lit(c) for c in codes) + ")")
    if min_size is not None:
        preds.append(f"member_count >= {int(min_size)}")
    if max_size is not None:
        preds.append(f"member_count <= {int(max_size)}")
    if min_panel_risk is not None:
        preds.append(f"avg_panel_risk_score > {float(min_panel_risk)}")
    flt = " AND ".join(preds) if preds else "member_count >= 1"
    t0 = time.perf_counter()
    tbl = _scan(FEED_GROUP, flt, [
        "group_enrlmt_id", "org_name", "group_state", "member_count", "top_specialty",
        "distinct_specialties", "total_medicare_paid_usd", "total_rx_cost_usd",
        "avg_panel_risk_score", "avg_mips_score", "independent_member_count",
        "total_op_payments_usd", "n_states"])
    results = _sort_limit(tbl, f"{rank} DESC NULLS LAST", limit)
    return {
        "specialty": specialty, "taxonomy_codes": codes, "state": st,
        "size_band": [min_size, max_size], "min_panel_risk": min_panel_risk, "rank_by": rank_by,
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
        "index_path": ("BITMAP(group_state) prefilter + " if st else "")
                      + "residual(top_specialty, member_count, avg_panel_risk_score); DuckDB top-N",
        "cohort_size": tbl.num_rows, "result_count": len(results), "results": results,
    }


def get_practice_group_roster(
    org_name: str | None = None,
    group_enrlmt_id: str | None = None,
) -> dict[str, Any]:
    """Regional-anchor audit — point lookup of one buyable group by ``org_name`` (BTREE,
    exact) or ``group_enrlmt_id`` (BTREE), returning the complete NPI roster
    (``member_npis``), member count, top specialty, multi-state spread, and the rolled-up
    clinical/economic footprint. Sub-150 ms, no scan. Returns ``{found, match_count,
    elapsed_ms, index_path, groups}``.
    """
    if not (org_name or "").strip() and not (group_enrlmt_id or "").strip():
        raise ValueError("pass org_name or group_enrlmt_id")
    if group_enrlmt_id:
        flt, idx = f"group_enrlmt_id = {_lit(group_enrlmt_id.strip())}", "BTREE(group_enrlmt_id) point lookup"
    else:
        flt, idx = f"org_name = {_lit(org_name.strip())}", "BTREE(org_name) point lookup"
    t0 = time.perf_counter()
    tbl = _open(FEED_GROUP).scanner(filter=flt, limit=50, prefilter=True).to_table()
    groups = [{c: _jsonify(v) for c, v in row.items()} for row in tbl.to_pylist()]
    return {
        "org_name": org_name, "group_enrlmt_id": group_enrlmt_id,
        "found": bool(groups), "match_count": len(groups),
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
        "index_path": idx, "groups": groups,
    }


def extract_practice_eins_for_matching(
    specialty: str,
    state: str,
    min_size: int = 2,
    max_size: int = 9,
) -> dict[str, Any]:
    """Corporate billing-anchor extractor for record matching — the distinct billing-org
    identifiers for independent partnerships in a specialty + state + size band.

    IMPORTANT: there is NO EIN/TIN anywhere in the provider_360/practice_group_360
    substrate (CMS PECOS exposes EINs only in raw enrollment, which was not carried into
    the serving layer). The stable matching anchor returned here is ``group_enrlmt_id``
    (the PECOS group enrollment id) with its ``org_name`` — the correct join key for
    record-matching a group to an external roster. ``org_name_coverage_pct`` is reported
    so the caller can apply the ≥99% validity gate. Returns the distinct anchors + the
    coverage stat.
    """
    codes = _resolve_specialties(specialty)
    if not codes:
        raise ValueError("specialty is required")
    st = _state(state)
    if not st:
        raise ValueError("state is required (2-letter USPS)")
    preds = [f"group_state = {_lit(st)}", f"member_count >= {int(min_size)}",
             f"member_count <= {int(max_size)}",
             "top_specialty IN (" + ",".join(_lit(c) for c in codes) + ")"]
    t0 = time.perf_counter()
    tbl = _scan(FEED_GROUP, " AND ".join(preds),
                ["group_enrlmt_id", "org_name", "group_state", "member_count", "top_specialty"])
    cov = _scalar(tbl, "SELECT round(100.0*count(org_name)/nullif(count(*),0),2) AS pct FROM c")
    anchors = _scalar(tbl, "SELECT DISTINCT group_enrlmt_id, org_name, member_count "
                           "FROM c ORDER BY member_count DESC, org_name")
    coverage = cov[0]["pct"] if cov else None
    return {
        "specialty": specialty, "taxonomy_codes": codes, "state": st,
        "size_band": [int(min_size), int(max_size)],
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
        "index_path": "BITMAP(group_state) prefilter + residual(member_count, top_specialty); DuckDB distinct",
        "anchor_key": "group_enrlmt_id",
        "org_name_coverage_pct": coverage,
        "coverage_gate_99_pct": (coverage is not None and coverage >= 99.0),
        "distinct_anchor_count": len(anchors),
        "anchors": anchors,
        "notes": "No EIN/TIN exists in the serving layer; group_enrlmt_id (PECOS group "
                 "enrollment id) is the canonical billing-org matching anchor.",
    }


def register(mcp) -> None:
    """Mount the provider_360 targeting tools onto the FastMCP server. Each function's
    signature + docstring becomes the tool's input schema + agent-facing contract."""
    for fn in (
        # provider_360 grain
        get_provider_360_profile, get_independent_platforms, find_heavy_prescribers,
        find_growth_and_quality_triggers, find_clean_independent_practices,
        find_dme_footprint, get_dual_pole_leakage,
        # practice_group_360 grain
        find_acquisition_target_groups, get_practice_group_roster,
        extract_practice_eins_for_matching,
    ):
        mcp.add_tool(fn)
