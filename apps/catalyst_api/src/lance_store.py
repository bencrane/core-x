"""Lance access layer for catalyst_api — domain → UEI → federal award profile.

THREE native ``BTREE`` point-lookups over the committed R2 sink, no DuckDB, no
full scans:

  1. ``firmographics_blitz`` — BTREE on ``domain_norm`` resolves the input domain
     to a company + its SAM.gov ``uei``.
  2. ``contractor_award_summary`` — BTREE on ``recipient_uei`` returns the 1-row
     federal award rollup (lifetime obligated, active/closed counts, top agencies,
     primary NAICS/PSC, action dates). This is the load-bearing sub-100 ms anchor.
  3. ``usaspending/award_search`` — BTREE on ``recipient_uei`` returns the prime
     award line items, for the optional recent-awards detail call (bounded).

DATASET HANDLES ARE WARM-CACHED (the gtm_mcp ``database._cached_dataset`` precedent):
pylance does NOT share a resident scalar index across handles, so re-opening per call
re-paid the BTREE load from R2 on every request (~78% of a cold lookup — see
``GTM_MCP_RECALL_ARCHITECTURE_DIAGNOSTIC.md``; measured here as the 3–4 s dossier
open). ``_dataset`` keeps ONE handle per URI with stale-while-revalidate semantics:

  • within ``CATALYST_DATASET_TTL_SECONDS`` (default 300 s) the cached handle is
    returned as-is — indices stay resident, point-lookups stay tens-of-ms;
  • past the TTL the STALE handle keeps serving (no request ever blocks on a
    refresh) while a single-flight background thread opens a fresh handle, WARMS
    its key BTREE (a no-match indexed probe), then swaps it in;
  • staleness is therefore bounded by ~TTL + one refresh: the serving tables are
    rebuilt by in-place ``mode="overwrite"`` (an ATOMIC manifest commit — unlike
    the directory-swap publishes gtm_mcp's double-listing guards, so no
    mid-swap-partial check is needed) at most daily, and a rebuild becomes
    visible here within ~5 minutes.

Memory bound: HANDLES are cached (a fixed set of ~9 config URIs), never scan
results; the heaviest index (award_search, ~379 MB) loads lazily only if its
opt-in detail surface is actually exercised.

The caller's domain is normalized to the stored anchor and validated against a
strict charset before it is ever interpolated into a Lance filter expression
(single quotes are additionally doubled), so the predicate cannot be broken out of.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date as dt_date, timedelta
from typing import Any

import lance

from . import config

log = logging.getLogger("catalyst_api.lance_store")

# A domain is not guaranteed unique in firmographics (multiple source runs may
# carry the same domain); cap the fan-out and pick the best row deterministically.
_FIRMO_LIMIT = 25
# Per-UEI prime-award fan-out ceiling for the recent-awards detail call.
_AWARDS_HARD_CAP = 100

# Domain anchor normalization — mirrors apps/gtm_mcp/src/tools/audience.py and
# pipelines/gtm/companies_people_bulk.py EXACTLY so a caller's raw input collapses
# to the same stored anchor the BTREE is built on.
_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")
# Post-normalization guard: a registrable domain is lowercase alnum + dot + hyphen,
# and must contain a dot. Defense-in-depth alongside _sql_str quote-doubling.
_DOMAIN_OK = re.compile(r"^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$")


def normalize_domain(raw: str) -> str | None:
    d = (raw or "").strip().lower()
    d = _SCHEME.sub("", d)
    d = _WWW.sub("", d)
    d = d.split("/", 1)[0]
    d = d.split("?", 1)[0]
    d = d.split(":", 1)[0]  # strip any :port
    d = d.rstrip(".")
    return d or None


def valid_domain(norm: str) -> bool:
    return bool(norm) and "." in norm and _DOMAIN_OK.match(norm) is not None


# A UEI is exactly 12 alphanumerics. Validated before interpolation into a Lance
# filter (defense-in-depth alongside _sql_str quote-doubling) — the BFF supplies a
# session-resolved UEI, but the gateway never trusts its callers blindly.
_UEI_OK = re.compile(r"^[A-Za-z0-9]{12}$")


def valid_uei(uei: str) -> bool:
    return bool(uei) and _UEI_OK.match(uei) is not None


def _sql_str(value: str) -> str:
    """A safe single-quoted SQL/Lance string literal (quotes doubled)."""
    return "'" + value.replace("'", "''") + "'"


# ── Warm dataset-handle cache (stale-while-revalidate) ───────────────────────
# {uri: (expiry_monotonic, handle)}. The URI set is fixed by config (~9 entries),
# so the cache is bounded by construction — no sweep needed.
_HANDLE_TTL_S = float(os.environ.get("CATALYST_DATASET_TTL_SECONDS", "300"))
_handle_lock = threading.Lock()
_handle_cache: dict[str, tuple[float, Any]] = {}
_refreshing: set[str] = set()
_REFRESH_POOL = ThreadPoolExecutor(max_workers=2, thread_name_prefix="lance-refresh")


def _now() -> float:
    return time.monotonic()


def _open_uri(uri: str):
    """One fresh manifest open against R2 — the only place a handle is created."""
    return lance.dataset(uri, storage_options=config.r2_storage_options())


def _warm_key_columns() -> dict[str, str]:
    """URI → the BTREE column a no-match probe warms. Covers the dossier-critical
    point-lookup surfaces; URIs absent here refresh manifest-only (their indices
    load lazily on first real use — deliberate, see the memory bound note above)."""
    return {
        config.ENTITY_PROFILE_GOLD_URI: "uei",
        config.CONTRACTOR_AWARD_SUMMARY_URI: "recipient_uei",
        config.MAP_DATASET_URIS["awards"]: "winner_uei",
    }


def _warm_handle(uri: str, ds) -> None:
    """Best-effort: page the handle's key BTREE into residency with a no-match
    indexed probe, so the FIRST real lookup on this handle is already warm."""
    col = _warm_key_columns().get(uri)
    if col is None:
        return
    try:
        ds.scanner(columns=[col], filter=f"{col} = '__warm__'").to_table()
    except Exception as exc:  # noqa: BLE001 — warming must never break serving
        log.warning("handle warm failed for %s: %s", uri, exc)


def _refresh_handle(uri: str) -> None:
    """Single-flight background refresh: open a fresh handle, warm its key index,
    THEN swap it into the cache — readers keep the stale-but-warm handle until the
    replacement is equally warm, so no request ever pays the cold-index cost."""
    try:
        ds = _open_uri(uri)
        _warm_handle(uri, ds)
        with _handle_lock:
            _handle_cache[uri] = (_now() + _HANDLE_TTL_S, ds)
        log.info("refreshed lance handle %s (version %s)", uri, getattr(ds, "version", "?"))
    except Exception as exc:  # noqa: BLE001 — keep serving the stale handle on failure
        log.warning("handle refresh failed for %s (stale handle keeps serving): %s", uri, exc)
    finally:
        with _handle_lock:
            _refreshing.discard(uri)


def _dataset(uri: str):
    """Warm, TTL'd ``LanceDataset`` for ``uri`` (stale-while-revalidate — module
    docstring has the full semantics). Concurrent FIRST-callers may each open once
    (cost-only, converges); after that the cached handle serves every request.

    The refresh is SUBMITTED OUTSIDE ``_handle_lock``: ``_refresh_handle`` takes the
    (non-reentrant) lock itself, so submitting under it would deadlock any executor
    that runs inline — and no lock should span an executor call anyway."""
    now = _now()
    schedule_refresh = False
    cached = None
    with _handle_lock:
        hit = _handle_cache.get(uri)
        if hit is not None:
            expiry, cached = hit
            if expiry <= now and uri not in _refreshing:
                _refreshing.add(uri)
                schedule_refresh = True
    if cached is not None:
        if schedule_refresh:
            _REFRESH_POOL.submit(_refresh_handle, uri)
        return cached                      # fresh OR stale-while-revalidating
    ds = _open_uri(uri)                    # first-ever open for this URI
    with _handle_lock:
        _handle_cache[uri] = (now + _HANDLE_TTL_S, ds)
    return ds


def prewarm_dossier_surfaces() -> dict[str, float]:
    """Boot-time prewarm: open + index-warm the dossier-critical handles so the
    FIRST real request after a deploy is already sub-second. Returns per-URI warm
    seconds for the boot log. Best-effort by contract — never raises."""
    timings: dict[str, float] = {}
    for uri in _warm_key_columns():
        t0 = _now()
        try:
            _warm_handle(uri, _dataset(uri))
        except Exception as exc:  # noqa: BLE001
            log.warning("prewarm failed for %s: %s", uri, exc)
        timings[uri.rstrip("/").rsplit("/", 1)[-1]] = round(_now() - t0, 3)
    return timings


# ── Lookup 1: domain → company + UEI ─────────────────────────────────────────
_FIRMO_COLS = [
    "domain_norm", "domain_raw", "uei", "company_name", "website", "industry",
    "employee_size_band", "founded_year", "hq_city", "hq_state", "hq_region",
    "source_updated_at",
]


def resolve_company_by_domain(norm_domain: str) -> dict[str, Any] | None:
    """BTREE point-lookup on ``firmographics_blitz.domain_norm``. Returns the best
    matching company row (prefers a row carrying a UEI, then the most recently
    sourced), or ``None`` when the domain is unknown. Caller pre-normalizes +
    validates ``norm_domain``."""
    rows = (
        _dataset(config.FIRMOGRAPHICS_URI)
        .scanner(columns=_FIRMO_COLS, filter=f"domain_norm = {_sql_str(norm_domain)}", limit=_FIRMO_LIMIT)
        .to_table()
        .to_pylist()
    )
    if not rows:
        return None

    def _rank(r: dict[str, Any]):
        has_uei = bool((r.get("uei") or "").strip())
        updated = r.get("source_updated_at")
        # Prefer a row with a UEI; then the most recent source_updated_at.
        return (1 if has_uei else 0, updated.timestamp() if updated else 0.0)

    return max(rows, key=_rank)


# ── Lookup 2: UEI → federal award summary (the sub-100 ms anchor) ────────────
_SUMMARY_COLS = [
    "recipient_uei", "lifetime_prime_obligated", "lifetime_subaward_obligated",
    "total_combined_obligated", "prime_total_awards", "prime_active_awards",
    "prime_closed_awards", "subaward_total", "subaward_active", "subaward_closed",
    "total_combined_awards", "contract_dollars", "grant_dollars", "other_dollars",
    "prime_first_award_date", "prime_most_recent_action_date",
    "prime_most_recent_obligation", "top_agency_1_name", "top_agency_1_dollars",
    "top_agency_2_name", "top_agency_2_dollars", "top_agency_3_name",
    "top_agency_3_dollars", "primary_naics", "primary_psc", "summary_as_of_date",
]


def award_summary_by_uei(uei: str) -> dict[str, Any] | None:
    """BTREE point-lookup on ``contractor_award_summary.recipient_uei`` (one row
    per recipient). Returns the federal award rollup, or ``None`` when the entity
    has no federal contracting footprint."""
    uei = (uei or "").strip()
    if not uei:
        return None
    rows = (
        _dataset(config.CONTRACTOR_AWARD_SUMMARY_URI)
        .scanner(columns=_SUMMARY_COLS, filter=f"recipient_uei = {_sql_str(uei)}", limit=1)
        .to_table()
        .to_pylist()
    )
    return rows[0] if rows else None


# ── Lookup 3: UEI → prime award line items (optional detail) ─────────────────
_AWARD_COLS = [
    "generated_unique_award_id", "display_award_id", "category", "type_description",
    "total_obligation", "award_amount", "naics_code", "naics_description",
    "product_or_service_description", "funding_toptier_agency_name",
    "awarding_toptier_agency_name", "period_of_performance_start_date",
    "period_of_performance_current_end_date", "description", "type_set_aside",
]


def recent_awards_by_uei(uei: str, limit: int) -> list[dict[str, Any]]:
    """BTREE point-lookup on ``award_search.recipient_uei`` for prime award line
    items, returned highest-obligation first. ``award_search`` is the 78M-row
    transactional dataset, so this is a separate, opt-in detail call (not part of
    the fast profile path): a bounded projection + hard fan-out cap keep it
    predictable."""
    uei = (uei or "").strip()
    if not uei:
        return []
    cap = max(1, min(limit, _AWARDS_HARD_CAP))
    rows = (
        _dataset(config.AWARD_SEARCH_URI)
        .scanner(columns=_AWARD_COLS, filter=f"recipient_uei = {_sql_str(uei)}", limit=_AWARDS_HARD_CAP)
        .to_table()
        .to_pylist()
    )
    rows.sort(key=lambda r: (r.get("total_obligation") or r.get("award_amount") or 0.0), reverse=True)
    return rows[:cap]


# ── Surface 1: UEI → SAM.gov entity profile ──────────────────────────────────
# Projection over sam_master_entities (the live SAM identity dataset). registration_status
# and registration_date have NO direct source column here — status is DERIVED from
# is_active + registration_expiration_date, and registration_date maps to
# initial_registration_date (see models.SamProfileResponse.from_row). business_types_raw is
# the raw ~-delimited bus_type_string; the parsed list lives in naics_codes/psc_codes/
# business_types but the surface emits the raw string per the operator no-parse ruling.
_SAM_ENTITY_COLS = [
    "uei", "legal_business_name", "cage_code", "is_active", "purpose_of_registration",
    "initial_registration_date", "registration_expiration_date", "activation_date",
    "last_update_date", "primary_naics", "naics_codes", "psc_codes", "bus_type_string",
    "physical_address_city", "physical_address_province_or_state", "physical_address_zip_postal_code",
]


def _scan(uri: str, **scanner_kwargs) -> list[dict[str, Any]]:
    """Open + scan a committed dataset, returning rows (``[]`` when no row matches the
    filter — a dataset that EXISTS but has no row for this UEI).

    A genuinely MISSING dataset (a wrong / unmaterialized URI) is NOT swallowed — it raises,
    surfacing the misconfiguration as a loud 5xx. The prior "not found → []" swallow made a
    misrouted SAM URI (``sam_entity_master`` vs the real ``sam_master_entities``) indis-
    tinguishable from "entity unregistered": every /sam-profile 404'd for a whole release.
    Absence-of-data is a zero-row scan of a dataset that exists, never a dataset that isn't
    there. Boot-time ``probe_surfaces()`` makes the same class of misconfig visible up front."""
    return _dataset(uri).scanner(**scanner_kwargs).to_table().to_pylist()


def sam_entity_by_uei(uei: str) -> dict[str, Any] | None:
    """BTREE point-lookup on ``sam_entity_master.uei`` (1 row/active-v2 UEI).
    Returns the SAM identity + NAICS/PSC arrays + raw ``business_types`` + physical
    city/state/zip5, or ``None`` when the UEI has no active registration (or the
    dataset is not yet materialized). Caller pre-validates ``uei`` charset."""
    uei = (uei or "").strip()
    if not uei:
        return None
    rows = _scan(config.SAM_ENTITY_MASTER_URI, columns=_SAM_ENTITY_COLS,
                 filter=f"uei = {_sql_str(uei)}", limit=1)
    return rows[0] if rows else None


_SAM_POC_COLS = [
    "uei", "source_family", "poc_type", "poc_slot_no", "full_name",
    "first_name", "last_name", "title", "city", "state",
]
# The POC slots come pre-nested per-UEI on the gold spine — a point-lookup on the
# 1.54M-row entity_profile_gold (BTREE uei) instead of the 8.07M-row sam_pocs scan.
_GOLD_POC_COLS = ["uei", "pocs"]
# 6 SAM POC slots × (primary + alternate) — a tight upper bound on rows per UEI.
_POC_HARD_CAP = 12


def sam_pocs_by_uei(uei: str) -> list[dict[str, Any]]:
    """Government POC slots for a UEI, ordered by slot. Primary source is the
    pre-nested ``entity_profile_gold.pocs`` list<struct> (built by pipelines/resolution/
    reconcile_entity_profiles.py _build_sam_spine → pocs_nested). Re-sourcing the POCs from
    that 1.54M-row indexed gold row collapses the lookup from a per-request COLD index load
    over the 8.07M-row ``sam_pocs`` dataset (~6–8 s — the same per-call-open / index-cold-load
    pattern that gated active-contracts/past-performance before they moved to the gold mirror)
    to a single sub-2 s point-lookup. The gold struct carries the EXACT fields
    ``models.SamPoc.from_row`` reads (poc_type, poc_slot_no, full_name, first/last_name, title,
    city, state), so there is no projection drift.

    ``sam_pocs`` stays the fallback ONLY when a UEI is absent from the gold spine (present in
    sam_master_entities but not in the normalized-name JOIN that builds gold). A gold row with
    no populated slots is authoritative ``[]`` — it does NOT trigger the fallback."""
    uei = (uei or "").strip()
    if not uei:
        return []
    gold = _scan(config.ENTITY_PROFILE_GOLD_URI, columns=_GOLD_POC_COLS,
                 filter=f"uei = {_sql_str(uei)}", limit=1)
    if gold:
        pocs = list(gold[0].get("pocs") or [])
        pocs.sort(key=lambda r: (r.get("poc_slot_no") or 0))
        return pocs[:_POC_HARD_CAP]
    return _sam_pocs_from_source(uei)


def _sam_pocs_from_source(uei: str) -> list[dict[str, Any]]:
    """Fallback: direct BTREE point-lookup on ``sam_pocs.uei`` → the government POC slots
    (v2 spine only; legacy cage-keyed rows are out of scope), ordered by slot. The source
    carries no email/phone columns. Reached only for a UEI absent from the gold spine."""
    rows = _scan(config.SAM_POCS_URI, columns=_SAM_POC_COLS,
                 filter=f"uei = {_sql_str(uei)} AND source_family = 'v2'",
                 limit=_POC_HARD_CAP)
    rows.sort(key=lambda r: (r.get("poc_slot_no") or 0))
    return rows


# ── Surface 2: UEI → unified gold profile (Overview + active/past headlines) ──
_GOLD_COLS = [
    "uei", "cage_code", "legal_business_name", "primary_naics", "is_active",
    "total_active_obligations", "total_lifetime_obligations", "award_count",
    "active_award_count", "has_federal_awards", "profile_as_of_date",
]


def entity_profile_by_uei(uei: str) -> dict[str, Any] | None:
    """BTREE point-lookup on ``entity_profile_gold.uei`` (1 row/UEI). Carries the
    pre-materialized lifetime/active obligation sums + award counts from the
    SAM×USAspending reconciliation — the Overview surface and the active/past
    count+total headlines read straight off this row (no DuckDB aggregate, per the
    Gold-Mirror ruling). ``None`` when the UEI is absent from the gold spine."""
    uei = (uei or "").strip()
    if not uei:
        return None
    rows = _scan(config.ENTITY_PROFILE_GOLD_URI, columns=_GOLD_COLS,
                 filter=f"uei = {_sql_str(uei)}", limit=1)
    return rows[0] if rows else None


# ── Dossier surface: UEI → identity + posture + recent actions + POCs ─────────
# The entity side-panel read path: THREE BTREE point-lookups composed by the route —
# entity_profile_gold (full identity projection incl. the pre-nested POC slots),
# contractor_award_summary (lifetime rollup + most-recent action date + top agencies,
# reused via award_summary_by_uei), and the 90d awards map serving table (per-action
# recent-activity feed). No DuckDB, no scans beyond the indexed point-lookups.
_DOSSIER_GOLD_COLS = [
    "uei", "cage_code", "is_active", "has_federal_awards", "legal_business_name",
    "dba_name", "primary_naics", "physical_address_line_1", "physical_address_city",
    "physical_address_state", "physical_address_zip_postal_code",
    "physical_address_country_code", "total_active_obligations",
    "total_lifetime_obligations", "award_count", "active_award_count", "pocs",
    "profile_as_of_date",
]


def entity_dossier_gold(uei: str) -> dict[str, Any] | None:
    """BTREE point-lookup on ``entity_profile_gold.uei`` with the FULL dossier
    projection (identity + address + DBA + rollups + nested POC slots). ``None``
    when the UEI is absent from the gold spine (the route 404s)."""
    uei = (uei or "").strip()
    if not uei:
        return None
    rows = _scan(config.ENTITY_PROFILE_GOLD_URI, columns=_DOSSIER_GOLD_COLS,
                 filter=f"uei = {_sql_str(uei)}", limit=1)
    return rows[0] if rows else None


_RECENT_ACTION_COLS = [
    "award_id", "action_date", "award_amount", "awarding_agency", "awarding_sub_agency",
    "winner_type", "pop_state", "pop_city", "set_aside", "naics_code",
]
# Drawer fan-out ceiling — the rolling-90d table naturally bounds a UEI's actions.
_RECENT_ACTIONS_HARD_CAP = 25


def recent_award_actions_by_uei(uei: str, limit: int = 10) -> list[dict[str, Any]]:
    """BTREE point-lookup on ``usaspending_awards_map_serving.winner_uei`` → the
    entity's award actions in the rolling ~90d window, newest first, capped.

    ``scanner(limit=)`` is deliberately NOT used (the pylance limit-before-filter
    planner returns a fraction of matches); the per-UEI fan-out is naturally small,
    so the filtered rows are fetched whole and sorted/capped in process."""
    uei = (uei or "").strip()
    if not uei:
        return []
    cap = max(1, min(limit, _RECENT_ACTIONS_HARD_CAP))
    rows = _scan(config.MAP_DATASET_URIS["awards"], columns=_RECENT_ACTION_COLS,
                 filter=f"winner_uei = {_sql_str(uei)}")
    rows.sort(key=lambda r: (r.get("action_date") or dt_date.min,
                             r.get("award_amount") or 0.0), reverse=True)
    return rows[:cap]


# ── Surface 3: UEI → active / past prime award line items (Gold-Mirror point-lookup) ──
# Pre-materialized per-UEI by pipelines/resolution/award_lines_gold.py into
# entity_award_lines_gold (1 row/uei, BTREE uei). active_contracts / past_performance are
# nested list<struct> columns, already PoP-classified (vs the build date, the same
# convention entity_profile_gold uses for its counts) and pre-sorted by obligation desc.
# This replaces the old per-request ~80s COLD scan of the 78.6M-row award_search — which
# reopened a fresh dataset handle each call and re-paid the 379 MB index cold-load over R2 —
# with a single sub-second indexed lookup. Each struct carries the EXACT award_search column
# names ActiveContract.from_row consumes, so there is no projection drift gateway-side.
def entity_award_lines_by_uei(uei: str, side: str, limit: int) -> list[dict[str, Any]]:
    """Top award line items for a UEI on one side of the PoP split. ``side`` is ``"active"``
    (period of performance not elapsed) or ``"closed"`` (elapsed → past performance). Returns
    the pre-sorted, pre-capped nested list (≤ limit), or ``[]`` when the UEI has no row in the
    gold mirror (no federal awards, or none on this side)."""
    uei = (uei or "").strip()
    if not uei:
        return []
    cap = max(1, min(limit, _AWARDS_HARD_CAP))
    col = "active_contracts" if side == "active" else "past_performance"
    rows = _scan(config.ENTITY_AWARD_LINES_GOLD_URI, columns=["uei", col],
                 filter=f"uei = {_sql_str(uei)}", limit=1)
    if not rows:
        return []
    items = rows[0].get(col) or []
    return items[:cap]


# ── Surface 4: UEI → subawardee capability profile + prime-contract (subaward) history ──
# The sub-side drill-down. The capability profile (govcon_subawardee_capability_profiles, 1 row/sub_uei,
# BTREE sub_uei) carries the structured capability block (scope/tags/clearance/teaming/POC) for the
# ~6,586 bridge subs; contract_subaward (the raw sub→prime fact) carries the prime-contract history for
# all ~25,449 subs. CUI: the profile carries only structured / controlled-vocab + sub-self-reported
# columns (no solicitation chunk verbatim — evidence_quote / requirement_detail are not on its schema);
# subaward_description is the firm's own SAM subaward report (sub-self-reported, not CUI).
_SUB_PROFILE_COLS = [
    "sub_uei", "sub_name", "has_extracted_scope", "scope_summary", "capability_tags",
    "requires_clearance", "req_clearance_level_max", "requires_cmmc", "req_cert_tags",
    "top_labor_categories", "n_subawards", "n_distinct_primes_subaward", "total_subaward_amount",
    "n_scope_solicitations", "n_teaming_primes", "teaming_dollars_5y", "teaming_prime_names",
    "poc_available", "poc_full_name", "poc_title", "poc_city", "poc_state",
    # Path B: self-reported capability (the sub's own descriptions) + provenance marker
    "self_reported_capability_tags", "n_self_reported_tags", "tag_source",
]
_SUBAWARD_HISTORY_COLS = [
    "subawardee_name", "prime_award_unique_key", "prime_award_piid", "prime_awardee_uei",
    "prime_awardee_name", "prime_award_awarding_agency_name", "subaward_amount", "subaward_number",
    "subaward_action_date", "subaward_description", "prime_award_naics_code", "usaspending_permalink",
]
_SUBAWARD_HISTORY_HARD_CAP = 200


def subaward_profile_by_uei(uei: str) -> dict[str, Any] | None:
    """BTREE point-lookup on ``govcon_subawardee_capability_profiles.sub_uei`` → the sub's capability
    profile, or ``None`` when the sub is not in the bridge universe (it may still have subaward history)."""
    uei = (uei or "").strip()
    if not uei:
        return None
    rows = _scan(config.SUBAWARDEE_CAPABILITY_PROFILES_URI, columns=_SUB_PROFILE_COLS,
                 filter=f"sub_uei = {_sql_str(uei)}", limit=1)
    return rows[0] if rows else None


def subaward_history_by_uei(uei: str, limit: int) -> list[dict[str, Any]]:
    """Point-lookup on ``contract_subaward.subawardee_uei`` → the prime contracts the sub won work
    under, projected to the join keys + $ + description, sorted by subaward amount desc, capped.
    ``[]`` when the sub has no subaward fact rows. (Amount is a source string → parsed for the sort
    only; the model coerces for the wire.)"""
    uei = (uei or "").strip()
    if not uei:
        return []
    cap = max(1, min(limit, _SUBAWARD_HISTORY_HARD_CAP))
    rows = _scan(config.CONTRACT_SUBAWARD_URI, columns=_SUBAWARD_HISTORY_COLS,
                 filter=f"subawardee_uei = {_sql_str(uei)}")

    def _amt(r: dict[str, Any]) -> float:
        try:
            return float(r.get("subaward_amount") or 0)
        except (TypeError, ValueError):
            return 0.0

    rows.sort(key=_amt, reverse=True)
    return rows[:cap]


# ── Map surface: compiled filter object → Lance scanner predicate → GeoJSON ──
# The deterministic EXECUTE side of the portal map. No DuckDB, no SQL engine: a
# decoder (map_decoders.py) names the allowlisted columns + ops + types, and the
# compiler builds a Lance scanner filter string from hardcoded column names +
# type-validated, _sql_str-escaped values. The Lance scanner filter grammar
# (=, >=, <=, IN(...), `a >= lo AND a <= hi`, unquoted bool, multi-clause AND) is
# verified against the live serving tables; index pushdown serves the BITMAP/BTREE
# filter columns. This is the same scanner(...).to_pylist() shape the per-entity
# lookups use — only the predicate is richer and the limit higher.
MAP_HARD_ROW_CAP = 20_000


class MapCompileError(ValueError):
    """An off-allowlist field/op, or a value that fails per-field type validation.
    Surfaced as a 422 by the route — never reaches Lance as a malformed predicate."""


def _map_coerce(value: Any, type_: str) -> str:
    """Return a Lance-literal token for a scalar. Strings go through _sql_str (the
    ONLY place a caller-supplied string is escaped); numbers/bools become bare
    literals. Raises MapCompileError on a type mismatch (a bool is NOT an int)."""
    if type_ == "bool":
        if not isinstance(value, bool):
            raise MapCompileError(f"expected bool, got {type(value).__name__}")
        return "true" if value else "false"            # unquoted Arrow boolean literal
    if type_ == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise MapCompileError(f"expected int, got {type(value).__name__}")
        return str(value)
    if type_ == "float":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise MapCompileError(f"expected number, got {type(value).__name__}")
        return repr(float(value))
    if type_ == "string":
        if not isinstance(value, str):
            raise MapCompileError(f"expected string, got {type(value).__name__}")
        return _sql_str(value)
    raise MapCompileError(f"unknown field type {type_!r}")


def _map_enum_check(spec, values) -> None:
    if spec.enum is not None:
        for v in values:
            if v not in spec.enum:
                raise MapCompileError(f"value {v!r} not allowed for this field")


def _days_ago_literal(value: Any, today: "dt_date") -> str:
    """A ``DATE 'YYYY-MM-DD'`` literal for an integer days-ago count, resolved against
    ``today`` at REQUEST time (never at translate time — a memoized relative-time filter
    must re-resolve on every execution). DataFusion accepts the DATE literal against both
    date32 columns (company) and ISO-string date columns (winners) — verified live."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MapCompileError("expected a non-negative whole-day count")
    return f"DATE '{(today - timedelta(days=value)).isoformat()}'"


def _map_days_ago_sql(spec, op: str, value: Any, today: "dt_date") -> str:
    """Compile a ``days_ago`` clause. The comparison INVERTS onto the date column:
    "within the last N days" (days_since <= N) means the action date is ON/AFTER
    today−N; days_since >= N means ON/BEFORE today−N. ``between [lo, hi]`` is a
    days-ago window (lo..hi days ago → dates today−hi .. today−lo). Rows with a NULL
    action date (never won) never match — correct for a recency predicate."""
    if op == "<=":
        return f"{spec.column} >= {_days_ago_literal(value, today)}"
    if op == ">=":
        return f"{spec.column} <= {_days_ago_literal(value, today)}"
    if op == "between":
        if not (isinstance(value, list) and len(value) == 2):
            raise MapCompileError("'between' needs a [lo, hi] array value")
        lo, hi = value
        return (f"{spec.column} >= {_days_ago_literal(hi, today)} AND "
                f"{spec.column} <= {_days_ago_literal(lo, today)}")
    raise MapCompileError(f"op {op!r} not allowed for a days_ago field")


def _map_list_sql(spec, op: str, value: Any) -> str:
    """Compile a ``list`` clause (set membership over a list<string> column — e.g. the
    PHASE-3 capability_tags / labor_categories controlled-vocab columns). ``has`` →
    ``array_has(col, 'v')`` (the row's list contains the value); ``has_any`` →
    ``array_has_any(col, ['a','b'])`` (the list contains ANY value). Values are
    enum-checked + ``_sql_str``-escaped (same quote-doubling as every other string
    literal); a NULL list never matches (correct — a winner with no extracted scope is
    excluded). Lance scanner grammar verified live (``array_has`` / ``array_has_any``)."""
    if op == "has":
        if not isinstance(value, str):
            raise MapCompileError("'has' needs a single string value")
        _map_enum_check(spec, [value])
        return f"array_has({spec.column}, {_sql_str(value)})"
    if op == "has_any":
        if not isinstance(value, list) or not value:
            raise MapCompileError("'has_any' needs a non-empty array value")
        for v in value:
            if not isinstance(v, str):
                raise MapCompileError("'has_any' values must all be strings")
        _map_enum_check(spec, value)
        lits = ", ".join(_sql_str(v) for v in value)
        return f"array_has_any({spec.column}, [{lits}])"
    raise MapCompileError(f"op {op!r} not allowed for a list field")


def _map_clause_sql(spec, op: str, value: Any, today: "dt_date") -> str:
    if op not in spec.ops:
        raise MapCompileError(f"op {op!r} not allowed for this field")
    if spec.type == "days_ago":
        return _map_days_ago_sql(spec, op, value, today)
    if spec.type == "list":
        return _map_list_sql(spec, op, value)
    if op in ("=", ">=", "<="):
        _map_enum_check(spec, [value])
        return f"{spec.column} {op} {_map_coerce(value, spec.type)}"
    if op == "in":
        if not isinstance(value, list) or not value:
            raise MapCompileError("'in' needs a non-empty array value")
        _map_enum_check(spec, value)
        lits = ", ".join(_map_coerce(v, spec.type) for v in value)
        return f"{spec.column} IN ({lits})"
    if op == "between":
        if not (isinstance(value, list) and len(value) == 2):
            raise MapCompileError("'between' needs a [lo, hi] array value")
        lo, hi = _map_coerce(value[0], spec.type), _map_coerce(value[1], spec.type)
        return f"{spec.column} >= {lo} AND {spec.column} <= {hi}"
    raise MapCompileError(f"unsupported op {op!r}")


def compile_map_filter(decoder, filters: list[dict[str, Any]], today: "dt_date | None" = None) -> str | None:
    """Compile an AND-combined list of ``{field, op, value}`` clauses into a Lance
    scanner predicate. Column names come ONLY from ``FieldSpec.column`` (a dict-key
    lookup on the decoder, never interpolated); values are type-validated + escaped.
    ``today`` anchors the ``days_ago`` clauses (injectable for tests; defaults to the
    request date). Returns ``None`` for an empty filter list (unfiltered scan).

    The scan is deliberately NOT restricted to plottable rows: a qualifying winner
    whose address did not geocode must still reach the TABLE view (the operator's
    Texas prospect wants every qualifying row, dot or no dot). ``to_geojson`` emits
    null-geometry features for those rows — the dot layer skips them, the table keeps
    them. Raises ``MapCompileError`` on any off-allowlist field/op or mistyped value.

    SCOPE-COVERAGE SAFETY GATE (PHASE 3, anti-pattern #4): the capability axes
    (``FieldSpec.gated``) only have meaning over the ~0.96%-of-awards slice that has
    extracted solicitation text. If any gated clause is present, ``has_extracted_scope
    = true`` is ANDed in DETERMINISTICALLY here — never left to the TRANSLATE prompt —
    so one LLM omission cannot filter ~1.25M winners through the coverage ceiling to an
    empty live map. Skipped when the caller already constrains ``has_extracted_scope``."""
    today = today or dt_date.today()
    parts: list[str] = []
    gated = False
    has_scope_clause = False
    for clause in filters:
        field = clause.get("field")
        spec = decoder.fields.get(field)
        if spec is None:
            raise MapCompileError(f"field {field!r} not in allowlist")
        parts.append(_map_clause_sql(spec, clause.get("op"), clause.get("value"), today))
        if getattr(spec, "gated", False):
            gated = True
        if spec.column == "has_extracted_scope":
            has_scope_clause = True
    if gated and not has_scope_clause:
        parts.append("has_extracted_scope = true")
    return " AND ".join(parts) if parts else None


def map_query(decoder, predicate: str | None, limit: int) -> list[dict[str, Any]]:
    """Scan the decoder's serving table with a compiled predicate (``None`` = whole
    table), projecting only the GeoJSON property + geometry columns. A genuinely
    missing dataset RAISES (loud 5xx) per ``_scan``; a zero-row match returns ``[]``.

    The row bound is enforced by STREAMING BATCHES, never by ``scanner(limit=)``:
    pylance 7.0.0 plans a filtered+limited scan with the limit applied against the
    PHYSICAL row stream, so a selective predicate silently returns a fraction of its
    matches (observed live: 3,011 of 21,026 matching rows, with the cap reported
    un-hit). Streaming the filtered scan and cutting at ``limit`` rows keeps the
    result exact without materializing the full match set."""
    uri = config.MAP_DATASET_URIS[decoder.dataset_key]
    cols = list(decoder.properties) + [decoder.geometry[0], decoder.geometry[1]]
    scanner = _dataset(uri).scanner(columns=cols, filter=predicate)
    rows: list[dict[str, Any]] = []
    for batch in scanner.to_batches():
        rows.extend(batch.to_pylist())
        if len(rows) >= limit:
            break
    return rows[:limit]


def map_count(decoder, predicate: str | None) -> int:
    """Exact match count for a compiled predicate (``count_rows`` pushdown — no row
    materialization). Drives ``meta.total`` + the honest ``capped`` flag."""
    return _dataset(config.MAP_DATASET_URIS[decoder.dataset_key]).count_rows(filter=predicate)


def _map_jsonable(v: Any) -> Any:
    """Lance ``date32[day]`` → ISO ``YYYY-MM-DD``; everything else (str/num/bool/None)
    passes through. Mirrors models._iso so the wire shape is consistent."""
    from datetime import date as _date
    return v.isoformat() if isinstance(v, _date) else v


def to_geojson(decoder, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Rows → GeoJSON ``FeatureCollection``. ``Point`` geometry as ``[lon, lat]``
    (GeoJSON axis order). A row missing either coordinate is emitted with ``geometry:
    null`` (valid per RFC 7946 §3.2) instead of being dropped: the qualifying-but-
    ungeocoded winner reaches the TABLE view while the dot layer skips it. The row
    count served therefore equals ``len(features)``; the plottable subset is the
    features whose geometry is non-null."""
    lon_c, lat_c = decoder.geometry
    feats: list[dict[str, Any]] = []
    for r in rows:
        lon, lat = r.get(lon_c), r.get(lat_c)
        geometry = (
            {"type": "Point", "coordinates": [lon, lat]}
            if lon is not None and lat is not None else None
        )
        feats.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": {k: _map_jsonable(r.get(k)) for k in decoder.properties},
        })
    return {"type": "FeatureCollection", "features": feats}


def reachable() -> bool:
    """Cheap liveness probe used at boot + /healthz: can we open the anchor
    dataset's manifest against R2 with the configured credentials?"""
    try:
        _dataset(config.CONTRACTOR_AWARD_SUMMARY_URI).count_rows()
        return True
    except Exception:
        return False


# Point-lookup surfaces every UI read depends on. Probed at boot + surfaced on /healthz so a
# wrong / unmaterialized URI is LOUD immediately — the failure mode that, silently swallowed,
# masked a misrouted SAM URI for an entire release. award_search/firmographics are excluded:
# they are heavier roots and the manifest open suffices for the point-lookup surfaces.
_SURFACE_DATASETS = {
    "sam_master_entities": lambda: config.SAM_ENTITY_MASTER_URI,
    "sam_pocs": lambda: config.SAM_POCS_URI,
    "entity_profile_gold": lambda: config.ENTITY_PROFILE_GOLD_URI,
    "entity_award_lines_gold": lambda: config.ENTITY_AWARD_LINES_GOLD_URI,
    "contractor_award_summary": lambda: config.CONTRACTOR_AWARD_SUMMARY_URI,
    "winners_map_serving": lambda: config.MAP_DATASET_URIS["winners"],
    "company_map_serving": lambda: config.MAP_DATASET_URIS["company"],
    "awards_map_serving": lambda: config.MAP_DATASET_URIS["awards"],
    "subawardee_capability_profiles": lambda: config.SUBAWARDEE_CAPABILITY_PROFILES_URI,
    "contract_subaward": lambda: config.CONTRACT_SUBAWARD_URI,
}


def probe_surfaces() -> dict[str, bool]:
    """Open each point-lookup surface's manifest against R2 — a name→reachable map for the
    boot log and /healthz. ``False`` means the configured URI does not resolve to a committed
    dataset (a deploy/config error), not that it is merely empty."""
    out: dict[str, bool] = {}
    for name, uri in _SURFACE_DATASETS.items():
        try:
            _dataset(uri()).count_rows()
            out[name] = True
        except Exception:  # noqa: BLE001 — reachability probe, never fatal
            out[name] = False
    return out


def verify_decoder_contract(schema_field_names, indices, decoder) -> dict[str, list[str]]:
    """PURE decoder schema/index contract checker (no R2 — unit-testable with synthetic
    inputs). Returns ``{"violations": [...], "notes": [...]}``.

    HARD ``violations`` (EXECUTE is broken — 5xx or silent full-scan; strict mode aborts boot):
      - a geometry / property / ``FieldSpec.column`` missing from the live schema,
      - a FieldSpec that DECLARES an index (``spec.index`` in BTREE/BITMAP) with NO live index
        whose ``fields`` list contains that column (declared-⊆-actual; live legitimately carries
        more indexes than the decoder declares — never assert the reverse).

    SOFT ``notes`` (degraded but still serving — advisory only, NEVER abort boot):
      - a column that IS indexed but with a different-but-valid scalar kind than declared.

    Matching is grounded in the real ``ds.list_indices()`` shape: entries are dicts with key
    ``fields`` (list of physical columns) and key ``type`` (mixed-case 'BTree'/'Bitmap'); we
    match on column membership in ``fields`` and normalize ``type`` via ``.upper()``. Column
    lookups use ``spec.column`` (the physical column), never the decoder dict key (query-name).
    """
    violations: list[str] = []
    notes: list[str] = []
    schema_cols = set(schema_field_names)
    dk = decoder.dataset_key

    for col in decoder.geometry:
        if col not in schema_cols:
            violations.append(f"{dk}: geometry column {col!r} missing from live schema")

    for col in decoder.properties:
        if col not in schema_cols:
            violations.append(f"{dk}: property column {col!r} missing from live schema")

    for qname, spec in decoder.fields.items():
        if spec.column not in schema_cols:
            violations.append(
                f"{dk}: field {qname!r} -> column {spec.column!r} missing from live schema"
            )

    # column -> set of live index types, built from the real `fields` lists.
    indexed_types: dict[str, set] = {}
    for entry in indices:
        etype = str(entry.get("type", "")).upper()
        for fcol in entry.get("fields", []):
            indexed_types.setdefault(fcol, set()).add(etype)
    for qname, spec in decoder.fields.items():
        declared = getattr(spec, "index", None)
        if declared is None:
            continue
        declared_u = str(declared).upper()
        live_types = indexed_types.get(spec.column)
        if not live_types:
            violations.append(
                f"{dk}: field {qname!r} declares {declared_u} index on column "
                f"{spec.column!r} but NO live index covers that column"
            )
        elif declared_u not in live_types:
            notes.append(
                f"{dk}: field {qname!r} column {spec.column!r} is indexed as "
                f"{sorted(live_types)} but decoder declares {declared_u} "
                f"(type-mismatch note, non-fatal)"
            )
    return {"violations": violations, "notes": notes}


def _live_stat_findings(dk: str, row_count: int, unindexed_by_index: dict[str, int]) -> dict[str, list[str]]:
    """PURE classification of a dataset's live stats (unit-testable). A 0-row dataset is a HARD
    violation — schema + indexes can be perfect while a half-built / failed materialization
    leaves EXECUTE serving empty maps (the gap #431 left open). Unindexed rows on a live index
    are SOFT notes — rows appended since the index was built scan unindexed (degraded, not broken)."""
    violations: list[str] = []
    notes: list[str] = []
    if row_count == 0:
        violations.append(f"{dk}: dataset has 0 rows — EXECUTE will serve empty results")
    for iname, n in unindexed_by_index.items():
        if n > 0:
            notes.append(f"{dk}: index {iname!r} has {n} unindexed rows (degraded scan, non-fatal)")
    return {"violations": violations, "notes": notes}


def check_decoder_contracts() -> dict[str, dict[str, list[str]]]:
    """LIVE caller: open each map decoder's dataset against R2 (via ``_dataset``), run the pure
    schema/index checker, then add the live-only checks (row count + per-index unindexed rows).
    Returns ``{dataset_key: {"violations": [...], "notes": [...]}}`` — empty ``violations`` means
    the contract holds. A dataset that cannot be opened/introspected is itself a HARD violation.
    Never raises: the caller (lifespan) owns the fail policy."""
    from .map_decoders import DECODERS  # local import: avoid import-time coupling

    report: dict[str, dict[str, list[str]]] = {}
    for name, decoder in DECODERS.items():
        dk = decoder.dataset_key
        try:
            ds = _dataset(config.MAP_DATASET_URIS[dk])
            schema_names = ds.schema.names
            indices = ds.list_indices()
        except Exception as exc:  # noqa: BLE001 — open/introspect failure IS a contract failure
            report[name] = {"violations": [f"{dk}: could not open/introspect dataset: {exc}"], "notes": []}
            continue
        findings = verify_decoder_contract(schema_names, indices, decoder)
        try:
            row_count = ds.count_rows()
        except Exception as exc:  # noqa: BLE001 — row count is a hard contract signal
            findings["violations"].append(f"{dk}: count_rows() failed: {exc}")
            row_count = -1
        unindexed: dict[str, int] = {}
        for entry in indices:
            iname = entry.get("name")
            if not iname:
                continue
            try:  # per-index stats are best-effort: a Lance API change must never break the check
                unindexed[iname] = int(ds.stats.index_stats(iname).get("num_unindexed_rows", 0) or 0)
            except Exception:  # noqa: BLE001
                pass
        if row_count >= 0:
            live = _live_stat_findings(dk, row_count, unindexed)
            findings["violations"].extend(live["violations"])
            findings["notes"].extend(live["notes"])
        report[name] = findings
    return report
