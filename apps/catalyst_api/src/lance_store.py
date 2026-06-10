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

Datasets are opened PER CALL (never cached as long-lived handles) so the gateway
always reflects the latest committed Lance version — the pipeline workers
overwrite these in place and a stale handle would serve the prior snapshot. A
dataset open is a single manifest GET; the indexed point-lookup stays fast.

The caller's domain is normalized to the stored anchor and validated against a
strict charset before it is ever interpolated into a Lance filter expression
(single quotes are additionally doubled), so the predicate cannot be broken out of.
"""

from __future__ import annotations

import re
from typing import Any

import lance

from . import config

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


def _dataset(uri: str):
    """Open a committed Lance dataset fresh, bound to R2."""
    return lance.dataset(uri, storage_options=config.r2_storage_options())


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
