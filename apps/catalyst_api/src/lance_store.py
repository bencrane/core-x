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
_SAM_ENTITY_COLS = [
    "uei", "legal_business_name", "cage_code", "registration_status",
    "purpose_of_registration", "registration_date", "expiration_date",
    "activation_date", "last_update_date", "primary_naics", "naics_codes",
    "psc_codes", "business_types", "physical_city", "physical_state", "physical_zip5",
]


def _scan(uri: str, **scanner_kwargs) -> list[dict[str, Any]]:
    """Open + scan a committed dataset, returning rows. A dataset that is not yet
    materialized in the sink degrades to ``[]`` (the surface renders its
    empty-state) rather than a 500 — e.g. a gold table whose build hasn't landed.
    Genuine errors (R2 auth, network, malformed query) still raise."""
    try:
        return _dataset(uri).scanner(**scanner_kwargs).to_table().to_pylist()
    except (ValueError, OSError) as exc:  # narrow: "dataset absent" only
        if "not found" in str(exc).lower():
            return []
        raise


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
# 6 SAM POC slots × (primary + alternate) — a tight upper bound on rows per UEI.
_POC_HARD_CAP = 12


def sam_pocs_by_uei(uei: str) -> list[dict[str, Any]]:
    """BTREE point-lookup on ``sam_pocs.uei`` → the government POC slots (v2 spine
    only; legacy cage-keyed rows are out of scope), ordered by slot. The source
    carries no email/phone columns."""
    uei = (uei or "").strip()
    if not uei:
        return []
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


# ── Surface 3: UEI → active / past prime award line items ─────────────────────
# RecentAward projection + action_date (awardDate). The PoP-end date predicate is
# pushed into the Lance filter so the hard fan-out cap applies to the requested
# subset (active vs. closed), not an arbitrary 100-row slice of the whole history.
_LINE_ITEM_COLS = _AWARD_COLS + ["action_date"]


def _today_iso() -> str:
    import datetime as _dt

    return _dt.date.today().isoformat()


def _award_line_items(uei: str, limit: int, *, active: bool) -> list[dict[str, Any]]:
    uei = (uei or "").strip()
    if not uei:
        return []
    cap = max(1, min(limit, _AWARDS_HARD_CAP))
    op = ">=" if active else "<"
    today = _today_iso()
    rows = _scan(
        config.AWARD_SEARCH_URI,
        columns=_LINE_ITEM_COLS,
        filter=(
            f"recipient_uei = {_sql_str(uei)} AND "
            f"period_of_performance_current_end_date {op} CAST('{today}' AS DATE)"
        ),
        limit=_AWARDS_HARD_CAP,
    )
    rows.sort(key=lambda r: (r.get("total_obligation") or r.get("award_amount") or 0.0), reverse=True)
    return rows[:cap]


def active_awards_by_uei(uei: str, limit: int) -> list[dict[str, Any]]:
    """Prime award line items whose period of performance has NOT elapsed
    (``period_of_performance_current_end_date >= today``), highest-obligation first.
    NULL PoP-end rows are treated as non-current and excluded."""
    return _award_line_items(uei, limit, active=True)


def closed_awards_by_uei(uei: str, limit: int) -> list[dict[str, Any]]:
    """Prime award line items whose period of performance has elapsed
    (``period_of_performance_current_end_date < today``) — the past-performance
    counterpart, highest-obligation first."""
    return _award_line_items(uei, limit, active=False)


def reachable() -> bool:
    """Cheap liveness probe used at boot + /healthz: can we open the anchor
    dataset's manifest against R2 with the configured credentials?"""
    try:
        _dataset(config.CONTRACTOR_AWARD_SUMMARY_URI).count_rows()
        return True
    except Exception:
        return False
