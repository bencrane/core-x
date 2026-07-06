"""Subout-opportunities recipe v3 — relationship-matched open prime awards
(POST /api/v1/market/subout-opportunities).

MATCHING (v3 — operator-directed 2026-07-06; REPLACES v2's code-lens matching):
a dot appears iff at least one of two demonstrated-relationship rules holds —

  RULE A (worked_under_prime): the award's prime is one the TARGET has received
      FSRS subawards from. Chain: target's subaward edges → its historical primes
      → those primes' OPEN awards.
  RULE B (peer_subawardee): the award has FSRS subawardees who WON prime awards
      from the same awarding agency in the same NAICS/PSC as prime awards the
      TARGET itself won. Chain: target's prime awards → (agency, code) pairs →
      other firms with prime awards in those pairs (peers) → OPEN awards carrying
      subaward edges to those peers. Empty by construction for targets that have
      never primed.

Nothing else matches: no SAM-claimed codes, no inferred codes, no code-similarity.
Period.

NO SCORING (v3): the list is FLAT — sorted by total_obligation DESC (deterministic;
id tiebreak), honest total, capped by ``limit``. No score, no components, no
weights on the wire. Each row instead carries ``matched_via``: the demonstrated
relationship(s) that admitted it (rule A: the prime + the $ the target received
from it; rule B: the peer firms and the shared (agency, code) pairs).

MAP-READY WIRE (unchanged from v2): rows carry the award's PoP centroid
(latitude/longitude, honest per pop_geo_precision), distance_mi from the target's
HQ AS A FACT (not a score), and the nearest_federal_site ENRICHMENT (informational;
never a score input); meta.target_hq carries the target's HQ point, computed up
front so empty answers still anchor the map.

HOT PATH: the open-award table (indexed by prime AND award id), the entity-geo
probe table, and the federal-sites grid are in-process boot caches (lazy,
TTL-refreshed in the background). v3 DROPS the v2 cube-marginal / lanes / SAM
caches and the inferred-handle warmup — nothing reads them anymore. Per-request
remote reads (all BTREE-indexed, bounded):
  1. target's subaward edges   (subawardee_uei point-lookup)        → rule A
  2. target's prime awards     (recipient_uei point-lookup)         → rule B pairs
  3. peer firms per pair       (agency+code scans, capped)          → rule B
  4. peers' subaward edges     (chunked subawardee_uei IN scans)    → rule B

BOUNDS (named parameters — adjust here, never silently): the target's top
PRIME_PAIR_CAP (agency, code) pairs by $ seed rule B; PEERS_PER_PAIR_CAP distinct
peer firms per pair; PEER_UNION_CAP peers total; PEER_EDGE_SCAN_CAP streamed edge
rows; AWARD_SCAN_CAP open awards per rule. Every applied cap rides meta.notes.

Fail-closed request contract: body is ``{uei, limit?}`` — v2's lenses /
codes_override / code_type / include_peers are GONE and now 422 like any unknown
key. A target with no matching relationships is a 200 with empty data +
meta.reason. Per-stage wall times ride meta.timings_ms; cache_state
(cold | warm | failed) + cache_build_ms ride meta.
"""
from __future__ import annotations

import logging
import math
import threading
import time
from dataclasses import dataclass
from datetime import date as dt_date
from typing import Any

from . import config, market_registry
from .lance_store import MapCompileError, _dataset, _map_jsonable, _sql_str, valid_uei
from .market_store import _in_predicate

log = logging.getLogger("catalyst_api.subout_store")

# ── The recipe id (bump on ANY matching/ordering/bound change) ─────────────────
# v3: relationship-based matching (rules A + B), flat obligation-sorted list, no
# scoring — operator-directed replacement of v2's code-lens matching + 7-component
# score. v2 history: code lenses + weighted components (see git history).
RECIPE_ID = "subout_opportunities.v3"
# The combos mode (operator-directed 2026-07-06, second mode alongside v3): its own
# recipe id — matching logic differs wholesale, so it versions independently.
COMBOS_RECIPE_ID = "subout_combos.v1"

MODES = ("relationships", "combos")
ALLOWED_BODY_KEYS = frozenset({"uei", "limit", "mode"})

DEFAULT_LIMIT = 50
LIMIT_CAP = 200
AWARD_SCAN_CAP = 2_000        # open-award rows admitted per rule before assembly

# ── Rule B bounds (named parameters; every application is noted on the wire) ──
PRIME_PAIR_CAP = 10           # target's top (agency, code) pairs by its own prime $
PEERS_PER_PAIR_CAP = 200      # distinct peer firms admitted per pair
PEER_UNION_CAP = 1_000        # peer firms total across pairs
PEER_EDGE_SCAN_CAP = 25_000   # streamed subaward-edge rows across the peer chunks
MATCH_PEERS_SHOWN = 5         # peer firms listed per row in matched_via (rest counted)

# ── Combos-mode bounds (named parameters; applications noted on the wire) ──────
TARGET_COMBO_CAP = 5          # target's top (naics, psc) sub combos by its own sub $
LOOKALIKE_EDGE_SCAN = 20_000  # streamed candidate edges per target combo
LOOKALIKE_CANDIDATE_CHECK = 25  # top overlap candidates checked for has-primed
LOOKALIKE_CT = 3              # named lookalikes on the POV
EXPANSION_COMBO_CAP = 10      # lookalikes' other sub combos admitted, by their sub $

# ── Federal-site enrichment (informational only in v3 — never a score input) ──
NEAREST_SITE_MAX_MI = 50.0
# FRPP rows whose reporting agency is GSA are SHADOWS of the gsa_building rows —
# excluded from the site grid. '47' = the value AS OBSERVED LIVE in
# federal_sites_lance.reporting_agency_code (v4; FRPP strips the leading zero).
FRPP_GSA_REPORTING_AGENCY_CODE = "47"
SITE_BUCKET_DEG = 0.1
_SITE_BUCKET_MIN_MI = 4.0
_SITE_MAX_RING = math.ceil(NEAREST_SITE_MAX_MI / _SITE_BUCKET_MIN_MI) + 1

# ── Cache contract ─────────────────────────────────────────────────────────────
CACHE_TTL_S = 6 * 3600

OPEN_AWARD_COLUMNS = [
    "generated_unique_award_id", "award_id_piid", "recipient_uei", "recipient_name",
    "naics_code", "product_or_service_code", "total_obligation",
    "base_and_all_options_value", "subaward_count", "total_subaward_amount",
    "subcontracting_plan_code", "period_of_performance_current_end_date",
    "ordering_period_end_date", "award_or_idv_flag", "idv_type_code",
    "type_of_set_aside_code", "awarding_agency_code", "awarding_agency_name",
    "primary_place_of_performance_state_code", "latitude", "longitude", "geo_precision",
]


@dataclass(frozen=True)
class SuboutCaches:
    """The in-process read models. ``open_awards`` is the row list;
    ``awards_by_prime`` maps recipient_uei → row indexes, ``awards_by_id`` maps
    generated_unique_award_id → row index (rule B joins subaward edges on it).
    ``geo_table`` is a uei-sorted single-chunk arrow table (binary-searched for the
    target HQ). ``federal_sites``/``federal_site_buckets`` back the informational
    nearest-site enrichment. v3 carries NO cube / lanes / SAM caches and no
    inferred warmup — nothing reads them."""

    open_awards: list[dict[str, Any]]
    awards_by_prime: dict[str, list[int]]
    awards_by_id: dict[str, int]
    awards_by_combo: dict[tuple[str, str], list[int]]
    geo_table: Any                       # pyarrow.Table, sorted by uei, single chunk
    federal_sites: list[tuple]
    federal_site_buckets: dict[tuple[int, int], list[int]]
    build_ms: float


_cache_lock = threading.Lock()
_build_lock = threading.Lock()
_caches: SuboutCaches | None = None
_caches_built_at: float | None = None
_cache_refreshing = False
# The LAST build failure, as a string — surfaced as cache_state='failed' + a meta
# note on every affected response (the 'silent unavailable' incident fix).
_last_build_error: str | None = None


# ── I/O seams (monkeypatch targets for the hermetic tests) ─────────────────────
def _scan_to_pylist(uri: str, columns: list[str], predicate: str | None) -> list[dict[str, Any]]:
    """One fresh scanner (one-shot by contract) → rows. Every per-request caller
    passes a BTREE point predicate — never an unfiltered projection."""
    return _dataset(uri).scanner(columns=columns, filter=predicate).to_table().to_pylist()


def _stream_rows(uri: str, columns: list[str], predicate: str | None, limit: int) -> list[dict[str, Any]]:
    """First ``limit`` rows of a filtered projection via streamed batches (never
    ``scanner(limit=)`` — the pylance limit-before-filter planner under-returns)."""
    scanner = _dataset(uri).scanner(columns=columns, filter=predicate)
    out: list[dict[str, Any]] = []
    for batch in scanner.to_batches():
        out.extend(batch.to_pylist())
        if len(out) >= limit:
            break
    return out[:limit]


def _load_open_awards() -> list[dict[str, Any]]:
    """The full gtm_open_awards table (~150-250K rows) as row dicts — the one
    full-table load the recipe makes, ONCE per cache build."""
    scanner = _dataset(config.GTM_OPEN_AWARDS_URI).scanner(columns=OPEN_AWARD_COLUMNS)
    rows: list[dict[str, Any]] = []
    for batch in scanner.to_batches():
        rows.extend(batch.to_pylist())
    return rows


def _load_entity_geo():
    """gtm_entity_geo (1.45M rows; uei, latitude, longitude) as a uei-sorted arrow
    table served by in-process binary search."""
    tbl = _dataset(config.GTM_ENTITY_GEO_URI).scanner(
        columns=["uei", "latitude", "longitude"]).to_table()
    return tbl.sort_by("uei").combine_chunks()


def _rows_for_uei(tbl, uei: str) -> list[dict[str, Any]]:
    """All rows for one uei off a uei-sorted single-chunk arrow table, via binary
    search (equal-range). [] when absent — same contract as a filtered point scan."""
    col = tbl.column("uei")
    if col.num_chunks == 0:
        return []
    arr = col.chunk(0)
    lo, hi = 0, len(arr)
    while lo < hi:                       # left bound
        mid = (lo + hi) // 2
        if arr[mid].as_py() < uei:
            lo = mid + 1
        else:
            hi = mid
    start, hi = lo, len(arr)
    while lo < hi:                       # right bound
        mid = (lo + hi) // 2
        if arr[mid].as_py() <= uei:
            lo = mid + 1
        else:
            hi = mid
    if lo == start:
        return []
    return tbl.slice(start, lo - start).to_pylist()


# ── Federal sites: point grid for the (informational) nearest-site enrichment ──
FEDERAL_SITE_COLUMNS = [
    "site_source", "reporting_agency_code", "site_name", "site_type",
    "latitude", "longitude", "lease_expiring_24mo_ct",
    "earliest_lease_expiration_date",
]


def _site_rows_to_points(rows) -> list[tuple]:
    """Raw federal_sites_lance row dicts → point tuples. PURE: keeps point rows only
    and EXCLUDES GSA-reported FRPP rows (shadows of the gsa_building rows)."""
    out: list[tuple] = []
    for row in rows:
        lat, lon = row.get("latitude"), row.get("longitude")
        if lat is None or lon is None:
            continue
        if (row.get("site_source") == "frpp_asset"
                and row.get("reporting_agency_code") == FRPP_GSA_REPORTING_AGENCY_CODE):
            continue
        out.append((float(lat), float(lon), row.get("site_name"),
                    row.get("site_type"), row.get("site_source"),
                    row.get("lease_expiring_24mo_ct"),
                    row.get("earliest_lease_expiration_date")))
    return out


def _load_federal_sites() -> list[tuple]:
    """federal_sites_lance → point tuples. BEST-EFFORT: an unreachable site layer
    degrades to zero sites (nearest is null), never a bricked recipe cache."""
    try:
        rows: list[dict[str, Any]] = []
        scanner = _dataset(config.FEDERAL_SITES_URI).scanner(
            columns=FEDERAL_SITE_COLUMNS,
            filter="latitude IS NOT NULL AND longitude IS NOT NULL")
        for batch in scanner.to_batches():
            rows.extend(batch.to_pylist())
        return _site_rows_to_points(rows)
    except Exception as exc:  # noqa: BLE001 — enrichment layer, never brick the build
        log.warning("federal_sites_lance unreachable (%s): serving zero federal "
                    "sites — nearest_federal_site will be null", exc)
        return []


def _site_bucket(lat: float, lon: float) -> tuple[int, int]:
    return int(round(lat / SITE_BUCKET_DEG)), int(round(lon / SITE_BUCKET_DEG))


def _index_federal_sites(sites: list[tuple]) -> dict[tuple[int, int], list[int]]:
    buckets: dict[tuple[int, int], list[int]] = {}
    for i, site in enumerate(sites):
        buckets.setdefault(_site_bucket(site[0], site[1]), []).append(i)
    return buckets


def _nearest_federal_site(lat: float, lon: float,
                          caches: SuboutCaches) -> dict[str, Any] | None:
    """Nearest federal site via expanding-ring grid search; None beyond the cap or
    when no sites are loaded — honestly absent, never a far-away pretend match."""
    if not caches.federal_site_buckets:
        return None
    bi, bj = _site_bucket(lat, lon)
    best: tuple | None = None
    best_d: float | None = None
    for ring in range(_SITE_MAX_RING + 1):
        if best_d is not None and (ring - 1) * _SITE_BUCKET_MIN_MI > best_d:
            break
        for di in range(-ring, ring + 1):
            for dj in range(-ring, ring + 1):
                if max(abs(di), abs(dj)) != ring:       # perimeter cells only
                    continue
                for idx in caches.federal_site_buckets.get((bi + di, bj + dj), ()):
                    site = caches.federal_sites[idx]
                    d = _haversine_mi(lat, lon, site[0], site[1])
                    if best_d is None or d < best_d:
                        best, best_d = site, d
    if best is None or best_d is None or best_d > NEAREST_SITE_MAX_MI:
        return None
    return {
        "site_name": best[2],
        "site_type": best[3],
        "site_source": best[4],
        "latitude": best[0],
        "longitude": best[1],
        "distance_mi": round(best_d, 1),
        "lease_expiring_24mo_ct": best[5],
        "earliest_lease_expiration_date": _map_jsonable(best[6]),
    }


# ── Cache build ────────────────────────────────────────────────────────────────
def _index_open_awards(rows: list[dict[str, Any]]) -> tuple[
        dict[str, list[int]], dict[str, int], dict[tuple[str, str], list[int]]]:
    by_prime: dict[str, list[int]] = {}
    by_id: dict[str, int] = {}
    by_combo: dict[tuple[str, str], list[int]] = {}
    for i, row in enumerate(rows):
        uei = row.get("recipient_uei")
        if uei:
            by_prime.setdefault(uei, []).append(i)
        award_id = row.get("generated_unique_award_id")
        if award_id:
            by_id[award_id] = i
        naics, psc = row.get("naics_code"), row.get("product_or_service_code")
        if naics and psc:
            by_combo.setdefault((naics, psc), []).append(i)
    return by_prime, by_id, by_combo


def _rss() -> int | None:
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:  # noqa: BLE001 — non-POSIX platforms
        return None


def _build_caches() -> SuboutCaches:
    """Build the in-process read models, LOUDLY. Called cold (first request /
    prewarm) and by the background TTL refresh — never on a warm request's path."""
    t0 = time.monotonic()
    rss_before = _rss()
    open_awards = _load_open_awards()
    by_prime, by_id, by_combo = _index_open_awards(open_awards)
    geo_table = _load_entity_geo()
    federal_sites = _load_federal_sites()
    federal_site_buckets = _index_federal_sites(federal_sites)
    try:
        import pyarrow as pa
        pa.default_memory_pool().release_unused()
    except Exception:  # noqa: BLE001 — pool release is best-effort
        pass
    build_ms = round((time.monotonic() - t0) * 1000.0, 1)
    rss_after = _rss()
    rss_delta = ((rss_after - rss_before)
                 if (rss_before is not None and rss_after is not None) else "n/a")
    log.info(
        "subout caches built in %.1fs: open_awards=%d rows (primes=%d ids=%d), "
        "geo=%d rows/%.0fMB (arrow), federal_sites=%d points over %d grid cells; "
        "ru_maxrss_delta=%s (platform units)",
        build_ms / 1000.0, len(open_awards), len(by_prime), len(by_id),
        geo_table.num_rows, geo_table.nbytes / 1e6,
        len(federal_sites), len(federal_site_buckets), rss_delta)
    return SuboutCaches(open_awards=open_awards, awards_by_prime=by_prime,
                        awards_by_id=by_id, awards_by_combo=by_combo,
                        geo_table=geo_table,
                        federal_sites=federal_sites,
                        federal_site_buckets=federal_site_buckets, build_ms=build_ms)


def _refresh_caches() -> None:
    """Background TTL refresh: build fresh, swap under the lock. Failure keeps the
    old (stale-but-serving) cache — a refresh NEVER degrades a working process."""
    global _caches, _caches_built_at, _cache_refreshing, _last_build_error
    try:
        fresh = _build_caches()
        with _cache_lock:
            _caches = fresh
            _caches_built_at = time.monotonic()
            _last_build_error = None
    except Exception as exc:  # noqa: BLE001 — keep serving the stale cache
        log.warning("subout cache refresh failed (stale cache stays live): %s", exc)
        with _cache_lock:
            _last_build_error = f"{type(exc).__name__}: {exc}"
    finally:
        with _cache_lock:
            _cache_refreshing = False


def _ensure_caches() -> tuple[str, SuboutCaches]:
    """(cache_state, caches). 'warm' serves the in-memory build (kicking a
    BACKGROUND refresh past TTL); 'cold' means THIS call built it. Raises when a
    cold build fails — the executor degrades to an empty answer with a note, and
    the NEXT request retries (a failed build is never cached)."""
    global _caches, _caches_built_at, _cache_refreshing, _last_build_error
    with _cache_lock:
        if _caches is not None:
            stale = (_caches_built_at is None
                     or (time.monotonic() - _caches_built_at) > CACHE_TTL_S)
            if stale and not _cache_refreshing:
                _cache_refreshing = True
                threading.Thread(target=_refresh_caches, daemon=True,
                                 name="subout-cache-refresh").start()
            return "warm", _caches
    with _build_lock:                    # serialize concurrent cold builds
        with _cache_lock:
            if _caches is not None:
                return "warm", _caches
        try:
            built = _build_caches()
        except Exception as exc:
            with _cache_lock:
                _last_build_error = f"{type(exc).__name__}: {exc}"
            raise
        with _cache_lock:
            _caches = built
            _caches_built_at = time.monotonic()
            _last_build_error = None
        return "cold", built


def last_build_error() -> str | None:
    """The most recent cache-build failure (None after a successful build)."""
    with _cache_lock:
        return _last_build_error


def prewarm_caches() -> None:
    """Boot-time best-effort warm (daemon thread in main.lifespan). Never raises —
    a failure is stored in ``_last_build_error`` AND logged with the traceback."""
    try:
        state, caches = _ensure_caches()
        log.info("subout cache prewarm: state=%s build_s=%.1f open_awards=%d",
                 state, caches.build_ms / 1000.0, len(caches.open_awards))
    except Exception:  # noqa: BLE001 — prewarm must never brick boot
        log.exception("subout cache prewarm FAILED (first request will retry; "
                      "error rides cache_state='failed' + meta.notes)")


def reset_caches_for_tests() -> None:
    """Test hook: drop the module cache state."""
    global _caches, _caches_built_at, _cache_refreshing, _last_build_error
    with _cache_lock:
        _caches = None
        _caches_built_at = None
        _cache_refreshing = False
        _last_build_error = None


# ── Request validation (fail-closed — MapCompileError → 422 at the route) ─────
def validate_request(body: Any) -> dict[str, Any]:
    """Body dict → normalized request. v3 contract: {uei, limit?} ONLY — v2's
    lenses / codes_override / code_type / include_peers are unknown keys now and
    refuse to compile like any other."""
    if not isinstance(body, dict):
        raise MapCompileError("request body must be an object")
    unknown = set(body) - ALLOWED_BODY_KEYS
    if unknown:
        raise MapCompileError(f"unknown body key(s) {sorted(unknown)!r}")

    uei = body.get("uei")
    if not isinstance(uei, str) or not valid_uei(uei.strip()):
        raise MapCompileError("uei is required and must be a 12-char alphanumeric SAM UEI")
    uei = uei.strip()

    limit = body.get("limit")
    if limit is None:
        limit = DEFAULT_LIMIT
    elif isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise MapCompileError("limit must be a positive whole number")
    limit = min(limit, LIMIT_CAP)

    mode = body.get("mode")
    if mode is None:
        mode = "relationships"
    elif mode not in MODES:
        raise MapCompileError(f"mode must be one of {list(MODES)} (or omitted)")

    return {"uei": uei, "limit": limit, "mode": mode}


def _haversine_mi(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in statute miles (R = 3958.8 mi)."""
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat, dlon = rlat2 - rlat1, rlon2 - rlon1
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 3958.8 * 2.0 * math.asin(min(1.0, math.sqrt(a)))


# ── Open-date re-check (the cache may be hours old within TTL) ─────────────────
def _is_open(row: dict[str, Any], today: dt_date) -> bool:
    """Keep when either end date is >= today; a row with NEITHER date trusts the
    builder's open-at-as_of guarantee."""
    pop_end = row.get("period_of_performance_current_end_date")
    ord_end = row.get("ordering_period_end_date")
    if pop_end is None and ord_end is None:
        return True
    for d in (pop_end, ord_end):
        if isinstance(d, dt_date) and d >= today:
            return True
    return False


# ── RULE A: the target's historical primes → their open awards ─────────────────
def _target_prime_relationships(uei: str) -> dict[str, dict[str, Any]]:
    """The target's FSRS subaward edges (BTREE subawardee_uei point-lookup) →
    {prime_uei: {subaward_amt_from_prime, edge_ct, last_action_date}} — the
    demonstrated relationship evidence rule A rides on."""
    rows = _scan_to_pylist(
        config.CONTRACT_SUBAWARD_URI,
        ["subawardee_uei", "prime_awardee_uei", "subaward_amount",
         "subaward_action_date"],
        f"subawardee_uei = {_sql_str(uei)}")
    rel: dict[str, dict[str, Any]] = {}
    for r in rows:
        prime = r.get("prime_awardee_uei")
        if not prime:
            continue
        entry = rel.setdefault(prime, {"subaward_amt_from_prime": 0.0, "edge_ct": 0,
                                       "last_action_date": None})
        entry["subaward_amt_from_prime"] += float(r.get("subaward_amount") or 0.0)
        entry["edge_ct"] += 1
        d = r.get("subaward_action_date")
        if d is not None and (entry["last_action_date"] is None or d > entry["last_action_date"]):
            entry["last_action_date"] = d
    return rel


def _rule_a_awards(rel: dict[str, dict[str, Any]], caches: SuboutCaches,
                   today: dt_date) -> dict[int, dict[str, Any]]:
    """Open awards held by the target's historical primes → {row_index: evidence}."""
    out: dict[int, dict[str, Any]] = {}
    for prime in sorted(rel):
        for idx in caches.awards_by_prime.get(prime, []):
            if not _is_open(caches.open_awards[idx], today):
                continue
            out[idx] = {
                "rule": "worked_under_prime",
                "prime_uei": prime,
                "subaward_amt_from_prime": round(rel[prime]["subaward_amt_from_prime"], 2),
                "edge_ct": rel[prime]["edge_ct"],
                "last_action_date": _map_jsonable(rel[prime]["last_action_date"]),
            }
            if len(out) >= AWARD_SCAN_CAP:
                return out
    return out


# ── RULE B: agency+code prime-award peers → open awards subbing to them ────────
def _target_prime_pairs(uei: str, notes: list[str]) -> list[dict[str, Any]]:
    """The target's OWN prime awards → its top (agency, code_type, code) pairs by
    its own prime $ (both code systems), capped at PRIME_PAIR_CAP. [] when the
    target has never primed — rule B is then empty by construction."""
    rows = _scan_to_pylist(
        config.USASPENDING_AWARD_CANONICAL_URI,
        ["recipient_uei", "awarding_agency_code", "naics_code",
         "product_or_service_code", "total_obligation"],
        f"recipient_uei = {_sql_str(uei)}")
    sums: dict[tuple[str, str, str], float] = {}
    for r in rows:
        agency = r.get("awarding_agency_code")
        if not agency:
            continue
        obl = float(r.get("total_obligation") or 0.0)
        if r.get("naics_code"):
            key = (agency, "naics", r["naics_code"])
            sums[key] = sums.get(key, 0.0) + obl
        if r.get("product_or_service_code"):
            key = (agency, "psc", r["product_or_service_code"])
            sums[key] = sums.get(key, 0.0) + obl
    ranked = sorted(sums.items(), key=lambda kv: (-kv[1], kv[0]))
    if len(ranked) > PRIME_PAIR_CAP:
        notes.append(f"rule B: target's (agency, code) pairs capped to the top "
                     f"{PRIME_PAIR_CAP} by prime $ (of {len(ranked)})")
        ranked = ranked[:PRIME_PAIR_CAP]
    return [{"agency_code": k[0], "code_type": k[1], "code": k[2],
             "target_prime_obl": round(v, 2)} for k, v in ranked]


def _peer_firms_for_pairs(uei: str, pairs: list[dict[str, Any]],
                          notes: list[str]) -> dict[str, list[dict[str, Any]]]:
    """Firms holding prime awards in the target's (agency, code) pairs —
    {peer_uei: [pairs shared]}, target excluded, capped per pair and in total."""
    peers: dict[str, list[dict[str, Any]]] = {}
    for pair in pairs:
        col = "naics_code" if pair["code_type"] == "naics" else "product_or_service_code"
        pred = (f"awarding_agency_code = {_sql_str(pair['agency_code'])} "
                f"AND {col} = {_sql_str(pair['code'])}")
        rows = _stream_rows(config.USASPENDING_AWARD_CANONICAL_URI,
                            ["recipient_uei"], pred, PEERS_PER_PAIR_CAP * 5)
        seen_this_pair: set[str] = set()
        for r in rows:
            peer = r.get("recipient_uei")
            if not peer or peer == uei or peer in seen_this_pair:
                continue
            seen_this_pair.add(peer)
            peers.setdefault(peer, []).append(
                {"agency_code": pair["agency_code"], "code_type": pair["code_type"],
                 "code": pair["code"]})
            if len(seen_this_pair) >= PEERS_PER_PAIR_CAP:
                break
        if len(peers) >= PEER_UNION_CAP:
            notes.append(f"rule B: peer-firm union capped at {PEER_UNION_CAP}")
            break
    return peers


def _rule_b_awards(peers: dict[str, list[dict[str, Any]]], caches: SuboutCaches,
                   today: dt_date, notes: list[str]) -> dict[int, dict[str, Any]]:
    """OPEN awards carrying FSRS subaward edges to the peer firms →
    {row_index: evidence}. Edges stream off the subaward canonical via chunked
    BTREE subawardee_uei IN scans, joined to the open-award cache on
    prime_award_unique_key = generated_unique_award_id (verified live: same
    CONT_AWD_* key space)."""
    if not peers:
        return {}
    out: dict[int, dict[str, Any]] = {}
    peer_list = sorted(peers)
    scanned = 0
    for i in range(0, len(peer_list), 500):
        chunk = peer_list[i:i + 500]
        rows = _stream_rows(
            config.CONTRACT_SUBAWARD_URI,
            ["subawardee_uei", "prime_award_unique_key"],
            _in_predicate("subawardee_uei", chunk),
            PEER_EDGE_SCAN_CAP - scanned)
        scanned += len(rows)
        for r in rows:
            award_id = r.get("prime_award_unique_key")
            peer = r.get("subawardee_uei")
            if not award_id or not peer:
                continue
            idx = caches.awards_by_id.get(award_id)
            if idx is None or not _is_open(caches.open_awards[idx], today):
                continue
            ev = out.setdefault(idx, {"rule": "peer_subawardee", "peers": {}})
            ev["peers"].setdefault(peer, peers[peer])
            if len(out) >= AWARD_SCAN_CAP:
                break
        if scanned >= PEER_EDGE_SCAN_CAP:
            notes.append(f"rule B: peer subaward-edge scan capped at {PEER_EDGE_SCAN_CAP} rows")
            break
        if len(out) >= AWARD_SCAN_CAP:
            break
    return out


def _finalize_rule_b_evidence(ev: dict[str, Any]) -> dict[str, Any]:
    """Peer dict → wire shape: up to MATCH_PEERS_SHOWN peers listed (each with its
    shared pairs), the rest counted — never silently dropped."""
    peer_items = sorted(ev["peers"].items())
    shown = [{"uei": u, "shared_pairs": pairs} for u, pairs in peer_items[:MATCH_PEERS_SHOWN]]
    return {
        "rule": "peer_subawardee",
        "peer_ct": len(peer_items),
        "peers": shown,
    }




# ═══════════════════════════════════════════════════════════════════════════════
# COMBOS MODE (subout_combos.v1) — lookalike sub-combo expansion + PoP-state POV
# (operator-directed 2026-07-06; ships ALONGSIDE the relationships mode)
#
# THE POV: (1) the target's own demonstrated sub combos — the (prime-award NAICS ×
# PSC) pairs on subawards it delivered; (2) its top-LOOKALIKE_CT lookalikes —
# firms sharing those sub combos (ranked by overlapping sub $) that ALSO PRIME
# (the has-primed clause is a lookalike QUALIFIER, never a combo source); (3) the
# expansion set — those lookalikes' OTHER sub combos; (4) the geography default —
# the states where the TARGET has actually performed (its subaward PoP states ∪
# its own prime awards' PoP states), applied as a filter on the award's
# pop_state_code and disclosed with its basis in meta.pov.
#
# Dots = OPEN awards whose (naics, psc) combo ∈ (target combos ∪ expansion
# combos), PoP-state-filtered per the default. Flat total_obligation sort — no
# scoring, per the standing directive. Every hop is demonstrated dollars.
# ═══════════════════════════════════════════════════════════════════════════════
def _target_sub_combo_profile(uei: str) -> tuple[dict[tuple[str, str], dict[str, Any]], set[str]]:
    """The target's demonstrated sub-combo profile + its subaward PoP states —
    ONE BTREE point-lookup on the subaward canonical. Returns
    ({(naics, psc): {amt, edge_ct}}, {states})."""
    rows = _scan_to_pylist(
        config.CONTRACT_SUBAWARD_URI,
        ["subawardee_uei", "prime_award_naics_code",
         "prime_award_product_or_service_code", "subaward_amount",
         "subaward_primary_place_of_performance_state_code"],
        f"subawardee_uei = {_sql_str(uei)}")
    combos: dict[tuple[str, str], dict[str, Any]] = {}
    states: set[str] = set()
    for r in rows:
        st = r.get("subaward_primary_place_of_performance_state_code")
        if st:
            states.add(st)
        naics = r.get("prime_award_naics_code")
        psc = r.get("prime_award_product_or_service_code")
        if not naics or not psc:
            continue
        entry = combos.setdefault((naics, psc), {"amt": 0.0, "edge_ct": 0})
        entry["amt"] += float(r.get("subaward_amount") or 0.0)
        entry["edge_ct"] += 1
    return combos, states


def _target_prime_pop_states(uei: str) -> set[str]:
    """PoP states of the target's OWN prime awards (recipient_uei point-lookup) —
    the prime half of the geography-default basis. Empty for never-primed firms."""
    rows = _scan_to_pylist(
        config.USASPENDING_AWARD_CANONICAL_URI,
        ["recipient_uei", "primary_place_of_performance_state_code"],
        f"recipient_uei = {_sql_str(uei)}")
    return {r["primary_place_of_performance_state_code"] for r in rows
            if r.get("primary_place_of_performance_state_code")}


def _lookalike_candidates(uei: str, combos: dict[tuple[str, str], dict[str, Any]],
                          notes: list[str]) -> list[dict[str, Any]]:
    """Firms sharing the target's top sub combos, ranked by overlapping sub $.
    Streams up to LOOKALIKE_EDGE_SCAN edges per combo (indexed
    prime_award_naics_code + prime_award_product_or_service_code)."""
    top = sorted(combos.items(), key=lambda kv: (-kv[1]["amt"], kv[0]))
    if len(top) > TARGET_COMBO_CAP:
        notes.append(f"combos: target's sub combos capped to the top {TARGET_COMBO_CAP} "
                     f"by sub $ (of {len(top)})")
        top = top[:TARGET_COMBO_CAP]
    cand: dict[str, dict[str, Any]] = {}
    for (naics, psc), _stats in top:
        rows = _stream_rows(
            config.CONTRACT_SUBAWARD_URI,
            ["subawardee_uei", "subaward_amount"],
            f"prime_award_naics_code = {_sql_str(naics)} AND "
            f"prime_award_product_or_service_code = {_sql_str(psc)}",
            LOOKALIKE_EDGE_SCAN)
        for r in rows:
            peer = r.get("subawardee_uei")
            if not peer or peer == uei:
                continue
            entry = cand.setdefault(peer, {"uei": peer, "overlap_amt": 0.0,
                                           "shared_combos": set()})
            entry["overlap_amt"] += float(r.get("subaward_amount") or 0.0)
            entry["shared_combos"].add((naics, psc))
    ranked = sorted(cand.values(), key=lambda c: (-c["overlap_amt"], c["uei"]))
    return ranked


def _pick_primed_lookalikes(ranked: list[dict[str, Any]],
                            notes: list[str]) -> list[dict[str, Any]]:
    """Top LOOKALIKE_CT candidates that ALSO PRIME (rollup point-check on the top
    LOOKALIKE_CANDIDATE_CHECK by overlap $), name-hydrated best-effort."""
    if not ranked:
        return []
    check = ranked[:LOOKALIKE_CANDIDATE_CHECK]
    primed: dict[str, bool] = {}
    try:
        rows = _scan_to_pylist(
            config.GTM_ENTITY_BEHAVIOR_ROLLUP_URI,
            ["uei", "prime_award_ct_lifetime"],
            _in_predicate("uei", [c["uei"] for c in check]))
        primed = {r["uei"]: bool(r.get("prime_award_ct_lifetime") or 0) for r in rows}
    except Exception as exc:  # noqa: BLE001 — degraded: nobody passes the qualifier
        log.warning("lookalike primed-check failed (%s): zero lookalikes served", exc)
        notes.append("lookalike primed-check unavailable — no lookalikes served")
        return []
    picked = [c for c in check if primed.get(c["uei"])][:LOOKALIKE_CT]
    if not picked:
        notes.append(f"no primed lookalike among the top {len(check)} overlap candidates")
        return []
    names: dict[str, Any] = {}
    try:
        rows = _scan_to_pylist(config.GTM_SAM_ENTITIES_URI,
                               ["uei", "legal_business_name"],
                               _in_predicate("uei", [c["uei"] for c in picked]))
        names = {r["uei"]: r.get("legal_business_name") for r in rows}
    except Exception as exc:  # noqa: BLE001 — names are hydration, never fatal
        log.warning("lookalike name hydration failed (%s): null names", exc)
    return [{"uei": c["uei"], "legal_business_name": names.get(c["uei"]),
             "overlap_amt": round(c["overlap_amt"], 2),
             "shared_combos": sorted(c["shared_combos"])} for c in picked]


def _lookalike_expansion_combos(
        lookalikes: list[dict[str, Any]],
        target_combos: dict[tuple[str, str], dict[str, Any]],
        notes: list[str]) -> dict[tuple[str, str], dict[str, Any]]:
    """The lookalikes' OTHER sub combos (their demonstrated sub receipts, minus the
    target's own combos), top EXPANSION_COMBO_CAP by their sub $."""
    if not lookalikes:
        return {}
    rows = _scan_to_pylist(
        config.CONTRACT_SUBAWARD_URI,
        ["subawardee_uei", "prime_award_naics_code",
         "prime_award_product_or_service_code", "subaward_amount"],
        _in_predicate("subawardee_uei", [lk["uei"] for lk in lookalikes]))
    exp: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        naics = r.get("prime_award_naics_code")
        psc = r.get("prime_award_product_or_service_code")
        peer = r.get("subawardee_uei")
        if not naics or not psc or not peer:
            continue
        combo = (naics, psc)
        if combo in target_combos:
            continue
        entry = exp.setdefault(combo, {"amt": 0.0, "lookalikes": set()})
        entry["amt"] += float(r.get("subaward_amount") or 0.0)
        entry["lookalikes"].add(peer)
    if len(exp) > EXPANSION_COMBO_CAP:
        notes.append(f"combos: lookalike expansion capped to the top "
                     f"{EXPANSION_COMBO_CAP} combos by lookalike sub $ (of {len(exp)})")
        kept = sorted(exp.items(), key=lambda kv: (-kv[1]["amt"], kv[0]))[:EXPANSION_COMBO_CAP]
        exp = dict(kept)
    return exp


def _execute_combos(req: dict[str, Any], today: dt_date) -> dict[str, Any]:
    """The combos-mode plan (block comment above). Same envelope discipline as the
    relationships mode: flat obligation-sorted list, matched_via evidence per row,
    honest empty answers, per-stage timings, POV fully disclosed in meta.pov."""
    timings: dict[str, float] = {}
    notes: list[str] = []
    cache_state = "unavailable"
    cache_build_ms: float | None = None
    target_hq: dict[str, float] | None = None
    pov: dict[str, Any] | None = None
    t0 = time.monotonic()

    def _mark(stage: str, since: float) -> float:
        now = time.monotonic()
        timings[stage] = round((now - since) * 1000.0, 1)
        return now

    def _meta(total: int, reason: str | None = None) -> dict[str, Any]:
        timings["total"] = round((time.monotonic() - t0) * 1000.0, 1)
        meta: dict[str, Any] = {
            "recipeId": COMBOS_RECIPE_ID,
            "registryVersion": market_registry.REGISTRY_VERSION,
            "uei": req["uei"],
            "mode": "combos",
            "cache_state": cache_state,
            "cache_build_ms": cache_build_ms,
            "target_hq": target_hq,
            "pov": pov,
            "timings_ms": timings,
            "total": total,
        }
        if reason is not None:
            meta["reason"] = reason
        if notes:
            meta["notes"] = notes
        return meta

    t = time.monotonic()
    try:
        cache_state, caches = _ensure_caches()
        cache_build_ms = caches.build_ms
    except Exception as exc:  # noqa: BLE001 — degrade; next request retries
        log.warning("subout caches unavailable (%s): serving degraded empty answer", exc)
        cache_state = "failed"
        notes.append("in-process cache build FAILED — no matches served; error: "
                     f"{type(exc).__name__}: {exc}")
        _mark("cache_ensure", t)
        return {"meta": _meta(0), "data": {"opportunities": []}}
    t = _mark("cache_ensure", t)

    for row in _rows_for_uei(caches.geo_table, req["uei"]):
        lat, lon = row.get("latitude"), row.get("longitude")
        if lat is not None and lon is not None:
            target_hq = {"latitude": float(lat), "longitude": float(lon)}
            break
    t = _mark("geo", t)

    # POV 1: the target's demonstrated sub combos + its performance states
    target_combos, sub_states = _target_sub_combo_profile(req["uei"])
    t = _mark("combo_profile", t)
    prime_states = _target_prime_pop_states(req["uei"])
    pop_states = sorted(sub_states | prime_states)
    t = _mark("pop_states", t)

    if not target_combos:
        pov = {"target_combos": [], "lookalikes": [], "expansion_combos": [],
               "pop_states": pop_states,
               "pop_state_basis": "target's historical subaward + prime-award PoP states"}
        return {"meta": _meta(0, reason="target has no demonstrated sub combos — "
                                        "the combos mode has nothing to expand from"),
                "data": {"opportunities": []}}

    # POV 2+3: lookalikes (share sub combos AND prime) → their other sub combos
    ranked = _lookalike_candidates(req["uei"], target_combos, notes)
    lookalikes = _pick_primed_lookalikes(ranked, notes)
    t = _mark("lookalikes", t)
    expansion = _lookalike_expansion_combos(lookalikes, target_combos, notes)
    t = _mark("expansion", t)

    lk_by_combo: dict[tuple[str, str], list[str]] = {
        combo: sorted(stats["lookalikes"]) for combo, stats in expansion.items()}
    pov = {
        "target_combos": [
            {"naics": c[0], "psc": c[1], "sub_amt": round(v["amt"], 2),
             "edge_ct": v["edge_ct"]}
            for c, v in sorted(target_combos.items(), key=lambda kv: -kv[1]["amt"])],
        "lookalikes": [{**lk, "shared_combos": [
            {"naics": n, "psc": p} for n, p in lk["shared_combos"]]}
            for lk in lookalikes],
        "expansion_combos": [
            {"naics": c[0], "psc": c[1], "lookalike_sub_amt": round(v["amt"], 2),
             "lookalikes": sorted(v["lookalikes"])}
            for c, v in sorted(expansion.items(), key=lambda kv: -kv[1]["amt"])],
        "pop_states": pop_states,
        "pop_state_basis": "target's historical subaward + prime-award PoP states "
                           "(the geography DEFAULT — awards outside these states, or "
                           "with no PoP state, are filtered out)",
    }

    # Dots: open awards whose combo ∈ (target ∪ expansion), PoP-state-filtered
    all_combos = set(target_combos) | set(expansion)
    state_set = set(pop_states)
    excluded_geo = 0
    rows_out: list[dict[str, Any]] = []
    for combo in sorted(all_combos):
        for idx in caches.awards_by_combo.get(combo, []):
            award = caches.open_awards[idx]
            if not _is_open(award, today):
                continue
            if state_set and award.get(
                    "primary_place_of_performance_state_code") not in state_set:
                excluded_geo += 1
                continue
            matched_via = []
            if combo in target_combos:
                matched_via.append({
                    "rule": "target_sub_combo",
                    "combo": {"naics": combo[0], "psc": combo[1]},
                    "target_sub_amt": round(target_combos[combo]["amt"], 2),
                    "target_edge_ct": target_combos[combo]["edge_ct"]})
            if combo in expansion:
                matched_via.append({
                    "rule": "lookalike_sub_combo",
                    "combo": {"naics": combo[0], "psc": combo[1]},
                    "lookalike_sub_amt": round(expansion[combo]["amt"], 2),
                    "lookalikes": lk_by_combo.get(combo, [])})
            distance_mi = None
            if (target_hq is not None and award.get("latitude") is not None
                    and award.get("longitude") is not None):
                distance_mi = round(_haversine_mi(
                    target_hq["latitude"], target_hq["longitude"],
                    float(award["latitude"]), float(award["longitude"])), 1)
            nearest_site = None
            if (award.get("geo_precision") == "zip5"
                    and award.get("latitude") is not None
                    and award.get("longitude") is not None):
                nearest_site = _nearest_federal_site(
                    float(award["latitude"]), float(award["longitude"]), caches)
            rows_out.append({
                "generated_unique_award_id": award.get("generated_unique_award_id"),
                "award_id_piid": award.get("award_id_piid"),
                "prime_uei": award.get("recipient_uei"),
                "prime_name": award.get("recipient_name"),
                "naics_code": award.get("naics_code"),
                "product_or_service_code": award.get("product_or_service_code"),
                "awarding_agency_code": award.get("awarding_agency_code"),
                "awarding_agency_name": award.get("awarding_agency_name"),
                "total_obligation": award.get("total_obligation"),
                "base_and_all_options_value": award.get("base_and_all_options_value"),
                "subaward_count": award.get("subaward_count"),
                "total_subaward_amount": award.get("total_subaward_amount"),
                "subcontracting_plan_code": award.get("subcontracting_plan_code"),
                "award_or_idv_flag": award.get("award_or_idv_flag"),
                "idv_type_code": award.get("idv_type_code"),
                "type_of_set_aside_code": award.get("type_of_set_aside_code"),
                "period_of_performance_current_end_date": _map_jsonable(
                    award.get("period_of_performance_current_end_date")),
                "ordering_period_end_date": _map_jsonable(award.get("ordering_period_end_date")),
                "pop_state_code": award.get("primary_place_of_performance_state_code"),
                "pop_geo_precision": award.get("geo_precision"),
                "latitude": award.get("latitude"),
                "longitude": award.get("longitude"),
                "distance_mi": distance_mi,
                "nearest_federal_site": nearest_site,
                "matched_via": matched_via,
            })
            if len(rows_out) >= AWARD_SCAN_CAP:
                break
        if len(rows_out) >= AWARD_SCAN_CAP:
            notes.append(f"combos: open-award assembly capped at {AWARD_SCAN_CAP}")
            break
    if excluded_geo:
        notes.append(f"geography default excluded {excluded_geo} open awards outside "
                     f"the target's historical PoP states {pop_states}")
    if not rows_out:
        return {"meta": _meta(0, reason="no open awards match the combo set inside "
                                        "the geography default"),
                "data": {"opportunities": []}}
    rows_out.sort(key=lambda o: (-(o.get("total_obligation") or 0.0),
                                 o.get("generated_unique_award_id") or ""))
    total = len(rows_out)
    rows_out = rows_out[:req["limit"]]
    _mark("assemble", t)
    return {"meta": _meta(total), "data": {"opportunities": rows_out}}


# ── The recipe executor ────────────────────────────────────────────────────────
def execute_subout_opportunities(body: Any, today: "dt_date | None" = None) -> dict[str, Any]:
    """The full v3 plan (module docstring). Returns ``{meta: {recipeId,
    registryVersion, uei, cache_state, cache_build_ms, target_hq, timings_ms,
    total, ...}, data: {opportunities}}`` — a FLAT list sorted by
    total_obligation DESC, each row carrying its ``matched_via`` relationship
    evidence. Raises ``MapCompileError`` (→ 422) on any off-contract body; a
    target with no matching relationships is a 200 with empty data + meta.reason."""
    req = validate_request(body)
    today = today or dt_date.today()
    if req["mode"] == "combos":
        return _execute_combos(req, today)
    timings: dict[str, float] = {}
    notes: list[str] = []
    cache_state = "unavailable"
    cache_build_ms: float | None = None
    target_hq: dict[str, float] | None = None
    t0 = time.monotonic()

    def _mark(stage: str, since: float) -> float:
        now = time.monotonic()
        timings[stage] = round((now - since) * 1000.0, 1)
        return now

    def _meta(total: int, reason: str | None = None) -> dict[str, Any]:
        timings["total"] = round((time.monotonic() - t0) * 1000.0, 1)
        meta: dict[str, Any] = {
            "recipeId": RECIPE_ID,
            "registryVersion": market_registry.REGISTRY_VERSION,
            "uei": req["uei"],
            "cache_state": cache_state,
            "cache_build_ms": cache_build_ms,
            "target_hq": target_hq,
            "timings_ms": timings,
            "total": total,
        }
        if reason is not None:
            meta["reason"] = reason
        if notes:
            meta["notes"] = notes
        return meta

    # 1. in-process caches. A failed build is NEVER silent: cache_state='failed'
    # + the error string ride the wire on every affected response.
    t = time.monotonic()
    try:
        cache_state, caches = _ensure_caches()
        cache_build_ms = caches.build_ms
    except Exception as exc:  # noqa: BLE001 — a failed build degrades; next request retries
        log.warning("subout caches unavailable (%s): serving degraded empty answer", exc)
        cache_state = "failed"
        notes.append("in-process cache build FAILED — no matches served; error: "
                     f"{type(exc).__name__}: {exc}")
        _mark("cache_ensure", t)
        return {"meta": _meta(0), "data": {"opportunities": []}}
    t = _mark("cache_ensure", t)

    # 2. target HQ off the boot geo cache — a fact about the TARGET, computed
    # before any matching so empty answers still carry the map anchor.
    for row in _rows_for_uei(caches.geo_table, req["uei"]):
        lat, lon = row.get("latitude"), row.get("longitude")
        if lat is not None and lon is not None:
            target_hq = {"latitude": float(lat), "longitude": float(lon)}
            break
    t = _mark("geo", t)

    # 3. RULE A: the target's subaward edges → its primes → their open awards
    rel = _target_prime_relationships(req["uei"])
    t = _mark("target_primes", t)
    rule_a = _rule_a_awards(rel, caches, today)
    t = _mark("rule_a", t)

    # 4. RULE B: the target's prime-award (agency, code) pairs → peer firms →
    # open awards with subaward edges to those peers. Empty when the target has
    # never primed (pairs = []) — no scans wasted.
    pairs = _target_prime_pairs(req["uei"], notes)
    t = _mark("prime_pairs", t)
    peers = _peer_firms_for_pairs(req["uei"], pairs, notes) if pairs else {}
    t = _mark("peer_firms", t)
    rule_b = _rule_b_awards(peers, caches, today, notes)
    t = _mark("peer_edges", t)

    if not rule_a and not rule_b:
        reason = ("no matching relationships: the target has no subaward history "
                  "under any prime with open awards (rule A) and "
                  + ("no prime awards of its own (rule B never fires)"
                     if not pairs else "no peer-linked open awards (rule B)"))
        return {"meta": _meta(0, reason=reason), "data": {"opportunities": []}}

    # 5. assemble the union (a row matched by both rules carries both evidences),
    # flat-sorted by total_obligation DESC — NO scoring, by direction.
    opportunities: list[dict[str, Any]] = []
    for idx in set(rule_a) | set(rule_b):
        award = caches.open_awards[idx]
        matched_via = []
        if idx in rule_a:
            matched_via.append(rule_a[idx])
        if idx in rule_b:
            matched_via.append(_finalize_rule_b_evidence(rule_b[idx]))
        distance_mi = None
        if (target_hq is not None and award.get("latitude") is not None
                and award.get("longitude") is not None):
            distance_mi = round(_haversine_mi(
                target_hq["latitude"], target_hq["longitude"],
                float(award["latitude"]), float(award["longitude"])), 1)
        nearest_site = None
        if (award.get("geo_precision") == "zip5"
                and award.get("latitude") is not None
                and award.get("longitude") is not None):
            nearest_site = _nearest_federal_site(
                float(award["latitude"]), float(award["longitude"]), caches)
        opportunities.append({
            "generated_unique_award_id": award.get("generated_unique_award_id"),
            "award_id_piid": award.get("award_id_piid"),
            "prime_uei": award.get("recipient_uei"),
            "prime_name": award.get("recipient_name"),
            "naics_code": award.get("naics_code"),
            "product_or_service_code": award.get("product_or_service_code"),
            "awarding_agency_code": award.get("awarding_agency_code"),
            "awarding_agency_name": award.get("awarding_agency_name"),
            "total_obligation": award.get("total_obligation"),
            "base_and_all_options_value": award.get("base_and_all_options_value"),
            "subaward_count": award.get("subaward_count"),
            "total_subaward_amount": award.get("total_subaward_amount"),
            "subcontracting_plan_code": award.get("subcontracting_plan_code"),
            "award_or_idv_flag": award.get("award_or_idv_flag"),
            "idv_type_code": award.get("idv_type_code"),
            "type_of_set_aside_code": award.get("type_of_set_aside_code"),
            "period_of_performance_current_end_date": _map_jsonable(
                award.get("period_of_performance_current_end_date")),
            "ordering_period_end_date": _map_jsonable(award.get("ordering_period_end_date")),
            "pop_state_code": award.get("primary_place_of_performance_state_code"),
            "pop_geo_precision": award.get("geo_precision"),
            "latitude": award.get("latitude"),
            "longitude": award.get("longitude"),
            "distance_mi": distance_mi,
            "nearest_federal_site": nearest_site,
            "matched_via": matched_via,
        })
    opportunities.sort(key=lambda o: (-(o.get("total_obligation") or 0.0),
                                      o.get("generated_unique_award_id") or ""))
    total = len(opportunities)
    opportunities = opportunities[:req["limit"]]
    _mark("assemble", t)

    return {"meta": _meta(total), "data": {"opportunities": opportunities}}
