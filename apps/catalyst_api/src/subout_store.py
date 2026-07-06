"""Subout-opportunities recipe — the per-UEI "open prime awards likely to be subbed
out to companies like yours" query (POST /api/v1/market/subout-opportunities).

A READ-TIME NAMED RECIPE over the pre-weighting substrate (pre-weighting doctrine: the
datasets carry raw evidence — counts, sums, dates — and every weight/threshold lives
HERE, versioned under RECIPE_ID, never baked into a build). Every score is returned
WITH its components on the wire: (name, raw_value, weight, contribution) per component,
score = Σ contributions. Fail-closed like the market compiler: any unknown body key,
off-vocabulary lens, or malformed value raises ``MapCompileError`` (→ 422 at the route).

HOT PATH = IN-PROCESS MEMORY (the latency rework). The first cut ran everything as
per-request R2 scans and timed 20-120s cold on Railway. Now the caches load ONCE per
process (lazy, thread-safe, TTL-refreshed in a background thread — a request never
blocks on a refresh):
  • gtm_open_awards   — every OPEN prime award (~150-250K rows), pre-joined with the
                        PoP centroid geo + agency names; indexed in-process by
                        recipient_uei / naics_code / product_or_service_code.
  • the cube marginal — gtm_primes_by_recipient_code (~1.7M rows, PRE-AGGREGATED
                        upstream from the 11.8M-cell cube to the
                        (recipient_code_type, recipient_code, prime_awardee_uei)
                        grain): a plain columnar load, regrouped in-process to
                        per-(code_type, code) blocks. Boot NEVER aggregates the
                        cube — the in-process aggregation was 10-20x slower on
                        Railway's throttled CPU than locally and starved the prewarm.
  • probe caches      — gtm_entity_code_lanes, the gtm_sam_entities NAICS slice,
                        and gtm_entity_geo as uei-sorted single-chunk ARROW tables
                        served by in-process binary search: the per-request remote
                        probes measured 0.7-5.8s live are now ~21 comparisons, and
                        arrow's raw columnar bytes are ~10x smaller than the
                        dict-of-python-objects shape at these row counts.
  • federal sites v2  — federal_sites_lance point rows (~294K; GSA-reported FRPP
                        shadows excluded — see FRPP_GSA_REPORTING_AGENCY_CODE) on a
                        0.1° grid: the nearest_federal_site enrichment + the
                        federal_site_proximity component. Best-effort load: an
                        unreachable site layer degrades to null enrichment, never
                        a bricked recipe cache.
  • inferred warmup   — gtm_entity_inferred_primeable_codes (263M rows) is the ONE
                        uncacheable table: its dataset handle + BTREE index pages
                        are warmed at build with a dummy probe (the FIRST cold probe
                        measured 516 SECONDS live — the storm is paid once, at boot).
Cache build stats (row counts, build seconds, inferred warm ms, ru_maxrss delta) are
logged LOUD at build; a FAILED build stores its error (last_build_error) and rides the
wire as cache_state='failed' + a meta note — never a silent 'unavailable'.

PER-REQUEST REMOTE HOPS (the wire shows exactly what is left):
  1. probe_inferred — the inferred lens (BTREE uei point-lookup on the 263M-row
                      projection, capped at the top INFERRED_PROBE_CAP codes by
                      support; cooccurrence evidence, NOT a demonstration). This is
                      the server-side latency floor.
  2. peers          — OPTIONAL (include_peers defaults FALSE: the evidence-table
                      query is a remote indexed scan measured ~14.5s live — opt-in).

IN-PROCESS STAGES: probe_cached (lanes/SAM dict hits + caller_declared) → cube_match
(dict lookups per probe code) → open_awards (index hits per matched prime, open-date
re-checked against TODAY — the cache may be hours old) → geo (target HQ dict hit;
distance_mi = haversine(HQ, the award row's own PoP centroid)) → score.

LENS VOCABULARY (how a code is known): awarded_prime_contracts_in_code (lanes
side='prime'); delivered_subawards_under_code (lanes side='sub' — the PRIME award's
code on subawards the firm delivered under, never a claim of the firm's own work);
sam_registered_naics (primary_naics + naics_codes); inferred_primeable;
caller_declared (codes_override).

A UEI with no code signals is a 200 with empty data + meta.reason — an empty market is
an answer, not an error. Per-stage wall times ride meta.timings_ms; cache_state
(cold | warm | failed) + cache_build_ms ride meta.

MAP-READY WIRE (coordinates are the primitive; distances are derived and ALSO served):
every opportunity row carries its PoP centroid (latitude/longitude, honest per
pop_geo_precision — null when ungeocoded); meta.target_hq carries the target's HQ point
(computed up front so empty answers still anchor the map); nearest_federal_site carries
the site's own point; opt-in peers hydrate legal_business_name (one chunked BTREE IN
scan on gtm_sam_entities) + HQ coordinates (boot geo cache).
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
from .market_store import _CODE_OK, _in_predicate

log = logging.getLogger("catalyst_api.subout_store")

# ── The recipe (id + weights live together — bump the id on ANY weight change) ─
# v2: + federal_site_proximity (nearest federal site to the award's PoP centroid,
# off the federal_sites_lance boot grid) at 0.05; the v1 weights renormalized ×0.95
# so Σ stays exactly 1.0. All v1 semantics otherwise unchanged.
RECIPE_ID = "subout_opportunities.v2"
# Component weights, normalized (Σ = 1.0). Each component contributes
# weight × normalized_value (normalized_value ∈ [0, 1]); score = Σ contributions.
COMPONENT_WEIGHTS: dict[str, float] = {
    "prime_subout_history": 0.285,   # log-scaled Σ subaward_amt_total over matched cells
    "award_already_subbing": 0.1425,  # the award ALREADY reports subaward activity
    "subcontracting_plan": 0.095,    # a subcontracting plan attached to the award
    "lens_strength": 0.1425,         # how strongly the target carries the matched codes
    "proximity": 0.095,              # HQ → place-of-performance distance decay
    "expiring_window": 0.19,         # nearer PoP end = nearer recompete/backfill window
    "federal_site_proximity": 0.05,  # a federal site near the work site (v2)
}

# ── Lens vocabulary (self-describing: each name states HOW the code is known) ──
LENS_AWARDED_PRIME = "awarded_prime_contracts_in_code"
LENS_DELIVERED_SUB = "delivered_subawards_under_code"
LENS_SAM_NAICS = "sam_registered_naics"
LENS_INFERRED_PRIMEABLE = "inferred_primeable"
LENS_CALLER_DECLARED = "caller_declared"        # codes_override only — never selectable
SELECTABLE_LENSES = (LENS_AWARDED_PRIME, LENS_DELIVERED_SUB,
                     LENS_SAM_NAICS, LENS_INFERRED_PRIMEABLE)

CODE_TYPES = ("naics", "psc")
ALLOWED_BODY_KEYS = frozenset(
    {"uei", "lenses", "codes_override", "code_type", "limit", "include_peers"})

DEFAULT_LIMIT = 50
LIMIT_CAP = 200
# Bounded fan-out: the inferred lens can carry ~400 codes per UEI (the all-lens smoke
# TIMED OUT on it live) — the probe keeps only the strongest-supported codes. Matched
# primes and scored awards stay bounded too.
INFERRED_PROBE_CAP = 100
PRIME_PROBE_CAP = 500
AWARD_SCAN_CAP = 2_000
# Peers: how many top matched codes probe the evidence table, how many evidence rows
# stream before the cut, and the peer cap itself. The peers stage is the ONLY remote
# non-point query left (measured ~14.5s live) — hence include_peers defaults FALSE.
PEER_CODE_PROBE = 3
PEER_EVIDENCE_ROW_SCAN = 2_000
PEER_CAP = 10

# ── Normalization constants (deterministic — pinned by tests) ──────────────────
LOG_DOLLAR_CEILING_EXP = 9.0        # log10 $ scale: $1B+ normalizes to 1.0
INFERRED_SUPPORT_CEILING = 20       # supporting_bothsider_firm_ct at which norm = 1.0
DECLARED_LENS_STRENGTH = 0.5        # sam_registered / caller_declared: a claim, not $
PROXIMITY_NEUTRAL = 0.5             # geo_precision != 'zip5' or distance unknown
PROXIMITY_ZERO_MI = 500.0           # linear decay: 0 mi → 1.0, ≥500 mi → 0.0
EXPIRING_HORIZON_DAYS = 1_080       # ~3y: ends today → 1.0, ≥horizon → 0.0
PLAN_REQUIRED_CODES = frozenset({"C", "D", "E", "F", "G", "H"})  # FAR 19.7 plan attached

# ── Federal-site proximity (recipe v2) ─────────────────────────────────────────
# Nearest federal site beyond this is not an opportunity signal → the enrichment
# field is null and the component contributes 0.0 (an absent site never penalizes).
NEAREST_SITE_MAX_MI = 50.0
FEDERAL_SITE_DECAY_ZERO_MI = 25.0   # linear decay: at the site → 1.0, ≥25 mi → 0.0
# FRPP rows whose reporting agency is GSA are SHADOWS of the gsa_building rows (the
# same buildings reported twice) — excluded from the site grid. '47' = the value AS
# OBSERVED LIVE in federal_sites_lance.reporting_agency_code (v4, 8,620 shadow rows;
# FRPP strips the toptier code's leading zero — NOT '047'). The builder prints the
# value at build; re-verify on any FRPP reload.
FRPP_GSA_REPORTING_AGENCY_CODE = "47"
# Grid buckets: 0.1° ≈ 6.9 mi of latitude (longitude shrinks by cos(lat) — ~4 mi/bucket
# at the CONUS/Alaska extreme, the conservative ring-pruning floor). The nearest-site
# search expands ring by ring until no closer site is geometrically possible, bounded
# by the ring that could still hold a site inside NEAREST_SITE_MAX_MI.
SITE_BUCKET_DEG = 0.1
_SITE_BUCKET_MIN_MI = 4.0
_SITE_MAX_RING = math.ceil(NEAREST_SITE_MAX_MI / _SITE_BUCKET_MIN_MI) + 1

# ── Cache contract ─────────────────────────────────────────────────────────────
CACHE_TTL_S = 6 * 3600              # staleness bound; refresh runs in the BACKGROUND

# The full projection loaded from gtm_open_awards (every column the wire row serves).
OPEN_AWARD_COLUMNS = [
    "generated_unique_award_id", "award_id_piid", "recipient_uei", "recipient_name",
    "naics_code", "product_or_service_code", "total_obligation",
    "base_and_all_options_value", "subaward_count", "total_subaward_amount",
    "subcontracting_plan_code", "period_of_performance_current_end_date",
    "ordering_period_end_date", "award_or_idv_flag", "idv_type_code",
    "type_of_set_aside_code", "awarding_agency_code", "awarding_agency_name",
    "primary_place_of_performance_state_code", "latitude", "longitude", "geo_precision",
]
# The pre-aggregated marginal's projection (gtm_primes_by_recipient_code, ~1.7M rows,
# 1 row / (recipient_code_type, recipient_code, prime_awardee_uei)). Built UPSTREAM
# from the 11.8M-cell cube — boot never aggregates, only loads and regroups (the
# in-process aggregation was 10-20x slower on Railway's throttled CPU than locally
# and starved the prewarm). Every recipient_code_source in the source cube is
# demonstrated/declared by construction, so the marginal's sums carry them all.
MARGINAL_COLUMNS = [
    "recipient_code_type", "recipient_code", "prime_awardee_uei",
    "subaward_amt_total", "subaward_edge_ct", "distinct_recipient_ct",
    "last_subaward_action_date",
]


@dataclass(frozen=True)
class SuboutCaches:
    """The in-process read models behind the hot path. ``open_awards`` is the row
    list; the ``awards_by_*`` dicts map key → row indexes into it. ``cube`` maps
    (recipient_code_type, recipient_code) → a COLUMNAR cell block
    ``(primes: tuple[str], amt_totals: array('d'), edge_cts: array('q'),
    distinct_cts: array('q'), last_dates: tuple[date|None])`` — parallel arrays
    instead of 1.7M per-cell dict entries, for RSS (measured: the dict-shaped cube
    blew the ~1GB budget).

    PROBE CACHES (the per-request remote-latency kill): ``lanes_table`` /
    ``sam_table`` / ``geo_table`` are pyarrow Tables SORTED BY uei and combined to a
    single chunk — served by in-process binary search (``_rows_for_uei``). Arrow
    keeps them as raw columnar bytes: measured, the dict-of-python-objects shape
    cost ~950MB across the three; the arrow shape is ~10x smaller (a lookup is ~21
    comparisons — nanoseconds against the 0.7-5.8s remote probes they replace).
    With these resident, the only remote hop left on the hot path is the inferred
    probe (a 263M-row projection — uncacheable; its dataset HANDLE + index pages
    are warmed at build instead).

    FEDERAL SITES (recipe v2): ``federal_sites`` is the point-site tuple list
    ``(latitude, longitude, site_name, site_type, site_source,
    lease_expiring_24mo_ct, earliest_lease_expiration_date)`` (GSA-reported FRPP
    shadows excluded); ``federal_site_buckets`` maps the 0.1° grid cell
    ``(round(lat/0.1), round(lon/0.1))`` → site indexes, for the expanding-ring
    nearest-site search."""

    open_awards: list[dict[str, Any]]
    awards_by_prime: dict[str, list[int]]
    awards_by_naics: dict[str, list[int]]
    awards_by_psc: dict[str, list[int]]
    cube: dict[tuple[str | None, str], tuple]
    cube_cell_count: int
    lanes_table: Any                     # pyarrow.Table, sorted by uei, single chunk
    sam_table: Any                       # pyarrow.Table, sorted by uei, single chunk
    geo_table: Any                       # pyarrow.Table, sorted by uei, single chunk
    federal_sites: list[tuple]
    federal_site_buckets: dict[tuple[int, int], list[int]]
    inferred_warm_ms: float
    build_ms: float


_cache_lock = threading.Lock()
_build_lock = threading.Lock()
_caches: SuboutCaches | None = None
_caches_built_at: float | None = None
_cache_refreshing = False
# The LAST build failure, as a string — a silently-dead prewarm was undiagnosable in
# prod (cache_state sat 'unavailable' with no reason on the wire). Set on any failed
# build (cold, prewarm, or refresh), cleared on the next successful one; surfaced as
# cache_state='failed' + a meta note carrying the error on every affected response.
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
    """The full gtm_open_awards table (~150-250K rows, tens of MB) as row dicts —
    the one full-table load the recipe makes, ONCE per cache build."""
    scanner = _dataset(config.GTM_OPEN_AWARDS_URI).scanner(columns=OPEN_AWARD_COLUMNS)
    rows: list[dict[str, Any]] = []
    for batch in scanner.to_batches():
        rows.extend(batch.to_pylist())
    return rows


def _iter_marginal_rows():
    """Streamed column-projected pass over the PRE-AGGREGATED
    gtm_primes_by_recipient_code marginal (~1.7M rows) — yields
    ``(code_type, code, prime, amt_total, edge_ct, distinct_ct, last_date)`` tuples.
    A plain columnar load: NO aggregation happens here, ever (the upstream builder
    owns the Σ/MAX over the 11.8M-cell cube)."""
    scanner = _dataset(config.GTM_PRIMES_BY_RECIPIENT_CODE_URI).scanner(
        columns=MARGINAL_COLUMNS)
    for batch in scanner.to_batches():
        cols = [batch.column(c).to_pylist() for c in MARGINAL_COLUMNS]
        yield from zip(*cols)


def _load_sorted_uei_table(uri: str, columns: list[str]):
    """One column-projected load → pyarrow Table SORTED BY uei, single chunk — the
    shape ``_rows_for_uei`` binary-searches. Arrow keeps the rows as raw columnar
    bytes (~10x smaller than dict-of-python-objects at these row counts)."""
    tbl = _dataset(uri).scanner(columns=columns).to_table()
    return tbl.sort_by("uei").combine_chunks()


def _load_entity_code_lanes():
    """gtm_entity_code_lanes (1.67M rows; uei, side, code_type, code, obl_lifetime)
    as a uei-sorted arrow table. Kills the per-request lanes probe."""
    return _load_sorted_uei_table(
        config.GTM_ENTITY_CODE_LANES_URI,
        ["uei", "side", "code_type", "code", "obl_lifetime"])


def _load_sam_registrations():
    """gtm_sam_entities NAICS slice (~2M rows; uei, primary_naics, naics_codes) as a
    uei-sorted arrow table. Kills the per-request SAM probe."""
    return _load_sorted_uei_table(
        config.GTM_SAM_ENTITIES_URI, ["uei", "primary_naics", "naics_codes"])


def _load_entity_geo():
    """gtm_entity_geo (1.45M rows; uei, latitude, longitude) as a uei-sorted arrow
    table. Kills the per-request target-HQ remote hop."""
    return _load_sorted_uei_table(
        config.GTM_ENTITY_GEO_URI, ["uei", "latitude", "longitude"])


def _rows_for_uei(tbl, uei: str) -> list[dict[str, Any]]:
    """All rows for one uei off a uei-sorted single-chunk arrow table, via binary
    search (equal-range: two bisects + one slice; ~21 comparisons at 1.45M rows).
    [] when the uei is absent — same contract as a filtered point scan."""
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


# ── Federal sites (recipe v2): point grid for nearest-site enrichment ──────────
FEDERAL_SITE_COLUMNS = [
    "site_source", "reporting_agency_code", "site_name", "site_type",
    "latitude", "longitude", "lease_expiring_24mo_ct",
    "earliest_lease_expiration_date",
]


def _site_rows_to_points(rows) -> list[tuple]:
    """Raw federal_sites_lance row dicts → point tuples
    ``(lat, lon, site_name, site_type, site_source, lease_expiring_24mo_ct,
    earliest_lease_expiration_date)``. PURE (unit-tested): keeps point rows only
    (both coordinates present) and EXCLUDES GSA-reported FRPP rows — those are
    shadows of the gsa_building rows (the same buildings reported twice; keeping
    both would double-count nearest-site hits)."""
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
    """federal_sites_lance (~316K rows) → point tuples via _site_rows_to_points.
    Coordinate presence rides the scan predicate (point rows only); the GSA-shadow
    exclusion is in-process. BEST-EFFORT: the site layer is an ENRICHMENT — if the
    dataset is unreachable the build proceeds with zero sites (nearest is null,
    the component contributes 0.0) and logs LOUD, rather than bricking the whole
    recipe cache."""
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
    """Point tuples → 0.1° grid buckets (cell → site indexes). Pure."""
    buckets: dict[tuple[int, int], list[int]] = {}
    for i, site in enumerate(sites):
        buckets.setdefault(_site_bucket(site[0], site[1]), []).append(i)
    return buckets


def _nearest_federal_site(lat: float, lon: float,
                          caches: SuboutCaches) -> dict[str, Any] | None:
    """The nearest federal site to a point, via expanding-ring search over the grid
    (ring r cells are ≥ (r-1)×_SITE_BUCKET_MIN_MI away — once the best hit beats
    that bound the search stops; bounded by the ring that could still hold a site
    inside NEAREST_SITE_MAX_MI). None beyond the cap or when no sites are loaded —
    the enrichment is honestly absent, never a far-away pretend match."""
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
        # the site's own point — the map primitive (distance_mi is the derived value)
        "latitude": best[0],
        "longitude": best[1],
        "distance_mi": round(best_d, 1),
        "lease_expiring_24mo_ct": best[5],
        "earliest_lease_expiration_date": _map_jsonable(best[6]),
    }


# ── Cache build (pure indexers over the loader seams) ─────────────────────────
def _index_open_awards(rows: list[dict[str, Any]]) -> tuple[
        dict[str, list[int]], dict[str, list[int]], dict[str, list[int]]]:
    by_prime: dict[str, list[int]] = {}
    by_naics: dict[str, list[int]] = {}
    by_psc: dict[str, list[int]] = {}
    for i, row in enumerate(rows):
        uei = row.get("recipient_uei")
        if uei:
            by_prime.setdefault(uei, []).append(i)
        naics = row.get("naics_code")
        if naics:
            by_naics.setdefault(naics, []).append(i)
        psc = row.get("product_or_service_code")
        if psc:
            by_psc.setdefault(psc, []).append(i)
    return by_prime, by_naics, by_psc


def _group_marginal(rows) -> tuple[dict[tuple[str | None, str], tuple], int]:
    """Marginal row tuples → (code_type, code) → COLUMNAR block
    ``(primes, amt_totals, edge_cts, distinct_cts, last_dates)`` (parallel sequences;
    array('d')/array('q') for the numbers). Columnar cells cost ~40B where per-cell
    dict entries cost ~300B — the difference is the RSS budget at 1.7M cells. Key
    strings are ``sys.intern``-ed (codes/primes repeat); date objects are memo-deduped
    (few thousand distinct dates across 1.7M rows). Pure — takes any iterable of
    ``(ct, code, prime, amt, edges, distinct, last)`` tuples."""
    from array import array
    from sys import intern

    grouped: dict[tuple[str | None, str], tuple[list, list, list, list, list]] = {}
    date_memo: dict[Any, Any] = {}
    n = 0
    for ct, code, prime, amt, edges, distinct, last in rows:
        if not code or not prime:
            continue
        key = (intern(ct) if ct else ct, intern(code))
        block = grouped.get(key)
        if block is None:
            block = ([], [], [], [], [])
            grouped[key] = block
        block[0].append(intern(prime))
        block[1].append(float(amt or 0.0))
        block[2].append(int(edges or 0))
        block[3].append(int(distinct or 0))
        block[4].append(date_memo.setdefault(last, last) if last is not None else None)
        n += 1
    cube: dict[tuple[str | None, str], tuple] = {
        key: (tuple(b[0]), array("d", b[1]), array("q", b[2]),
              array("q", b[3]), tuple(b[4]))
        for key, b in grouped.items()
    }
    return cube, n


def _rss() -> int | None:
    """Best-effort process max-RSS reading (platform units: bytes on macOS, KB on
    Linux — logged as observability, never a decision input)."""
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:  # noqa: BLE001 — non-POSIX platforms
        return None


# The dummy probe that warms the inferred projection's dataset handle + BTREE index
# pages at build time. The projection is 263M rows and uncacheable — but the FIRST
# filtered scan against a cold handle measured 516 SECONDS live (index page fetch
# storm), so the storm is paid ONCE at build, never by a request. Valid UEI charset,
# guaranteed-no-match.
_INFERRED_WARM_UEI = "ZZZZZZZZZZZZ"


def _warm_inferred_handle() -> float:
    """Open (and module-cache, via lance_store._dataset) the inferred projection's
    handle and run one dummy BTREE uei probe to page in the index. Returns the wall
    ms. Best-effort: a warm failure logs + stores the error string but never fails
    the build (the probe caches still serve; the first real inferred probe pays)."""
    t0 = time.monotonic()
    try:
        _scan_to_pylist(config.GTM_INFERRED_PRIMEABLE_URI,
                        ["uei", "code_type", "code", "supporting_bothsider_firm_ct"],
                        f"uei = {_sql_str(_INFERRED_WARM_UEI)}")
    except Exception as exc:  # noqa: BLE001 — warmup is best-effort
        log.warning("inferred-handle warmup failed (first real probe pays the cold "
                    "cost): %s", exc)
    return round((time.monotonic() - t0) * 1000.0, 1)


def _build_caches() -> SuboutCaches:
    """Build every in-process read model, LOUDLY (row counts / build seconds / RSS
    delta in the log — the numbers that diagnose a starved prewarm). Called cold
    (first request / prewarm) and by the background TTL refresh — never on the hot
    path of a warm request. All loads are plain columnar streams; NO aggregation."""
    t0 = time.monotonic()
    rss_before = _rss()
    cube, cube_cells = _group_marginal(_iter_marginal_rows())
    open_awards = _load_open_awards()
    by_prime, by_naics, by_psc = _index_open_awards(open_awards)
    lanes_table = _load_entity_code_lanes()
    sam_table = _load_sam_registrations()
    geo_table = _load_entity_geo()
    federal_sites = _load_federal_sites()
    federal_site_buckets = _index_federal_sites(federal_sites)
    inferred_warm_ms = _warm_inferred_handle()
    try:
        # Hand the sort/load transients back to the OS — the arrow pool otherwise
        # retains them as slack (measured: ~250MB of phantom RSS post-build).
        import pyarrow as pa
        pa.default_memory_pool().release_unused()
    except Exception:  # noqa: BLE001 — pool release is best-effort observability
        pass
    build_ms = round((time.monotonic() - t0) * 1000.0, 1)
    rss_after = _rss()
    rss_delta = ((rss_after - rss_before)
                 if (rss_before is not None and rss_after is not None) else "n/a")
    log.info(
        "subout caches built in %.1fs: open_awards=%d rows (primes=%d naics=%d "
        "psc=%d), marginal=%d cells over %d (code_type, code) keys, "
        "lanes=%d rows/%.0fMB sam=%d rows/%.0fMB geo=%d rows/%.0fMB (arrow), "
        "federal_sites=%d points over %d grid cells, "
        "inferred_handle_warm_ms=%s; ru_maxrss_delta=%s (platform units)",
        build_ms / 1000.0, len(open_awards), len(by_prime), len(by_naics),
        len(by_psc), cube_cells, len(cube),
        lanes_table.num_rows, lanes_table.nbytes / 1e6,
        sam_table.num_rows, sam_table.nbytes / 1e6,
        geo_table.num_rows, geo_table.nbytes / 1e6,
        len(federal_sites), len(federal_site_buckets),
        inferred_warm_ms, rss_delta)
    return SuboutCaches(open_awards=open_awards, awards_by_prime=by_prime,
                        awards_by_naics=by_naics, awards_by_psc=by_psc,
                        cube=cube, cube_cell_count=cube_cells,
                        lanes_table=lanes_table, sam_table=sam_table,
                        geo_table=geo_table, federal_sites=federal_sites,
                        federal_site_buckets=federal_site_buckets,
                        inferred_warm_ms=inferred_warm_ms, build_ms=build_ms)


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
    """(cache_state, caches). 'warm' serves the in-memory build (kicking a BACKGROUND
    refresh when past TTL — the request itself never waits on a rebuild); 'cold'
    means THIS call built it (first request or post-failure retry). Raises when a
    cold build fails — the executor degrades to an empty answer with a note, and the
    NEXT request retries (a failed build is never cached)."""
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
    """The most recent cache-build failure as a string (None after a successful
    build) — the diagnosis the 'silent unavailable' production incident lacked."""
    with _cache_lock:
        return _last_build_error


def prewarm_caches() -> None:
    """Boot-time best-effort warm (called from a daemon thread in main.lifespan) so
    the FIRST request after a deploy is already in-memory. Never raises — but a
    failure is stored in ``_last_build_error`` (via _ensure_caches) AND logged with
    the full traceback, so a dead prewarm is never silent again."""
    try:
        state, caches = _ensure_caches()
        log.info("subout cache prewarm: state=%s build_s=%.1f open_awards=%d "
                 "marginal_cells=%d", state, caches.build_ms / 1000.0,
                 len(caches.open_awards), caches.cube_cell_count)
    except Exception:  # noqa: BLE001 — prewarm must never brick boot
        log.exception("subout cache prewarm FAILED (first request will retry; "
                      "error rides cache_state='failed' + meta.notes)")


def reset_caches_for_tests() -> None:
    """Test hook: drop the module cache state (hermetic tests build via the stubbed
    loader seams)."""
    global _caches, _caches_built_at, _cache_refreshing, _last_build_error
    with _cache_lock:
        _caches = None
        _caches_built_at = None
        _cache_refreshing = False
        _last_build_error = None


# ── Request validation (fail-closed — MapCompileError → 422 at the route) ─────
def validate_request(body: Any) -> dict[str, Any]:
    """Body dict → normalized request. Unknown keys, off-vocabulary lenses, malformed
    codes, and mistyped knobs all refuse to compile — nothing off-contract reaches a
    Lance filter."""
    if not isinstance(body, dict):
        raise MapCompileError("request body must be an object")
    unknown = set(body) - ALLOWED_BODY_KEYS
    if unknown:
        raise MapCompileError(f"unknown body key(s) {sorted(unknown)!r}")

    uei = body.get("uei")
    if not isinstance(uei, str) or not valid_uei(uei.strip()):
        raise MapCompileError("uei is required and must be a 12-char alphanumeric SAM UEI")
    uei = uei.strip()

    lenses = body.get("lenses")
    if lenses is None:
        lenses = list(SELECTABLE_LENSES)
    else:
        if not isinstance(lenses, list) or not lenses:
            raise MapCompileError(
                f"lenses must be a non-empty array of {list(SELECTABLE_LENSES)} (omit for all)")
        for lens in lenses:
            if lens not in SELECTABLE_LENSES:
                raise MapCompileError(
                    f"unknown lens {lens!r} — one of {list(SELECTABLE_LENSES)}")
        lenses = list(dict.fromkeys(lenses))            # dedupe, order-preserving

    codes_override = body.get("codes_override")
    if codes_override is None:
        codes_override = []
    else:
        if not isinstance(codes_override, list):
            raise MapCompileError("codes_override must be an array of code strings")
        for c in codes_override:
            if not isinstance(c, str) or not _CODE_OK.match(c):
                raise MapCompileError(f"codes_override code {c!r} is not a valid NAICS/PSC code")
        codes_override = list(dict.fromkeys(codes_override))

    code_type = body.get("code_type")
    if code_type is not None and code_type not in CODE_TYPES:
        raise MapCompileError(f"code_type must be one of {list(CODE_TYPES)} (or omitted)")

    limit = body.get("limit")
    if limit is None:
        limit = DEFAULT_LIMIT
    elif isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise MapCompileError("limit must be a positive whole number")
    limit = min(limit, LIMIT_CAP)

    # include_peers defaults FALSE: the peers stage is the one remote non-point query
    # left (evidence-table indexed scan, ~14.5s measured live) — strictly opt-in.
    include_peers = body.get("include_peers")
    peers_defaulted = include_peers is None
    if peers_defaulted:
        include_peers = False
    elif not isinstance(include_peers, bool):
        raise MapCompileError("include_peers must be a boolean")

    return {"uei": uei, "lenses": lenses, "codes_override": codes_override,
            "code_type": code_type, "limit": limit, "include_peers": include_peers,
            "peers_defaulted": peers_defaulted}


# ── Normalizations (pure — component-math determinism is pinned by tests) ─────
def _log_dollar_norm(amount: Any) -> float:
    """USD → [0, 1] on a log10 scale: $0 → 0.0, $1B+ → 1.0."""
    amt = float(amount or 0.0)
    if amt <= 0:
        return 0.0
    return min(1.0, math.log10(1.0 + amt) / LOG_DOLLAR_CEILING_EXP)


def _lens_strength(lens: str, evidence: dict[str, Any]) -> float:
    """One lens entry → [0, 1] strength. Demonstrated $ log-scales; inferred support
    counts normalize against INFERRED_SUPPORT_CEILING; a bare claim (SAM registration /
    caller-declared) is DECLARED_LENS_STRENGTH — a claim, never dollars."""
    if lens in (LENS_AWARDED_PRIME, LENS_DELIVERED_SUB):
        return _log_dollar_norm(evidence.get("obl_lifetime"))
    if lens == LENS_INFERRED_PRIMEABLE:
        ct = evidence.get("supporting_bothsider_firm_ct") or 0
        return min(1.0, float(ct) / INFERRED_SUPPORT_CEILING)
    return DECLARED_LENS_STRENGTH


def _plan_norm(subcontracting_plan_code: Any) -> float:
    """Subcontracting-plan code → [0, 1]. Plan-attached codes (FAR 19.7 C/D/E/F/G/H)
    → 1.0; 'B' (below thresholds) → 0.25; 'A' (no subcontracting possibilities) and
    NULL → 0.0."""
    code = (subcontracting_plan_code or "").strip().upper()
    if code in PLAN_REQUIRED_CODES:
        return 1.0
    if code == "B":
        return 0.25
    return 0.0


def _proximity_norm(distance_mi: float | None, geo_precision: Any) -> float:
    """Distance decay, honest about precision: only a 'zip5' place-of-performance
    centroid with a known distance participates (linear: 0 mi → 1.0, ≥500 mi → 0.0);
    anything coarser or unknown is NEUTRAL (0.5), never a fake signal."""
    if distance_mi is None or geo_precision != "zip5":
        return PROXIMITY_NEUTRAL
    return max(0.0, 1.0 - distance_mi / PROXIMITY_ZERO_MI)


def _expiring_norm(days_to_end: int | None) -> float:
    """Days until the nearest open end date → [0, 1]: ends today → 1.0, decaying
    linearly to 0.0 at EXPIRING_HORIZON_DAYS. No open end date → 0.0."""
    if days_to_end is None or days_to_end < 0:
        return 0.0
    return max(0.0, 1.0 - days_to_end / EXPIRING_HORIZON_DAYS)


def _federal_site_norm(distance_mi: float | None) -> float:
    """Nearest-federal-site distance → [0, 1]: at the site → 1.0, decaying linearly
    to 0.0 at FEDERAL_SITE_DECAY_ZERO_MI. None (no zip5 award geo, or nothing inside
    NEAREST_SITE_MAX_MI) → 0.0 — an absent site contributes NOTHING, it never
    penalizes (unlike ``proximity``'s 0.5 neutral: proximity is about an unknown,
    this is about a genuine absence of the bonus signal)."""
    if distance_mi is None:
        return 0.0
    return max(0.0, 1.0 - distance_mi / FEDERAL_SITE_DECAY_ZERO_MI)


def _haversine_mi(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in statute miles (R = 3958.8 mi)."""
    rlat1, rlon1, rlat2, rlon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    dlat, dlon = rlat2 - rlat1, rlon2 - rlon1
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 3958.8 * 2.0 * math.asin(min(1.0, math.sqrt(a)))


# ── Stage 1: the target's probe codes, by lens ─────────────────────────────────
# Split in two on the wire (meta.timings_ms): probe_cached = the lanes/SAM dict hits
# + caller_declared (pure in-process); probe_inferred = the ONE remote hop left on
# the hot path (the 263M-row inferred projection — uncacheable).
def _entry_add(entries: dict, code_type: str | None, lens: str, ct: str | None,
               code: Any, evidence: dict[str, Any]) -> None:
    """Shared lens-entry accumulator: charset-validated, request-code_type-filtered,
    deduped on (lens, code_type, code) keeping the strongest evidence."""
    code = (code or "").strip() if isinstance(code, str) else code
    if not code or not isinstance(code, str) or not _CODE_OK.match(code):
        return
    if code_type is not None and ct is not None and ct != code_type:
        return
    key = (lens, ct, code)
    entry = {"lens": lens, "code_type": ct, "code": code, "evidence": evidence,
             "strength": _lens_strength(lens, evidence)}
    prior = entries.get(key)
    if prior is None or entry["strength"] > prior["strength"]:
        entries[key] = entry


def _probe_cached(uei: str, lenses: list[str], code_type: str | None,
                  codes_override: list[str], caches: SuboutCaches,
                  entries: dict) -> None:
    """The in-process lens probes: demonstrated lanes + SAM registrations off the
    boot arrow tables (binary search — no I/O), plus caller_declared."""
    if LENS_AWARDED_PRIME in lenses or LENS_DELIVERED_SUB in lenses:
        for row in _rows_for_uei(caches.lanes_table, uei):
            lens = (LENS_AWARDED_PRIME if row.get("side") == "prime"
                    else LENS_DELIVERED_SUB)
            if lens in lenses:
                _entry_add(entries, code_type, lens, row.get("code_type"),
                           row.get("code"), {"obl_lifetime": row.get("obl_lifetime")})

    if LENS_SAM_NAICS in lenses and code_type in (None, "naics"):
        for row in _rows_for_uei(caches.sam_table, uei):
            _entry_add(entries, code_type, LENS_SAM_NAICS, "naics",
                       row.get("primary_naics"), {"registration": "primary_naics"})
            for c in (row.get("naics_codes") or []):
                _entry_add(entries, code_type, LENS_SAM_NAICS, "naics", c,
                           {"registration": "naics_codes"})

    for c in codes_override:
        # caller-declared probe codes carry the request's code_type restriction when
        # set, else no type claim (the cube lookup then probes both code systems).
        _entry_add(entries, code_type, LENS_CALLER_DECLARED, code_type, c,
                   {"declared_by_caller": True})


def _probe_inferred(uei: str, lenses: list[str], code_type: str | None,
                    notes: list[str], entries: dict) -> None:
    """The inferred lens probe — the one REMOTE hop left on the hot path (BTREE uei
    point-lookup on the 263M-row projection; its handle + index pages are warmed at
    cache build). Capped at the top INFERRED_PROBE_CAP codes by
    supporting_bothsider_firm_ct (a ~400-code inferred profile exploded the fan-out
    live — the cap is noted on the wire when applied)."""
    if LENS_INFERRED_PRIMEABLE not in lenses:
        return
    pred = f"uei = {_sql_str(uei)}"
    if code_type is not None:
        pred += f" AND code_type = {_sql_str(code_type)}"
    inferred = _scan_to_pylist(config.GTM_INFERRED_PRIMEABLE_URI,
                               ["uei", "code_type", "code",
                                "supporting_bothsider_firm_ct"], pred)
    if len(inferred) > INFERRED_PROBE_CAP:
        inferred.sort(key=lambda r: (-(r.get("supporting_bothsider_firm_ct") or 0),
                                     r.get("code") or ""))
        inferred = inferred[:INFERRED_PROBE_CAP]
        notes.append(
            f"inferred_primeable probe capped to the top {INFERRED_PROBE_CAP} "
            "codes by supporting_bothsider_firm_ct")
    for row in inferred:
        _entry_add(entries, code_type, LENS_INFERRED_PRIMEABLE, row.get("code_type"),
                   row.get("code"),
                   {"supporting_bothsider_firm_ct": row.get("supporting_bothsider_firm_ct")})


# ── Stage 2 (in-process): primes that sub out into the target's codes ─────────
def _match_primes_cached(lens_entries: list[dict[str, Any]],
                         caches: SuboutCaches) -> dict[str, list[dict[str, Any]]]:
    """Dict lookups against the in-process cube marginal → matched cells grouped by
    prime, capped at PRIME_PROBE_CAP primes ranked by matched sub-out $. A probe code
    with no type claim (caller_declared without code_type) probes both systems."""
    probe_keys: set[tuple[str | None, str]] = set()
    for entry in lens_entries:
        if entry["code_type"] is not None:
            probe_keys.add((entry["code_type"], entry["code"]))
        else:
            for ct in CODE_TYPES:
                probe_keys.add((ct, entry["code"]))

    by_prime: dict[str, list[dict[str, Any]]] = {}
    for key in sorted(probe_keys, key=lambda k: (k[0] or "", k[1])):
        block = caches.cube.get(key)
        if block is None:
            continue
        primes, amts, edges, distincts, lasts = block
        for prime, amt, edge_ct, distinct, last in zip(primes, amts, edges, distincts, lasts):
            by_prime.setdefault(prime, []).append(
                {"recipient_code_type": key[0], "recipient_code": key[1],
                 "subaward_amt_total": amt, "subaward_edge_ct": edge_ct,
                 "distinct_recipient_ct": distinct, "last_subaward_action_date": last})
    if len(by_prime) > PRIME_PROBE_CAP:
        ranked = sorted(
            by_prime,
            key=lambda p: (-sum(float(c.get("subaward_amt_total") or 0.0)
                                for c in by_prime[p]), p))
        by_prime = {p: by_prime[p] for p in ranked[:PRIME_PROBE_CAP]}
    return by_prime


# ── Stage 3 (in-process): those primes' OPEN awards ────────────────────────────
def _is_open(row: dict[str, Any], today: dt_date) -> bool:
    """Open-date re-check at request time (the cache may be hours old within TTL):
    keep when either end date is >= today; a row with NEITHER date trusts the
    builder's open-at-as_of guarantee."""
    pop_end = row.get("period_of_performance_current_end_date")
    ord_end = row.get("ordering_period_end_date")
    if pop_end is None and ord_end is None:
        return True
    for d in (pop_end, ord_end):
        if isinstance(d, dt_date) and d >= today:
            return True
    return False


def _open_awards_for(primes: list[str], caches: SuboutCaches,
                     today: dt_date) -> list[dict[str, Any]]:
    """In-process index hits: awards_by_prime rows for the matched primes, open-date
    re-checked, bounded at AWARD_SCAN_CAP."""
    out: list[dict[str, Any]] = []
    for prime in sorted(primes):
        for idx in caches.awards_by_prime.get(prime, []):
            row = caches.open_awards[idx]
            if _is_open(row, today):
                out.append(row)
                if len(out) >= AWARD_SCAN_CAP:
                    return out
    return out


# ── Stage 4 (in-process): the target's HQ off the boot geo cache ───────────────
def _target_hq(uei: str, caches: SuboutCaches) -> tuple[float, float] | None:
    """uei → (lat, lon) off the in-process gtm_entity_geo arrow table (None when
    the entity has no coordinates — no fake centroids). The former per-request
    remote point-lookup measured 0.1-1.0s live; this is a binary search."""
    for row in _rows_for_uei(caches.geo_table, uei):
        lat, lon = row.get("latitude"), row.get("longitude")
        if lat is not None and lon is not None:
            return float(lat), float(lon)
    return None


# ── Stage 5 (in-process): score — components EXPLICIT on the wire ──────────────
def _matched_evidence(cells: list[dict[str, Any]],
                      entries_by_code: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """The (lens, code) hits connecting the target to one prime, each carrying BOTH
    sides of the evidence: how the TARGET knows the code (lens evidence) and how the
    PRIME's sub-out history hits it (the cube marginal measures — Σ across the
    aggregated source/context dims). Deduped on (lens, code), keeping the largest-$
    cell."""
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for cell in cells:
        code = cell.get("recipient_code")
        for entry in entries_by_code.get(code, []):
            if (entry["code_type"] is not None
                    and cell.get("recipient_code_type") is not None
                    and entry["code_type"] != cell.get("recipient_code_type")):
                continue
            key = (entry["lens"], code)
            matched = {
                "lens": entry["lens"],
                "code": code,
                "evidence": {
                    **entry["evidence"],
                    "recipient_code_type": cell.get("recipient_code_type"),
                    "subaward_edge_ct": cell.get("subaward_edge_ct"),
                    "subaward_amt_total": cell.get("subaward_amt_total"),
                    "distinct_recipient_ct": cell.get("distinct_recipient_ct"),
                    "last_subaward_action_date": _map_jsonable(
                        cell.get("last_subaward_action_date")),
                },
            }
            prior = best.get(key)
            if (prior is None or float(matched["evidence"].get("subaward_amt_total") or 0.0)
                    > float(prior["evidence"].get("subaward_amt_total") or 0.0)):
                best[key] = matched
    return sorted(best.values(), key=lambda m: (m["lens"], m["code"]))


def _components(award: dict[str, Any], cells: list[dict[str, Any]],
                matched_strength: float, distance_mi: float | None,
                pop_geo_precision: Any, today: dt_date,
                nearest_site_distance_mi: float | None) -> list[dict[str, Any]]:
    """The seven scored components for one award: (name, raw_value, weight,
    contribution) each — contribution = weight × normalized raw signal.
    Deterministic and pure."""
    subout_total = sum(float(c.get("subaward_amt_total") or 0.0) for c in cells)

    sub_ct = award.get("subaward_count") or 0
    sub_amt = float(award.get("total_subaward_amount") or 0.0)
    already_subbing = bool(sub_ct > 0 or sub_amt > 0)

    plan_code = award.get("subcontracting_plan_code")

    ends = [d for d in (award.get("period_of_performance_current_end_date"),
                        award.get("ordering_period_end_date"))
            if isinstance(d, dt_date) and d >= today]
    days_to_end = min((d - today).days for d in ends) if ends else None

    norms = {
        "prime_subout_history": (subout_total, _log_dollar_norm(subout_total)),
        "award_already_subbing": (already_subbing, 1.0 if already_subbing else 0.0),
        "subcontracting_plan": (plan_code, _plan_norm(plan_code)),
        "lens_strength": (round(matched_strength, 6), matched_strength),
        "proximity": (distance_mi, _proximity_norm(distance_mi, pop_geo_precision)),
        "expiring_window": (days_to_end, _expiring_norm(days_to_end)),
        "federal_site_proximity": (nearest_site_distance_mi,
                                   _federal_site_norm(nearest_site_distance_mi)),
    }
    return [
        {"name": name, "raw_value": raw, "weight": weight,
         "contribution": round(weight * norms[name][1], 6)}
        for name, weight in COMPONENT_WEIGHTS.items()
        for raw in (norms[name][0],)
    ]


# ── Stage 6 (remote, OPT-IN): peers ────────────────────────────────────────────
def _peers(cells_by_prime: dict[str, list[dict[str, Any]]], target_uei: str,
           notes: list[str], caches: SuboutCaches) -> list[dict[str, Any]]:
    """Recipients sharing the target's top matched codes — the "companies like yours"
    the primes already sub to. Top codes = the matched cube cells heaviest in
    distinct_recipient_ct; peer UEIs stream off gtm_subaward_recipient_code_evidence
    (BTREE code), deduped, target excluded, capped at PEER_CAP. Each peer is then
    hydrated map-ready: legal_business_name via ONE chunked BTREE IN point-scan on
    gtm_sam_entities (≤PEER_CAP ueis — invisible next to the evidence scan), HQ
    coordinates off the boot geo cache (binary search, no I/O; null when ungeocoded —
    never a fake centroid). The one remote non-point query in the recipe (~14.5s
    measured live) — opt-in via include_peers. Defensive: an unreachable evidence
    table degrades to zero peers with a note; a failed name scan degrades to null
    names with a note."""
    all_cells = [c for cells in cells_by_prime.values() for c in cells]
    if not all_cells:
        return []
    ranked = sorted(all_cells,
                    key=lambda c: (-(c.get("distinct_recipient_ct") or 0),
                                   c.get("recipient_code") or ""))
    top_codes: list[str] = []
    for cell in ranked:
        code = cell.get("recipient_code")
        if code and code not in top_codes:
            top_codes.append(code)
        if len(top_codes) >= PEER_CODE_PROBE:
            break
    if not top_codes:
        return []
    try:
        rows = _stream_rows(config.GTM_SUBAWARD_RECIPIENT_CODE_EVIDENCE_URI,
                            ["subawardee_uei", "code"],
                            _in_predicate("code", top_codes), PEER_EVIDENCE_ROW_SCAN)
    except Exception as exc:  # noqa: BLE001 — evidence table may not be materialized yet
        log.warning("peer evidence table unreachable (%s): serving zero peers", exc)
        notes.append("gtm_subaward_recipient_code_evidence unreachable — no peers served")
        return []
    peers: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        peer = row.get("subawardee_uei")
        if not peer or peer == target_uei or peer in seen:
            continue
        seen.add(peer)
        peers.append({"uei": peer, "shared_code": row.get("code")})
        if len(peers) >= PEER_CAP:
            break
    if peers:
        names: dict[str, Any] = {}
        try:
            name_rows = _scan_to_pylist(
                config.GTM_SAM_ENTITIES_URI, ["uei", "legal_business_name"],
                _in_predicate("uei", [p["uei"] for p in peers]))
            names = {r["uei"]: r.get("legal_business_name") for r in name_rows}
        except Exception as exc:  # noqa: BLE001 — hydration is enrichment, never fatal
            log.warning("peer name hydration failed (%s): peers carry null names", exc)
            notes.append("peer name hydration unavailable — peers carry uei only")
        for p in peers:
            p["legal_business_name"] = names.get(p["uei"])
            geo = _target_hq(p["uei"], caches)
            p["latitude"] = geo[0] if geo else None
            p["longitude"] = geo[1] if geo else None
    return peers


# ── The recipe executor ────────────────────────────────────────────────────────
def execute_subout_opportunities(body: Any, today: "dt_date | None" = None) -> dict[str, Any]:
    """The full plan (module docstring). Returns the wire envelope
    ``{meta: {recipeId, registryVersion, componentWeights, cache_state,
    cache_build_ms, timings_ms, total, ...}, data: {opportunities, peers}}``. Raises
    ``MapCompileError`` (→ 422) on any off-contract body; a UEI with no code signals
    is a 200 with empty data + meta.reason; an unavailable cache degrades to an empty
    answer with a note (the next request retries the build)."""
    req = validate_request(body)
    today = today or dt_date.today()
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
            "componentWeights": dict(COMPONENT_WEIGHTS),
            "uei": req["uei"],
            "lenses": req["lenses"],
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

    if req["peers_defaulted"]:
        notes.append("peers omitted by default — pass include_peers: true to fetch "
                     "(a remote indexed query; adds latency)")

    # 1. the in-process caches FIRST (the lens probes read them). A failed build is
    # NEVER silent: cache_state='failed' + the error string ride the wire on every
    # affected response (the 'silent unavailable' incident fix).
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
        return {"meta": _meta(0), "data": {"opportunities": [], "peers": []}}
    t = _mark("cache_ensure", t)

    # 2. target HQ off the boot geo cache (binary search — no I/O). A fact about the
    # TARGET, not about the matches: computed before any matching so the empty-market
    # and no-signal answers still carry the map's anchor pin in meta.target_hq.
    hq = _target_hq(req["uei"], caches)
    if hq is not None:
        target_hq = {"latitude": hq[0], "longitude": hq[1]}
    t = _mark("geo", t)

    # 2a. cached lens probes (lanes/SAM dict hits + caller_declared — no I/O)
    entries: dict[tuple[str, str | None, str], dict[str, Any]] = {}
    _probe_cached(req["uei"], req["lenses"], req["code_type"],
                  req["codes_override"], caches, entries)
    t = _mark("probe_cached", t)
    # 2b. inferred lens probe — the ONE remote hop on the hot path (handle warmed
    # at build; the wire now shows exactly what the remote floor costs)
    _probe_inferred(req["uei"], req["lenses"], req["code_type"], notes, entries)
    t = _mark("probe_inferred", t)
    lens_entries = list(entries.values())
    if not lens_entries:
        return {"meta": _meta(0, reason="uei has no code signals"),
                "data": {"opportunities": [], "peers": []}}

    entries_by_code: dict[str, list[dict[str, Any]]] = {}
    for e in lens_entries:
        entries_by_code.setdefault(e["code"], []).append(e)

    # 3. primes whose sub-out history hits those codes (in-process dict lookups)
    cells_by_prime = _match_primes_cached(lens_entries, caches)
    t = _mark("cube_match", t)

    # 4. their OPEN awards (in-process index hits, open-date re-checked)
    awards = _open_awards_for(list(cells_by_prime), caches, today) if cells_by_prime else []
    t = _mark("open_awards", t)

    # 5. score — every component explicit on the wire (award PoP geo rides each
    # cached row; the target HQ was resolved up front, after the cache ensure).
    # The display-only ``matched`` evidence list is built AFTER the sort for the
    # returned rows only (it is the
    # single per-award cost that scales with the matched-cell fan-out; scoring needs
    # just the aggregate strength + $ sum). Nearest-site searches memoize on the
    # exact centroid — zip5 centroids repeat heavily across awards (measured: the
    # unmemoized per-award ring search alone cost ~400ms at 2,000 awards).
    site_memo: dict[tuple[float, float], dict[str, Any] | None] = {}
    opportunities: list[dict[str, Any]] = []
    for award in awards:
        prime = award.get("recipient_uei")
        cells = cells_by_prime.get(prime, [])
        matched_codes = {c.get("recipient_code") for c in cells}
        matched_strength = max(
            (e["strength"] for code in matched_codes for e in entries_by_code.get(code, [])),
            default=0.0)
        distance_mi = None
        if (hq is not None and award.get("latitude") is not None
                and award.get("longitude") is not None):
            distance_mi = round(_haversine_mi(hq[0], hq[1], float(award["latitude"]),
                                              float(award["longitude"])), 1)
        # v2: nearest federal site to the award's PoP centroid — zip5-precision dots
        # only (a county centroid would fabricate proximity to sites it isn't near)
        nearest_site = None
        if (award.get("geo_precision") == "zip5"
                and award.get("latitude") is not None
                and award.get("longitude") is not None):
            pt = (float(award["latitude"]), float(award["longitude"]))
            if pt in site_memo:
                nearest_site = site_memo[pt]
            else:
                nearest_site = _nearest_federal_site(pt[0], pt[1], caches)
                site_memo[pt] = nearest_site
        components = _components(award, cells, matched_strength, distance_mi,
                                 award.get("geo_precision"), today,
                                 nearest_site["distance_mi"] if nearest_site else None)
        score = round(sum(c["contribution"] for c in components), 6)
        opportunities.append({
            "generated_unique_award_id": award.get("generated_unique_award_id"),
            "award_id_piid": award.get("award_id_piid"),
            "prime_uei": prime,
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
            # the award's PoP centroid — the map primitive (distance_mi is derived);
            # null when the award has no zip5 geocode, honest per pop_geo_precision
            "latitude": award.get("latitude"),
            "longitude": award.get("longitude"),
            "distance_mi": distance_mi,
            "nearest_federal_site": nearest_site,
            "matched": cells,               # placeholder — swapped for evidence below
            "score": score,
            "components": components,
        })
    opportunities.sort(key=lambda o: (-o["score"], o["generated_unique_award_id"] or ""))
    total = len(opportunities)
    opportunities = opportunities[:req["limit"]]
    for opp in opportunities:               # evidence for the RETURNED rows only
        opp["matched"] = _matched_evidence(opp["matched"], entries_by_code)
    t = _mark("score", t)

    # 7. peers (remote, opt-in)
    peers: list[dict[str, Any]] = []
    if req["include_peers"]:
        peers = _peers(cells_by_prime, req["uei"], notes, caches)
        _mark("peers", t)

    return {"meta": _meta(total), "data": {"opportunities": opportunities, "peers": peers}}
