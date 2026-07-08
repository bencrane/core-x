"""sub_universe_pairs — the v3 pair-grain precompute replacing the per-UEI blob.

build_target(uei) -> {"pairs": [...], "target": {...}}

THE ARCHITECTURE (freeze-doc §0, operator-ratified 2026-07-08). The per-UEI blob
is dead: it denormalized shared node-grain facts (award-state, demand events,
entity, win portfolio) into every overlapping target's payload (v1 hit 136 MB;
multi-TB at fleet scale), and the two-tier rescue degraded exact-day time windows
to monthly buckets. Replacement: relational pair-grain precompute over two Lance
datasets, served later by the standard executor pattern (NOT this module's scope).

  • gtm_sub_universe_pairs   — 1 row / (target_uei × node_uei): ONLY pair-specific
    scalars — facts that depend on the (target, node) pair and cannot be read from
    a node-grain mart. BTREE target_uei AND node_uei.
  • gtm_sub_universe_targets — 1 row / target: target_analytics JSON (pre-call
    brief Acts 1–3) + target scalars. BTREE uei.

NODE-GRAIN FACTS ARE NOT STORED PER PAIR. Award-state, demand events (raw,
EXACT-DAY), entity enrichment, and win portfolio serve at QUERY TIME from the
already-indexed node-grain marts (gtm_prime_demand_events / gtm_sam_entities /
usaspending_fpds_prime_award_state / gtm_prime_combo_lanes — all BTREE uei). They
live once, node-grain, never copied per overlapping target. This restores
exact-day time windows (the raw rows are read at query time). The one measured
cost that killed the blob batch — an indexed 16K-row award-state pull at 11.8s —
is precisely what moves to query time here, paid once per node, never precomputed
per target. The ONE per-node hydration retained inline is HQ geo (gtm_entity_geo,
chunked) — its build cost is measured and returned in target["timings_ms"].

UNIVERSE (freeze §0.1.2, v2): the sub_universe.v3 universe in build_mode — the FULL
membership set, no serving page/quota (MAX_LIMIT is serving-only). Every member of
the lookalike-winner universe gets exactly one pair row. The only build truncation
is the store's BUILD_NODE_CAP mega-guard (reseller-class), always disclosed via
nodes_truncated. build_mode also skips the node-grain hydration the pair writer
never persists (demand events / vehicles / gate_facts / page geo) — the measured
fix for the 627–820s laptop builds.

FAMILY ROLLUPS (freeze §0.1.3, v2): family_key = NAICS[:4] × (PSC[0] alpha | PSC[:2]
FSC group). Pair rows carry family_matched_obl_60mo + family_tcf_farmout_60mo as
compact JSON dicts (family → $, desc). family_tcf sums DISCLOSED lanes at the
target's combos only — a family with no disclosed lane is ABSENT (null ≠ zero);
negative farm-out passes through unclamped. The targets row carries the target's
demonstrated_families (title, n_edges, total_usd, share_pct, member combos) and
its county-grain footprint (scopes.pop_counties) — input 1/2 baked defaults.

NULL SEMANTICS (unknown ≠ zero, non-negotiable — inherited from sub_universe.v3):
  • undisclosed node: matched_farmout_60mo is null (not 0).
  • no farm-out evidence at ANY of the target's demonstrated combos:
    tcf_farmout_60mo / tcf_n_combos null (Definition C = no evidence, not "doesn't
    buy"). Definition C keys farm-out by the TARGET's combos — a node matched via
    anchor combo X can carry tcf at target combo Y outside the anchor portfolio.
  • no gtm_prime_sub_pairs rows: all three teaming scalars null (absent pair
    history is an absent fact, not zero partners).
  • band_fit: node_median_chunk_60mo null when no matched target-combo farm-out
    lane discloses a median; band_overlap null when either the node median or the
    target band is absent (true/false only when both present).

The target_analytics computation (pool / peers / percentiles / lane trends,
Acts 1–3) is REUSED verbatim from the superseded blob-era sub_universe_full — that
logic is sound; only the storage substrate changed.

WRITE PATH: rebuild-per-target, monotonic grow. lance delete `target_uei = 'X'`
(pairs) + the target row (targets), then append; datasets created with BTREEs if
absent. Operator-triggered ONLY (no cron, no schedule). See build_sub_universe_target.py.
"""
from __future__ import annotations

import json
import logging
import math
import statistics
import time
from datetime import date as dt_date, timedelta
from typing import Any

from . import config
from . import sub_universe_store as S
from .psc_families import family_key, psc_family_name

log = logging.getLogger("catalyst.sub_universe_pairs")

PAIRS_RECIPE_ID = "sub_universe_pairs.v2"   # v2: uncapped build_mode + family rollups

MATCHED_VIA_JSON_CAP = 5          # top-5 matched combos in the compact json string
POOL_NAMED_PRIMES_CAP = 100
PEERS_NAMED_CAP = 100
TREND_MIN_N = 5                   # reuse of the MVS_MIN_N doctrine
DEAL_BAND_LO, DEAL_BAND_HI = 0.20, 0.80    # pinned p20–p80
PEER_MIN_SHARED_LANES = 3
# deal-fit histogram: 11 log-spaced interior edges $25K → $2M ⇒ 12 buckets. Edges
# ride IN the target_analytics payload.
DEAL_FIT_BIN_EDGES = [round(25_000 * (2_000_000 / 25_000) ** (i / 10), 2)
                      for i in range(11)]

_CHUNK = 400
_GEO_CHUNK = 1000   # build-path bulk geo: a 30K-node universe in ~30 round trips
                    # (the store's 100-per-chunk serving size measured 527s at 25K
                    # nodes — 89% of the whole build; RTT dominates, not payload)


# ── scan seams (reuse the store's + blob-era build-path seams) ────────────────
def _scan(uri: str, columns: list[str] | None, predicate: str | None) -> list[dict[str, Any]]:
    return S._scan(uri, columns, predicate)


def _scan_sam_entities(ueis: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i in range(0, len(ueis), _CHUNK):
        pred = "uei IN (" + ",".join(f"'{u}'" for u in ueis[i:i + _CHUNK]) + ")"
        out += _scan(config.GTM_SAM_ENTITIES_URI,
                     ["uei", "cage_code", "legal_business_name", "sam_is_active",
                      "in_dsbs", "business_types", "primary_naics", "naics_codes",
                      "psc_codes", "physical_city", "physical_state", "physical_zip"],
                     pred)
    return out


def _scan_award_rows_by_piid(piids: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    piids = [p for p in piids if p]
    for i in range(0, len(piids), _CHUNK):
        plist = ",".join(f"'{p}'" for p in piids[i:i + _CHUNK])
        out += _scan(config.FPDS_PRIME_AWARD_STATE_URI,
                     ["award_id_piid", "award_topology", "type_of_set_aside_code",
                      "current_end_date", "potential_end_date", "idv_type_code"],
                     f"award_id_piid IN ({plist})")
    return out


def _scan_pool(lanes: list[tuple[str, str]], states: list[str], w24: str) -> list[dict[str, Any]]:
    """The Act-2 pool: sub-out placed in the target's lanes ∩ states, trailing 24mo.

    State membership is enforced in Python (null-safe trim), not in the pushdown:
    Lance's scan planner rejects TRIM/COALESCE and a bare IN would silently drop
    whitespace-padded state codes (null ≠ zero). Only the lane + date predicate —
    both Lance-supported and selective — is pushed down. Verbatim from the sound
    blob-era build path."""
    if not lanes or not states:
        return []
    states_set = set(states)
    out: list[dict[str, Any]] = []
    for i in range(0, len(lanes), 50):
        combo_pred = " OR ".join(
            f"(prime_award_naics_code = '{n}' AND prime_award_product_or_service_code = '{p}')"
            for n, p in lanes[i:i + 50])
        rows = _scan(config.CONTRACT_SUBAWARD_URI,
                     ["prime_awardee_uei", "prime_awardee_name", "subawardee_uei",
                      "subaward_amount", "subaward_action_date", "prime_award_piid",
                      "prime_award_naics_code", "prime_award_product_or_service_code",
                      "subaward_primary_place_of_performance_state_code"],
                     f"({combo_pred}) AND subaward_action_date >= DATE '{w24}'")
        out += [r for r in rows
                if (r.get("subaward_primary_place_of_performance_state_code") or "").strip()
                in states_set]
    return out


def _scan_sub_lanes_for_combos(lanes: list[tuple[str, str]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i in range(0, len(lanes), 50):
        combo_pred = " OR ".join(f"(naics_code = '{n}' AND psc_code = '{p}')"
                                 for n, p in lanes[i:i + 50])
        out += _scan(config.GTM_SUB_COMBO_LANES_URI,
                     ["uei", "naics_code", "psc_code"], combo_pred)
    return out


def _scan_sub_profiles(ueis: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i in range(0, len(ueis), _CHUNK):
        pred = "uei IN (" + ",".join(f"'{u}'" for u in ueis[i:i + _CHUNK]) + ")"
        out += _scan(config.GTM_SUB_PROFILES_URI, None, pred)
    return out


def _scan_naics_reference() -> list[dict[str, Any]]:
    return _scan(config.NAICS_REFERENCE_URI, ["naics_code", "naics_title"], None)


def _scan_geo_bulk(ueis: list[str]) -> list[dict[str, Any]]:
    """Build-path geo hydration at bulk chunk size (vs the store's serving-page
    100/chunk — measured 527s of a 591s build at 25K nodes)."""
    out: list[dict[str, Any]] = []
    for i in range(0, len(ueis), _GEO_CHUNK):
        pred = "uei IN (" + ",".join(f"'{u}'" for u in ueis[i:i + _GEO_CHUNK]) + ")"
        out += _scan(config.GTM_ENTITY_GEO_URI,
                     ["uei", "latitude", "longitude", "geo_precision"], pred)
    return out


_naics4_titles: dict[str, str] | None = None


def _naics4_title_map() -> dict[str, str]:
    """naics4 → title, cached per process. Titles are display sugar — an
    unreachable reference degrades to the bare naics4 code, never fails a build."""
    global _naics4_titles
    if _naics4_titles is None:
        try:
            rows = _scan_naics_reference()
        except Exception:
            log.warning("naics_reference unavailable — family titles fall back to codes")
            rows = []
        _naics4_titles = {str(r["naics_code"]): r["naics_title"] for r in rows
                          if r.get("naics_code") and len(str(r["naics_code"])) == 4}
    return _naics4_titles


# ── small helpers ──────────────────────────────────────────────────────────────
def _f(v: Any) -> float:
    return S._f(v)


def _d(v: Any) -> str | None:
    return S._d(v)


def _pct(part: float, whole: float) -> float | None:
    return round(100.0 * part / whole, 1) if whole else None


def _quantile(vals: list[float], q: float) -> float | None:
    if not vals:
        return None
    vs = sorted(vals)
    idx = q * (len(vs) - 1)
    lo, hi = int(math.floor(idx)), int(math.ceil(idx))
    if lo == hi:
        return vs[lo]
    return vs[lo] + (vs[hi] - vs[lo]) * (idx - lo)


def _percentile_rank(value: float, population: list[float]) -> int | None:
    if not population:
        return None
    below = sum(1 for v in population if v < value)
    return round(100 * below / len(population))


def _hist(vals: list[float]) -> list[int]:
    buckets = [0] * (len(DEAL_FIT_BIN_EDGES) + 1)
    for v in vals:
        i = 0
        while i < len(DEAL_FIT_BIN_EDGES) and v >= DEAL_FIT_BIN_EDGES[i]:
            i += 1
        buckets[i] += 1
    return buckets


# ── the builder ────────────────────────────────────────────────────────────────
def build_target(uei: str) -> dict[str, Any]:
    """Compute the full pair-grain universe + target_analytics for one target.

    Returns {"pairs": [pair_row, ...], "target": target_row}. The store's paging
    is stripped: the pair mart is TOTAL — one row per node of the full lookalike-
    winner universe. Node-grain facts are NOT hydrated here; they serve at query
    time from the indexed node-grain marts. The ONE inline per-node hydration is
    HQ geo (measured)."""
    today = dt_date.today()
    today_iso = today.isoformat()
    w24 = (today - timedelta(days=730)).isoformat()
    timings: dict[str, int] = {}
    t0 = time.monotonic()

    # 1. FULL universe via the shipped recipe in build_mode (freeze §0.1.2): total
    #    membership, no serving page/quota, no node-grain hydration the pair
    #    writer does not persist. The only truncation is the store's
    #    BUILD_NODE_CAP mega-guard, disclosed via meta.capped → nodes_truncated.
    base = S.execute_sub_universe({"uei": uei}, build_mode=True)
    target = base["target"]
    demonstrated = target["demonstrated_combos"]
    lanes = [(c["naics_code"], c["psc_code"]) for c in demonstrated]
    lane_set = set(lanes)
    states = [s["state"] for s in target["pop_states"]]
    anchor_ueis = {a["prime_uei"] for a in target["anchors"]}
    nodes = base["data"]
    universe_truncated = base["meta"]["capped"]
    timings["base_ms"] = int((time.monotonic() - t0) * 1000)

    # the target's own deal band (p20–p80 + median), the band_fit comparator.
    edges = S._scan_target_edges(uei)
    amts = [_f(r["subaward_amount"]) for r in edges]
    deal_band = {"low": _quantile(amts, DEAL_BAND_LO), "high": _quantile(amts, DEAL_BAND_HI),
                 "median": statistics.median(amts) if amts else None}

    # input 1's baked default at county grain (freeze §0.1; addendum §4.2): the
    # target's evidenced sub-side footprint. Rows without a county fact are
    # skipped (null ≠ zero), state-grain totals still ride in pop_states.
    county_agg: dict[tuple, dict[str, Any]] = {}
    for r in edges:
        st = (r.get("subaward_primary_place_of_performance_state_code") or "").strip()
        cc = (r.get("sub_place_of_perform_county_code") or "").strip()
        cn = (r.get("sub_place_of_perform_county_name") or "").strip()
        if not st or not (cc or cn):
            continue
        e = county_agg.setdefault((st, cc or cn.upper()), {
            "state": st, "county_code": cc or None, "county_name": cn or None,
            "sub_usd": 0.0, "n_edges": 0})
        e["sub_usd"] += _f(r["subaward_amount"])
        e["n_edges"] += 1
    pop_counties = sorted(({**c, "sub_usd": round(c["sub_usd"], 2)}
                           for c in county_agg.values()), key=lambda c: -c["sub_usd"])

    # 2. per-node HQ geo — the ONE inline per-node hydration retained (measured).
    #    Every other node-grain fact serves at query time from the indexed marts.
    t = time.monotonic()
    node_ueis = [n["uei"] for n in nodes]
    geo = {g["uei"]: g for g in _scan_geo_bulk(node_ueis)} if node_ueis else {}
    timings["geo_hydrate_ms"] = int((time.monotonic() - t) * 1000)
    timings["n_nodes"] = len(nodes)

    # 3. pair rows — pair-specific scalars ONLY.
    t = time.monotonic()
    pairs: list[dict[str, Any]] = []
    n_disclosed = 0
    n_undisclosed = 0
    for n in nodes:
        cu = n["uei"]
        disclosed = bool(n.get("disclosed_sub_buyer"))
        if disclosed:
            n_disclosed += 1
        else:
            n_undisclosed += 1

        # Definition C: farm-out keyed to the TARGET's demonstrated combos.
        tcf = n.get("target_combo_farmout")
        tcf_farmout_60mo = tcf["farmout_60mo"] if tcf else None
        tcf_n_combos = tcf["n_combos"] if tcf else None

        # band_fit: node median chunk across its matched TARGET-combo farm-out
        # lanes vs the target's p20–p80 band. node_median = median of the per-lane
        # 60mo medians disclosed at target combos; null when none disclose.
        node_medians = [c["median_chunk_60mo"] for c in (tcf["combos"] if tcf else [])
                        if c.get("median_chunk_60mo") is not None]
        node_median_chunk_60mo = (round(statistics.median(node_medians), 2)
                                  if node_medians else None)
        if node_median_chunk_60mo is None or deal_band["low"] is None or deal_band["high"] is None:
            band_overlap = None
        else:
            band_overlap = bool(deal_band["low"] <= node_median_chunk_60mo <= deal_band["high"])

        # family rollups (freeze §0.1.3): matched-obl over ALL matched combos;
        # family_tcf sums DISCLOSED lanes at the target's combos only — a family
        # with no disclosed lane is ABSENT (null ≠ zero); negatives unclamped.
        fam_obl: dict[str, float] = {}
        for mc in n.get("matched_combos") or []:
            fk = family_key(mc["naics_code"], mc["psc_code"])
            if fk:
                fam_obl[fk] = fam_obl.get(fk, 0.0) + _f(mc["prime_obl_60mo"])
        family_matched_obl_json = json.dumps(
            {k: round(v, 2) for k, v in sorted(fam_obl.items(), key=lambda kv: -kv[1])},
            separators=(",", ":"))
        family_tcf_json = None
        if tcf:
            fam_tcf: dict[str, float] = {}
            for c in tcf["combos"]:
                fk = family_key(c["naics_code"], c["psc_code"])
                if fk:
                    fam_tcf[fk] = fam_tcf.get(fk, 0.0) + _f(c["farmout_amt_60mo"])
            if fam_tcf:
                family_tcf_json = json.dumps(
                    {k: round(v, 2) for k, v in sorted(fam_tcf.items(), key=lambda kv: -kv[1])},
                    separators=(",", ":"))

        teaming = n.get("teaming") or {}
        mv = n.get("matched_via") or []
        matched_via_json = json.dumps([{
            "combo": m["combo"],
            "candidate_prime_obl_60mo": m["candidate_prime_obl_60mo"],
            "farmout_amt_60mo": m["farmout_amt_60mo"],
            "median_chunk_60mo": m["median_chunk_60mo"],
            "anchor_uei": m["anchor_uei"],
            "prime_backed": m["prime_backed"],
        } for m in mv[:MATCHED_VIA_JSON_CAP]], separators=(",", ":"))
        matched_via_truncated = bool(n.get("matched_via_truncated")
                                     or len(mv) > MATCHED_VIA_JSON_CAP)

        g = geo.get(cu)
        pairs.append({
            "target_uei": uei,
            "node_uei": cu,
            "node_name": n.get("name"),
            "disclosed_sub_buyer": disclosed,
            "matched_prime_obl_60mo": n.get("matched_prime_obl_60mo"),
            "matched_farmout_60mo": n.get("matched_farmout_60mo"),   # null when undisclosed
            "n_matched_combos": n.get("n_matched_combos"),
            # Definition C totals (null when no evidence at the target's combos)
            "tcf_farmout_60mo": tcf_farmout_60mo,
            "tcf_n_combos": tcf_n_combos,
            # family rollups (freeze §0.1.3) — compact JSON dicts, family → $
            "family_matched_obl_60mo": family_matched_obl_json,
            "family_tcf_farmout_60mo": family_tcf_json,
            # teaming (null when the node has no gtm_prime_sub_pairs rows)
            "teaming_n_sub_partners_5y": teaming.get("n_sub_partners_5y"),
            "teaming_deepest_repeat_edges_5y": teaming.get("deepest_repeat_edges_5y"),
            "teaming_n_partners_ge_3_edges": teaming.get("n_partners_ge_3_edges"),
            # HQ geo — the one inline per-node hydration
            "latitude": g["latitude"] if g else None,
            "longitude": g["longitude"] if g else None,
            "geo_precision": g["geo_precision"] if g else None,
            # band_fit vs the target's own p20–p80 band
            "node_median_chunk_60mo": node_median_chunk_60mo,
            "band_overlap": band_overlap,
            # compact matched-combo evidence (top-5) + truncation flag
            "matched_via_json": matched_via_json,
            "matched_via_truncated": matched_via_truncated,
            "as_of": today_iso,
            "recipe": PAIRS_RECIPE_ID,
        })
    timings["pairs_ms"] = int((time.monotonic() - t) * 1000)

    # 4. target_analytics — Acts 1–3, reused verbatim from the blob-era build path.
    analytics = _target_analytics(uei, today, w24, target, demonstrated, lanes,
                                  lane_set, states, anchor_ueis, edges, amts,
                                  deal_band, pop_counties, timings)

    # input 2's baked default: demonstrated capability FAMILIES (freeze §0.1.3) —
    # frequency + $ rollup of the demonstrated combos, titles inline, exact combos
    # retained underneath for drilldown.
    fam_rollup: dict[str, dict[str, Any]] = {}
    for c in demonstrated:
        fk = family_key(c["naics_code"], c["psc_code"])
        if not fk:
            continue
        e = fam_rollup.setdefault(fk, {"family": fk, "n_edges": 0,
                                       "total_usd": 0.0, "combos": []})
        e["n_edges"] += c["n_edges"]
        e["total_usd"] = round(e["total_usd"] + c["total_usd"], 2)
        e["combos"].append(c["combo"])
    n4 = _naics4_title_map()
    fam_total = sum(e["total_usd"] for e in fam_rollup.values())
    families = sorted(fam_rollup.values(), key=lambda e: -e["total_usd"])
    for e in families:
        naics4, fam = e["family"].split("x", 1)
        e["title"] = f"{n4.get(naics4, naics4)} × {psc_family_name(fam)}"
        e["share_pct"] = _pct(e["total_usd"], fam_total)

    target_row = {
        "uei": uei,
        "as_of": today_iso,
        "recipe": PAIRS_RECIPE_ID,
        "n_nodes": len(nodes),
        "n_disclosed": n_disclosed,
        "n_undisclosed": n_undisclosed,
        "nodes_truncated": universe_truncated,
        "demonstrated_families": json.dumps(families, separators=(",", ":")),
        "target_analytics": json.dumps(analytics, separators=(",", ":")),
        "timings_ms": json.dumps(timings, separators=(",", ":")),
    }
    return {"pairs": pairs, "target": target_row,
            "meta": {"timings_ms": timings, "n_nodes": len(nodes),
                     "n_disclosed": n_disclosed, "n_undisclosed": n_undisclosed,
                     "nodes_truncated": universe_truncated}}


# ── write path — rebuild-per-target, monotonic grow ──────────────────────────
# The two datasets GROW as targets are built. Rebuild-per-target semantics: delete
# this target's existing rows (pairs on target_uei, targets on uei), then append
# the fresh set. Datasets are created (with BTREEs) on first write when absent.
# Operator-triggered ONLY — no cron, no schedule (freeze-doc §0 doctrine).
#
# NOTE on indexing: unlike the snapshot-overwrite marts (which stage locally to
# dodge R2's InvalidPart on wide index page_data), these grow incrementally and
# must be written to R2 in place. BTREE index page_data only widens past ~30M rows;
# the per-target pair mart is far below that, so create_scalar_index against R2 is
# safe here. Indices are (re)created after each append so new rows are covered.
_PAIRS_SCHEMA_ORDER: list[str] = [
    "target_uei", "node_uei", "node_name", "disclosed_sub_buyer",
    "matched_prime_obl_60mo", "matched_farmout_60mo", "n_matched_combos",
    "tcf_farmout_60mo", "tcf_n_combos",
    "family_matched_obl_60mo", "family_tcf_farmout_60mo",
    "teaming_n_sub_partners_5y", "teaming_deepest_repeat_edges_5y",
    "teaming_n_partners_ge_3_edges",
    "latitude", "longitude", "geo_precision",
    "node_median_chunk_60mo", "band_overlap",
    "matched_via_json", "matched_via_truncated", "as_of", "recipe",
]


def _dataset_exists(uri: str, storage_options: dict) -> bool:
    import lance
    try:
        lance.dataset(uri, storage_options=storage_options)
        return True
    except Exception:
        return False


def write_target(result: dict[str, Any], *,
                 pairs_uri: str | None = None,
                 targets_uri: str | None = None,
                 storage_options: dict | None = None,
                 recreate: bool = False) -> dict[str, Any]:
    """Land one target's pair rows + target row via rebuild-per-target append.

    Deletes the target's prior rows from both datasets then appends the fresh set,
    creating each dataset (with BTREEs) if absent. On a recipe schema evolution
    (existing dataset columns ≠ this recipe's columns) the write REFUSES — pass
    recreate=True (script: --recreate, first target of the run) to overwrite both
    datasets at the new schema; prior targets rebuild on demand (rebuild-per-target
    is the write path, freeze §0). Returns {pairs_rows, targets_rows,
    pairs_version, targets_version}."""
    import lance
    import pyarrow as pa

    pairs_uri = pairs_uri or config.GTM_SUB_UNIVERSE_PAIRS_URI
    targets_uri = targets_uri or config.GTM_SUB_UNIVERSE_TARGETS_URI
    so = storage_options if storage_options is not None else config.r2_storage_options()

    uei = result["target"]["uei"]
    pairs = result["pairs"]

    # ── pairs dataset (target_uei × node_uei) ──
    pairs_tbl = pa.Table.from_pylist(
        [{k: p[k] for k in _PAIRS_SCHEMA_ORDER} for p in pairs]
    ) if pairs else pa.table({k: pa.array([], type=_empty_type(k)) for k in _PAIRS_SCHEMA_ORDER})
    if _dataset_exists(pairs_uri, so) and not recreate:
        pds = lance.dataset(pairs_uri, storage_options=so)
        existing = [f.name for f in pds.schema]
        if existing != list(pairs_tbl.schema.names):
            raise RuntimeError(
                f"pairs schema mismatch (dataset {existing} vs recipe "
                f"{list(pairs_tbl.schema.names)}) — recipe evolved; rerun with "
                "--recreate to rebuild the datasets at the new schema")
        pds.delete(f"target_uei = '{uei}'")
        if pairs:
            lance.write_dataset(pairs_tbl, pairs_uri, mode="append", storage_options=so)
        pds = lance.dataset(pairs_uri, storage_options=so)
    else:
        mode = "overwrite" if _dataset_exists(pairs_uri, so) else "create"
        lance.write_dataset(pairs_tbl, pairs_uri, mode=mode, storage_options=so)
        pds = lance.dataset(pairs_uri, storage_options=so)
    _ensure_indices(pds, [("target_uei", "BTREE"), ("node_uei", "BTREE")])
    pds = lance.dataset(pairs_uri, storage_options=so)

    # ── targets dataset (1 row / target uei) ──
    tgt_tbl = pa.Table.from_pylist([result["target"]])
    if _dataset_exists(targets_uri, so) and not recreate:
        tds = lance.dataset(targets_uri, storage_options=so)
        existing = [f.name for f in tds.schema]
        if existing != list(tgt_tbl.schema.names):
            raise RuntimeError(
                f"targets schema mismatch (dataset {existing} vs recipe "
                f"{list(tgt_tbl.schema.names)}) — recipe evolved; rerun with "
                "--recreate to rebuild the datasets at the new schema")
        tds.delete(f"uei = '{uei}'")
        lance.write_dataset(tgt_tbl, targets_uri, mode="append", storage_options=so)
        tds = lance.dataset(targets_uri, storage_options=so)
    else:
        mode = "overwrite" if _dataset_exists(targets_uri, so) else "create"
        lance.write_dataset(tgt_tbl, targets_uri, mode=mode, storage_options=so)
        tds = lance.dataset(targets_uri, storage_options=so)
    _ensure_indices(tds, [("uei", "BTREE")])
    tds = lance.dataset(targets_uri, storage_options=so)

    return {"pairs_rows": len(pairs),
            "targets_rows": 1,
            "pairs_total_rows": pds.count_rows(),
            "targets_total_rows": tds.count_rows(),
            "pairs_version": pds.version,
            "targets_version": tds.version}


def _empty_type(col: str):
    import pyarrow as pa
    floats = {"matched_prime_obl_60mo", "matched_farmout_60mo", "tcf_farmout_60mo",
              "latitude", "longitude", "node_median_chunk_60mo"}
    ints = {"n_matched_combos", "tcf_n_combos", "teaming_n_sub_partners_5y",
            "teaming_deepest_repeat_edges_5y", "teaming_n_partners_ge_3_edges"}
    bools = {"disclosed_sub_buyer", "band_overlap", "matched_via_truncated"}
    if col in floats:
        return pa.float64()
    if col in ints:
        return pa.int64()
    if col in bools:
        return pa.bool_()
    return pa.string()


def _ensure_indices(ds, indices: list[tuple[str, str]]) -> None:
    """Create scalar indices covering all rows. Recreated after each append so new
    rows are indexed (Lance replaces the index on the column when re-created)."""
    for col, kind in indices:
        try:
            ds.create_scalar_index(col, kind, replace=True)
        except TypeError:
            # older pylance without replace= — drop-then-create
            try:
                ds.drop_index(col)
            except Exception:
                pass
            ds.create_scalar_index(col, kind)


def _target_analytics(uei, today, w24, target, demonstrated, lanes, lane_set,
                      states, anchor_ueis, edges, amts, deal_band, pop_counties,
                      timings: dict[str, int]) -> dict[str, Any]:
    """The pre-call brief, Acts 1–3 — pool/peers/percentiles/lane trends. Reused
    verbatim from the (sound) blob-era sub_universe_full.build_blob; only the
    storage substrate changed. Named pool primes + named ranked peers + histogram
    bin edges + p25/p75 per percentile dimension all ride in the payload."""
    lifetime = round(sum(amts), 2)

    # Act 1 — the target's own record
    t = time.monotonic()
    comp: dict[str, dict] = {}
    for a in target["anchors"]:
        comp[a["prime_uei"]] = {"prime_uei": a["prime_uei"], "prime_name": a["prime_name"],
                                "dollars": a["sub_usd_lifetime"],
                                "pct": _pct(a["sub_usd_lifetime"], lifetime)}
    composition = sorted(comp.values(), key=lambda c: -c["dollars"])
    top_buyer_pct = composition[0]["pct"] if composition else None
    top_two_pct = (round(sum(c["pct"] for c in composition[:2]), 1)
                   if len(composition) >= 2 else top_buyer_pct)
    capabilities = [{**c, "share_pct": _pct(c["total_usd"], lifetime),
                     "is_lead": i == 0} for i, c in enumerate(demonstrated)]
    by_year: dict[int, float] = {}
    for r in edges:
        d = _d(r["subaward_action_date"])
        if d:
            y = int(d[:4])
            if today.year - 5 <= y <= today.year - 1:
                by_year[y] = by_year.get(y, 0.0) + _f(r["subaward_amount"])
    yrs = sorted(y for y, v in by_year.items() if v > 0)
    if len(yrs) >= 2 and by_year[yrs[0]] > 0:
        span = yrs[-1] - yrs[0]
        cagr = round(100.0 * ((by_year[yrs[-1]] / by_year[yrs[0]]) ** (1.0 / span) - 1), 1)
    else:
        cagr = None
    anchor_geo = S._scan_geo(sorted(anchor_ueis)) if anchor_ueis else []
    veh = target["vehicles"]
    vehicle_exposure = None
    if veh:
        top_v = veh[0]
        idv = _scan_award_rows_by_piid([top_v["parent_piid"]])
        idv_row = next((r for r in idv if r["award_topology"] == "vehicle"), idv[0] if idv else None)
        ceiling = None
        if idv_row is not None:
            end = idv_row["potential_end_date"] or idv_row["current_end_date"]
            ceiling = int(_d(end)[:4]) if end else None
        vehicle_exposure = {"parent_piid": top_v["parent_piid"], "dollars": top_v["sub_usd"],
                            "pct": _pct(top_v["sub_usd"], lifetime), "ceiling_year": ceiling}
    top_lane = capabilities[0] if capabilities else None
    lane_year: dict[tuple, dict[int, list[float]]] = {}
    for r in edges:
        k = (r["prime_award_naics_code"], r["prime_award_product_or_service_code"])
        d = _d(r["subaward_action_date"])
        if k in lane_set and d:
            lane_year.setdefault(k, {}).setdefault(int(d[:4]), []).append(_f(r["subaward_amount"]))
    lane_trends = []
    for k, years in lane_year.items():
        series = []
        for y in sorted(years):
            vals = years[y]
            series.append({"bucket": y, "n": len(vals), "total_usd": round(sum(vals), 2),
                           "median_chunk": (round(statistics.median(vals), 2)
                                            if len(vals) >= TREND_MIN_N else None)})
        meds = [(s["bucket"], s["median_chunk"]) for s in series if s["median_chunk"]]
        slope = None
        if len(meds) >= 2 and meds[0][1] and meds[-1][0] > meds[0][0]:
            slope = round(100.0 * ((meds[-1][1] / meds[0][1]) ** (1.0 / (meds[-1][0] - meds[0][0])) - 1), 1)
        lane_trends.append({"combo": f"{k[0]}x{k[1]}", "own_series": series,
                            "median_slope_pct_yr": slope,
                            "slope_basis": None if slope is not None else
                            f"fewer than 2 buckets with n>={TREND_MIN_N}"})
    timings["act1_ms"] = int((time.monotonic() - t) * 1000)

    # Act 2 — the pool (lanes ∩ states ∩ 24mo), named primes, placement, fit
    t = time.monotonic()
    pool_rows = _scan_pool(lanes, states, w24)
    pool_total = round(sum(_f(r["subaward_amount"]) for r in pool_rows), 2)
    by_prime: dict[str, dict] = {}
    for r in pool_rows:
        pu = r["prime_awardee_uei"]
        if not pu:
            continue
        e = by_prime.setdefault(pu, {"prime_uei": pu, "prime_name": r["prime_awardee_name"],
                                     "dollars": 0.0, "n_subawards": 0})
        e["dollars"] += _f(r["subaward_amount"])
        e["n_subawards"] += 1
    ranked_primes = sorted(by_prime.values(), key=lambda p: -p["dollars"])
    for p in ranked_primes:
        p["dollars"] = round(p["dollars"], 2)
        p["pct"] = _pct(p["dollars"], pool_total)
    frag = [{"bucket": "top_5", "prime_count": min(5, len(ranked_primes)),
             "dollars": round(sum(p["dollars"] for p in ranked_primes[:5]), 2)},
            {"bucket": "next_20", "prime_count": max(0, min(20, len(ranked_primes) - 5)),
             "dollars": round(sum(p["dollars"] for p in ranked_primes[5:25]), 2)},
            {"bucket": "rest", "prime_count": max(0, len(ranked_primes) - 25),
             "dollars": round(sum(p["dollars"] for p in ranked_primes[25:]), 2)}]
    for b in frag:
        b["pct"] = _pct(b["dollars"], pool_total)
    entity_capture = round(sum(_f(r["subaward_amount"]) for r in pool_rows
                               if r["subawardee_uei"] == uei), 2)
    pool_amts = [_f(r["subaward_amount"]) for r in pool_rows]
    within = [v for v in pool_amts
              if deal_band["low"] is not None and deal_band["high"] is not None
              and deal_band["low"] <= v <= deal_band["high"]]
    deal_fit = {"placed_median": (round(statistics.median(pool_amts), 2) if pool_amts else None),
                "entity_median": deal_band["median"],
                "within_band_pct": _pct(len(within), len(pool_amts)) if pool_amts else None,
                "distribution": _hist(pool_amts), "bin_edges": DEAL_FIT_BIN_EDGES}
    pool_piids = sorted({r["prime_award_piid"] for r in pool_rows if r["prime_award_piid"]})
    aw_tag = {r["award_id_piid"]: r for r in _scan_award_rows_by_piid(pool_piids)}
    piid_dollars: dict[str, float] = {}
    for r in pool_rows:
        p = r["prime_award_piid"]
        if p:
            piid_dollars[p] = piid_dollars.get(p, 0.0) + _f(r["subaward_amount"])
    tag_sa = sum(v for p, v in piid_dollars.items()
                 if (aw_tag.get(p) or {}).get("type_of_set_aside_code") not in (None, "NONE", "NO SET ASIDE USED."))
    tag_order = sum(v for p, v in piid_dollars.items()
                    if (aw_tag.get(p) or {}).get("award_topology") == "vehicle_order")
    placement = [
        {"code": "set_aside_present", "label": "Prime award carried a set-aside",
         "dollars": round(tag_sa, 2), "pct": _pct(tag_sa, pool_total)},
        {"code": "vehicle_order", "label": "Placed as orders on existing vehicles",
         "dollars": round(tag_order, 2), "pct": _pct(tag_order, pool_total)},
    ]
    unresolved = sum(v for p, v in piid_dollars.items() if p not in aw_tag)
    timings["act2_ms"] = int((time.monotonic() - t) * 1000)

    # Act 3 — the field: peers from gtm_sub_combo_lanes ∩ gtm_sub_profiles
    t = time.monotonic()
    lane_rows = _scan_sub_lanes_for_combos(lanes)
    shared: dict[str, int] = {}
    for r in lane_rows:
        if r["uei"] != uei:
            shared[r["uei"]] = shared.get(r["uei"], 0) + 1
    lane_peers = [u for u, k in shared.items() if k >= PEER_MIN_SHARED_LANES]
    profiles = {p["uei"]: p for p in _scan_sub_profiles(lane_peers)} if lane_peers else {}
    state_set = set(states)
    peers = []
    for u in lane_peers:
        p = profiles.get(u)
        if not p:
            continue
        pstates = set(p["pop_states"] or [])
        if not (pstates & state_set):
            continue
        plo, phi = p["p20_chunk_lifetime"], p["p80_chunk_lifetime"]
        if (plo is None or phi is None or deal_band["low"] is None
                or float(phi) < deal_band["low"] or float(plo) > deal_band["high"]):
            continue
        peers.append(p)
    peer_ueis = {p["uei"] for p in peers}
    peer_capture: dict[str, float] = {}
    for r in pool_rows:
        su = r["subawardee_uei"]
        if su in peer_ueis:
            peer_capture[su] = peer_capture.get(su, 0.0) + _f(r["subaward_amount"])
    set_capture = round(sum(peer_capture.values()), 2)
    named_peers = sorted(
        ({"uei": p["uei"], "shared_lanes": shared[p["uei"]],
          "capture_24mo": round(peer_capture.get(p["uei"], 0.0), 2),
          "median_chunk": (float(p["median_chunk_lifetime"])
                           if p["median_chunk_lifetime"] is not None else None)}
         for p in peers), key=lambda r: -r["capture_24mo"])
    percentiles = []
    for dim, mine, key, higher_better in (
            ("5-yr trajectory", cagr, "cagr_5y_pct", True),
            ("median action", deal_band["median"], "median_chunk_lifetime", True),
            ("prime buyers", len(composition), "n_distinct_primes_lifetime", True),
            ("lane breadth", len(demonstrated), "n_lanes_lifetime", True),
            ("top-buyer share", top_buyer_pct, "top_buyer_share_lifetime_pct", False)):
        popn = [float(p[key]) for p in peers if p.get(key) is not None]
        percentiles.append({
            "dimension": dim, "entity_value": mine,
            "peer_median": (round(statistics.median(popn), 1) if popn else None),
            "p25": (round(_quantile(popn, 0.25), 1) if popn else None),
            "p75": (round(_quantile(popn, 0.75), 1) if popn else None),
            "percentile": (_percentile_rank(float(mine), popn) if mine is not None and popn else None),
            "higher_is_better": higher_better})
    conv_sa = conv_open = 0.0
    for r in pool_rows:
        if r["subawardee_uei"] not in peer_ueis:
            continue
        amt = _f(r["subaward_amount"])
        tagrow = aw_tag.get(r["prime_award_piid"])
        sa = (tagrow or {}).get("type_of_set_aside_code")
        if sa not in (None, "NONE", "NO SET ASIDE USED."):
            conv_sa += amt
        else:
            conv_open += amt
    converting = [
        {"profile": "Set-aside prime award", "dollars": round(conv_sa, 2),
         "pct": _pct(conv_sa, set_capture)},
        {"profile": "Unrestricted prime award", "dollars": round(conv_open, 2),
         "pct": _pct(conv_open, set_capture)},
    ]
    timings["act3_ms"] = int((time.monotonic() - t) * 1000)

    # target identity from SAM (the pre-call header)
    tgt_sam = ({r["uei"]: r for r in _scan_sam_entities([uei])}).get(uei)
    return {
        "entity": {
            "uei": uei,
            "name": tgt_sam["legal_business_name"] if tgt_sam else None,
            "cage": tgt_sam["cage_code"] if tgt_sam else None,
            "city": tgt_sam["physical_city"] if tgt_sam else None,
            "state": tgt_sam["physical_state"] if tgt_sam else None,
            "sam_is_active": tgt_sam["sam_is_active"] if tgt_sam else None,
            "business_types": tgt_sam["business_types"] if tgt_sam else None,
            "primary_naics": tgt_sam["primary_naics"] if tgt_sam else None,
            "sub_dollars_lifetime": lifetime,
            "prime_buyer_count": len(composition),
            "trajectory_5yr_pct": cagr,
            "median_chunk": deal_band["median"],
        },
        "scopes": {"lanes": [{"naics": n_, "psc": p_} for n_, p_ in lanes],
                   "performance_states": states, "pop_counties": pop_counties,
                   "deal_band": deal_band, "window_months": 24},
        "current_performance": {
            "customer_composition": composition,
            "top_buyer_pct": top_buyer_pct, "top_two_pct": top_two_pct,
            "capabilities": capabilities,
            "geography": {"states": target["pop_states"],
                          "buyer_hq": [{"uei": g["uei"], "lat": g["latitude"],
                                        "lon": g["longitude"]} for g in anchor_geo]},
            "dependencies": {
                "top_buyer": composition[0] if composition else None,
                "top_lane": ({"lane": top_lane["combo"], "pct": top_lane["share_pct"]}
                             if top_lane else None),
                "vehicle_exposure": vehicle_exposure},
            "lane_trends": lane_trends,
        },
        "adjacent_market": {
            "pool": {"total_dollars": pool_total, "prime_count": len(ranked_primes),
                     "largest_single_share_pct": (ranked_primes[0]["pct"] if ranked_primes else None),
                     "fragmentation": frag, "entity_capture": entity_capture,
                     "named_primes": ranked_primes[:POOL_NAMED_PRIMES_CAP],
                     "named_primes_truncated": len(ranked_primes) > POOL_NAMED_PRIMES_CAP},
            "placement": placement,
            "placement_method": ("award-state tagging by pool award piid; shares "
                                 "non-mutually-exclusive; "
                                 f"${round(unresolved, 2)} unresolved piids"),
            "deal_fit": deal_fit,
        },
        "field": {
            "comparable_set": {
                "count": len(peers),
                "definition": (f">= {PEER_MIN_SHARED_LANES} shared lanes, >= 1 shared "
                               "performance state, deal-band overlap (p20-p80)"),
                "set_capture": set_capture,
                "median_peer_capture": (round(statistics.median(list(peer_capture.values())), 2)
                                        if peer_capture else None),
                "entity_capture": entity_capture,
                "pool_total": pool_total,
                "others_capture": round(pool_total - set_capture - entity_capture, 2),
                "named_peers": named_peers[:PEERS_NAMED_CAP],
                "named_peers_truncated": len(named_peers) > PEERS_NAMED_CAP},
            "percentiles": percentiles,
            "converting": converting,
        },
        "meta": {
            "base_recipe": S.RECIPE_ID,
            "sources": ["gtm_prime_sub_pairs", "gtm_prime_combo_lanes",
                        "gtm_prime_farmout_combo_lanes", "gtm_prime_vehicle_lanes",
                        "gtm_entity_geo", "usaspending_subaward_canonical",
                        "gtm_sam_entities", "usaspending_fpds_prime_award_state",
                        "gtm_sub_combo_lanes", "gtm_sub_profiles"],
            "doctrine": ("facts only, no scoring; unknown != zero — absent facts are "
                         "null with the basis disclosed"),
        },
    }
