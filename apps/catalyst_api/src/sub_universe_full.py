"""sub_universe_full — the per-UEI blob builder (build path, not the serving path).

build_blob(uei) -> blob. TWO-TIER (v2, Surface-1). ONE payload per target UEI: the
FULL universe node map (sub_universe.v3 facts, paging/quota stripped) +
target_analytics (pre-call brief Acts 1–3). Kept single-digit MB so the one
per-call load is sub-second.
  • MATERIAL nodes (disclosed sub-buyers, ranked, capped MATERIAL_CAP) carry full
    hot hydration: entity, award-state, matched_via (capped), and per-node MONTHLY
    event buckets — time / plan / set-aside predicates serve from these in-memory.
  • the undisclosed tail rides as lean STUBS: membership + ranking scalars + the
    base recipe's cheap demand summary (needs-subs-now / by-action-type / counts).
Raw event grain and win_portfolio are NOT stored — row-exact drilldown reads the
indexed marts (gtm_prime_demand_events / gtm_prime_combo_lanes) by uei point-lookup
(see sub_universe_serve). The batch runner writes the hot blob ->
gtm_sub_universe_blobs (BTREE uei) — the ONLY new dataset. Serving = one indexed
blob fetch + in-memory filter; drilldown = one indexed mart point-lookup.

BUILD-TIME SPINE ACCESS IS SANCTIONED HERE (the request-path prohibition does not
apply to the batch): the pool scan reads usaspending_subaward_canonical, the
award-state block reads usaspending_fpds_prime_award_state, placement/converting
tag via award-state rows. All request-time consumers only ever touch the blob.

v1 DEFERRALS (disclosed in meta.deferred):
  • node.pop (target-relative PoP distance) — lands with the (uei × pop_state)
    rollup mart; null until then.
  • placement/converting subcontracting-plan tagging uses award-state set-aside
    + demand-events plan codes where the node's events are in cache; shares are
    non-mutually-exclusive by design and the method is disclosed.

NULL DOCTRINE: unknown ≠ zero, everywhere. Trend buckets below TREND_MIN_N ride
null with the basis disclosed. Truncations always flagged, never silent.
"""
from __future__ import annotations

import logging
import math
import statistics
import time
from datetime import date as dt_date, timedelta
from typing import Any

from . import config
from . import sub_universe_store as S

log = logging.getLogger("catalyst.sub_universe_full")

BLOB_RECIPE_ID = "sub_universe_blob.v2"   # two-tier: hot blob + mart drilldown; bakes sub_universe.v3 node facts

EVENT_ROWS_PER_NODE_CAP = 500             # raw rows per node returned on drilldown (serve path cap)
WIN_PORTFOLIO_CAP = 50                    # portfolio entries per node returned on drilldown
# ── two-tier trim (Surface-1: single-digit-MB hot blob, sub-second load) ──────
MATERIAL_CAP = 1000                       # max fully-hydrated (disclosed) nodes per blob
                                          # (~9.4 KB/material node -> keeps the hot blob
                                          #  single-digit MB even for the widest universes;
                                          #  probe 933 material = 8.80 MB)
MATCHED_VIA_HOT_CAP = 5                   # matched_via combos kept per material node (hot)
POOL_NAMED_PRIMES_CAP = 100
PEERS_NAMED_CAP = 100
TREND_MIN_N = 5                            # reuse of the MVS_MIN_N doctrine
DEAL_BAND_LO, DEAL_BAND_HI = 0.20, 0.80    # pinned p20–p80
PEER_MIN_SHARED_LANES = 3
# deal-fit histogram: 11 log-spaced interior edges $25K → $2M ⇒ 12 buckets
# (< first edge, 10 interior, > last edge). Edges ride IN the payload.
DEAL_FIT_BIN_EDGES = [round(25_000 * (2_000_000 / 25_000) ** (i / 10), 2)
                      for i in range(11)]

_CHUNK = 400


# ── scan seams (monkeypatch targets; build-path only) ─────────────────────────
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


def _scan_award_state(ueis: list[str], today_iso: str) -> list[dict[str, Any]]:
    """Non-terminated awards with a live end date for the given recipients."""
    out: list[dict[str, Any]] = []
    for i in range(0, len(ueis), _CHUNK):
        ulist = ",".join(f"'{u}'" for u in ueis[i:i + _CHUNK])
        out += _scan(config.FPDS_PRIME_AWARD_STATE_URI,
                     ["recipient_uei", "award_id_piid", "award_topology",
                      "current_end_date", "potential_end_date", "days_to_expiry",
                      "type_of_set_aside_code", "awarding_agency_code"],
                     f"recipient_uei IN ({ulist}) AND is_terminated = false "
                     f"AND current_end_date >= DATE '{today_iso}'")
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


def _scan_demand_events_full(ueis: list[str]) -> list[dict[str, Any]]:
    """Event-grain rows (blob §2.3) — wider projection than the store's summary scan."""
    cols = ["uei", "award_key", "action_date", "obligation_delta", "naics_code",
            "psc_code", "action_type_code", "award_type_code", "subcontracting_plan",
            "type_of_set_aside_code", "extent_competed", "idv_type_code",
            "is_first_action", "has_disclosed_subs"]
    out: list[dict[str, Any]] = []
    for i in range(0, len(ueis), _CHUNK):
        pred = "uei IN (" + ",".join(f"'{u}'" for u in ueis[i:i + _CHUNK]) + ")"
        out += _scan(config.GTM_PRIME_DEMAND_EVENTS_URI, cols, pred)
    return out


def _scan_pool(lanes: list[tuple[str, str]], states: list[str], w24: str) -> list[dict[str, Any]]:
    """The Act-2 pool: sub-out placed in the target's lanes ∩ states, trailing 24mo.

    State membership is enforced in Python (null-safe trim), not in the pushdown:
    Lance's scan planner rejects TRIM/COALESCE, and a bare `IN` on the raw column
    would silently drop whitespace-padded state codes (null ≠ zero). Only the
    lane + date predicate — both Lance-supported and selective — is pushed down.
    """
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
    """gtm_sub_combo_lanes rows in the target's lanes — peer-candidate discovery."""
    out: list[dict[str, Any]] = []
    for i in range(0, len(lanes), 50):
        combo_pred = " OR ".join(f"(naics_code = '{n}' AND psc_code = '{p}')"
                                 for n, p in lanes[i:i + 50])
        out += _scan(f"{config.GTM_SUB_COMBO_LANES_URI}",
                     ["uei", "naics_code", "psc_code"], combo_pred)
    return out


def _scan_sub_profiles(ueis: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i in range(0, len(ueis), _CHUNK):
        pred = "uei IN (" + ",".join(f"'{u}'" for u in ueis[i:i + _CHUNK]) + ")"
        out += _scan(config.GTM_SUB_PROFILES_URI, None, pred)
    return out


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


def _month_buckets(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Per-node MONTHLY event buckets — every time / plan / set-aside /
    action-type predicate serves from these in-memory; the raw rows drill down
    from the gtm_prime_demand_events mart (BTREE uei). ~18x smaller than raw
    grain (measured). Aggregates the FULL restricted event set for the node."""
    cells: dict[str, Any] = {}
    for e in events:
        d = _d(e.get("action_date"))
        if not d:
            continue
        c = cells.get(d[:7])
        if c is None:
            c = cells[d[:7]] = {"n": 0, "obl": 0.0, "at": {}, "plan": {},
                                "sa": {}, "first": 0, "needs": 0}
        c["n"] += 1
        c["obl"] = round(c["obl"] + _f(e.get("obligation_delta")), 2)
        for axis, key in (("at", "action_type_code"),
                          ("plan", "subcontracting_plan"),
                          ("sa", "type_of_set_aside_code")):
            v = e.get(key)
            c[axis][v] = c[axis].get(v, 0) + 1
        if e.get("is_first_action"):
            c["first"] += 1
            if not e.get("has_disclosed_subs"):
                c["needs"] += 1
    return cells


# ── the builder ────────────────────────────────────────────────────────────────
def build_blob(uei: str) -> dict[str, Any]:
    today = dt_date.today()
    today_iso = today.isoformat()
    w24 = (today - timedelta(days=730)).isoformat()
    timings: dict[str, int] = {}
    t0 = time.monotonic()

    # 1. the full universe via the shipped recipe at MAX limit for node facts;
    #    the blob then re-hydrates EVERYTHING the page path only did per-page.
    base = S.execute_sub_universe({"uei": uei, "limit": S.MAX_LIMIT})
    target = base["target"]
    demonstrated = target["demonstrated_combos"]
    lanes = [(c["naics_code"], c["psc_code"]) for c in demonstrated]
    lane_set = set(lanes)
    states = [s["state"] for s in target["pop_states"]]
    anchor_ueis = {a["prime_uei"] for a in target["anchors"]}
    timings["base_ms"] = int((time.monotonic() - t0) * 1000)

    # full node set: the store caps at MAX_LIMIT; the blob carries the full
    # universe when it fits, else the store's quota page with the cap disclosed.
    nodes = base["data"]
    universe_truncated = base["meta"]["capped"]

    # 2. TIER the universe (Surface-1 two-tier trim). MATERIAL nodes = disclosed
    #    sub-buyers (ranked by prime_obl_60mo, capped) get full hot hydration:
    #    entity, award-state, matched_via (capped), and per-node MONTHLY event
    #    buckets — time / plan / set-aside predicates serve from these in-memory.
    #    The undisclosed tail rides as lean STUBS carrying membership + ranking
    #    scalars + the base recipe's cheap demand summary (needs-subs-now /
    #    by-action-type / counts), enough for coarse frontier prospecting.
    #    Raw event grain and win_portfolio are NOT stored: row-exact drilldown
    #    reads the indexed marts (gtm_prime_demand_events / gtm_prime_combo_lanes)
    #    by uei point-lookup (see sub_universe_serve). Every build_blob hydrate
    #    scan is MATERIAL-ONLY — the tail costs nothing here.
    disclosed = sorted((n for n in nodes if n.get("disclosed_sub_buyer")),
                       key=lambda n: -_f(n.get("matched_prime_obl_60mo")))
    material_ueis = {n["uei"] for n in disclosed[:MATERIAL_CAP]}
    material_ueis.add(uei)                        # the target's own header row
    mat_list = sorted(material_ueis)

    t = time.monotonic()
    sam = {r["uei"]: r for r in _scan_sam_entities(mat_list)}
    aw_rows = _scan_award_state(mat_list, today_iso)
    aw_by_uei: dict[str, list[dict]] = {}
    for r in aw_rows:
        aw_by_uei.setdefault(r["recipient_uei"], []).append(r)
    ev_rows = _scan_demand_events_full(mat_list)      # MATERIAL-ONLY: for buckets
    ev_by_uei: dict[str, list[dict]] = {}
    for r in ev_rows:
        ev_by_uei.setdefault(r["uei"], []).append(r)
    timings["hydrate_ms"] = int((time.monotonic() - t) * 1000)
    timings["n_material"] = len(material_ueis)

    # the base recipe's cheap per-node demand summary (needs-subs-now, by-action-
    # type, counts) rides on every node; keep a trimmed copy on stubs.
    STUB_SUMMARY_KEYS = ("n_events_24mo", "by_action_type", "n_plan_added_Y",
                         "n_terminations_EFX", "needs_subs_now_total")
    hot_nodes: list[dict[str, Any]] = []
    for n in nodes:
        cu = n["uei"]
        base_summary = n.pop("demand_events") if isinstance(n.get("demand_events"), dict) else None
        if cu in material_ueis:
            sr = sam.get(cu)
            n["entity"] = ({
                "cage": sr["cage_code"], "name": sr["legal_business_name"],
                "sam_is_active": sr["sam_is_active"], "in_dsbs": sr["in_dsbs"],
                "business_types": sr["business_types"], "primary_naics": sr["primary_naics"],
                "naics_codes": sr["naics_codes"], "psc_codes": sr["psc_codes"],
                "physical_city": sr["physical_city"], "physical_state": sr["physical_state"],
                "physical_zip": sr["physical_zip"],
            } if sr else None)
            aws = aw_by_uei.get(cu)
            if aws is not None:
                exp = [a for a in aws if a["days_to_expiry"] is not None
                       and 0 <= int(a["days_to_expiry"]) <= 180]
                ends = sorted(_d(a["current_end_date"]) for a in aws if a["current_end_date"])
                n["award_state"] = {"n_active_awards": len(aws),
                                    "n_expiring_180d": len(exp),
                                    "next_expiry_date": ends[0] if ends else None}
            else:
                n["award_state"] = None          # no live rows found ≠ zero awards ever
            full_via = n["matched_via"]
            n["matched_via"] = full_via[:MATCHED_VIA_HOT_CAP]
            n["matched_via_truncated"] = n.get("matched_via_truncated") or \
                len(full_via) > MATCHED_VIA_HOT_CAP
            # event grain restricted to this node's matched combos -> monthly
            # buckets (hot). Raw rows drill down from gtm_prime_demand_events.
            mset = {(m["naics_code"], m["psc_code"]) for m in full_via} | \
                   {tuple(k.split("x", 1)) for k in (n["gate_facts"] or {})}
            evs = [e for e in ev_by_uei.get(cu, [])
                   if (e["naics_code"], e["psc_code"]) in mset]
            n["demand_events"] = {"summary": base_summary,
                                  "buckets": _month_buckets(evs), "grain": "month",
                                  "detail_in_mart": "gtm_prime_demand_events"}
            n["pop"] = None                      # v1 deferral (meta.deferred)
            n["tier"] = "material"
            hot_nodes.append(n)
        else:
            hot_nodes.append({                   # lean STUB — detail via mart drilldown
                "uei": cu, "name": n.get("name"),
                "disclosed_sub_buyer": n.get("disclosed_sub_buyer"),
                "matched_farmout_60mo": n.get("matched_farmout_60mo"),
                "matched_prime_obl_60mo": n.get("matched_prime_obl_60mo"),
                "n_matched_combos": n.get("n_matched_combos"),
                "gate_facts": n.get("gate_facts"),   # combo keys preserve membership evidence
                "demand_summary": ({k: base_summary.get(k) for k in STUB_SUMMARY_KEYS}
                                   if base_summary else None),
                "tier": "stub",
            })

    # 3. target analytics — Act 1: the target's own record
    t = time.monotonic()
    edges = S._scan_target_edges(uei)
    amts = [_f(r["subaward_amount"]) for r in edges]
    lifetime = round(sum(amts), 2)
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
    # trajectory: CAGR over complete calendar years (mirror of gtm_sub_profiles)
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
    deal_band = {"low": _quantile(amts, DEAL_BAND_LO), "high": _quantile(amts, DEAL_BAND_HI),
                 "median": statistics.median(amts) if amts else None}
    # buyer HQ points for the geography panel
    anchor_geo = S._scan_geo(sorted(anchor_ueis)) if anchor_ueis else []
    # vehicle exposure + ordering-period ceiling (IDV self-row)
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
    # lane trends — own-side series per demonstrated combo (min-n doctrine)
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

    # 4. Act 2 — the pool (lanes ∩ states ∩ 24mo), named primes, placement, fit
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
    # placement: tag the pool's distinct prime awards via award-state rows
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

    # 5. Act 3 — the field: peers from gtm_sub_combo_lanes ∩ gtm_sub_profiles
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
        # deal-band overlap: intervals intersect (target band vs peer p20–p80)
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
    # converting: the peer-captured flow by prime-award profile (set-aside axis)
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

    tgt_sam = sam.get(uei)
    blob = {
        "uei": uei,
        "as_of": today_iso,
        "recipe": BLOB_RECIPE_ID,
        "universe": {
            "nodes": hot_nodes,
            "n_material": len(material_ueis),
            "n_disclosed": base["meta"]["n_disclosed_universe"],
            "n_undisclosed": base["meta"]["n_undisclosed_universe"],
            "n_total": base["meta"]["total"],
            "nodes_truncated": universe_truncated,
        },
        "target_analytics": {
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
                       "performance_states": states, "deal_band": deal_band,
                       "window_months": 24},
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
        },
        "meta": {
            "base_recipe": base["meta"]["recipe"],
            "tiering": {
                "material": f"disclosed sub-buyers, ranked by prime_obl_60mo, cap {MATERIAL_CAP}",
                "stub": "undisclosed tail — membership + ranking scalars + base demand summary",
                "event_grain": "monthly buckets hot (material); raw grain via mart drilldown",
                "drilldown_marts": ["gtm_prime_demand_events", "gtm_prime_combo_lanes"],
                "matched_via_hot_cap": MATCHED_VIA_HOT_CAP,
            },
            "deferred": ["node.pop (awaits uei×pop_state rollup mart)",
                         "placement subcontracting-plan axis (awaits agency/plan tagging pass)"],
            "timings_ms": timings,
            "sources": base["meta"]["sources"] + [
                "gtm_sam_entities", "usaspending_fpds_prime_award_state",
                "gtm_sub_combo_lanes", "gtm_sub_profiles"],
            "doctrine": base["meta"]["doctrine"],
        },
    }
    return blob
