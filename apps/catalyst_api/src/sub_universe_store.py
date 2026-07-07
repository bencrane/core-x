"""Sub-universe recipe v1 — the per-sub eligible-buyer map universe.

POST /api/v1/market/sub-universe  {uei, limit?}  →  one payload the map form runs on:
the target's ground truth + every eligible buyer node WITH its gate facts attached.
Form parameters (MVS floor, repeat depth, vehicles, PoP, combo set) are CLIENT-SIDE
predicates over these facts — the server computes the universe once per target; no
re-query on a slider move. Culls dim with disclosed reasons, never delete
(operator doctrine 2026-07-06). NO SCORING anywhere.

UNIVERSE DEFINITION (sub_universe.v1):
  anchors    = primes the target has received FSRS subawards from (govcon_teaming_edges)
  combo set  = the anchors' own 5y prime-award portfolios (gtm_prime_combo_lanes)
  node       = a prime with DISCLOSED 5y sub-buying (gtm_prime_farmout_combo_lanes)
               in ≥1 of those combos — i.e. a demonstrated sub-buyer of work shaped
               like what the target's own buyers win. Target + anchors excluded.
Matching farm-out lanes (not prime-win lanes) is deliberate: it admits only lanes
where the candidate demonstrably pushes $ to subs — the demand direction. Every
node carries matched_via evidence (combo, candidate farm-out $, best anchor's obl).

PER-NODE GATE FACTS (all materialized marts, boot-cached in-process):
  farm-out lanes   median/p25/p75 chunk + windowed $  → MVS floor
  teaming stats    n partners, deepest repeat edges   → workhorse/geometry
  vehicles         parent_piid × farm-out $           → vehicle portability
  geo              HQ lat/lon (gtm_entity_geo)        → the dots

TARGET BLOCK: anchors, demonstrated sub combos (median chunk, windows), PoP states,
vehicles ridden, own prime combos (dual-side → prime_backed stamps), and form
DEFAULTS derived from the target's own history (default MVS = their median chunk;
default PoP = their demonstrated states).

HOT PATH: whole-mart boot caches (farm-out 37.5K, teaming 89K, vehicles 16K — all
tiny); per request: anchor lookup + anchor-lanes IN scan + target edge scan +
chunked geo lookups. No large scans.
"""
from __future__ import annotations

import logging
import time
from datetime import date as dt_date
from typing import Any

from . import config
from . import lance_store

log = logging.getLogger("catalyst.sub_universe")

RECIPE_ID = "sub_universe.v1"
CACHE_TTL_S = 6 * 3600
DEFAULT_LIMIT = 1000
MAX_LIMIT = 5000
MATCHED_VIA_CAP = 25          # combos listed per node (honest total alongside)
DEFAULT_REPEAT_K = 3


# ── I/O seams (monkeypatch targets for the hermetic tests) ─────────────────────
def _scan(uri: str, columns: list[str] | None, predicate: str | None) -> list[dict[str, Any]]:
    import lance
    ds = lance.dataset(uri, storage_options=config.r2_storage_options())
    return ds.scanner(columns=columns, filter=predicate).to_table().to_pylist()


def _scan_teaming() -> list[dict[str, Any]]:
    return _scan(config.GOVCON_TEAMING_EDGES_URI,
                 ["prime_uei", "sub_uei", "prime_name", "sub_name",
                  "edge_dollars_5y", "edge_count_5y", "last_action_date"], None)


def _scan_farmout() -> list[dict[str, Any]]:
    return _scan(config.GTM_PRIME_FARMOUT_COMBO_LANES_URI,
                 ["uei", "naics_code", "psc_code", "naics_title", "psc_title",
                  "farmout_amt_60mo", "farmout_amt_lifetime", "median_chunk_60mo",
                  "median_chunk_lifetime", "p75_chunk_60mo", "n_subawards_lifetime",
                  "n_distinct_subs_60mo", "last_action_date"], None)


def _scan_vehicles() -> list[dict[str, Any]]:
    return _scan(config.GTM_PRIME_VEHICLE_LANES_URI,
                 ["uei", "parent_piid", "farmout_amt_60mo", "farmout_amt_lifetime",
                  "n_subawards_lifetime", "last_action_date"], None)


def _scan_prime_lanes(ueis: list[str]) -> list[dict[str, Any]]:
    pred = "uei IN (" + ",".join(f"'{u}'" for u in ueis) + ") AND prime_obl_60mo > 0"
    return _scan(config.GTM_PRIME_COMBO_LANES_URI,
                 ["uei", "naics_code", "psc_code", "prime_obl_60mo", "last_action_date"], pred)


def _scan_target_edges(uei: str) -> list[dict[str, Any]]:
    return _scan(config.CONTRACT_SUBAWARD_URI,
                 ["subaward_amount", "subaward_action_date", "prime_awardee_uei",
                  "prime_award_naics_code", "prime_award_product_or_service_code",
                  "prime_award_parent_piid",
                  "subaward_primary_place_of_performance_state_code"],
                 f"subawardee_uei = '{uei}'")


def _scan_geo(ueis: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i in range(0, len(ueis), 100):
        chunk = ueis[i:i + 100]
        pred = "uei IN (" + ",".join(f"'{u}'" for u in chunk) + ")"
        out += _scan(config.GTM_ENTITY_GEO_URI,
                     ["uei", "latitude", "longitude", "geo_precision"], pred)
    return out


# ── Boot caches ────────────────────────────────────────────────────────────────
_caches: dict[str, Any] | None = None
_caches_built_at: float = 0.0


def _build_caches() -> dict[str, Any]:
    t0 = time.monotonic()
    teaming = _scan_teaming()
    by_sub: dict[str, list[dict]] = {}
    by_prime: dict[str, dict[str, Any]] = {}
    for r in teaming:
        by_sub.setdefault(r["sub_uei"], []).append(r)
        st = by_prime.setdefault(r["prime_uei"], {
            "prime_name": r["prime_name"], "n_sub_partners_5y": 0,
            "deepest_repeat_edges_5y": 0, "partner_edge_counts": []})
        st["n_sub_partners_5y"] += 1
        ec = int(r["edge_count_5y"] or 0)
        st["partner_edge_counts"].append(ec)
        st["deepest_repeat_edges_5y"] = max(st["deepest_repeat_edges_5y"], ec)

    farmout = _scan_farmout()
    fo_by_combo: dict[tuple, list[dict]] = {}
    fo_by_uei: dict[str, list[dict]] = {}
    for r in farmout:
        if r["naics_code"] and r["psc_code"] and (r["farmout_amt_60mo"] or 0) > 0:
            fo_by_combo.setdefault((r["naics_code"], r["psc_code"]), []).append(r)
        fo_by_uei.setdefault(r["uei"], []).append(r)

    vehicles = _scan_vehicles()
    veh_by_uei: dict[str, list[dict]] = {}
    for r in vehicles:
        veh_by_uei.setdefault(r["uei"], []).append(r)

    build_ms = int((time.monotonic() - t0) * 1000)
    log.info("sub-universe caches built in %dms: teaming=%d farmout=%d vehicles=%d",
             build_ms, len(teaming), len(farmout), len(vehicles))
    return {"teaming_by_sub": by_sub, "teaming_by_prime": by_prime,
            "farmout_by_combo": fo_by_combo, "farmout_by_uei": fo_by_uei,
            "vehicles_by_uei": veh_by_uei, "build_ms": build_ms}


def _ensure_caches() -> tuple[str, dict[str, Any]]:
    global _caches, _caches_built_at
    if _caches is None or (time.monotonic() - _caches_built_at) > CACHE_TTL_S:
        state = "cold"
        _caches = _build_caches()
        _caches_built_at = time.monotonic()
    else:
        state = "warm"
    return state, _caches


def reset_caches_for_tests() -> None:
    global _caches, _caches_built_at
    _caches = None
    _caches_built_at = 0.0


# ── Request validation (fail-closed) ───────────────────────────────────────────
def validate_request(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise lance_store.MapCompileError("body must be an object")
    unknown = set(body) - {"uei", "limit"}
    if unknown:
        raise lance_store.MapCompileError(f"unknown keys: {sorted(unknown)}")
    uei = str(body.get("uei") or "").strip().upper()
    if not lance_store.valid_uei(uei):
        raise lance_store.MapCompileError("uei must be a 12-char SAM.gov UEI")
    limit = body.get("limit", DEFAULT_LIMIT)
    if not isinstance(limit, int) or limit < 1 or limit > MAX_LIMIT:
        raise lance_store.MapCompileError(f"limit must be an int in [1, {MAX_LIMIT}]")
    return {"uei": uei, "limit": limit}


def _f(v: Any) -> float:
    return float(v) if v is not None else 0.0


def _d(v: Any) -> str | None:
    return str(v)[:10] if v is not None else None


def _median(vals: list[float]) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2


# ── The recipe ─────────────────────────────────────────────────────────────────
def execute_sub_universe(body: Any) -> dict[str, Any]:
    req = validate_request(body)
    uei, limit = req["uei"], req["limit"]
    timings: dict[str, int] = {}
    t0 = time.monotonic()
    cache_state, caches = _ensure_caches()
    timings["caches_ms"] = int((time.monotonic() - t0) * 1000)

    # 1. anchors
    t = time.monotonic()
    anchor_edges = sorted(caches["teaming_by_sub"].get(uei, []),
                          key=lambda r: -_f(r["edge_dollars_5y"]))
    anchors = [{"prime_uei": r["prime_uei"], "prime_name": r["prime_name"],
                "edge_dollars_5y": round(_f(r["edge_dollars_5y"]), 2),
                "edge_count_5y": int(r["edge_count_5y"] or 0),
                "last_action_date": _d(r["last_action_date"])} for r in anchor_edges]
    anchor_ueis = {a["prime_uei"] for a in anchors}
    timings["anchors_ms"] = int((time.monotonic() - t) * 1000)

    # 2. target ground truth (edges + own prime combos)
    t = time.monotonic()
    edges = _scan_target_edges(uei)
    by_combo: dict[tuple, list[dict]] = {}
    pop_states: dict[str, float] = {}
    target_vehicles: dict[str, float] = {}
    for r in edges:
        amt = _f(r["subaward_amount"])
        k = (r["prime_award_naics_code"], r["prime_award_product_or_service_code"])
        if k[0] and k[1]:
            by_combo.setdefault(k, []).append({"amt": amt, "date": _d(r["subaward_action_date"])})
        st = (r["subaward_primary_place_of_performance_state_code"] or "").strip()
        if st:
            pop_states[st] = pop_states.get(st, 0.0) + amt
        veh = (r["prime_award_parent_piid"] or "").strip()
        if veh:
            target_vehicles[veh] = target_vehicles.get(veh, 0.0) + amt
    demonstrated = []
    all_amts: list[float] = []
    for k, items in sorted(by_combo.items(), key=lambda kv: -sum(i["amt"] for i in kv[1])):
        amts = [i["amt"] for i in items]
        all_amts += amts
        demonstrated.append({"combo": f"{k[0]}x{k[1]}", "naics_code": k[0], "psc_code": k[1],
                             "n_edges": len(amts), "total_usd": round(sum(amts), 2),
                             "median_chunk_usd": round(_median(amts) or 0, 2),
                             "last_action_date": max(i["date"] for i in items)})
    own_prime = _scan_prime_lanes([uei])
    prime_combos = sorted(
        ({"combo": f"{r['naics_code']}x{r['psc_code']}", "naics_code": r["naics_code"],
          "psc_code": r["psc_code"], "prime_obl_60mo": round(_f(r["prime_obl_60mo"]), 2)}
         for r in own_prime if r["uei"] == uei and r["naics_code"] and r["psc_code"]),
        key=lambda x: -x["prime_obl_60mo"])
    prime_combo_set = {(c["naics_code"], c["psc_code"]) for c in prime_combos}
    timings["target_ms"] = int((time.monotonic() - t) * 1000)

    # 3. anchor portfolio combos
    t = time.monotonic()
    combo_anchor_obl: dict[tuple, dict[str, float]] = {}
    if anchor_ueis:
        for r in _scan_prime_lanes(sorted(anchor_ueis)):
            if r["naics_code"] and r["psc_code"]:
                k = (r["naics_code"], r["psc_code"])
                combo_anchor_obl.setdefault(k, {})
                combo_anchor_obl[k][r["uei"]] = (
                    combo_anchor_obl[k].get(r["uei"], 0.0) + _f(r["prime_obl_60mo"]))
    timings["anchor_lanes_ms"] = int((time.monotonic() - t) * 1000)

    # 4. universe: sub-buying primes with farm-out lanes in the anchor combo set
    t = time.monotonic()
    cand: dict[str, list[dict]] = {}
    for k in combo_anchor_obl:
        for row in caches["farmout_by_combo"].get(k, []):
            cu = row["uei"]
            if cu == uei or cu in anchor_ueis:
                continue
            cand.setdefault(cu, []).append(row)
    total_candidates = len(cand)

    def node_total(rows: list[dict]) -> float:
        return sum(_f(r["farmout_amt_60mo"]) for r in rows)

    ordered = sorted(cand.items(), key=lambda kv: -node_total(kv[1]))[:limit]
    geo = {g["uei"]: g for g in _scan_geo([u for u, _ in ordered])} if ordered else {}
    nodes = []
    for cu, rows in ordered:
        rows_sorted = sorted(rows, key=lambda r: -_f(r["farmout_amt_60mo"]))
        matched = []
        for r in rows_sorted[:MATCHED_VIA_CAP]:
            k = (r["naics_code"], r["psc_code"])
            best_anchor = max(combo_anchor_obl.get(k, {}).items(),
                              key=lambda kv: kv[1], default=(None, 0.0))
            matched.append({
                "combo": f"{k[0]}x{k[1]}", "naics_code": k[0], "psc_code": k[1],
                "naics_title": r["naics_title"], "psc_title": r["psc_title"],
                "farmout_amt_60mo": round(_f(r["farmout_amt_60mo"]), 2),
                "median_chunk_60mo": (round(_f(r["median_chunk_60mo"]), 2)
                                      if r["median_chunk_60mo"] is not None else None),
                "median_chunk_lifetime": (round(_f(r["median_chunk_lifetime"]), 2)
                                          if r["median_chunk_lifetime"] is not None else None),
                "n_subawards_lifetime": int(r["n_subawards_lifetime"] or 0),
                "n_distinct_subs_60mo": int(r["n_distinct_subs_60mo"] or 0),
                "last_action_date": _d(r["last_action_date"]),
                "anchor_uei": best_anchor[0],
                "anchor_obl_60mo": round(best_anchor[1], 2),
                "prime_backed": k in prime_combo_set,
            })
        stats = caches["teaming_by_prime"].get(cu, {})
        g = geo.get(cu)
        nodes.append({
            "uei": cu,
            "name": stats.get("prime_name"),
            "latitude": g["latitude"] if g else None,
            "longitude": g["longitude"] if g else None,
            "geo_precision": g["geo_precision"] if g else None,
            "matched_farmout_60mo": round(node_total(rows), 2),
            "n_matched_combos": len(rows),
            "matched_via": matched,
            "teaming": {"n_sub_partners_5y": stats.get("n_sub_partners_5y", 0),
                        "deepest_repeat_edges_5y": stats.get("deepest_repeat_edges_5y", 0),
                        "n_partners_ge_3_edges": sum(
                            1 for c in stats.get("partner_edge_counts", []) if c >= 3)},
            "vehicles": [{"parent_piid": v["parent_piid"],
                          "farmout_amt_60mo": round(_f(v["farmout_amt_60mo"]), 2),
                          "last_action_date": _d(v["last_action_date"])}
                         for v in sorted(caches["vehicles_by_uei"].get(cu, []),
                                         key=lambda v: -_f(v["farmout_amt_60mo"]))],
        })
    timings["universe_ms"] = int((time.monotonic() - t) * 1000)

    target_median = _median(all_amts)
    return {
        "data": nodes,
        "target": {
            "uei": uei,
            "anchors": anchors,
            "demonstrated_combos": demonstrated,
            "pop_states": [{"state": s, "sub_usd": round(v, 2)}
                           for s, v in sorted(pop_states.items(), key=lambda kv: -kv[1])],
            "vehicles": [{"parent_piid": k, "sub_usd": round(v, 2)}
                         for k, v in sorted(target_vehicles.items(), key=lambda kv: -kv[1])],
            "prime_combos": prime_combos,
            "defaults": {
                "mvs_usd": round(target_median, 2) if target_median is not None else None,
                "repeat_k": DEFAULT_REPEAT_K,
                "pop_states": [s for s, _ in sorted(pop_states.items(), key=lambda kv: -kv[1])],
                "window": "60mo",
            },
        },
        "meta": {
            "recipe": RECIPE_ID,
            "generated_at": dt_date.today().isoformat(),
            "n_anchors": len(anchors),
            "n_anchor_combos": len(combo_anchor_obl),
            "total": total_candidates,
            "returned": len(nodes),
            "capped": total_candidates > len(nodes),
            "reason": (None if anchors else
                       "target has no FSRS subaward edges — no anchors to derive a universe from"),
            "cache_state": cache_state,
            "cache_build_ms": caches.get("build_ms"),
            "timings_ms": timings,
            "sources": ["govcon_teaming_edges", "gtm_prime_combo_lanes",
                        "gtm_prime_farmout_combo_lanes", "gtm_prime_vehicle_lanes",
                        "gtm_entity_geo", "usaspending_subaward_canonical"],
            "doctrine": "facts only, no scoring; form gates run client-side; culls dim, never delete",
        },
    }
