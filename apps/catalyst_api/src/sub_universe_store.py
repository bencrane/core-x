"""Sub-universe recipe v2 — the per-sub eligible-buyer map universe.

POST /api/v1/market/sub-universe  {uei, limit?}  →  one payload the map form runs on:
the target's ground truth + every eligible buyer node WITH its gate facts attached.
Form parameters (MVS floor, repeat depth, vehicles, PoP, combo set) are CLIENT-SIDE
predicates over these facts — the server computes the universe once per target; no
re-query on a slider move. Culls dim with disclosed reasons, never delete
(operator doctrine 2026-07-06). NO SCORING anywhere.

UNIVERSE DEFINITION (sub_universe.v2 — FULL lookalike winners):
  anchors    = primes the target has received FSRS subawards from (spine-derived,
               usaspending_subaward_canonical — the pair mart is stats-only)
  combo set  = the anchors' own 5y prime-award portfolios (gtm_prime_combo_lanes)
  node       = ANY prime with prime_obl_60mo > 0 in ≥1 anchor-portfolio combo
               (gtm_prime_combo_lanes) — i.e. a demonstrated WINNER of work shaped
               like what the target's own buyers win. Target + anchors excluded.
Farm-out lanes (gtm_prime_farmout_combo_lanes) are LEFT-joined per matched combo:
node.disclosed_sub_buyer marks nodes with ≥1 disclosed farm-out lane among their
matched combos. v1 admitted only disclosed sub-buyers — that silently culled every
winner whose sub-buying is simply not FSRS-visible; v2 carries them, dimmed by
absent facts, never deleted.

NULL SEMANTICS (unknown ≠ zero, non-negotiable):
  • undisclosed nodes: matched_farmout_60mo is null (not 0); per-combo farm-out
    fields in matched_via are null with candidate_prime_obl_60mo always present.
  • nodes with no gtm_prime_sub_pairs rows: all three teaming fields are null —
    no disclosed pair history is an absent fact, not zero partners.
  • gate_facts medians are null where no farm-out lane discloses one.

QUOTA PAGE (so H3's frontier is never truncated away): the page reserves a share
(UNDISCLOSED_PAGE_QUOTA) of `limit` for undisclosed winners — a large target can
have tens of thousands of disclosed buyers that would otherwise consume the whole
cap and hide the undisclosed tier the client's disclosed-toggle is meant to reveal.
Disclosed nodes fill first, then the reserved undisclosed slice; unused quota
backfills from disclosed (and vice-versa) so no slot is wasted while either tier
has candidates. meta carries n_disclosed_universe / n_undisclosed_universe (full
counts) and returned_disclosed / returned_undisclosed (page counts). Within the
page: disclosed first by matched farm-out $ (60mo) desc, then undisclosed by
matched prime obligations (60mo) desc. Order is presentation, not scoring.

PER-NODE GATE FACTS (all materialized marts, boot-cached in-process):
  gate_facts       {combo: {m: farm-out median chunk 60mo | null,
                            pb: combo ∈ target's own prime combos}} over the FULL
                   matched-combo set (matched_via display list caps at 25 with
                   matched_via_truncated disclosed)
  teaming stats    n partners / deepest repeat / ≥3-edge partners, from the
                   gtm_prime_sub_pairs mart (pair-complete; edges stay OUT of
                   the store — only per-prime stats are cached)
  vehicles         parent_piid × farm-out $           → vehicle portability
  geo              HQ lat/lon (gtm_entity_geo)        → the dots
  demand events    per-node chunked lookups over gtm_prime_demand_events (v2:
                   ALL primes) — the ~24mo action pulse + needs-subs-now evidence

TARGET BLOCK: anchors, demonstrated sub combos (median chunk, windows), PoP states,
vehicles ridden, own prime combos (dual-side → prime_backed stamps), and form
DEFAULTS derived from the target's own history. defaults.mvs_n = the number of
combo-bearing edges behind the default MVS; below 5 edges the default floor is
null with defaults.mvs_reason disclosing the basis — a 2-edge median is not a
floor.

HOT PATH: boot caches = farm-out lanes (~37.5K, keyed (uei, combo)), pair stats
(per-prime rollup of gtm_prime_sub_pairs), vehicles (~16K), and the WINNERS
INVERTED INDEX — one full scan of gtm_prime_combo_lanes (uei/naics/psc/
prime_obl_60mo, prime_obl_60mo > 0) → dict[(naics,psc)] → [(uei, obl)]; build
time + entry count measured and logged. Per request: anchor lookup + anchor-lane
IN scan + target edge scan + chunked geo/demand lookups. No large per-request scans.
"""
from __future__ import annotations

import logging
import time
from datetime import date as dt_date, timedelta
from typing import Any

from . import config
from . import lance_store

log = logging.getLogger("catalyst.sub_universe")

RECIPE_ID = "sub_universe.v2"
CACHE_TTL_S = 6 * 3600
DEFAULT_LIMIT = 1000
MAX_LIMIT = 5000
MATCHED_VIA_CAP = 25          # combos listed per node (honest total alongside)
DEFAULT_REPEAT_K = 3
MVS_MIN_N = 5                 # combo-bearing edges below which no default floor is set
UNDISCLOSED_PAGE_QUOTA = 0.30  # min share of the page reserved for undisclosed winners
                               # so the prospecting frontier is always represented (H3):
                               # the cap can never be fully consumed by disclosed buyers.
                               # Unused quota backfills from disclosed; both tiers are
                               # exhausted before any slot is wasted.
DISPLAY_ORDER = ("quota page: disclosed sub-buyers by matched farm-out $ (60mo) desc, "
                 "then a reserved slice of undisclosed winners by matched prime "
                 "obligations (60mo) desc — both tiers represented when both exist")


# ── I/O seams (monkeypatch targets for the hermetic tests) ─────────────────────
def _scan(uri: str, columns: list[str] | None, predicate: str | None) -> list[dict[str, Any]]:
    import lance
    ds = lance.dataset(uri, storage_options=config.r2_storage_options())
    return ds.scanner(columns=columns, filter=predicate).to_table().to_pylist()


def _scan_pairs() -> list[dict[str, Any]]:
    """Buyer teaming-stat inputs from the pair-complete gtm_prime_sub_pairs mart.
    Only the columns the per-prime rollup needs — the edges themselves never
    enter the store."""
    return _scan(config.GTM_PRIME_SUB_PAIRS_URI,
                 ["prime_uei", "prime_name", "edge_count_5y"], None)


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


def _scan_winner_lanes() -> list[dict[str, Any]]:
    """One full scan of gtm_prime_combo_lanes for the winners inverted index —
    minimal projection, 60mo-positive lanes only."""
    return _scan(config.GTM_PRIME_COMBO_LANES_URI,
                 ["uei", "naics_code", "psc_code", "prime_obl_60mo"],
                 "prime_obl_60mo > 0")


def _scan_prime_lanes(ueis: list[str]) -> list[dict[str, Any]]:
    pred = "uei IN (" + ",".join(f"'{u}'" for u in ueis) + ") AND prime_obl_60mo > 0"
    return _scan(config.GTM_PRIME_COMBO_LANES_URI,
                 ["uei", "naics_code", "psc_code", "prime_obl_60mo", "last_action_date"], pred)


def _scan_target_edges(uei: str) -> list[dict[str, Any]]:
    return _scan(config.CONTRACT_SUBAWARD_URI,
                 ["subaward_amount", "subaward_action_date", "prime_awardee_uei",
                  "prime_awardee_name",
                  "prime_award_naics_code", "prime_award_product_or_service_code",
                  "prime_award_parent_piid",
                  "subaward_primary_place_of_performance_state_code"],
                 f"subawardee_uei = '{uei}'")


PLAN_REQUIRED = {"C", "D", "E", "F", "G", "H"}
TERMINATION_CODES = {"E", "F", "X"}
NEEDS_SUBS_NOW_CAP = 5


def _scan_demand_events(ueis: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i in range(0, len(ueis), 100):
        chunk = ueis[i:i + 100]
        pred = "uei IN (" + ",".join(f"'{u}'" for u in chunk) + ")"
        out += _scan(config.GTM_PRIME_DEMAND_EVENTS_URI,
                     ["uei", "award_key", "action_date", "obligation_delta",
                      "naics_code", "psc_code", "action_type_code",
                      "action_type_description", "award_type_code",
                      "subcontracting_plan", "subcontracting_plan_desc",
                      "is_first_action", "has_disclosed_subs"], pred)
    return out


def _demand_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Per-node pulse summary over the ~24mo event window. Facts + the flagship
    recipe evidence (new type-C order, plan required, no disclosed subs yet) —
    full event grain stays query-side in the mart."""
    by_type: dict[str, int] = {}
    needs_now: list[dict[str, Any]] = []
    n_plan_added = 0
    n_terminations = 0
    for e in events:
        at = (e["action_type_code"] or "").strip()
        if at:
            by_type[at] = by_type.get(at, 0) + 1
            if at == "Y":
                n_plan_added += 1
            if at in TERMINATION_CODES:
                n_terminations += 1
        if (e["is_first_action"] and (e["award_type_code"] or "").strip() == "C"
                and (e["subcontracting_plan"] or "").strip() in PLAN_REQUIRED
                and not e["has_disclosed_subs"]):
            needs_now.append({
                "award_key": e["award_key"],
                "combo": (f"{e['naics_code']}x{e['psc_code']}"
                          if e["naics_code"] and e["psc_code"] else None),
                "action_date": _d(e["action_date"]),
                "obligation": round(_f(e["obligation_delta"]), 2),
                "subcontracting_plan": e["subcontracting_plan"],
                "subcontracting_plan_desc": e["subcontracting_plan_desc"],
            })
    needs_now.sort(key=lambda r: -(r["obligation"] or 0))
    return {"n_events_24mo": len(events),
            "by_action_type": dict(sorted(by_type.items())),
            "n_plan_added_Y": n_plan_added,
            "n_terminations_EFX": n_terminations,
            "needs_subs_now_total": len(needs_now),
            "needs_subs_now": needs_now[:NEEDS_SUBS_NOW_CAP]}


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
    # per-prime teaming stats from the pair mart (edges never cached whole)
    pairs = _scan_pairs()
    by_prime: dict[str, dict[str, Any]] = {}
    for r in pairs:
        st = by_prime.setdefault(r["prime_uei"], {
            "prime_name": r["prime_name"], "n_sub_partners_5y": 0,
            "deepest_repeat_edges_5y": 0, "n_partners_ge_3_edges": 0})
        ec = int(r["edge_count_5y"] or 0)
        if ec > 0:
            st["n_sub_partners_5y"] += 1
            st["deepest_repeat_edges_5y"] = max(st["deepest_repeat_edges_5y"], ec)
            if ec >= 3:
                st["n_partners_ge_3_edges"] += 1

    farmout = _scan_farmout()
    fo_by_uei_combo: dict[tuple, dict] = {}
    for r in farmout:
        if r["naics_code"] and r["psc_code"]:
            fo_by_uei_combo[(r["uei"], r["naics_code"], r["psc_code"])] = r

    vehicles = _scan_vehicles()
    veh_by_uei: dict[str, list[dict]] = {}
    for r in vehicles:
        veh_by_uei.setdefault(r["uei"], []).append(r)

    # winners inverted index — the H3 universe substrate (measured + logged)
    tw = time.monotonic()
    winner_lanes = _scan_winner_lanes()
    winners_by_combo: dict[tuple, list[tuple[str, float]]] = {}
    for r in winner_lanes:
        if r["naics_code"] and r["psc_code"]:
            winners_by_combo.setdefault((r["naics_code"], r["psc_code"]), []).append(
                (r["uei"], _f(r["prime_obl_60mo"])))
    winners_ms = int((time.monotonic() - tw) * 1000)
    log.info("winners inverted index built in %dms: %d lanes -> %d combo entries",
             winners_ms, len(winner_lanes), len(winners_by_combo))

    build_ms = int((time.monotonic() - t0) * 1000)
    log.info("sub-universe caches built in %dms: pair_primes=%d farmout=%d vehicles=%d "
             "winner_combos=%d", build_ms, len(by_prime), len(farmout), len(vehicles),
             len(winners_by_combo))
    return {"pairs_by_prime": by_prime,
            "farmout_by_uei_combo": fo_by_uei_combo,
            "vehicles_by_uei": veh_by_uei,
            "winners_by_combo": winners_by_combo,
            "winners_build_ms": winners_ms,
            "winners_combo_entries": len(winners_by_combo),
            "build_ms": build_ms}


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

    # 1+2. target edges (the spine is the anchor source of record — anchors are
    # derived from the edges directly; the pair mart serves stats only)
    t = time.monotonic()
    edges = _scan_target_edges(uei)
    cutoff_5y = (dt_date.today() - timedelta(days=1826)).isoformat()
    by_anchor: dict[str, dict[str, Any]] = {}
    for r in edges:
        pu = r["prime_awardee_uei"]
        if not pu:
            continue
        a = by_anchor.setdefault(pu, {"prime_uei": pu,
                                      "prime_name": r["prime_awardee_name"],
                                      "sub_usd_lifetime": 0.0, "edge_dollars_5y": 0.0,
                                      "edge_count_5y": 0, "last_action_date": None})
        amt = _f(r["subaward_amount"])
        d = _d(r["subaward_action_date"])
        a["sub_usd_lifetime"] += amt
        if d and d >= cutoff_5y:
            a["edge_dollars_5y"] += amt
            a["edge_count_5y"] += 1
        if d and (a["last_action_date"] is None or d > a["last_action_date"]):
            a["last_action_date"] = d
    anchors = sorted(({**a, "sub_usd_lifetime": round(a["sub_usd_lifetime"], 2),
                       "edge_dollars_5y": round(a["edge_dollars_5y"], 2)}
                      for a in by_anchor.values()),
                     key=lambda a: -(a["sub_usd_lifetime"]))
    anchor_ueis = {a["prime_uei"] for a in anchors}
    timings["anchors_ms"] = int((time.monotonic() - t) * 1000)
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

    # 4. universe: FULL lookalike winners — every prime with prime_obl_60mo > 0
    # in ≥1 anchor-portfolio combo (winners inverted index), farm-out LEFT-joined
    t = time.monotonic()
    winners = caches["winners_by_combo"]
    fo_lookup = caches["farmout_by_uei_combo"]
    cand: dict[str, dict[tuple, float]] = {}      # uei -> {combo: prime_obl_60mo}
    for k in combo_anchor_obl:
        for cu, obl in winners.get(k, []):
            if cu == uei or cu in anchor_ueis:
                continue
            cand.setdefault(cu, {})[k] = obl
    total_candidates = len(cand)

    # cheap per-candidate totals for ordering; full hydration only for the page
    ranked: list[tuple[str, bool, float, float]] = []  # (uei, disclosed, fo_total, obl_total)
    for cu, combos in cand.items():
        fo_total = 0.0
        disclosed = False
        for k in combos:
            fo = fo_lookup.get((cu, k[0], k[1]))
            if fo is not None:
                disclosed = True
                fo_total += _f(fo["farmout_amt_60mo"])
        ranked.append((cu, disclosed, fo_total, sum(combos.values())))
    ranked.sort(key=lambda r: (not r[1], -(r[2] if r[1] else 0.0), -r[3]))
    disc_ranked = [r for r in ranked if r[1]]
    undisc_ranked = [r for r in ranked if not r[1]]
    n_disclosed_universe = len(disc_ranked)
    n_undisclosed_universe = len(undisc_ranked)
    # Quota: reserve a slice of the page for undisclosed winners so the frontier
    # is always visible; backfill unused quota from disclosed (and vice-versa) so
    # no slot is wasted while either tier has candidates.
    reserved_undisc = int(limit * UNDISCLOSED_PAGE_QUOTA)
    d_take = min(len(disc_ranked), limit - min(len(undisc_ranked), reserved_undisc))
    u_take = min(len(undisc_ranked), limit - d_take)
    page = disc_ranked[:d_take] + undisc_ranked[:u_take]

    geo = {g["uei"]: g for g in _scan_geo([u for u, *_ in page])} if page else {}
    demand_by_uei: dict[str, list[dict]] = {}
    if page:
        for e in _scan_demand_events([u for u, *_ in page]):
            demand_by_uei.setdefault(e["uei"], []).append(e)
    nodes = []
    for cu, disclosed, fo_total, obl_total in page:
        combos = cand[cu]
        # matched combos: disclosed lanes first by farm-out $ desc, then
        # undisclosed by the candidate's own prime obl desc
        entries = []
        for k, obl in combos.items():
            fo = fo_lookup.get((cu, k[0], k[1]))
            entries.append((k, obl, fo))
        entries.sort(key=lambda e: (e[2] is None,
                                    -_f(e[2]["farmout_amt_60mo"]) if e[2] is not None else 0.0,
                                    -e[1]))
        gate_facts: dict[str, dict[str, Any]] = {}
        matched = []
        for i, (k, obl, fo) in enumerate(entries):
            combo_str = f"{k[0]}x{k[1]}"
            gate_facts[combo_str] = {
                "m": (round(_f(fo["median_chunk_60mo"]), 2)
                      if fo is not None and fo["median_chunk_60mo"] is not None else None),
                "pb": k in prime_combo_set,
            }
            if i >= MATCHED_VIA_CAP:
                continue
            best_anchor = max(combo_anchor_obl.get(k, {}).items(),
                              key=lambda kv: kv[1], default=(None, 0.0))
            matched.append({
                "combo": combo_str, "naics_code": k[0], "psc_code": k[1],
                "naics_title": fo["naics_title"] if fo is not None else None,
                "psc_title": fo["psc_title"] if fo is not None else None,
                "candidate_prime_obl_60mo": round(obl, 2),
                "farmout_amt_60mo": (round(_f(fo["farmout_amt_60mo"]), 2)
                                     if fo is not None else None),
                "median_chunk_60mo": (round(_f(fo["median_chunk_60mo"]), 2)
                                      if fo is not None and fo["median_chunk_60mo"] is not None
                                      else None),
                "median_chunk_lifetime": (round(_f(fo["median_chunk_lifetime"]), 2)
                                          if fo is not None and fo["median_chunk_lifetime"] is not None
                                          else None),
                "n_subawards_lifetime": (int(fo["n_subawards_lifetime"] or 0)
                                         if fo is not None else None),
                "n_distinct_subs_60mo": (int(fo["n_distinct_subs_60mo"] or 0)
                                         if fo is not None else None),
                "last_action_date": _d(fo["last_action_date"]) if fo is not None else None,
                "anchor_uei": best_anchor[0],
                "anchor_obl_60mo": round(best_anchor[1], 2),
                "prime_backed": k in prime_combo_set,
            })
        stats = caches["pairs_by_prime"].get(cu)
        g = geo.get(cu)
        nodes.append({
            "uei": cu,
            "name": stats["prime_name"] if stats else None,
            "latitude": g["latitude"] if g else None,
            "longitude": g["longitude"] if g else None,
            "geo_precision": g["geo_precision"] if g else None,
            "disclosed_sub_buyer": disclosed,
            "matched_farmout_60mo": round(fo_total, 2) if disclosed else None,
            "matched_prime_obl_60mo": round(obl_total, 2),
            "n_matched_combos": len(combos),
            "matched_via": matched,
            "matched_via_truncated": len(entries) > MATCHED_VIA_CAP,
            "gate_facts": gate_facts,
            # unknown ≠ zero: a prime with no pair rows has UNDISCLOSED teaming,
            # not zero partners — all three fields ride as null
            "teaming": ({"n_sub_partners_5y": stats["n_sub_partners_5y"],
                         "deepest_repeat_edges_5y": stats["deepest_repeat_edges_5y"],
                         "n_partners_ge_3_edges": stats["n_partners_ge_3_edges"]}
                        if stats is not None else
                        {"n_sub_partners_5y": None,
                         "deepest_repeat_edges_5y": None,
                         "n_partners_ge_3_edges": None}),
            "demand_events": _demand_summary(demand_by_uei.get(cu, [])),
            "vehicles": [{"parent_piid": v["parent_piid"],
                          "farmout_amt_60mo": round(_f(v["farmout_amt_60mo"]), 2),
                          "last_action_date": _d(v["last_action_date"])}
                         for v in sorted(caches["vehicles_by_uei"].get(cu, []),
                                         key=lambda v: -_f(v["farmout_amt_60mo"]))],
        })
    timings["universe_ms"] = int((time.monotonic() - t) * 1000)

    # form defaults: the MVS floor comes from the target's own combo-bearing
    # edges; below MVS_MIN_N edges no floor is defaulted (n rides alongside)
    mvs_n = len(all_amts)
    target_median = _median(all_amts)
    if mvs_n < MVS_MIN_N or target_median is None:
        mvs_usd = None
        mvs_reason = f"insufficient history (n={mvs_n}) to set a default floor"
    else:
        mvs_usd = round(target_median, 2)
        mvs_reason = None
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
                "mvs_usd": mvs_usd,
                "mvs_n": mvs_n,
                "mvs_reason": mvs_reason,
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
            "n_disclosed_universe": n_disclosed_universe,
            "n_undisclosed_universe": n_undisclosed_universe,
            "returned_disclosed": d_take,
            "returned_undisclosed": u_take,
            "undisclosed_page_quota": UNDISCLOSED_PAGE_QUOTA,
            "display_order": DISPLAY_ORDER,
            "reason": (None if anchors else
                       "target has no FSRS subaward edges — no anchors to derive a universe from"),
            "cache_state": cache_state,
            "cache_build_ms": caches.get("build_ms"),
            "winners_index_build_ms": caches.get("winners_build_ms"),
            "winners_index_combo_entries": caches.get("winners_combo_entries"),
            "timings_ms": timings,
            "sources": ["gtm_prime_sub_pairs", "gtm_prime_combo_lanes",
                        "gtm_prime_farmout_combo_lanes", "gtm_prime_vehicle_lanes",
                        "gtm_prime_demand_events", "gtm_entity_geo",
                        "usaspending_subaward_canonical"],
            "doctrine": ("facts only, no scoring; unknown != zero — absent facts are null "
                         "with the basis disclosed; form gates run client-side; "
                         "culls dim, never delete"),
        },
    }
