#!/usr/bin/env python3
"""emit_entity_vectors — the parameterized per-sub diagnostic emitter (vectors + labor block).

For each UEI, emits one JSON document of raw form variables for the diagnostic call:

  windows.lifetime / windows.trailing_60mo — the four FSRS behavioral vectors:
    v1_concentration      top-1/top-3 prime share of sub $
    v2_chunk_delta        per-combo median chunk ÷ market median (subaward_combo_nodes)
    v3_vehicle_dependency parent-PIID $ shares
    v4_edge_frequency     volume/$ profile, repeat depth, task-order share
  flags.concentration_rotation — lifetime vs 60mo top-1 divergence (identity change
    OR >30-point share delta) → the frontend triggers the pivot question instead of
    quoting stale concentration.

  labor — the execution-mechanics assertions (operator-directed 2026-07-06),
  point-lookups only, facts only:
    staffing_identity   per demonstrated combo → SOC roles (rank/class/median wage/
                        growth) from naics_psc_labor_profile_categories, plus the
                        distinct-SOC collapse across the book
    compliance_corridor primary-PoP-county SCA posture (sca_wd_county_rollup) +
                        the sub's own OLMS CBA hits (olms_cba_crosswalk)
    chunk_capacity      per-combo median chunk ÷ loaded cost of the #1
                        core_deliverable SOC → implied FTE-years per subaward
                        (LOAD_FACTOR disclosed; the elicited form variable is
                        capacity_concurrent_starts — the one number FSRS can't know)
    oews_annotation     BLS OEWS location quotient + total employment for the sub's
                        core SOCs in their home metro — the map annotation
                        ("home-turf density")

CORRECTIONS ROUTING. Every assertion block carries an `assertion_key`. When the sub
corrects an assertion on the call, the correction is one ledger append:
    python3 scripts/sub_corrections.py add UEI ASSERTION_KEY DISCLOSED CORRECTED SOURCE
(disclosed value = the JSON in this document under that key). Binary interaction,
no re-computation mid-call.

Usage:
  doppler run -p core-x -c prd -- uv run --no-project --with 'pylance>=8' \
      python3 scripts/emit_entity_vectors.py [--out DIR] UEI [UEI ...]
Output: DIR/{uei}.vectors.json   (default DIR: cwd; the GovCon viewer reads
        ~/Desktop/hq/viewer/public/data/cards/)
"""
from __future__ import annotations

import datetime as dt
import json
import os
import statistics
import sys

import lance

A = "s3://data-sink/active"
ROTATION_PCT_POINTS = 30.0
LOAD_FACTOR = 1.5          # loaded-cost multiplier over OEWS median salary (disclosed)
TOP_COMBOS_FOR_LABOR = 5   # labor lookups run for the top-N combos by lifetime sub $


def so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


S = so()
_NODES = None


def _ds(name: str):
    return lance.dataset(f"{A}/{name}/", storage_options=S)


def market_median(naics: str, psc: str) -> float | None:
    global _NODES
    if _NODES is None:
        _NODES = {(r["naics"], r["psc"]): float(r["median_subaward_amt"])
                  for r in _ds("subaward_combo_nodes")
                  .scanner(columns=["naics", "psc", "median_subaward_amt"]).to_table().to_pylist()
                  if r["median_subaward_amt"] is not None}
    return _NODES.get((naics, psc))


def _r(v: float, nd: int = 2) -> float:
    return round(v, nd)


def _num(v) -> float | None:
    """BLS/EP fields carry suppression markers ('*', '#', ''); parse or None."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ── The four FSRS vectors over one window ──────────────────────────────────────
def vectors_for(rows: list[dict], window: str) -> dict:
    n = len(rows)
    total = sum(r["amt"] for r in rows)
    if n == 0 or total == 0:
        return {"window": window, "n_edges": n, "total_sub_usd": _r(total),
                "v1_concentration": None, "v2_chunk_delta_by_combo": [],
                "v3_vehicle_dependency": None, "v4_edge_frequency": None}

    by_prime: dict = {}
    for r in rows:
        k = (r["prime_uei"], r["prime_name"])
        by_prime[k] = by_prime.get(k, 0.0) + r["amt"]
    top = sorted(by_prime.items(), key=lambda kv: -kv[1])
    v1 = {"n_distinct_primes": len(by_prime),
          "top1": {"prime_uei": top[0][0][0], "prime_name": top[0][0][1],
                   "usd": _r(top[0][1]), "pct_of_total": _r(100 * top[0][1] / total, 1)},
          "top3_pct_of_total": _r(100 * sum(v for _, v in top[:3]) / total, 1),
          "top3": [{"prime_uei": k[0], "prime_name": k[1], "usd": _r(v),
                    "pct": _r(100 * v / total, 1)} for k, v in top[:3]]}

    by_combo: dict = {}
    for r in rows:
        if r["naics"] and r["psc"]:
            by_combo.setdefault((r["naics"], r["psc"]), []).append(r["amt"])
    v2 = []
    for k, amts in sorted(by_combo.items(), key=lambda kv: (-len(kv[1]), -sum(kv[1]))):
        mkt = market_median(k[0], k[1])
        med = statistics.median(amts)
        v2.append({"combo": f"{k[0]}x{k[1]}", "n_edges": len(amts),
                   "target_median_usd": _r(med), "target_total_usd": _r(sum(amts)),
                   "market_median_usd": mkt,
                   "chunk_delta": _r(med / mkt) if mkt else None})

    by_veh: dict = {}
    for r in rows:
        by_veh[r["vehicle"] or "__NO_PARENT__"] = by_veh.get(r["vehicle"] or "__NO_PARENT__", 0.0) + r["amt"]
    tv = sorted(((k, v) for k, v in by_veh.items() if k != "__NO_PARENT__"), key=lambda kv: -kv[1])
    v3 = {"n_distinct_parent_piids": len(tv),
          "no_parent_pct": _r(100 * by_veh.get("__NO_PARENT__", 0.0) / total, 1),
          "top_vehicle": None if not tv else
          {"parent_piid": tv[0][0], "usd": _r(tv[0][1]), "pct_of_total": _r(100 * tv[0][1] / total, 1)},
          "top3_vehicles_pct": _r(100 * sum(v for _, v in tv[:3]) / total, 1)}

    amts = sorted(r["amt"] for r in rows)
    dates = sorted(r["date"] for r in rows)
    years = max((dt.date.fromisoformat(dates[-1]) - dt.date.fromisoformat(dates[0])).days / 365.25, 1 / 365.25)
    edges_per_prime: dict = {}
    for r in rows:
        edges_per_prime[r["prime_name"]] = edges_per_prime.get(r["prime_name"], 0) + 1
    deepest = max(edges_per_prime, key=lambda k: edges_per_prime[k])
    q = statistics.quantiles(amts, n=4) if n >= 2 else [amts[0], amts[0], amts[0]]
    v4 = {"n_edges": n, "first_edge": dates[0], "last_edge": dates[-1],
          "edges_per_year": _r(n / years),
          "median_edge_usd": _r(statistics.median(amts)),
          "mean_edge_usd": _r(statistics.mean(amts)),
          "p25_usd": _r(q[0]), "p75_usd": _r(q[2]),
          "pct_edges_below_100k": _r(100 * sum(1 for a in amts if a < 100_000) / n, 1),
          "pct_dollars_in_edges_above_1m": _r(100 * sum(a for a in amts if a >= 1_000_000) / total, 1),
          "deepest_repeat_prime": {"prime_name": deepest, "n_edges": edges_per_prime[deepest]},
          "pct_awards_task_order_type": _r(
              100 * sum(1 for r in rows if (r["type"] or "").strip() in ("A", "C", "IDV_B_A", "IDV_B_B")) / n, 1)}

    return {"window": window, "n_edges": n, "total_sub_usd": _r(total),
            "v1_concentration": v1, "v2_chunk_delta_by_combo": v2,
            "v3_vehicle_dependency": v3, "v4_edge_frequency": v4}


# ── The labor block ────────────────────────────────────────────────────────────
def labor_block(uei: str, rows: list[dict], life: dict, entity: dict | None) -> dict:
    """Traps 1/3/4 + OEWS annotation. Point-lookups only; absent facts are absent,
    never invented."""
    # top demonstrated combos by lifetime $
    by_combo: dict = {}
    for r in rows:
        if r["naics"] and r["psc"]:
            k = (r["naics"], r["psc"])
            by_combo[k] = by_combo.get(k, 0.0) + r["amt"]
    top_combos = [k for k, _ in sorted(by_combo.items(), key=lambda kv: -kv[1])[:TOP_COMBOS_FOR_LABOR]]

    # Trap 1 — staffing identity: SOC roles per combo
    staffing = {"assertion_key": "labor.staffing_identity", "combos": [], "distinct_soc_codes": []}
    socs: dict = {}
    if top_combos:
        pred = " OR ".join(f"(naics_code = '{n}' AND psc_code = '{p}')" for n, p in top_combos)
        cats = _ds("naics_psc_labor_profile_categories").scanner(
            filter=pred,
            columns=["naics_code", "psc_code", "rank", "soc_code", "soc_title",
                     "role_class", "a_median", "ep_growth_2024_2034_pct", "sca_code"]).to_table().to_pylist()
        for k in top_combos:
            mine = sorted((c for c in cats if (c["naics_code"], c["psc_code"]) == k),
                          key=lambda c: c["rank"])
            staffing["combos"].append({
                "combo": f"{k[0]}x{k[1]}", "sub_usd_lifetime": _r(by_combo[k]),
                "roles": [{"rank": c["rank"], "soc_code": c["soc_code"],
                           "soc_title": c["soc_title"], "role_class": c["role_class"],
                           "a_median": _num(c["a_median"]),
                           "growth_2024_2034_pct": _num(c["ep_growth_2024_2034_pct"]),
                           "sca_code": c["sca_code"]} for c in mine]})
            for c in mine:
                if c["soc_code"]:
                    socs.setdefault(c["soc_code"], c)
    staffing["distinct_soc_codes"] = sorted(socs)

    # primary PoP county fips: modal by $ over the sub's own edges
    by_fips: dict = {}
    for r in rows:
        f = (r.get("pop_county_fips") or "").strip()
        if f:
            by_fips[f] = by_fips.get(f, 0.0) + r["amt"]
    county_fips = max(by_fips, key=by_fips.get) if by_fips else None

    # Trap 3 — compliance corridor: county SCA posture + own CBA hits
    compliance = {"assertion_key": "labor.compliance_corridor",
                  "primary_pop_county_fips": county_fips,
                  "county_fips_basis": "modal subaward PoP county by $" if county_fips else "no county fips on edges",
                  "sca": None, "cba_hits": []}
    if county_fips:
        cr = _ds("sca_wd_county_rollup").scanner(
            filter=f"county_fips = '{county_fips}'").to_table().to_pylist()
        if cr:
            compliance["sca"] = {"has_sca_coverage": bool(cr[0]["has_sca_coverage"]),
                                 "wd_count": cr[0]["wd_count"],
                                 "canonical_wd_id": cr[0]["canonical_wd_id"]}
    cba = _ds("olms_cba_crosswalk").scanner(filter=f"uei = '{uei}'").to_table().to_pylist()
    compliance["cba_hits"] = [{"emp_name": r["emp_name"], "union_name": r["union_name"],
                               "exp_date": str(r["exp_date"] or ""), "is_active": bool(r["is_active"]),
                               "tier": r["tier"]} for r in cba]

    # Trap 4 — chunk → implied FTE-years per combo
    capacity = {"assertion_key": "labor.chunk_capacity", "load_factor": LOAD_FACTOR,
                "elicit_variable": "capacity_concurrent_starts", "combos": []}
    med_by_combo = {c["combo"]: c["target_median_usd"]
                    for c in life.get("v2_chunk_delta_by_combo", [])}
    for entry in staffing["combos"]:
        core = next((x for x in entry["roles"] if x["role_class"] == "core_deliverable"), None) \
            or (entry["roles"][0] if entry["roles"] else None)
        med = med_by_combo.get(entry["combo"])
        if core and core["a_median"] is not None and med:
            loaded = core["a_median"] * LOAD_FACTOR
            capacity["combos"].append({
                "combo": entry["combo"], "median_chunk_usd": med,
                "basis_soc": core["soc_code"], "basis_soc_title": core["soc_title"],
                "soc_median_salary": core["a_median"], "loaded_cost_per_fte_year": _r(loaded),
                "implied_fte_years_per_subaward": _r(med / loaded)})

    # Trap 2 — OEWS home-metro annotation for the core SOCs
    oews = {"assertion_key": "labor.oews_annotation", "area_match_basis": None, "rows": []}
    city = (entity or {}).get("physical_city")
    state = (entity or {}).get("physical_state")
    if city and socs:
        title = city.strip().title()
        soc_pred = " OR ".join(f"occ_code = '{s}'" for s in sorted(socs))
        hits = _ds("bls_oews_2025").scanner(
            filter=f"area_title LIKE '{title}%' AND ({soc_pred})",
            columns=["area_title", "prim_state", "naics_title", "occ_code", "occ_title",
                     "tot_emp", "jobs_1000", "loc_quotient"]).to_table().to_pylist()
        hits = [h for h in hits if h["naics_title"] == "Cross-industry"
                and (not state or h["prim_state"] == state)]
        oews["area_match_basis"] = f"area_title LIKE '{title}%' (physical_city), cross-industry rows"
        for h in hits:
            oews["rows"].append({"area_title": h["area_title"], "soc_code": h["occ_code"],
                                 "soc_title": h["occ_title"], "tot_emp": _num(h["tot_emp"]),
                                 "jobs_per_1000": _num(h["jobs_1000"]),
                                 "location_quotient": _num(h["loc_quotient"])})

    return {"staffing_identity": staffing, "compliance_corridor": compliance,
            "chunk_capacity": capacity, "oews_annotation": oews}


# ── Emit ───────────────────────────────────────────────────────────────────────
def emit(uei: str) -> dict:
    raw = _ds("usaspending_subaward_canonical").scanner(
        filter=f"subawardee_uei = '{uei}'",
        columns=["subaward_amount", "subaward_action_date", "prime_awardee_uei",
                 "prime_awardee_name", "prime_award_naics_code",
                 "prime_award_product_or_service_code", "prime_award_parent_piid",
                 "prime_award_type", "place_of_perform_county_fips"]).to_table().to_pylist()
    rows = [{"amt": float(r["subaward_amount"] or 0),
             "date": str(r["subaward_action_date"])[:10],
             "prime_uei": r["prime_awardee_uei"], "prime_name": r["prime_awardee_name"],
             "naics": r["prime_award_naics_code"],
             "psc": r["prime_award_product_or_service_code"],
             "vehicle": (r["prime_award_parent_piid"] or "").strip() or None,
             "type": r["prime_award_type"],
             "pop_county_fips": r["place_of_perform_county_fips"]}
            for r in raw if r["subaward_action_date"] is not None]
    ent = _ds("gtm_audience_entities").scanner(
        filter=f"uei = '{uei}'",
        columns=["uei", "legal_business_name", "physical_city", "physical_state"]).to_table().to_pylist()
    entity = ent[0] if ent else None

    cutoff = (dt.date.today() - dt.timedelta(days=1826)).isoformat()
    life = vectors_for(rows, "lifetime")
    w60 = vectors_for([r for r in rows if r["date"] >= cutoff], "60mo")

    rotation = False
    evidence = None
    l1 = (life.get("v1_concentration") or {}).get("top1") if life.get("v1_concentration") else None
    w1 = (w60.get("v1_concentration") or {}).get("top1") if w60.get("v1_concentration") else None
    if l1 and w1:
        identity_changed = l1["prime_uei"] != w1["prime_uei"]
        pct_delta = abs(l1["pct_of_total"] - w1["pct_of_total"])
        rotation = identity_changed or pct_delta > ROTATION_PCT_POINTS
        evidence = {"lifetime_top1": {"prime_name": l1["prime_name"], "pct": l1["pct_of_total"]},
                    "trailing60_top1": {"prime_name": w1["prime_name"], "pct": w1["pct_of_total"]},
                    "top1_identity_changed": identity_changed,
                    "top1_pct_delta_points": _r(pct_delta, 1)}
    elif l1 and not w1:
        rotation = True
        evidence = {"lifetime_top1": {"prime_name": l1["prime_name"], "pct": l1["pct_of_total"]},
                    "trailing60_top1": None, "note": "no sub edges in trailing 60mo"}

    return {"uei": uei,
            "entity_name": (entity or {}).get("legal_business_name"),
            "generated_at": dt.date.today().isoformat(),
            "meta": {"source": "usaspending_subaward_canonical",
                     "market_median_source": "subaward_combo_nodes (lifetime market grain)",
                     "labor_sources": ["naics_psc_labor_profile_categories",
                                       "sca_wd_county_rollup", "olms_cba_crosswalk",
                                       "bls_oews_2025", "gtm_audience_entities"],
                     "trailing_window_cutoff": cutoff,
                     "rotation_rule": f"top1 identity change OR |top1 pct delta| > {ROTATION_PCT_POINTS} points",
                     "corrections_routing":
                         "on correction: scripts/sub_corrections.py add UEI ASSERTION_KEY "
                         "DISCLOSED CORRECTED SOURCE (disclosed = this document's value at that key)"},
            "flags": {"concentration_rotation": rotation,
                      "concentration_rotation_evidence": evidence},
            "windows": {"lifetime": life, "trailing_60mo": w60},
            "labor": labor_block(uei, rows, life, entity)}


def main() -> None:
    args = sys.argv[1:]
    out_dir = os.getcwd()
    if "--out" in args:
        i = args.index("--out")
        out_dir = os.path.expanduser(args[i + 1])
        args = args[:i] + args[i + 2:]
    ueis = [u.strip().upper() for u in args if u.strip() and not u.startswith("-")]
    if not ueis:
        print("usage: emit_entity_vectors.py [--out DIR] UEI [UEI ...]", file=sys.stderr)
        sys.exit(2)
    os.makedirs(out_dir, exist_ok=True)
    for uei in ueis:
        doc = emit(uei)
        path = os.path.join(out_dir, f"{uei}.vectors.json")
        with open(path, "w") as f:
            json.dump(doc, f, indent=1, default=str)
        lab = doc["labor"]
        print(f"{uei} ({doc['entity_name']}): edges life={doc['windows']['lifetime']['n_edges']} "
              f"60mo={doc['windows']['trailing_60mo']['n_edges']} · "
              f"rotation={doc['flags']['concentration_rotation']} · "
              f"socs={len(lab['staffing_identity']['distinct_soc_codes'])} · "
              f"cba_hits={len(lab['compliance_corridor']['cba_hits'])} · "
              f"oews_rows={len(lab['oews_annotation']['rows'])} · {path}")


if __name__ == "__main__":
    main()
