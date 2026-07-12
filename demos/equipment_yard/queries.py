"""Equipment-yard presentation — named query registry + snapshot runner.

Every deck section is a NAMED, PARAMETERIZED sidecar query. The only
parameters are the yard anchor (zip → centroid) and radius. Acts 1–2 are
zip-independent (computed once, reusable across prospects); Act 3 takes
(lat, lon, radius). All requests pin `require_artifact` so a mid-run
artifact swap 409s instead of mixing snapshots.

Usage:
    doppler run -p core-x -c prd -- python demos/equipment_yard/queries.py \
        snapshot --zip 79925 --radius 50

Writes snapshot_<zip>.json next to this file; deck.html reads it.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
import urllib.request
from pathlib import Path

SIDECAR = "https://query-sidecar-api.onrender.com/api/v1/sql"
HEALTHZ = "https://query-sidecar-api.onrender.com/healthz"

# Equipment scope = the deterministic heavy-iron slice of the combo verdicts.
EQ_JOIN = ("JOIN naics_psc_equipment_needs e ON {a}.naics_code = e.naics_code "
           "AND {p} = e.psc_code")

# ── the registry ───────────────────────────────────────────────────────────────
# {lat} {lon} {radius} {lat_lo} {lat_hi} {lon_lo} {lon_hi} substituted at run.
HAVERSINE = ("3958.8*2*asin(sqrt(pow(sin(radians(latitude-{lat})/2),2)"
             "+cos(radians({lat}))*cos(radians(latitude))"
             "*pow(sin(radians(longitude-({lon}))/2),2)))")

LOCAL_CTE = (
    "WITH local AS (SELECT a.*, " + HAVERSINE + " AS dist_mi "
    "FROM gtm_open_awards a "
    "WHERE latitude BETWEEN {lat_lo} AND {lat_hi} "
    "AND longitude BETWEEN {lon_lo} AND {lon_hi}) ")

QUERIES: dict[str, str] = {
    # ── resolution ────────────────────────────────────────────────────────────
    "zip_centroid": (
        "SELECT count(*) award_pops, avg(latitude) lat, avg(longitude) lon "
        "FROM usaspending_award_pop_centroids "
        "WHERE state_code = '{state}' AND zip5 = '{zip}'"),

    # ── Act 1 · the national market ───────────────────────────────────────────
    "national_buckets": (
        "SELECT e.primary_bucket, sum(c.active_award_ct) awards, "
        "sum(c.active_recipients) firms, "
        "round(sum(c.active_obligated)/1e9, 2) active_obl_b "
        "FROM combo_award_active_state c "
        + EQ_JOIN.format(a="c", p="c.psc_code") +
        " WHERE e.in_scope GROUP BY 1 ORDER BY active_obl_b DESC"),
    "national_states": (
        "SELECT t.pop_state, round(sum(t.obligation)/1e9, 2) obl_b "
        "FROM txn_events_combo t "
        + EQ_JOIN.format(a="t", p="t.psc_code") +
        " WHERE e.in_scope AND t.fy >= 2024 AND t.pop_state IS NOT NULL "
        "GROUP BY 1 ORDER BY obl_b DESC LIMIT 15"),

    # ── Act 2 · the state zoom ────────────────────────────────────────────────
    "state_counties": (
        "SELECT t.pop_county_name, round(sum(t.obligation)/1e6, 1) obl_m, "
        "count(DISTINCT t.uei) firms "
        "FROM txn_events_combo_by_geo t "
        + EQ_JOIN.format(a="t", p="t.psc_code") +
        " WHERE e.in_scope AND t.pop_state = '{state}' AND t.fy >= 2024 "
        "AND t.pop_county_name IS NOT NULL "
        "GROUP BY 1 ORDER BY obl_m DESC LIMIT 15"),

    # ── Act 3 · the yard (all take lat/lon/radius) ────────────────────────────
    "local_buckets": (
        LOCAL_CTE +
        "SELECT coalesce(e.primary_bucket, 'other_federal_work') bucket, "
        "count(*) awards, count(DISTINCT l.recipient_uei) firms, "
        "round(sum(l.total_obligation)/1e6, 1) obl_m, "
        "round(sum(l.total_subaward_amount)/1e6, 1) subbed_m "
        "FROM local l LEFT JOIN naics_psc_equipment_needs e "
        "ON l.naics_code = e.naics_code AND l.product_or_service_code = e.psc_code "
        "AND e.in_scope "
        "WHERE l.dist_mi <= {radius} GROUP BY 1 ORDER BY obl_m DESC"),
    "local_awards": (
        LOCAL_CTE +
        "SELECT l.recipient_name, l.recipient_uei, l.award_id_piid, "
        "l.naics_code, l.product_or_service_code psc_code, e.primary_bucket, "
        "round(l.total_obligation/1e6, 2) obl_m, l.subaward_count, "
        "l.period_of_performance_current_end_date pop_end, "
        "l.awarding_agency_name, l.type_of_set_aside_code, "
        "round(l.dist_mi, 1) dist_mi, l.latitude, l.longitude, "
        "d.what_was_done "
        "FROM local l "
        "JOIN naics_psc_equipment_needs e ON l.naics_code = e.naics_code "
        "AND l.product_or_service_code = e.psc_code AND e.in_scope "
        "LEFT JOIN naics_psc_deliverable d ON l.naics_code = d.naics_code "
        "AND l.product_or_service_code = d.psc_code "
        "WHERE l.dist_mi <= {radius} ORDER BY l.total_obligation DESC LIMIT 400"),
    "local_expiry": (
        LOCAL_CTE +
        "SELECT strftime(l.period_of_performance_current_end_date, '%Y-%m') end_month, "
        "count(*) awards, round(sum(l.total_obligation)/1e6, 1) obl_m "
        "FROM local l "
        "JOIN naics_psc_equipment_needs e ON l.naics_code = e.naics_code "
        "AND l.product_or_service_code = e.psc_code AND e.in_scope "
        "WHERE l.dist_mi <= {radius} "
        "AND l.period_of_performance_current_end_date "
        "BETWEEN current_date AND current_date + INTERVAL 24 MONTH "
        "GROUP BY 1 ORDER BY 1"),
    "local_subout": (
        LOCAL_CTE +
        "SELECT count(*) awards, "
        "count(*) FILTER (l.subaward_count > 0) awards_with_subs, "
        "round(sum(l.total_subaward_amount)/1e6, 1) subbed_m, "
        "round(sum(l.total_obligation)/1e6, 1) obl_m "
        "FROM local l "
        "JOIN naics_psc_equipment_needs e ON l.naics_code = e.naics_code "
        "AND l.product_or_service_code = e.psc_code AND e.in_scope "
        "WHERE l.dist_mi <= {radius}"),
    "local_fresh_money": (
        LOCAL_CTE +
        ", ueis AS (SELECT DISTINCT l.recipient_uei FROM local l "
        "JOIN naics_psc_equipment_needs e ON l.naics_code = e.naics_code "
        "AND l.product_or_service_code = e.psc_code AND e.in_scope "
        "WHERE l.dist_mi <= {radius} AND l.total_obligation >= 100000) "
        "SELECT t.uei, any_value(l.recipient_name) recipient_name, "
        "count(*) actions, round(sum(t.obligation)/1e6, 2) fresh_obl_m "
        "FROM gtm_txn_events_slim t "
        "JOIN ueis u ON t.uei = u.recipient_uei "
        "JOIN local l ON l.recipient_uei = t.uei "
        "JOIN naics_psc_equipment_needs te ON t.naics_code = te.naics_code "
        "AND t.psc_code = te.psc_code AND te.in_scope "
        "WHERE t.action_date >= current_date - INTERVAL 90 DAY "
        "AND t.obligation > 0 "
        "GROUP BY 1 ORDER BY fresh_obl_m DESC LIMIT 25"),
    "local_work_language": (
        LOCAL_CTE +
        "SELECT l.naics_code, l.product_or_service_code psc_code, "
        "e.primary_bucket, count(*) awards, "
        "round(sum(l.total_obligation)/1e6, 1) obl_m, "
        "any_value(d.what_was_done) what_was_done, "
        "any_value(e.proposed_equipment_needs) equipment_needs "
        "FROM local l "
        "JOIN naics_psc_equipment_needs e ON l.naics_code = e.naics_code "
        "AND l.product_or_service_code = e.psc_code AND e.in_scope "
        "LEFT JOIN naics_psc_deliverable d ON l.naics_code = d.naics_code "
        "AND l.product_or_service_code = d.psc_code "
        "WHERE l.dist_mi <= {radius} "
        "GROUP BY 1, 2, 3 ORDER BY obl_m DESC LIMIT 12"),
}

ACT3 = {k for k in QUERIES if k.startswith("local_")}


def _post(sql: str, token: str, artifact: str | None, limit: int = 1000) -> dict:
    body: dict = {"sql": sql, "limit": limit}
    if artifact:
        body["require_artifact"] = artifact
    req = urllib.request.Request(
        SIDECAR, data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {token}",
                 "content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=180) as r:
        return json.loads(r.read())


def run_query(name: str, params: dict, token: str, artifact: str | None) -> dict:
    sql = QUERIES[name].format(**params)
    t0 = time.time()
    out = _post(sql, token, artifact)
    out["query_name"] = name
    out["wall_ms"] = round((time.time() - t0) * 1000, 1)
    return out


def snapshot(zip5: str, state: str, radius: float, token: str) -> dict:
    with urllib.request.urlopen(HEALTHZ, timeout=30) as r:
        health = json.loads(r.read())
    artifact = health["artifact"]
    print(f"artifact: {artifact}", file=sys.stderr)

    cent = _post(QUERIES["zip_centroid"].format(zip=zip5, state=state),
                 token, artifact)
    pops, lat, lon = cent["rows"][0]
    if not pops:
        raise SystemExit(f"zip {zip5}/{state}: no award PoP centroids — check zip")
    dlat = radius / 68.97                      # deg latitude per radius
    dlon = radius / (69.17 * math.cos(math.radians(lat)))
    params = {"zip": zip5, "state": state, "radius": radius,
              "lat": round(lat, 5), "lon": round(lon, 5),
              "lat_lo": round(lat - dlat, 3), "lat_hi": round(lat + dlat, 3),
              "lon_lo": round(lon - dlon, 3), "lon_hi": round(lon + dlon, 3)}
    print(f"anchor: {zip5} → ({params['lat']}, {params['lon']}), "
          f"{radius} mi, {pops:,} award PoPs in zip", file=sys.stderr)

    out = {"anchor": params, "artifact": artifact,
           "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "results": {}}
    for name in QUERIES:
        if name == "zip_centroid":
            continue
        res = run_query(name, params, token, artifact)
        out["results"][name] = {k: res[k] for k in
                                ("columns", "rows", "row_count", "elapsed_ms")}
        print(f"  {name:<22} {res['row_count']:>5} rows  "
              f"{res['elapsed_ms']:>8.1f} ms", file=sys.stderr)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("snapshot")
    s.add_argument("--zip", required=True)
    s.add_argument("--state", default="TX")
    s.add_argument("--radius", type=float, default=50.0)
    args = ap.parse_args()

    token = os.environ.get("QUERY_SIDECAR_TOKEN")
    if not token:
        raise SystemExit("QUERY_SIDECAR_TOKEN not set — run under "
                         "`doppler run -p core-x -c prd --`")
    snap = snapshot(args.zip, args.state, args.radius, token)
    dest = Path(__file__).parent / f"snapshot_{args.zip}.json"
    dest.write_text(json.dumps(snap))
    print(f"wrote {dest} ({dest.stat().st_size/1e3:.0f} kB)", file=sys.stderr)


if __name__ == "__main__":
    main()
