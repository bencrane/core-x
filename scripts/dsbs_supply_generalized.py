"""Generalized Active-Demand × Supply overlap — SUPPLY IS A FLAGGED UNIVERSE, DSBS IS A FILTER.

Supersedes the DSBS-hard-filtered dsbs_active_demand_overlap.py. The supply side is the full
proven-execution universe (subawardees ∪ DSBS roster), and every (uei, combo) carries queryable
attribute flags — so "DSBS-qualified", "all subawardees", or any cert cohort become WHERE-clause
predicates over ONE artifact instead of a rebuild.

  Universe        = distinct UEIs from subaward_naics_psc_wide  ∪  sba_dsbs_certified_firms
  proven_sub      = firm subbed the (naics,psc) combo           (subaward_naics_psc_wide)
  proven_prime    = firm primed the combo                       (usaspending_fpds_canonical_txn, all-time)
  is_dsbs         = firm ∈ DSBS roster                          (filter, not population)
  is_subawardee   = firm has ≥1 proven sub combo
  is_{8a,hubzone,wosb,edwosb,sdvosb,vosb} = DSBS cert cohort    (roster-derived; FALSE for non-DSBS)

Demand ledger (unchanged): govcon_active_awards WHERE has_subcontracting_plan AND (active_current OR
active_potential), award grain, obligated = total_dollars_obligated.

Emits reports/dsbs_overlap/supply_footprint_flagged.parquet + overlap_combos_flagged.parquet +
generalized_summary.json (population comparison). READ-ONLY; no Lance writes.

    cd /Users/benjamincrane/core-x && doppler run -p core-x -c prd -- \
      .venv/bin/python .claude/worktrees/dsbs-generalize/scripts/dsbs_supply_generalized.py
"""
from __future__ import annotations
import json, os, time
import duckdb, lance

A = "s3://data-sink/active"
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports", "dsbs_overlap")
SPILL = "/tmp/_dsbs_gen_spill"

# population predicates over the flagged footprint (proven_prime OR proven_sub is implicit — every row is proven)
POPULATIONS = {
    "all_proven":     "TRUE",
    "subawardee":     "is_subawardee",
    "dsbs":           "is_dsbs",
    "dsbs_subawardee":"is_dsbs AND is_subawardee",
    "nondsbs_sub":    "is_subawardee AND NOT is_dsbs",
    "8a":             "is_8a",
    "hubzone":        "is_hubzone",
    "wosb":           "is_wosb",
    "edwosb":         "is_edwosb",
    "sdvosb":         "is_sdvosb",
    "vosb":           "is_vosb",
}


def so():
    ep = os.environ.get("R2_ENDPOINT") or (f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com" if os.environ.get("R2_ACCOUNT_ID") else None)
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"], "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": ep, "region": "auto"}


def rdr(n, c, flt=None):
    return lance.dataset(f"{A}/{n}/", storage_options=so()).scanner(columns=c, filter=flt).to_reader()


def main():
    t0 = time.time(); os.makedirs(OUT, exist_ok=True); os.makedirs(SPILL, exist_ok=True)
    con = duckdb.connect(); con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='16GB'"); con.execute(f"SET temp_directory='{SPILL}'")
    con.execute("SET preserve_insertion_order=false")
    def step(m): print(f"[{time.time()-t0:6.1f}s] {m}", flush=True)

    # ── DSBS roster (flags) ──
    con.register("_d", rdr("sba_dsbs_certified_firms", [
        "uei", "active_8a_boolean", "active_8a_jv_boolean", "active_hz_boolean", "active_wosb_boolean",
        "active_edwosb_boolean", "active_sdvosb_boolean", "active_sdvosb_jv_boolean",
        "active_vosb_boolean", "active_vosb_jv_boolean"]))
    con.execute("""CREATE TABLE dsbs AS SELECT upper(trim(uei)) uei,
        bool_or(coalesce(active_8a_boolean,FALSE) OR coalesce(active_8a_jv_boolean,FALSE)) is_8a,
        bool_or(coalesce(active_hz_boolean,FALSE)) is_hubzone,
        bool_or(coalesce(active_wosb_boolean,FALSE)) is_wosb,
        bool_or(coalesce(active_edwosb_boolean,FALSE)) is_edwosb,
        bool_or(coalesce(active_sdvosb_boolean,FALSE) OR coalesce(active_sdvosb_jv_boolean,FALSE)) is_sdvosb,
        bool_or(coalesce(active_vosb_boolean,FALSE) OR coalesce(active_vosb_jv_boolean,FALSE)) is_vosb
        FROM _d WHERE uei IS NOT NULL AND trim(uei)<>'' GROUP BY 1""")
    con.unregister("_d")

    # ── proven-as-SUB (defines subawardee universe) ──
    step("proven-as-sub")
    con.register("_s", rdr("subaward_naics_psc_wide", ["subawardee_uei", "prime_naics_code", "prime_psc_code"]))
    con.execute("""CREATE TABLE sub_proven AS SELECT DISTINCT upper(trim(subawardee_uei)) uei,
        trim(prime_naics_code)||'|'||upper(trim(prime_psc_code)) combo FROM _s
        WHERE subawardee_uei IS NOT NULL AND trim(subawardee_uei)<>''
          AND prime_naics_code IS NOT NULL AND trim(prime_naics_code)<>''
          AND prime_psc_code IS NOT NULL AND trim(prime_psc_code)<>''""")
    con.unregister("_s")
    con.execute("CREATE TABLE universe AS SELECT DISTINCT uei FROM (SELECT uei FROM sub_proven UNION SELECT uei FROM dsbs)")
    nu = con.execute("SELECT count(*) FROM universe").fetchone()[0]
    step(f"  universe (subs ∪ dsbs) = {nu:,}")

    # ── proven-as-PRIME over the WHOLE universe (108M scan, semijoin to 123.8k) ──
    step("proven-as-prime (108M spine, universe semijoin) [heavy]")
    con.register("_p", rdr("usaspending_fpds_canonical_txn", ["recipient_uei", "naics_code", "product_or_service_code"]))
    con.execute("""CREATE TABLE prime_proven AS SELECT DISTINCT upper(trim(recipient_uei)) uei,
        trim(naics_code)||'|'||upper(trim(product_or_service_code)) combo FROM _p
        WHERE recipient_uei IS NOT NULL AND trim(recipient_uei)<>''
          AND naics_code IS NOT NULL AND trim(naics_code)<>''
          AND product_or_service_code IS NOT NULL AND trim(product_or_service_code)<>''
          AND upper(trim(recipient_uei)) IN (SELECT uei FROM universe)""")
    con.unregister("_p")
    step(f"  prime pairs={con.execute('SELECT count(*) FROM prime_proven').fetchone()[0]:,}")

    # ── flagged footprint ──
    step("assemble flagged footprint")
    con.execute("""CREATE TABLE footprint AS
        WITH u AS (
            SELECT uei, combo, bool_or(src='prime') proven_prime, bool_or(src='sub') proven_sub
            FROM (SELECT uei, combo, 'prime' src FROM prime_proven
                  UNION ALL SELECT uei, combo, 'sub' src FROM sub_proven) GROUP BY uei, combo)
        SELECT u.uei, u.combo, u.proven_prime, u.proven_sub,
            (u.uei IN (SELECT uei FROM sub_proven)) is_subawardee,
            (d.uei IS NOT NULL) is_dsbs,
            coalesce(d.is_8a,FALSE) is_8a, coalesce(d.is_hubzone,FALSE) is_hubzone,
            coalesce(d.is_wosb,FALSE) is_wosb, coalesce(d.is_edwosb,FALSE) is_edwosb,
            coalesce(d.is_sdvosb,FALSE) is_sdvosb, coalesce(d.is_vosb,FALSE) is_vosb
        FROM u LEFT JOIN dsbs d ON u.uei = d.uei""")
    fp = con.execute("""SELECT count(*) pairs, count(DISTINCT uei) firms, count(DISTINCT combo) combos,
        count(DISTINCT uei) FILTER(WHERE is_dsbs) dsbs, count(DISTINCT uei) FILTER(WHERE is_subawardee) subs
        FROM footprint""").fetchone()
    step(f"  footprint: {fp[0]:,} pairs · {fp[1]:,} firms · {fp[2]:,} combos [dsbs={fp[3]:,} subs={fp[4]:,}]")
    con.execute(f"COPY footprint TO '{OUT}/supply_footprint_flagged.parquet' (FORMAT parquet)")

    # ── demand ──
    step("demand")
    con.register("_g", rdr("govcon_active_awards", [
        "contract_award_unique_key", "recipient_uei", "recipient_name",
        "naics_code", "naics_description", "psc_code", "psc_description",
        "total_dollars_obligated", "potential_total_value_of_award",
        "has_subcontracting_plan", "active_current", "active_potential"]))
    con.execute("""CREATE TABLE demand AS SELECT trim(contract_award_unique_key) cauk,
        upper(trim(recipient_uei)) recipient_uei, recipient_name,
        trim(naics_code)||'|'||upper(trim(psc_code)) combo,
        any_value(naics_description) OVER (PARTITION BY trim(naics_code)) naics_desc,
        any_value(psc_description) OVER (PARTITION BY upper(trim(psc_code))) psc_desc,
        trim(naics_code) naics, upper(trim(psc_code)) psc,
        coalesce(TRY_CAST(total_dollars_obligated AS DOUBLE),0.0) obligated,
        greatest(coalesce(TRY_CAST(potential_total_value_of_award AS DOUBLE),0.0)-coalesce(TRY_CAST(total_dollars_obligated AS DOUBLE),0.0),0.0) unspent
        FROM _g WHERE coalesce(has_subcontracting_plan,FALSE) AND (coalesce(active_current,FALSE) OR coalesce(active_potential,FALSE))
          AND naics_code IS NOT NULL AND trim(naics_code)<>'' AND psc_code IS NOT NULL AND trim(psc_code)<>''""")
    con.unregister("_g")
    con.execute("""CREATE TABLE demand_combo AS SELECT combo, any_value(naics) naics, any_value(psc) psc,
        any_value(naics_desc) naics_desc, any_value(psc_desc) psc_desc,
        count(*) n_awards, sum(obligated) obligated, sum(unspent) unspent FROM demand GROUP BY combo""")
    dtot = con.execute("SELECT count(*), sum(n_awards), sum(obligated) FROM demand_combo").fetchone()
    demand_obl = dtot[2]
    step(f"  demand combos={dtot[0]:,} awards={int(dtot[1]):,} obligated=${demand_obl/1e9:,.1f}B")

    # ── per-combo supply counts by population (footprint ∩ demand combos) ──
    step("supply per demand-combo (population-tagged)")
    con.execute("""CREATE TABLE supply_combo AS SELECT combo,
        count(DISTINCT uei) n_all,
        count(DISTINCT CASE WHEN is_subawardee THEN uei END) n_sub,
        count(DISTINCT CASE WHEN is_dsbs THEN uei END) n_dsbs,
        count(DISTINCT CASE WHEN is_dsbs AND is_subawardee THEN uei END) n_dsbs_sub,
        count(DISTINCT CASE WHEN is_subawardee AND NOT is_dsbs THEN uei END) n_nondsbs_sub,
        count(DISTINCT CASE WHEN is_8a THEN uei END) n_8a,
        count(DISTINCT CASE WHEN is_hubzone THEN uei END) n_hubzone,
        count(DISTINCT CASE WHEN is_wosb THEN uei END) n_wosb,
        count(DISTINCT CASE WHEN is_sdvosb THEN uei END) n_sdvosb,
        count(DISTINCT CASE WHEN is_vosb THEN uei END) n_vosb
        FROM footprint WHERE combo IN (SELECT combo FROM demand_combo) GROUP BY combo""")
    con.execute(f"""COPY (SELECT d.*, s.n_all, s.n_sub, s.n_dsbs, s.n_dsbs_sub, s.n_nondsbs_sub,
        s.n_8a, s.n_hubzone, s.n_wosb, s.n_sdvosb, s.n_vosb
        FROM demand_combo d JOIN supply_combo s ON d.combo=s.combo ORDER BY d.obligated DESC)
        TO '{OUT}/overlap_combos_flagged.parquet' (FORMAT parquet)""")

    # ── reachability edges (uei, cauk) universe ∩ addressable, flag-tagged — for saturation ──
    step("reachability edges (universe ∩ addressable)")
    con.execute("""CREATE TABLE reach AS
        SELECT f.uei, d.cauk, f.is_subawardee, f.is_dsbs, f.is_8a, f.is_hubzone, f.is_wosb,
               f.is_edwosb, f.is_sdvosb, f.is_vosb
        FROM footprint f JOIN demand d ON f.combo = d.combo""")
    nedges = con.execute("SELECT count(*) FROM reach").fetchone()[0]
    step(f"  reach edges = {nedges:,}")

    # ── population comparison: Layer-1 coverage + Layer-3 saturation ──
    step("population comparison")
    def col_for(pop):
        return {"all_proven": "n_all", "subawardee": "n_sub", "dsbs": "n_dsbs",
                "dsbs_subawardee": "n_dsbs_sub", "nondsbs_sub": "n_nondsbs_sub",
                "8a": "n_8a", "hubzone": "n_hubzone", "wosb": "n_wosb",
                "edwosb": None, "sdvosb": "n_sdvosb", "vosb": "n_vosb"}.get(pop)

    comp = []
    for pop, pred in POPULATIONS.items():
        c = col_for(pop)
        if c:
            cov = con.execute(f"""SELECT count(*) addr_combos, sum(n_awards) addr_awards, sum(obligated) addr_obl, sum(unspent) addr_uns
                FROM (SELECT d.combo, d.n_awards, d.obligated, d.unspent, s.{c} nP
                      FROM demand_combo d JOIN supply_combo s ON d.combo=s.combo) WHERE nP>0""").fetchone()
        else:  # edwosb has no precomputed combo col; derive live
            cov = con.execute("""SELECT count(*), sum(n_awards), sum(obligated), sum(unspent) FROM (
                SELECT d.combo, d.n_awards, d.obligated, d.unspent FROM demand_combo d
                WHERE d.combo IN (SELECT combo FROM footprint WHERE is_edwosb GROUP BY combo))""").fetchone()
        # saturation over reach filtered to population
        sat = con.execute(f"""WITH t AS (SELECT uei, count(DISTINCT cauk) n FROM reach WHERE {pred} GROUP BY uei)
            SELECT count(*) firms, coalesce(median(n),0) med, coalesce(max(n),0) mx,
                   count(*) FILTER(WHERE n=1) b1, count(*) FILTER(WHERE n BETWEEN 2 AND 5) b2, count(*) FILTER(WHERE n>=6) b6
            FROM t""").fetchone()
        comp.append({"population": pop, "predicate": pred,
            "addr_combos": int(cov[0] or 0), "addr_awards": int(cov[1] or 0),
            "addr_obl": float(cov[2] or 0.0), "addr_obl_pct": float((cov[2] or 0)/demand_obl),
            "addr_unspent": float(cov[3] or 0.0),
            "firms_with_target": int(sat[0] or 0), "median_targets": float(sat[1] or 0),
            "max_targets": int(sat[2] or 0), "band_1": int(sat[3] or 0), "band_2_5": int(sat[4] or 0), "band_6p": int(sat[5] or 0)})

    summary = {"universe": int(nu), "footprint_pairs": int(fp[0]), "footprint_firms": int(fp[1]),
               "footprint_combos": int(fp[2]), "footprint_dsbs_firms": int(fp[3]), "footprint_sub_firms": int(fp[4]),
               "demand_combos": int(dtot[0]), "demand_awards": int(dtot[1]), "demand_obl": float(demand_obl),
               "reach_edges": int(nedges), "populations": comp}
    with open(f"{OUT}/generalized_summary.json", "w") as fh:
        json.dump(summary, fh, indent=2, default=str)

    print("\n===== POPULATION COMPARISON — addressability of the $%.0fB / %d-award demand pool =====" % (demand_obl/1e9, dtot[1]))
    print(f"{'population':16s} {'combos':>7s} {'awards':>7s} {'obl$B':>8s} {'obl%':>6s} {'firms':>7s} {'med':>5s} {'1':>6s} {'2-5':>6s} {'6+':>7s}")
    for r in comp:
        print(f"{r['population']:16s} {r['addr_combos']:>7,d} {r['addr_awards']:>7,d} {r['addr_obl']/1e9:>8,.1f} "
              f"{r['addr_obl_pct']*100:>5.1f}% {r['firms_with_target']:>7,d} {r['median_targets']:>5.0f} "
              f"{r['band_1']:>6,d} {r['band_2_5']:>6,d} {r['band_6p']:>7,d}")
    step(f"DONE — {OUT}/generalized_summary.json + supply_footprint_flagged.parquet + overlap_combos_flagged.parquet")
    con.close()


if __name__ == "__main__":
    main()
