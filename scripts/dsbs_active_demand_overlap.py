"""Active Demand × DSBS Supply — Opportunity Density Audit (READ-ONLY).

Intersects ACTIVE prime contracts legally obligated to subcontract (FAR 52.219-9)
against the historically-proven (NAICS, PSC) execution footprint of the SBA/DSBS
small-business cohort — as outright PRIMES (all-time FPDS canonical spine) and as
SUBAWARDEES (2021+ FSRS subaward universe).

DEFINITIONS (corrected vs. the source directive):
  * "active subcontracting demand" = govcon_active_awards WHERE
      has_subcontracting_plan (subcontracting_plan_code IN C,D,E,F,G,H) AND
      (active_current OR active_potential).  [directive's `subcontract_plan IS NOT NULL`
      would admit code B = "plan not required" (~840k txns) and is semantically wrong.]
  * dollar pool measured at AWARD grain via total_dollars_obligated (no txn double-count).
  * "proven competency" = a (NAICS, PSC) combo the firm has ACTUALLY EXECUTED (primed or
      subbed) — NOT capability_lanes (a fwd-looking recommender that mixes inferred hops
      + SAM-declared-never-executed lanes and caps at 10/firm).

OUTPUTS (reports/dsbs_overlap/): summary.json + parquet artifacts + console report.
NO Lance writes. NO materialization.

    cd /Users/benjamincrane/core-x && doppler run -p core-x -c prd -- \
      .venv/bin/python .claude/worktrees/adoring-turing-bbe45f/scripts/dsbs_active_demand_overlap.py
"""
from __future__ import annotations

import json
import os
import time

import duckdb
import lance

A = "s3://data-sink/active"
OUTDIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "reports", "dsbs_overlap")
DUCK_MEM = os.environ.get("DUCK_MEM", "16GB")
SPILL = "/tmp/_dsbs_overlap_spill"


def so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("no R2 endpoint")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def ds(name):
    return lance.dataset(f"{A}/{name}/", storage_options=so())


def reader(name, cols):
    return ds(name).scanner(columns=cols).to_reader()


def main() -> None:
    t0 = time.time()
    os.makedirs(OUTDIR, exist_ok=True)
    os.makedirs(SPILL, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{SPILL}'")
    con.execute("SET preserve_insertion_order=false")

    def step(msg):
        print(f"[{time.time()-t0:6.1f}s] {msg}", flush=True)

    # ────────────────────────── DSBS roster ──────────────────────────
    step("load DSBS roster")
    con.register("_dsbs", reader("sba_dsbs_certified_firms", [
        "uei", "legal_business_name", "state", "cert_programs",
        "active_8a_boolean", "active_hz_boolean", "active_wosb_boolean",
        "active_edwosb_boolean", "active_sdvosb_boolean", "active_vosb_boolean",
        "active_8a_jv_boolean", "active_sdvosb_jv_boolean", "active_vosb_jv_boolean"]))
    con.execute("""CREATE TABLE dsbs AS
        SELECT upper(trim(uei)) uei,
               any_value(legal_business_name) firm_name,
               any_value(state) state,
               bool_or(coalesce(active_8a_boolean,FALSE) OR coalesce(active_8a_jv_boolean,FALSE)) is_8a,
               bool_or(coalesce(active_hz_boolean,FALSE)) is_hubzone,
               bool_or(coalesce(active_wosb_boolean,FALSE)) is_wosb,
               bool_or(coalesce(active_edwosb_boolean,FALSE)) is_edwosb,
               bool_or(coalesce(active_sdvosb_boolean,FALSE) OR coalesce(active_sdvosb_jv_boolean,FALSE)) is_sdvosb,
               bool_or(coalesce(active_vosb_boolean,FALSE) OR coalesce(active_vosb_jv_boolean,FALSE)) is_vosb
        FROM _dsbs WHERE uei IS NOT NULL AND trim(uei)<>'' GROUP BY 1""")
    con.unregister("_dsbs")
    n_dsbs = con.execute("SELECT count(*) FROM dsbs").fetchone()[0]
    step(f"  DSBS distinct UEIs = {n_dsbs:,}")

    # ────────────────────────── DEMAND ledger ──────────────────────────
    step("load DEMAND (govcon_active_awards, filtered)")
    con.register("_gaa", reader("govcon_active_awards", [
        "contract_award_unique_key", "recipient_uei", "recipient_name", "recipient_parent_uei",
        "naics_code", "naics_description", "psc_code", "psc_description",
        "total_dollars_obligated", "potential_total_value_of_award", "current_total_value_of_award",
        "has_subcontracting_plan", "active_current", "active_potential",
        "subcontracting_plan_code", "business_size", "type_of_set_aside", "pop_state_code"]))
    con.execute("""CREATE TABLE demand AS
        SELECT
            trim(contract_award_unique_key) cauk,
            upper(trim(recipient_uei)) recipient_uei, recipient_name,
            upper(trim(recipient_parent_uei)) recipient_parent_uei,
            trim(naics_code) naics, upper(trim(psc_code)) psc,
            trim(naics_code)||'|'||upper(trim(psc_code)) combo,
            any_value(naics_description) OVER (PARTITION BY trim(naics_code)) naics_desc,
            any_value(psc_description) OVER (PARTITION BY upper(trim(psc_code))) psc_desc,
            coalesce(TRY_CAST(total_dollars_obligated AS DOUBLE),0.0) obligated,
            coalesce(TRY_CAST(potential_total_value_of_award AS DOUBLE),0.0) potential,
            greatest(coalesce(TRY_CAST(potential_total_value_of_award AS DOUBLE),0.0)
                     - coalesce(TRY_CAST(total_dollars_obligated AS DOUBLE),0.0), 0.0) unspent,
            coalesce(active_current,FALSE) active_current,
            subcontracting_plan_code, business_size, type_of_set_aside, pop_state_code
        FROM _gaa
        WHERE coalesce(has_subcontracting_plan,FALSE)
          AND (coalesce(active_current,FALSE) OR coalesce(active_potential,FALSE))
          AND naics_code IS NOT NULL AND trim(naics_code)<>''
          AND psc_code IS NOT NULL AND trim(psc_code)<>''""")
    con.unregister("_gaa")
    # de-dup guard: award grain must be unique
    n_dem, n_dem_dist = con.execute(
        "SELECT count(*), count(DISTINCT cauk) FROM demand").fetchone()
    step(f"  demand awards = {n_dem:,} (distinct cauk = {n_dem_dist:,})")
    assert n_dem == n_dem_dist, "demand grain not unique on cauk"

    # demand combo rollup
    con.execute("""CREATE TABLE demand_combo AS
        SELECT combo, any_value(naics) naics, any_value(psc) psc,
               any_value(naics_desc) naics_desc, any_value(psc_desc) psc_desc,
               count(*) n_awards, count(DISTINCT recipient_uei) n_primes,
               sum(obligated) obligated, sum(potential) potential, sum(unspent) unspent
        FROM demand GROUP BY combo""")
    tot = con.execute("""SELECT count(*) combos, sum(n_awards) awards,
        sum(obligated) obl, sum(potential) pot, sum(unspent) uns FROM demand_combo""").fetchone()
    step(f"  demand combos={tot[0]:,} awards={tot[1]:,} "
         f"obl=${tot[2]/1e9:,.1f}B pot=${tot[3]/1e9:,.1f}B unspent=${tot[4]/1e9:,.1f}B")

    # ────────────────────────── SUPPLY: proven-as-sub ──────────────────────────
    step("load SUPPLY proven-as-SUB (subaward_naics_psc_wide)")
    con.register("_sub", reader("subaward_naics_psc_wide",
                                ["subawardee_uei", "prime_naics_code", "prime_psc_code"]))
    con.execute("""CREATE TABLE sub_proven AS
        SELECT DISTINCT upper(trim(subawardee_uei)) uei,
               trim(prime_naics_code)||'|'||upper(trim(prime_psc_code)) combo
        FROM _sub
        WHERE subawardee_uei IS NOT NULL AND trim(subawardee_uei)<>''
          AND prime_naics_code IS NOT NULL AND trim(prime_naics_code)<>''
          AND prime_psc_code IS NOT NULL AND trim(prime_psc_code)<>''
          AND upper(trim(subawardee_uei)) IN (SELECT uei FROM dsbs)""")
    con.unregister("_sub")
    n_sub = con.execute("SELECT count(*) FROM sub_proven").fetchone()[0]
    step(f"  DSBS proven-as-sub (uei,combo) pairs = {n_sub:,}")

    # ────────────────────── SUPPLY: proven-as-prime (108M spine) ──────────────────────
    step("scan SPINE (108M) for DSBS proven-as-PRIME combos ... [heavy]")
    con.register("_spine", reader("usaspending_fpds_canonical_txn",
                                  ["recipient_uei", "naics_code", "product_or_service_code"]))
    con.execute("""CREATE TABLE prime_proven AS
        SELECT DISTINCT upper(trim(recipient_uei)) uei,
               trim(naics_code)||'|'||upper(trim(product_or_service_code)) combo
        FROM _spine
        WHERE recipient_uei IS NOT NULL AND trim(recipient_uei)<>''
          AND naics_code IS NOT NULL AND trim(naics_code)<>''
          AND product_or_service_code IS NOT NULL AND trim(product_or_service_code)<>''
          AND upper(trim(recipient_uei)) IN (SELECT uei FROM dsbs)""")
    con.unregister("_spine")
    n_prime = con.execute("SELECT count(*) FROM prime_proven").fetchone()[0]
    step(f"  DSBS proven-as-prime (uei,combo) pairs = {n_prime:,}")

    # ────────────────────────── unified DSBS proven footprint ──────────────────────────
    step("union proven footprint (provenance-tagged)")
    con.execute("""CREATE TABLE dsbs_proven AS
        SELECT uei, combo,
               bool_or(src='prime') proven_prime,
               bool_or(src='sub')   proven_sub
        FROM (SELECT uei, combo, 'prime' src FROM prime_proven
              UNION ALL
              SELECT uei, combo, 'sub'  src FROM sub_proven)
        GROUP BY uei, combo""")
    fp = con.execute("""SELECT count(*) pairs, count(DISTINCT uei) firms, count(DISTINCT combo) combos,
        count(*) FILTER(WHERE proven_prime) via_prime, count(*) FILTER(WHERE proven_sub) via_sub,
        count(*) FILTER(WHERE proven_prime AND proven_sub) via_both FROM dsbs_proven""").fetchone()
    step(f"  proven footprint: {fp[0]:,} (uei,combo) · {fp[1]:,} firms · {fp[2]:,} combos "
         f"[prime={fp[3]:,} sub={fp[4]:,} both={fp[5]:,}]")

    # supply per demand-combo (only combos that appear in demand matter for overlap)
    con.execute("""CREATE TABLE supply_combo AS
        SELECT p.combo,
               count(DISTINCT p.uei) n_dsbs_firms,
               count(DISTINCT CASE WHEN p.proven_prime THEN p.uei END) n_dsbs_primed,
               count(DISTINCT CASE WHEN p.proven_sub   THEN p.uei END) n_dsbs_subbed
        FROM dsbs_proven p
        WHERE p.combo IN (SELECT combo FROM demand_combo)
        GROUP BY p.combo""")

    # ────────────────────────── OVERLAP ──────────────────────────
    step("compute overlap (Layers 1-3 + enrichments)")
    con.execute("""CREATE TABLE overlap AS
        SELECT d.*, s.n_dsbs_firms, s.n_dsbs_primed, s.n_dsbs_subbed,
               d.obligated / nullif(s.n_dsbs_firms,0) obl_per_firm
        FROM demand_combo d JOIN supply_combo s ON d.combo = s.combo""")

    # ---- Layer 1: macro ----
    L1 = con.execute("""
        SELECT
          (SELECT count(*) FROM demand_combo) demand_combos,
          (SELECT count(*) FROM overlap)      addressable_combos,
          (SELECT sum(n_awards) FROM demand_combo) demand_awards,
          (SELECT sum(n_awards) FROM overlap)      addressable_awards,
          (SELECT sum(obligated) FROM demand_combo) demand_obl,
          (SELECT sum(obligated) FROM overlap)      addressable_obl,
          (SELECT sum(potential) FROM demand_combo) demand_pot,
          (SELECT sum(potential) FROM overlap)      addressable_pot,
          (SELECT sum(unspent) FROM demand_combo)   demand_unspent,
          (SELECT sum(unspent) FROM overlap)        addressable_unspent
    """).fetchdf().iloc[0].to_dict()
    L1 = {k: (float(v) if v is not None else 0.0) for k, v in L1.items()}
    step("  Layer-1 macro:")
    print(f"    addressable combos : {int(L1['addressable_combos']):,} / {int(L1['demand_combos']):,} "
          f"= {L1['addressable_combos']/L1['demand_combos']:.1%}")
    print(f"    addressable awards : {int(L1['addressable_awards']):,} / {int(L1['demand_awards']):,} "
          f"= {L1['addressable_awards']/L1['demand_awards']:.1%}")
    print(f"    addressable OBLIG  : ${L1['addressable_obl']/1e9:,.1f}B / ${L1['demand_obl']/1e9:,.1f}B "
          f"= {L1['addressable_obl']/L1['demand_obl']:.1%}")
    print(f"    addressable POTENT : ${L1['addressable_pot']/1e9:,.1f}B / ${L1['demand_pot']/1e9:,.1f}B "
          f"= {L1['addressable_pot']/L1['demand_pot']:.1%}")
    print(f"    addressable UNSPENT: ${L1['addressable_unspent']/1e9:,.1f}B / ${L1['demand_unspent']/1e9:,.1f}B "
          f"= {L1['addressable_unspent']/L1['demand_unspent']:.1%}")

    # ---- Layer 2: combo density (top by obligated $) ----
    top_combos = con.execute("""
        SELECT combo, naics, psc, substr(naics_desc,1,44) naics_desc, substr(psc_desc,1,40) psc_desc,
               n_awards, n_primes, obligated, potential, unspent,
               n_dsbs_firms, n_dsbs_primed, n_dsbs_subbed, obl_per_firm
        FROM overlap ORDER BY obligated DESC LIMIT 40""").fetchdf()

    # ---- Layer 3: entity saturation ----
    step("  Layer-3 entity saturation")
    con.execute("""CREATE TABLE dsbs_addr AS
        SELECT uei, combo FROM dsbs_proven WHERE combo IN (SELECT combo FROM overlap)""")
    con.execute("""CREATE TABLE entity_targets AS
        SELECT a.uei, count(DISTINCT d.cauk) n_active_targets,
               sum(x.obl) addressable_obl
        FROM dsbs_addr a
        JOIN demand d ON a.combo = d.combo
        JOIN (SELECT cauk, any_value(obligated) obl FROM demand GROUP BY cauk) x ON x.cauk = d.cauk
        GROUP BY a.uei""")
    sat = con.execute("""
        SELECT
          count(*) FILTER (WHERE n_active_targets = 1)                 b1,
          count(*) FILTER (WHERE n_active_targets BETWEEN 2 AND 5)     b2_5,
          count(*) FILTER (WHERE n_active_targets BETWEEN 6 AND 20)    b6_20,
          count(*) FILTER (WHERE n_active_targets > 20)                b21p,
          count(*)                                                     firms_ge1,
          max(n_active_targets)                                        max_targets,
          round(avg(n_active_targets),1)                              avg_targets,
          round(median(n_active_targets),1)                           med_targets
        FROM entity_targets""").fetchdf().iloc[0].to_dict()
    dsbs_with_proven = con.execute("SELECT count(DISTINCT uei) FROM dsbs_proven").fetchone()[0]
    step(f"    DSBS firms with any active target: {int(sat['firms_ge1']):,} "
         f"(of {dsbs_with_proven:,} with proven history; {n_dsbs:,} rostered)")
    print(f"    saturation  1:{int(sat['b1']):,}  2-5:{int(sat['b2_5']):,}  "
          f"6-20:{int(sat['b6_20']):,}  21+:{int(sat['b21p']):,}  "
          f"max={int(sat['max_targets'])} avg={sat['avg_targets']} med={sat['med_targets']}")

    # ---- Enrichment A: top active primes to approach (DSBS-addressable $) ----
    top_primes = con.execute("""
        SELECT d.recipient_uei, any_value(d.recipient_name) prime_name,
               count(DISTINCT d.cauk) n_awards,
               count(DISTINCT d.combo) n_addr_combos,
               sum(d.obligated) obligated, sum(d.unspent) unspent
        FROM demand d WHERE d.combo IN (SELECT combo FROM overlap)
        GROUP BY d.recipient_uei ORDER BY obligated DESC LIMIT 30""").fetchdf()

    # ---- Enrichment B: cert-cohort lens ----
    # firm_addr_award: distinct (DSBS firm, reachable active award, that award's obligated $)
    con.execute("""CREATE TABLE firm_addr_award AS
        SELECT DISTINCT a.uei, d.cauk, x.obl
        FROM dsbs_addr a JOIN demand d ON a.combo=d.combo
        JOIN (SELECT cauk, any_value(obligated) obl FROM demand GROUP BY cauk) x ON x.cauk=d.cauk""")
    cohort_rows = []
    for name, col in [("8a", "is_8a"), ("hubzone", "is_hubzone"), ("wosb", "is_wosb"),
                      ("edwosb", "is_edwosb"), ("sdvosb", "is_sdvosb"), ("vosb", "is_vosb")]:
        firms, targets = con.execute(f"""
            SELECT count(DISTINCT f.uei), count(DISTINCT f.cauk)
            FROM firm_addr_award f JOIN dsbs db ON db.uei=f.uei WHERE db.{col}""").fetchone()
        # exposure: each reachable award counted ONCE (distinct cauk) across the cohort
        exposure = con.execute(f"""
            SELECT coalesce(sum(obl),0) FROM (
              SELECT DISTINCT f.cauk, f.obl
              FROM firm_addr_award f JOIN dsbs db ON db.uei=f.uei WHERE db.{col})""").fetchone()[0]
        cohort_rows.append({"cohort": name, "addressable_firms": int(firms),
                            "distinct_active_targets": int(targets),
                            "target_obl_usd": float(exposure or 0.0)})

    # ────────────────────────── PERSIST ──────────────────────────
    step("persist artifacts")
    con.execute(f"COPY (SELECT * FROM demand_combo ORDER BY obligated DESC) "
                f"TO '{OUTDIR}/demand_combos.parquet' (FORMAT parquet)")
    con.execute(f"COPY (SELECT * FROM overlap ORDER BY obligated DESC) "
                f"TO '{OUTDIR}/overlap_combos.parquet' (FORMAT parquet)")
    con.execute(f"COPY (SELECT * FROM entity_targets ORDER BY n_active_targets DESC) "
                f"TO '{OUTDIR}/entity_saturation.parquet' (FORMAT parquet)")
    con.execute(f"COPY (SELECT * FROM dsbs_proven) TO '{OUTDIR}/dsbs_proven_footprint.parquet' (FORMAT parquet)")
    top_combos.to_parquet(f"{OUTDIR}/top_combos.parquet")
    top_primes.to_parquet(f"{OUTDIR}/top_primes.parquet")

    summary = {
        "generated_epoch": t0,
        "params": {
            "demand_source": "govcon_active_awards",
            "demand_filter": "has_subcontracting_plan AND (active_current OR active_potential) AND naics_code<>'' AND psc_code<>''",
            "dollar_measure": "total_dollars_obligated (award grain)",
            "supply_prime_source": "usaspending_fpds_canonical_txn (all-time)",
            "supply_sub_source": "subaward_naics_psc_wide (2021+ FSRS)",
            "dsbs_roster": "sba_dsbs_certified_firms",
        },
        "roster": {"dsbs_ueis": int(n_dsbs),
                   "dsbs_with_proven_history": int(dsbs_with_proven)},
        "footprint": {"pairs": int(fp[0]), "firms": int(fp[1]), "combos": int(fp[2]),
                      "via_prime": int(fp[3]), "via_sub": int(fp[4]), "via_both": int(fp[5]),
                      "prime_pairs": int(n_prime), "sub_pairs": int(n_sub)},
        "layer1_macro": L1,
        "layer1_pct": {
            "combo_coverage": L1["addressable_combos"] / L1["demand_combos"],
            "award_coverage": L1["addressable_awards"] / L1["demand_awards"],
            "obligated_coverage": L1["addressable_obl"] / L1["demand_obl"],
            "potential_coverage": L1["addressable_pot"] / L1["demand_pot"],
            "unspent_coverage": L1["addressable_unspent"] / L1["demand_unspent"],
        },
        "layer3_saturation": {k: (int(v) if v is not None and k not in
                                  ("avg_targets", "med_targets") else v) for k, v in sat.items()},
        "cohort_lens": cohort_rows,
    }
    with open(f"{OUTDIR}/summary.json", "w") as fh:
        json.dump(summary, fh, indent=2, default=str)

    # console: top 25 combos + top primes + cohort
    import pandas as pd
    pd.set_option("display.width", 240); pd.set_option("display.max_columns", 30)
    print("\n===== LAYER 2 — TOP 25 ADDRESSABLE (NAICS,PSC) BY ACTIVE OBLIGATED $ =====")
    t25 = top_combos.head(25).copy()
    t25["obl_$M"] = (t25["obligated"]/1e6).round(1)
    t25["unspent_$M"] = (t25["unspent"]/1e6).round(1)
    t25["oblPerFirm_$M"] = (t25["obl_per_firm"]/1e6).round(2)
    print(t25[["naics", "psc", "naics_desc", "n_awards", "obl_$M", "unspent_$M",
               "n_dsbs_firms", "n_dsbs_primed", "n_dsbs_subbed", "oblPerFirm_$M"]].to_string(index=False))

    print("\n===== ENRICHMENT A — TOP 15 ACTIVE PRIMES BY DSBS-ADDRESSABLE OBLIGATED $ =====")
    tp = top_primes.head(15).copy()
    tp["obl_$M"] = (tp["obligated"]/1e6).round(1)
    tp["unspent_$M"] = (tp["unspent"]/1e6).round(1)
    print(tp[["recipient_uei", "prime_name", "n_awards", "n_addr_combos", "obl_$M", "unspent_$M"]].to_string(index=False))

    print("\n===== ENRICHMENT B — DSBS CERT-COHORT LENS =====")
    for c in cohort_rows:
        print(f"  {c['cohort']:8s} firms_addressable={c['addressable_firms']:>6,}  "
              f"active_targets={c['distinct_active_targets']:>6,}  "
              f"target_obl=${c['target_obl_usd']/1e9:,.1f}B")

    step(f"DONE — artifacts in {OUTDIR}")
    con.close()


if __name__ == "__main__":
    main()
