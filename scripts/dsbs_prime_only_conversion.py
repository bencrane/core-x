"""DSBS prime-only → subcontract conversion cohort. READ-ONLY; no Lance writes.

Part III of the opportunity-density audit. Isolates DSBS firms with PROVEN PRIME execution
but ZERO subaward history ("prime-only"), then dollar-weights each by the ACTIVE
subcontracting demand sitting in the exact (NAICS,PSC) combos they have already primed —
the "you've primed it, go sub it" conversion cohort.

  prime-only firm  = DSBS ∧ has FPDS prime combo ∧ NOT in FSRS subawardee universe
  conversion-ready = has primed ≥1 combo that now carries active sub-obligated demand
  dollar weight    = active-demand obligated / unspent $ pooled across the firm's primed
                     demand combos (fixes the low-$ catch-all combo-count inflation)

Sources: sba_dsbs_certified_firms, usaspending_fpds_canonical_txn (all-time prime),
subaward_naics_psc_wide (sub universe), govcon_active_awards (active sub-demand).
Emits reports/dsbs_overlap/dsbs_prime_only_conversion.parquet + prime_only_conversion_summary.json.

    cd /Users/benjamincrane/core-x && doppler run -p core-x -c prd -- \
      .venv/bin/python .claude/worktrees/dsbs-part3/scripts/dsbs_prime_only_conversion.py
"""
from __future__ import annotations
import json, os, time
import duckdb, lance

A = "s3://data-sink/active"
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports", "dsbs_overlap")
SPILL = "/tmp/_dsbs_po_spill"


def so():
    ep = os.environ.get("R2_ENDPOINT") or (f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com" if os.environ.get("R2_ACCOUNT_ID") else None)
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"], "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"], "endpoint": ep, "region": "auto"}


def rdr(n, c):
    return lance.dataset(f"{A}/{n}/", storage_options=so()).scanner(columns=c).to_reader()


def main():
    t0 = time.time(); os.makedirs(OUT, exist_ok=True); os.makedirs(SPILL, exist_ok=True)
    con = duckdb.connect(); con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='16GB'"); con.execute(f"SET temp_directory='{SPILL}'")
    con.execute("SET preserve_insertion_order=false")
    def step(m): print(f"[{time.time()-t0:5.1f}s] {m}", flush=True)

    # DSBS roster (+ identity + cert cohorts for the cohort artifact)
    con.register("_d", rdr("sba_dsbs_certified_firms", [
        "uei", "legal_business_name", "state", "naics_primary",
        "active_8a_boolean", "active_8a_jv_boolean", "active_hz_boolean", "active_wosb_boolean",
        "active_edwosb_boolean", "active_sdvosb_boolean", "active_sdvosb_jv_boolean",
        "active_vosb_boolean", "active_vosb_jv_boolean"]))
    con.execute("""CREATE TABLE dsbs AS SELECT upper(trim(uei)) uei,
        any_value(legal_business_name) firm_name, any_value(state) state, any_value(naics_primary) naics_primary,
        bool_or(coalesce(active_8a_boolean,FALSE) OR coalesce(active_8a_jv_boolean,FALSE)) is_8a,
        bool_or(coalesce(active_hz_boolean,FALSE)) is_hubzone,
        bool_or(coalesce(active_wosb_boolean,FALSE)) is_wosb,
        bool_or(coalesce(active_edwosb_boolean,FALSE)) is_edwosb,
        bool_or(coalesce(active_sdvosb_boolean,FALSE) OR coalesce(active_sdvosb_jv_boolean,FALSE)) is_sdvosb,
        bool_or(coalesce(active_vosb_boolean,FALSE) OR coalesce(active_vosb_jv_boolean,FALSE)) is_vosb
        FROM _d WHERE uei IS NOT NULL AND trim(uei)<>'' GROUP BY 1""")
    con.unregister("_d")

    # sub universe (DSBS subawardees)
    con.register("_s", rdr("subaward_naics_psc_wide", ["subawardee_uei", "prime_naics_code", "prime_psc_code"]))
    con.execute("""CREATE TABLE sub_uei AS SELECT DISTINCT upper(trim(subawardee_uei)) uei
        FROM _s WHERE subawardee_uei IS NOT NULL AND trim(subawardee_uei)<>''
          AND prime_naics_code IS NOT NULL AND trim(prime_naics_code)<>''
          AND prime_psc_code IS NOT NULL AND trim(prime_psc_code)<>''
          AND upper(trim(subawardee_uei)) IN (SELECT uei FROM dsbs)""")
    con.unregister("_s")

    # DSBS prime footprint (combos + firm-level prime scale) [108M scan]
    step("spine scan (108M) — DSBS prime footprint")
    con.register("_p", rdr("usaspending_fpds_canonical_txn",
                           ["recipient_uei", "naics_code", "product_or_service_code", "federal_action_obligation"]))
    con.execute("""CREATE TABLE prime_raw AS SELECT upper(trim(recipient_uei)) uei,
        trim(naics_code)||'|'||upper(trim(product_or_service_code)) combo,
        coalesce(TRY_CAST(federal_action_obligation AS DOUBLE),0.0) fao
        FROM _p WHERE recipient_uei IS NOT NULL AND trim(recipient_uei)<>''
          AND naics_code IS NOT NULL AND trim(naics_code)<>'' AND product_or_service_code IS NOT NULL AND trim(product_or_service_code)<>''
          AND upper(trim(recipient_uei)) IN (SELECT uei FROM dsbs)""")
    con.unregister("_p")
    con.execute("CREATE TABLE prime_combo AS SELECT DISTINCT uei, combo FROM prime_raw")
    con.execute("CREATE TABLE prime_scale AS SELECT uei, sum(fao) prime_obl_alltime, count(DISTINCT combo) n_prime_combos FROM prime_raw GROUP BY uei")

    # active sub-demand at combo grain
    con.register("_g", rdr("govcon_active_awards", [
        "naics_code", "naics_description", "psc_code", "psc_description",
        "total_dollars_obligated", "potential_total_value_of_award",
        "has_subcontracting_plan", "active_current", "active_potential"]))
    con.execute("""CREATE TABLE demand_combo AS
        SELECT trim(naics_code)||'|'||upper(trim(psc_code)) combo,
               any_value(naics_description) naics_desc, any_value(psc_description) psc_desc,
               count(*) n_awards,
               sum(coalesce(TRY_CAST(total_dollars_obligated AS DOUBLE),0.0)) obligated,
               sum(greatest(coalesce(TRY_CAST(potential_total_value_of_award AS DOUBLE),0.0)-coalesce(TRY_CAST(total_dollars_obligated AS DOUBLE),0.0),0.0)) unspent
        FROM _g WHERE coalesce(has_subcontracting_plan,FALSE) AND (coalesce(active_current,FALSE) OR coalesce(active_potential,FALSE))
          AND naics_code IS NOT NULL AND trim(naics_code)<>'' AND psc_code IS NOT NULL AND trim(psc_code)<>''
        GROUP BY 1""")
    con.unregister("_g")

    # ── partition summary ──
    part = con.execute("""SELECT
        (SELECT count(*) FROM dsbs) roster,
        (SELECT count(DISTINCT uei) FROM prime_combo) has_prime,
        (SELECT count(*) FROM sub_uei) has_sub,
        (SELECT count(DISTINCT uei) FROM prime_combo WHERE uei NOT IN (SELECT uei FROM sub_uei)) prime_only,
        (SELECT count(*) FROM sub_uei WHERE uei NOT IN (SELECT DISTINCT uei FROM prime_combo)) sub_only,
        (SELECT count(DISTINCT uei) FROM prime_combo WHERE uei IN (SELECT uei FROM sub_uei)) both_ps,
        (SELECT count(*) FROM dsbs WHERE uei NOT IN (SELECT DISTINCT uei FROM prime_combo) AND uei NOT IN (SELECT uei FROM sub_uei)) neither
    """).fetchdf().iloc[0].to_dict()
    part = {k: int(v) for k, v in part.items()}
    step(f"partition: prime_only={part['prime_only']:,} sub_only={part['sub_only']:,} both={part['both_ps']:,} neither={part['neither']:,}")

    # ── prime-only cohort with dollar-weighted conversion metrics ──
    con.execute("CREATE TABLE prime_only AS SELECT uei FROM prime_combo WHERE uei NOT IN (SELECT uei FROM sub_uei) GROUP BY uei")
    con.execute("""CREATE TABLE cohort AS
        WITH conv AS (
            SELECT p.uei,
                   count(DISTINCT p.combo) demand_combos_primed,
                   sum(dc.n_awards)  reachable_active_awards,
                   sum(dc.obligated) reachable_obligated_usd,
                   sum(dc.unspent)   reachable_unspent_usd
            FROM prime_combo p JOIN prime_only po ON p.uei=po.uei
            JOIN demand_combo dc ON p.combo = dc.combo
            GROUP BY p.uei)
        SELECT d.uei, d.firm_name, d.state, d.naics_primary,
               d.is_8a, d.is_hubzone, d.is_wosb, d.is_edwosb, d.is_sdvosb, d.is_vosb,
               ps.prime_obl_alltime, ps.n_prime_combos,
               coalesce(c.demand_combos_primed,0)      demand_combos_primed,
               coalesce(c.reachable_active_awards,0)   reachable_active_awards,
               coalesce(c.reachable_obligated_usd,0.0) reachable_obligated_usd,
               coalesce(c.reachable_unspent_usd,0.0)   reachable_unspent_usd,
               (c.demand_combos_primed IS NOT NULL)    conversion_ready
        FROM prime_only po
        JOIN dsbs d ON d.uei=po.uei
        JOIN prime_scale ps ON ps.uei=po.uei
        LEFT JOIN conv c ON c.uei=po.uei""")
    con.execute(f"COPY (SELECT * FROM cohort ORDER BY reachable_obligated_usd DESC) TO '{OUT}/dsbs_prime_only_conversion.parquet' (FORMAT parquet)")

    agg = con.execute("""SELECT count(*) prime_only, count(*) FILTER(WHERE conversion_ready) conv_ready,
        sum(reachable_obligated_usd) tot_obl, sum(reachable_unspent_usd) tot_uns,
        count(DISTINCT uei) FILTER(WHERE reachable_obligated_usd>=1e9) firms_ge_1b
        FROM cohort""").fetchone()
    step(f"cohort: {agg[0]:,} prime-only · {agg[1]:,} conversion-ready · {agg[4]:,} firms with ≥$1B reachable pool")

    summary = {"partition": part,
               "prime_only_firms": int(agg[0]), "conversion_ready_firms": int(agg[1]),
               "conversion_ready_pct": float(agg[1]/agg[0]),
               "reachable_obligated_usd_total": float(agg[2] or 0), "reachable_unspent_usd_total": float(agg[3] or 0),
               "firms_reachable_pool_ge_1b": int(agg[4] or 0),
               "note": "reachable_* is combo-pool-scale active demand in the firm's primed combos (not firm-winnable); rank by $ not combo count to defeat catch-all inflation."}
    with open(f"{OUT}/prime_only_conversion_summary.json", "w") as fh:
        json.dump(summary, fh, indent=2, default=str)

    # console — dollar-weighted top 25 (with top demand combos per firm)
    import pandas as pd
    pd.set_option("display.width", 240); pd.set_option("display.max_columns", 20)
    top = con.execute("""
        WITH tc AS (
            SELECT p.uei, dc.combo, dc.obligated,
                   row_number() OVER (PARTITION BY p.uei ORDER BY dc.obligated DESC) rn,
                   dc.naics_desc, dc.psc_desc
            FROM prime_combo p JOIN prime_only po ON p.uei=po.uei JOIN demand_combo dc ON p.combo=dc.combo),
        tops AS (SELECT uei, string_agg(combo, ', ' ORDER BY obligated DESC) top_combos
                 FROM tc WHERE rn<=3 GROUP BY uei)
        SELECT c.firm_name, c.state,
               CASE WHEN c.is_8a THEN '8a ' ELSE '' END || CASE WHEN c.is_hubzone THEN 'HZ ' ELSE '' END ||
               CASE WHEN c.is_wosb THEN 'WOSB ' ELSE '' END || CASE WHEN c.is_sdvosb THEN 'SDVOSB ' ELSE '' END ||
               CASE WHEN c.is_vosb THEN 'VOSB ' ELSE '' END certs,
               c.demand_combos_primed dc_primed, c.reachable_active_awards awards,
               round(c.reachable_obligated_usd/1e9,2) reach_obl_B, round(c.reachable_unspent_usd/1e9,2) reach_uns_B,
               t.top_combos
        FROM cohort c JOIN tops t ON t.uei=c.uei
        ORDER BY c.reachable_obligated_usd DESC LIMIT 25""").fetchdf()
    print("\n===== PRIME-ONLY DSBS — TOP 25 CONVERSION TARGETS (dollar-weighted) =====")
    print(top.to_string(index=False))
    step("DONE")
    con.close()


if __name__ == "__main__":
    main()
