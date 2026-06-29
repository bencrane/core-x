#!/usr/bin/env python3
"""capability_profile — the per-firm /ask card object (one row per UEI).

Pure ASSEMBLY (no new analytics): fuses the golden activity record, designations, and the
recommendation surface into the single object the call screen reads. Role is a STATUS, not the
name — `federal_status` + flags cover active subs and never-subbed DSBS in one shape.

  identity + activity   <- subawardee_work_profile (sub-side + prime-side summaries, top lists)
  designations[]        <- govcon_subawardee_designations (+ is_dsbs from the DSBS roster)
  recommended_lanes[]   <- capability_lanes (top-10/firm, evidence-tiered), nested
  (opportunities[]      <- Stage 4, later)

POPULATION: firms with something to show — distinct UEIs in subawardee_work_profile (subs)
UNION capability_lanes (anyone with >=1 recommended lane, incl never-subbed DSBS).

KNOWN v1 gaps (bounded, documented): (1) prime-side activity is sourced from work_profile, which
only covers subs — a never-subbed-but-primed DSBS gets primed lanes but a sparse activity panel
until its prime activity is added from contract_prime_txn. (2) designations[] is sub-scoped
(the designations table covers subs); never-subbed DSBS show is_dsbs but not their specific certs.

TARGET: s3://data-sink/active/capability_profile/ (overwrite; derived serving object)
    BTREE uei · BITMAP is_dsbs, federal_status

    doppler run -- python3 scripts/build_capability_profile.py [--demo]
Read-only sources; one Lance write. Doppler core-x/prd.
"""
from __future__ import annotations
import os, sys
import duckdb, lance

A = "s3://data-sink/active"
OUT = f"{A}/capability_profile/"


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def main() -> int:
    demo = "--demo" in sys.argv
    opt = so(); con = duckdb.connect()
    con.execute("SET memory_limit='14GB'; SET threads TO 4;")
    os.makedirs("/tmp/duck_spill", exist_ok=True); con.execute("SET temp_directory='/tmp/duck_spill';")

    def reg(name, cols, t):
        ds = lance.dataset(f"{A}/{name}/", storage_options=opt)
        con.register("_r", ds.scanner(columns=cols).to_table()); con.execute(f"CREATE TABLE {t} AS SELECT * FROM _r"); con.unregister("_r")

    reg("subaward_work_profile" if False else "subawardee_work_profile", [
        "subawardee_uei","subawardee_name","subawardee_parent_uei","subawardee_state_code",
        "sub_amount_5y","sub_received_5y","sub_distinct_primes_5y","sub_distinct_prime_partners_5y",
        "recent_subawards_90d","recent_subaward_amount_90d","recent_latest_action_date","recent_top_prime_name",
        "recent_top_naics_code","recent_top_naics_description","recent_subaward_scope",
        "sub_top_prime_partners","sub_top_naics",
        "prime_awards_5y","prime_obligated_5y","prime_competed_awards_5y","prime_distinct_naics_5y",
        "prime_top_naics","prime_top_psc","prime_top_agencies"], "wp")
    con.execute("UPDATE wp SET subawardee_uei = upper(trim(subawardee_uei))")
    reg("capability_lanes", ["uei","firm_name","is_dsbs","has_sub_history","has_prime_history","rank","evidence_tier",
        "dst_naics","dst_psc","dst_naics_description","dst_psc_description","score","lane_n_primes","lane_median_amt","top_primes"], "cl")
    reg("sba_dsbs_certified_firms", ["uei","legal_business_name"], "_d")
    con.execute("CREATE TABLE dsbs AS SELECT upper(trim(uei)) uei, any_value(legal_business_name) lnm FROM _d WHERE uei IS NOT NULL GROUP BY 1")
    reg("govcon_subawardee_designations", ["subawardee_uei","subawardee_name",
        "service_disabled_veteran_owned_business","veteran_owned_business","women_owned_small_business",
        "economically_disadvantaged_women_owned_small_business","historically_underutilized_business_zone_hubzone_firm",
        "c8a_program_participant","small_disadvantaged_business","minority_owned_business","emerging_small_business"], "dg0")
    con.execute("CREATE TABLE dg AS SELECT upper(trim(subawardee_uei)) uei, * EXCLUDE(subawardee_uei) FROM dg0")

    # recommendation surface -> nested list + summary, per firm
    con.execute("""CREATE TABLE lanes AS SELECT uei,
        list(struct_pack(rank:=rank, evidence_tier:=evidence_tier, dst_naics:=dst_naics, dst_psc:=dst_psc,
            naics_desc:=dst_naics_description, psc_desc:=dst_psc_description, score:=score,
            lane_n_primes:=lane_n_primes, lane_median_amt:=lane_median_amt, top_primes:=top_primes) ORDER BY rank) AS recommended_lanes,
        count(*) AS n_recommended_lanes, arg_min(evidence_tier, rank) AS top_evidence_tier
        FROM cl GROUP BY uei""")
    con.execute("CREATE TABLE clstat AS SELECT uei, any_value(has_sub_history) hs, any_value(has_prime_history) hp, any_value(firm_name) cl_name FROM cl GROUP BY uei")

    # base = anyone with activity (work_profile) or recommendations (capability_lanes)
    con.execute("CREATE TABLE base AS SELECT DISTINCT uei FROM (SELECT subawardee_uei uei FROM wp UNION SELECT uei FROM cl)")

    con.execute("""CREATE TABLE final AS SELECT
        b.uei,
        coalesce(wp.subawardee_name, cs.cl_name, dg.subawardee_name, d.lnm) AS firm_name,
        wp.subawardee_state_code AS state_code, wp.subawardee_parent_uei AS parent_uei,
        (b.uei IN (SELECT uei FROM dsbs)) AS is_dsbs,
        coalesce(cs.hs, wp.subawardee_uei IS NOT NULL) AS has_sub_history,
        coalesce(cs.hp, wp.prime_awards_5y > 0) AS has_prime_history,
        CASE WHEN coalesce(cs.hs, wp.subawardee_uei IS NOT NULL) THEN 'active_sub'
             WHEN (b.uei IN (SELECT uei FROM dsbs)) THEN 'dsbs_prospect' ELSE 'other' END AS federal_status,
        list_filter([
            CASE WHEN dg.service_disabled_veteran_owned_business THEN 'SDVOSB' END,
            CASE WHEN dg.veteran_owned_business THEN 'VOSB' END,
            CASE WHEN dg.women_owned_small_business THEN 'WOSB' END,
            CASE WHEN dg.economically_disadvantaged_women_owned_small_business THEN 'EDWOSB' END,
            CASE WHEN dg.historically_underutilized_business_zone_hubzone_firm THEN 'HUBZone' END,
            CASE WHEN dg.c8a_program_participant THEN '8(a)' END,
            CASE WHEN dg.small_disadvantaged_business THEN 'SDB' END,
            CASE WHEN dg.minority_owned_business THEN 'MBE' END,
            CASE WHEN dg.emerging_small_business THEN 'ESB' END
        ], x -> x IS NOT NULL) AS designations,
        wp.sub_amount_5y, wp.sub_received_5y, wp.sub_distinct_primes_5y, wp.sub_distinct_prime_partners_5y,
        wp.recent_subawards_90d, wp.recent_subaward_amount_90d, wp.recent_latest_action_date, wp.recent_top_prime_name,
        wp.recent_top_naics_code, wp.recent_top_naics_description, wp.recent_subaward_scope,
        wp.sub_top_prime_partners, wp.sub_top_naics,
        wp.prime_awards_5y, wp.prime_obligated_5y, wp.prime_competed_awards_5y, wp.prime_distinct_naics_5y,
        wp.prime_top_naics, wp.prime_top_psc, wp.prime_top_agencies,
        coalesce(l.recommended_lanes, []) AS recommended_lanes,
        coalesce(l.n_recommended_lanes, 0) AS n_recommended_lanes, l.top_evidence_tier,
        now() AS materialized_at
        FROM base b
        LEFT JOIN wp ON b.uei = wp.subawardee_uei
        LEFT JOIN clstat cs ON b.uei = cs.uei
        LEFT JOIN lanes l ON b.uei = l.uei
        LEFT JOIN dg ON b.uei = dg.uei
        LEFT JOIN dsbs d ON b.uei = d.uei""")

    n = con.execute("SELECT count(*) FROM final").fetchone()[0]
    fs = con.execute("SELECT federal_status, count(*) n, count(*) FILTER (WHERE n_recommended_lanes>0) with_lanes FROM final GROUP BY 1 ORDER BY 2 DESC").df()
    rich = con.execute("""SELECT count(*) FILTER (WHERE has_sub_history) subs, count(*) FILTER (WHERE has_prime_history) primes,
        count(*) FILTER (WHERE is_dsbs) dsbs, count(*) FILTER (WHERE len(designations)>0) with_desig,
        count(*) FILTER (WHERE n_recommended_lanes>0) with_lanes FROM final""").fetchone()
    print(f"capability_profile rows (1/firm) = {n:,}")
    print(f"  has_sub_history={rich[0]:,} · has_prime_history={rich[1]:,} · is_dsbs={rich[2]:,} · with designations={rich[3]:,} · with >=1 lane={rich[4]:,}")
    print("  by federal_status:"); print(fs.to_string(index=False))

    lance.write_dataset(con.execute("SELECT * FROM final").to_arrow_table(), OUT,
                        mode="overwrite", data_storage_version="2.1", max_rows_per_file=1_048_576, storage_options=opt)
    ds = lance.dataset(OUT, storage_options=opt)
    for col, it in [("uei","BTREE"),("is_dsbs","BITMAP"),("federal_status","BITMAP")]:
        try: ds.create_scalar_index(col, index_type=it, replace=True)
        except Exception as exc: print(f"  index {col} skipped: {exc}")  # noqa: BLE001
    print(f"wrote {OUT} ({ds.count_rows():,} rows, schema cols={len(ds.schema)}, 3 indexes)")

    if demo:
        import json
        row = con.execute("""SELECT uei, firm_name, federal_status, is_dsbs, has_sub_history, has_prime_history, designations,
            sub_amount_5y, sub_received_5y, sub_distinct_primes_5y, recent_subawards_90d, recent_subaward_amount_90d,
            prime_awards_5y, prime_obligated_5y, n_recommended_lanes, top_evidence_tier,
            recommended_lanes[1:3] AS first_3_lanes FROM final WHERE uei='DF1HR8L5BDB4'""").df().to_dict("records")[0]
        print("\n== DEMO capability_profile row — G.C. Micro (DF1HR8L5BDB4) ==")
        print(json.dumps(row, indent=2, default=str)[:2200])
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
