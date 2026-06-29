#!/usr/bin/env python3
"""capability_lanes — unified, evidence-tiered sub-lane recommender (subs + DSBS).

Supersedes subawardee_combo_expansion (warm) and folds in dsbs_combo_coldstart (cold) as
one firm-keyed surface feeding the capability_profile card. Population = entities that have
subbed  UNION  DSBS roster (designation-favored). No pure-prime, never-subbed firms.

CAPABILITY EVIDENCE per firm (combos), strongest -> weakest:
    subbed a lane  -> EXCLUDED from recs (already there); seeds hops
    primed a lane  -> seed: DIRECT match (if sub-demand exists) + hops      [demonstrated]
    declared (SAM) -> only when NO demonstrated activity (never-active DSBS) [declared]
Prime combos are EVIDENCE, not exclusions: a lane they PRIMED but never SUBBED, where
sub-demand exists, is the strongest recommendation ("you've done this as a prime -> go sub it").

TIERS (rank within firm by tier_order, then score, then lane demand; top 10):
    1 primed-direct  firm primed this exact sub-demand lane, doesn't sub it
    2 subbed-hop     adjacency neighbor reachable from a lane they SUBBED
    3 primed-hop     adjacency neighbor reachable only from a lane they PRIMED
    4 declared       never-active DSBS, lane sits in a declared NAICS (+PSC if declared)
Target is ALWAYS a sub-demand node (subaward_combo_nodes) -> there's a roster to join;
buyers = the primes who sub that lane out (top_primes).

SOURCES (read-only): subaward_naics_psc, contract_prime_txn (recipient_uei -> prime combos),
subaward_combo_nodes, subaward_combo_edges, sba_dsbs_certified_firms, sam_master_entities.
TARGET: s3://data-sink/active/capability_lanes/  (overwrite; derived rollup)
    BTREE uei, dst_combo, dst_naics · BITMAP is_dsbs, evidence_tier

    doppler run -- python3 scripts/build_capability_lanes.py [--demo]
Read-only sources; one Lance write. Doppler core-x/prd.
"""
from __future__ import annotations
import os, sys
import duckdb, lance

A = "s3://data-sink/active"
OUT = f"{A}/capability_lanes/"
TOP_N = 10
FREE = ("gmail.com","yahoo.com","hotmail.com","outlook.com","aol.com","icloud.com","comcast.net","verizon.net","sbcglobal.net","att.net","cox.net","msn.com","live.com")


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

    # ── sub side: held combos, names, top primes per combo ───────────────────────────
    reg("subaward_naics_psc", ["subawardee_uei","subawardee_name","prime_awardee_name","prime_naics_code","prime_psc_code","subaward_amount"], "_sub")
    con.execute("""CREATE TABLE sub AS SELECT upper(trim(subawardee_uei)) uei, subawardee_name snm, prime_awardee_name pn,
        prime_naics_code||'|'||prime_psc_code combo, coalesce(subaward_amount,0) amt FROM _sub
        WHERE subawardee_uei IS NOT NULL AND trim(subawardee_uei)<>'' AND prime_naics_code IS NOT NULL AND trim(prime_naics_code)<>'' AND prime_psc_code IS NOT NULL AND trim(prime_psc_code)<>''""")
    con.execute("CREATE TABLE subheld AS SELECT DISTINCT uei, combo FROM sub")
    con.execute("CREATE TABLE names AS SELECT uei, any_value(snm) firm_name FROM sub WHERE snm IS NOT NULL GROUP BY 1")
    con.execute("CREATE TABLE pc AS SELECT combo, pn, sum(amt) a FROM sub WHERE pn IS NOT NULL AND trim(pn)<>'' GROUP BY 1,2")
    con.execute("CREATE TABLE topp AS SELECT combo, list(pn ORDER BY a DESC) top_primes FROM (SELECT combo,pn,a,row_number() OVER (PARTITION BY combo ORDER BY a DESC) rn FROM pc) WHERE rn<=5 GROUP BY combo")

    # ── DSBS roster (declared NAICS + name) ──────────────────────────────────────────
    reg("sba_dsbs_certified_firms", ["uei","legal_business_name","naics_all_codes"], "_d")
    con.execute("CREATE TABLE dsbs AS SELECT upper(trim(uei)) uei, any_value(legal_business_name) lnm, any_value(naics_all_codes) nac FROM _d WHERE uei IS NOT NULL GROUP BY 1")

    # ── universe = subs UNION DSBS ───────────────────────────────────────────────────
    con.execute("CREATE TABLE univ AS SELECT DISTINCT uei FROM (SELECT uei FROM sub UNION SELECT uei FROM dsbs)")

    # ── prime side: firm's own prime-award combos (recipient_uei), restricted to universe ──
    reg("usaspending_api_fresh/contract_prime_txn", ["recipient_uei","naics_code","product_or_service_code"], "_p")
    con.execute("""CREATE TABLE primec AS SELECT DISTINCT upper(trim(recipient_uei)) uei, naics_code||'|'||product_or_service_code combo
        FROM _p WHERE recipient_uei IS NOT NULL AND trim(recipient_uei)<>'' AND naics_code IS NOT NULL AND trim(naics_code)<>'' AND product_or_service_code IS NOT NULL AND trim(product_or_service_code)<>''
          AND upper(trim(recipient_uei)) IN (SELECT uei FROM univ)""")

    reg("subaward_combo_nodes", ["combo_id","naics","psc","naics_description","psc_description","n_primes","n_subawardees","median_subaward_amt","total_subaward_amt"], "nodes")
    reg("subaward_combo_edges", ["src_combo","dst_combo","n_both","confidence","lift"], "edges")

    # status flags
    con.execute("""CREATE TABLE status AS SELECT u.uei,
        (u.uei IN (SELECT uei FROM dsbs)) is_dsbs,
        (u.uei IN (SELECT uei FROM subheld)) has_sub_history,
        (u.uei IN (SELECT uei FROM primec)) has_prime_history,
        (SELECT count(*) FROM subheld s WHERE s.uei=u.uei) n_held_sub_combos,
        (SELECT count(*) FROM primec p WHERE p.uei=u.uei) n_prime_combos
        FROM univ u""")

    # ── TIER 1 — primed-direct: primed combo that is a sub-demand node, not subbed ────
    con.execute("""CREATE TABLE direct AS
        SELECT p.uei, p.combo dst_combo, 'primed-direct' evidence_tier, 1 tier_order,
               n.n_primes::DOUBLE raw_score, NULL::BIGINT n_supporting, NULL::BIGINT max_n_both
        FROM primec p JOIN nodes n ON p.combo = n.combo_id
        WHERE NOT EXISTS (SELECT 1 FROM subheld s WHERE s.uei=p.uei AND s.combo=p.combo)""")

    # ── TIER 2/3 — hops off seed (sub combos ∪ prime combos that are nodes) ───────────
    con.execute("""CREATE TABLE seed AS
        SELECT uei, combo, TRUE from_sub FROM subheld
        UNION
        SELECT uei, combo, FALSE FROM primec WHERE combo IN (SELECT combo_id FROM nodes)""")
    con.execute("""CREATE TABLE hop AS
        SELECT uei, dst_combo,
               CASE WHEN bool_or(from_sub) THEN 'subbed-hop' ELSE 'primed-hop' END evidence_tier,
               CASE WHEN bool_or(from_sub) THEN 2 ELSE 3 END tier_order,
               round(sum(confidence*lift),4) raw_score, count(*) n_supporting, max(n_both) max_n_both
        FROM (SELECT sd.uei, e.dst_combo, e.confidence, e.lift, e.n_both, sd.from_sub
              FROM seed sd JOIN edges e ON sd.combo=e.src_combo WHERE e.n_both>=2 AND e.lift>=1) x
        GROUP BY uei, dst_combo""")
    # drop hops already subbed or already in direct
    con.execute("""CREATE TABLE hopf AS SELECT h.* FROM hop h
        WHERE NOT EXISTS (SELECT 1 FROM subheld s WHERE s.uei=h.uei AND s.combo=h.dst_combo)
          AND NOT EXISTS (SELECT 1 FROM direct d WHERE d.uei=h.uei AND d.dst_combo=h.dst_combo)""")

    # ── TIER 4 — declared: never-active DSBS (no sub AND no prime combos) ─────────────
    free = "(" + ",".join("'"+d+"'" for d in FREE) + ")"  # noqa: F841 (kept for parity; declared uses NAICS only)
    con.execute("""CREATE TABLE coldf AS SELECT d.uei, d.nac FROM dsbs d
        WHERE d.uei NOT IN (SELECT uei FROM subheld) AND d.uei NOT IN (SELECT uei FROM primec)""")
    con.execute("""CREATE TABLE fnaics AS SELECT DISTINCT uei, t naics FROM (
        SELECT uei, unnest(regexp_split_to_array(coalesce(nac,''),'[^0-9]+')) t FROM coldf) WHERE length(t)=6""")
    reg("sam_master_entities", ["uei","psc_code_string"], "_s")
    con.execute("""CREATE TABLE fpsc AS SELECT DISTINCT uei, upper(t) psc FROM (
        SELECT upper(trim(uei)) uei, unnest(string_split(psc_code_string,'~')) t FROM _s WHERE psc_code_string IS NOT NULL AND trim(psc_code_string)<>'') s
        WHERE regexp_matches(t,'^[A-Za-z0-9]{4}$') AND uei IN (SELECT uei FROM coldf)""")
    con.execute("""CREATE TABLE declared AS
        SELECT fn.uei, n.combo_id dst_combo, 'declared' evidence_tier, 4 tier_order,
               round((CASE WHEN fp.psc IS NOT NULL THEN 2.0 ELSE 1.0 END)*ln(1+n.n_primes),4) raw_score,
               NULL::BIGINT n_supporting, NULL::BIGINT max_n_both
        FROM fnaics fn JOIN nodes n ON fn.naics = n.naics
        LEFT JOIN fpsc fp ON fp.uei=fn.uei AND fp.psc=n.psc""")

    # ── assemble + rank top-N per firm ───────────────────────────────────────────────
    con.execute("CREATE TABLE cand AS SELECT * FROM direct UNION ALL SELECT * FROM hopf UNION ALL SELECT * FROM declared")
    con.execute(f"""CREATE TABLE final AS
        WITH r AS (
          SELECT c.uei, coalesce(nm.firm_name, ds.lnm) firm_name,
            st.is_dsbs, st.has_sub_history, st.has_prime_history, st.n_held_sub_combos, st.n_prime_combos,
            c.dst_combo, n.naics dst_naics, n.psc dst_psc, n.naics_description dst_naics_description, n.psc_description dst_psc_description,
            c.evidence_tier, round(c.raw_score,4) score, c.n_supporting AS n_supporting_combos, c.max_n_both,
            n.n_primes lane_n_primes, n.n_subawardees lane_n_subawardees, n.median_subaward_amt lane_median_amt, n.total_subaward_amt lane_total_amt,
            coalesce(tp.top_primes, []::VARCHAR[]) top_primes,
            row_number() OVER (PARTITION BY c.uei ORDER BY c.tier_order ASC, c.raw_score DESC, n.total_subaward_amt DESC) rank
          FROM cand c
          JOIN nodes n ON c.dst_combo = n.combo_id
          JOIN status st ON c.uei = st.uei
          LEFT JOIN names nm ON c.uei = nm.uei
          LEFT JOIN dsbs ds ON c.uei = ds.uei
          LEFT JOIN topp tp ON c.dst_combo = tp.combo
        )
        SELECT * FROM r WHERE rank <= {TOP_N}""")

    nrow = con.execute("SELECT count(*) FROM final").fetchone()[0]
    cov = con.execute("SELECT count(DISTINCT uei) FROM final").fetchone()[0]
    by = con.execute("SELECT evidence_tier, count(*) n, count(DISTINCT uei) firms FROM final GROUP BY 1 ORDER BY 2 DESC").df()
    uflags = con.execute("""SELECT count(DISTINCT uei) FILTER (WHERE is_dsbs) dsbs, count(DISTINCT uei) FILTER (WHERE has_prime_history) wprime,
        count(DISTINCT uei) FILTER (WHERE has_sub_history) wsub FROM final""").fetchone()
    print(f"capability_lanes rows={nrow:,} · firms covered={cov:,}")
    print(f"  covered firms: DSBS={uflags[0]:,} · with prime history={uflags[1]:,} · with sub history={uflags[2]:,}")
    print("  by evidence_tier:"); print(by.to_string(index=False))

    lance.write_dataset(con.execute("SELECT * FROM final").to_arrow_table(), OUT,
                        mode="overwrite", data_storage_version="2.1", max_rows_per_file=1_048_576, storage_options=opt)
    ds = lance.dataset(OUT, storage_options=opt)
    for col, it in [("uei","BTREE"),("dst_combo","BTREE"),("dst_naics","BTREE"),("is_dsbs","BITMAP"),("evidence_tier","BITMAP")]:
        try: ds.create_scalar_index(col, index_type=it, replace=True)
        except Exception as exc: print(f"  index {col} skipped: {exc}")  # noqa: BLE001
    print(f"wrote {OUT} ({ds.count_rows():,} rows, 5 indexes)")

    if demo:
        print("\n== DEMO — G.C. Micro (DF1HR8L5BDB4): does priming software/IT now surface as a direct sub-lane? ==")
        for r in con.execute("""SELECT rank, evidence_tier, dst_combo, substr(dst_naics_description,1,30) nd, substr(dst_psc_description,1,26) pd, score, lane_n_primes
            FROM final WHERE uei='DF1HR8L5BDB4' ORDER BY rank""").df().itertuples():
            print(f"   #{r.rank} [{r.evidence_tier:<12}] {r.dst_combo:<13} primes={r.lane_n_primes:<4} {str(r.nd):<30} | {r.pd}")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
