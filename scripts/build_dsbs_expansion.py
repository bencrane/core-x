#!/usr/bin/env python3
"""Per-DSBS expansion lanes — consumer of the combo-adjacency model.

WHY
    The adjacency model (subaward_combo_nodes / subaward_combo_edges) is firm-agnostic.
    This projects it onto the DSBS population that already subcontracts: for each DSBS sub,
    rank the (NAICS,PSC) lanes it does NOT yet hold but is empirically adjacent to (via the
    combos it DOES hold), and attach the lane's demand + the primes buying there (teaming
    targets). "Firms like you that win X also win Y — here's Y, the $ in it, and who buys it."

SCOPE
    DSBS firms with subaward history (a demonstrated wheelhouse to seed neighbors from) —
    ~2,474. Cold-start DSBS (no sub history) are out of scope here; their lanes would have to
    be seeded from SAM self-declared NAICS/PSC (a weaker, ~38%-covered signal) — a later add.

SOURCES (read-only, Gen-3 Lance)
    subaward_naics_psc        firm -> held combos · combo -> primes · amounts
    subaward_combo_nodes      lane demand stats + descriptions
    subaward_combo_edges      empirical adjacency (src->dst, confidence/lift/n_both)
    sba_dsbs_certified_firms  DSBS roster + legal name

SCORING (per candidate dst lane, summed over the firm's held combos that point to it)
    edges pre-filtered to n_both>=2 AND lift>=1 (drop single-firm coincidences + non-assoc).
    score = sum(confidence * lift) over supporting held combos.
      confidence = P(dst | held)  → directional pull toward dst
      lift       = popularity-corrected association (penalizes generic, everyone-has lanes)
    Carries n_supporting_combos + max_n_both so a consumer can sanity-threshold.
    Top 10 lanes per firm.

TARGET (Gen-3 Lance v2.1, full-snapshot overwrite — derived rollup)
    s3://data-sink/active/dsbs_combo_expansion/
    Indexes: BTREE subawardee_uei, dst_combo, dst_naics

    doppler run -- python3 scripts/build_dsbs_expansion.py [--demo]
Read-only sources; one Lance write. Doppler core-x/prd.
"""
from __future__ import annotations
import os, sys
import duckdb, lance

A = "s3://data-sink/active"
OUT = f"{A}/dsbs_combo_expansion/"
TOP_N = 10


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def main() -> int:
    demo = "--demo" in sys.argv
    opt = so(); con = duckdb.connect()
    con.execute("SET memory_limit='12GB'; SET threads TO 4;")
    os.makedirs("/tmp/duck_spill", exist_ok=True); con.execute("SET temp_directory='/tmp/duck_spill';")

    def reg(name, cols, t):
        ds = lance.dataset(f"{A}/{name}/", storage_options=opt)
        con.register("_r", ds.scanner(columns=cols).to_reader())
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM _r"); con.unregister("_r")

    reg("sba_dsbs_certified_firms", ["uei", "legal_business_name"], "_d")
    con.execute("CREATE TABLE dsbs AS SELECT upper(trim(uei)) uei, any_value(legal_business_name) nm FROM _d WHERE uei IS NOT NULL GROUP BY 1")
    reg("subaward_naics_psc", ["subawardee_uei", "prime_awardee_name", "prime_naics_code", "prime_psc_code", "subaward_amount"], "_e")
    con.execute("""CREATE TABLE base AS SELECT upper(trim(subawardee_uei)) uei, prime_awardee_name pn,
        prime_naics_code || '|' || prime_psc_code AS combo, coalesce(subaward_amount,0) amt FROM _e
        WHERE subawardee_uei IS NOT NULL AND trim(subawardee_uei)<>''
          AND prime_naics_code IS NOT NULL AND trim(prime_naics_code)<>''
          AND prime_psc_code  IS NOT NULL AND trim(prime_psc_code)<>''""")
    reg("subaward_combo_nodes", ["combo_id", "naics", "psc", "naics_description", "psc_description",
                                 "n_subawardees", "n_primes", "median_subaward_amt", "total_subaward_amt"], "nodes")
    reg("subaward_combo_edges", ["src_combo", "dst_combo", "n_both", "confidence", "lift"], "edges")

    # firm's demonstrated wheelhouse (DSBS only)
    con.execute("CREATE TABLE held AS SELECT DISTINCT b.uei, b.combo FROM base b JOIN dsbs d ON b.uei=d.uei")
    con.execute("CREATE TABLE heldcnt AS SELECT uei, count(*) n_held FROM held GROUP BY 1")
    n_firms = con.execute("SELECT count(*) FROM heldcnt").fetchone()[0]

    # candidate lanes: neighbors of held combos, not already held, real association
    con.execute("""CREATE TABLE cand AS
        SELECT h.uei, e.dst_combo, e.confidence, e.lift, e.n_both
        FROM held h JOIN edges e ON h.combo = e.src_combo
        WHERE e.n_both >= 2 AND e.lift >= 1
          AND NOT EXISTS (SELECT 1 FROM held h2 WHERE h2.uei = h.uei AND h2.combo = e.dst_combo)""")
    con.execute("""CREATE TABLE agg AS SELECT uei, dst_combo,
        round(sum(confidence * lift), 4) AS score, count(*) AS n_supporting, max(n_both) AS max_n_both
        FROM cand GROUP BY 1, 2""")

    # top-5 primes per lane (teaming targets), by $ in that combo (whole market)
    con.execute("CREATE TABLE pc AS SELECT combo, pn, sum(amt) a FROM base WHERE pn IS NOT NULL AND trim(pn)<>'' GROUP BY 1,2")
    con.execute("CREATE TABLE pcr AS SELECT combo, pn, a, row_number() OVER (PARTITION BY combo ORDER BY a DESC) rn FROM pc")
    con.execute("CREATE TABLE topp AS SELECT combo, list(pn ORDER BY a DESC) AS top_primes FROM pcr WHERE rn <= 5 GROUP BY combo")

    # assemble + rank top-N per firm
    con.execute(f"""CREATE TABLE final AS
        WITH r AS (
          SELECT a.uei AS subawardee_uei, d.nm AS legal_business_name, hc.n_held AS n_held_combos,
            a.dst_combo, n.naics AS dst_naics, n.psc AS dst_psc,
            n.naics_description AS dst_naics_description, n.psc_description AS dst_psc_description,
            a.score, a.n_supporting AS n_supporting_combos, a.max_n_both,
            n.n_primes AS lane_n_primes, n.n_subawardees AS lane_n_subawardees,
            n.median_subaward_amt AS lane_median_amt, n.total_subaward_amt AS lane_total_amt,
            coalesce(tp.top_primes, []::VARCHAR[]) AS top_primes,
            row_number() OVER (PARTITION BY a.uei ORDER BY a.score DESC, a.max_n_both DESC, n.total_subaward_amt DESC) AS rank
          FROM agg a
          JOIN dsbs d ON a.uei = d.uei
          JOIN heldcnt hc ON a.uei = hc.uei
          JOIN nodes n ON a.dst_combo = n.combo_id
          LEFT JOIN topp tp ON a.dst_combo = tp.combo
        )
        SELECT * FROM r WHERE rank <= {TOP_N}""")
    n_rows = con.execute("SELECT count(*) FROM final").fetchone()[0]
    n_covered = con.execute("SELECT count(DISTINCT subawardee_uei) FROM final").fetchone()[0]
    print(f"DSBS subs with a wheelhouse: {n_firms:,} · with >=1 recommended lane: {n_covered:,} · rows (<= {TOP_N}/firm): {n_rows:,}")

    lance.write_dataset(con.execute("SELECT * FROM final").to_arrow_table(), OUT,
                        mode="overwrite", data_storage_version="2.1", max_rows_per_file=1_048_576, storage_options=opt)
    ds = lance.dataset(OUT, storage_options=opt)
    for col in ["subawardee_uei", "dst_combo", "dst_naics"]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  index {col} skipped: {exc}")
    print(f"wrote {OUT} ({ds.count_rows():,} rows, 3 indexes)")

    if demo:
        print("\n" + "=" * 80)
        print("DEMO — a DSBS firm's top expansion lanes (held wheelhouse -> recommended, with buyers)")
        print("=" * 80)
        firm = con.execute("""SELECT subawardee_uei FROM final f JOIN (
            SELECT uei, count(*) c FROM held GROUP BY 1 HAVING c BETWEEN 2 AND 4) h ON f.subawardee_uei=h.uei
            GROUP BY 1 ORDER BY 1 LIMIT 1""").fetchone()[0]
        nm = con.execute(f"SELECT any_value(nm) FROM dsbs WHERE uei='{firm}'").fetchone()[0]
        held = [r[0] for r in con.execute(f"SELECT combo FROM held WHERE uei='{firm}'").fetchall()]
        print(f"  firm {firm} ({nm})\n  HOLDS: {held}\n  RECOMMENDED:")
        for r in con.execute(f"""SELECT rank, dst_combo, dst_naics_description, dst_psc_description, score,
            lane_n_primes, round(lane_median_amt) med, top_primes FROM final WHERE subawardee_uei='{firm}' ORDER BY rank""").fetchall():
            tp = ", ".join(list(r[7])[:3]) if r[7] else "-"
            print(f"   #{r[0]} {r[1]:<13} score={r[4]:<7} primes={r[5]:<4} ${int(r[6]):>8,}  {str(r[2])[:24]:<24} | {str(r[3])[:30]}")
            print(f"        buyers: {tp[:90]}")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
