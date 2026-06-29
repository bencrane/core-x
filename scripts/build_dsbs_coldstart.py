#!/usr/bin/env python3
"""Cold-start expansion lanes for DSBS with NO subaward history.

WHY
    dsbs_combo_expansion seeds from a firm's DEMONSTRATED wheelhouse (combos it has subbed).
    The ~64.8k DSBS that have never subcontracted have no wheelhouse — empirical adjacency
    ("firms like you win Y") can't apply. Instead seed from DECLARED capability and surface
    the realized sub-lanes that sit inside it, ranked by demand + the primes who buy there.

SEED SIGNAL (graded)
    NAICS : DSBS-native naics_all_codes (100% populated) — the always-present backbone.
    PSC   : SAM self-declared psc_code_string (~38% of DSBS) — the precision signal.
    A candidate lane = a realized combo node whose NAICS the firm declares. Tier:
       naics+psc  the firm ALSO declares that combo's PSC  (claimed both — strong)
       naics-only declared the industry, not the specific PSC (medium)
    No award inference on award inference: this is "active sub-lanes inside your declared
    capability," NOT adjacency expansion. Honest cold-start; weaker than the warm signal.

SCORING
    rank within firm by: tier (naics+psc first), then lane buyer-count (n_primes), then $.
    score = tier_weight * ln(1 + lane_n_primes)   [naics+psc=2.0, naics-only=1.0]
    Top 10 lanes/firm. Lanes carry top-5 primes (teaming targets).

SOURCES (read-only, Gen-3 Lance)
    sba_dsbs_certified_firms  roster + name + naics_all_codes
    sam_master_entities       psc_code_string (self-declared)  [uei bridge]
    subaward_naics_psc        warm-set exclusion + combo->primes
    subaward_combo_nodes      realized lane nodes + demand + descriptions

TARGET (Gen-3 Lance v2.1, overwrite — derived rollup)
    s3://data-sink/active/dsbs_combo_coldstart/   BTREE subawardee_uei, dst_combo, dst_naics

    doppler run -- python3 scripts/build_dsbs_coldstart.py [--demo]
Read-only sources; one Lance write. Doppler core-x/prd.
"""
from __future__ import annotations
import os, sys
import duckdb, lance

A = "s3://data-sink/active"
OUT = f"{A}/dsbs_combo_coldstart/"
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

    reg("sba_dsbs_certified_firms", ["uei", "legal_business_name", "naics_all_codes"], "_d")
    con.execute("CREATE TABLE dsbs AS SELECT upper(trim(uei)) uei, any_value(legal_business_name) nm, any_value(naics_all_codes) nac FROM _d WHERE uei IS NOT NULL GROUP BY 1")
    n_dsbs = con.execute("SELECT count(*) FROM dsbs").fetchone()[0]

    # warm set (DSBS with sub history) — excluded here
    reg("subaward_naics_psc", ["subawardee_uei", "prime_awardee_name", "prime_naics_code", "prime_psc_code", "subaward_amount"], "_e")
    con.execute("""CREATE TABLE base AS SELECT upper(trim(subawardee_uei)) uei, prime_awardee_name pn,
        prime_naics_code || '|' || prime_psc_code AS combo, coalesce(subaward_amount,0) amt FROM _e
        WHERE prime_naics_code IS NOT NULL AND trim(prime_naics_code)<>'' AND prime_psc_code IS NOT NULL AND trim(prime_psc_code)<>''""")
    con.execute("CREATE TABLE warm AS SELECT DISTINCT uei FROM base WHERE uei IS NOT NULL AND trim(uei)<>''")
    con.execute("CREATE TABLE cold AS SELECT d.* FROM dsbs d WHERE d.uei NOT IN (SELECT uei FROM warm)")
    n_cold = con.execute("SELECT count(*) FROM cold").fetchone()[0]

    # declared NAICS (DSBS native, pipe/anything-delimited -> 6-digit tokens)
    con.execute("""CREATE TABLE fnaics AS SELECT DISTINCT uei, t AS naics FROM (
        SELECT uei, unnest(regexp_split_to_array(coalesce(nac,''), '[^0-9]+')) t FROM cold) WHERE length(t)=6""")
    # declared PSC (SAM self-declared psc_code_string -> 4-char tokens), cold firms only
    reg("sam_master_entities", ["uei", "psc_code_string"], "_s")
    con.execute("""CREATE TABLE fpsc AS SELECT DISTINCT uei, upper(t) AS psc FROM (
        SELECT upper(trim(uei)) uei, unnest(string_split(psc_code_string, '~')) t FROM _s
        WHERE psc_code_string IS NOT NULL AND trim(psc_code_string)<>'') s
        WHERE regexp_matches(t, '^[A-Za-z0-9]{4}$') AND uei IN (SELECT uei FROM cold)""")
    n_psc_firms = con.execute("SELECT count(DISTINCT uei) FROM fpsc").fetchone()[0]

    reg("subaward_combo_nodes", ["combo_id", "naics", "psc", "naics_description", "psc_description",
                                 "n_primes", "n_subawardees", "median_subaward_amt", "total_subaward_amt"], "nodes")

    # top-5 primes per lane (teaming targets) — whole-market, by $
    con.execute("CREATE TABLE pc AS SELECT combo, pn, sum(amt) a FROM base WHERE pn IS NOT NULL AND trim(pn)<>'' GROUP BY 1,2")
    con.execute("CREATE TABLE pcr AS SELECT combo, pn, a, row_number() OVER (PARTITION BY combo ORDER BY a DESC) rn FROM pc")
    con.execute("CREATE TABLE topp AS SELECT combo, list(pn ORDER BY a DESC) AS top_primes FROM pcr WHERE rn <= 5 GROUP BY combo")

    # per-firm declared-signal context
    con.execute("CREATE TABLE ndecl AS SELECT uei, count(*) n_declared_naics FROM fnaics GROUP BY 1")
    con.execute("CREATE TABLE pdecl AS SELECT uei, count(*) n_declared_psc FROM fpsc GROUP BY 1")

    # candidate lanes = realized combos under the firm's declared NAICS; tier by PSC declaration
    con.execute("""CREATE TABLE cand AS
        SELECT fn.uei, n.combo_id, n.naics, n.psc, n.naics_description, n.psc_description,
               n.n_primes, n.n_subawardees, n.median_subaward_amt, n.total_subaward_amt,
               CASE WHEN fp.psc IS NOT NULL THEN 'naics+psc' ELSE 'naics-only' END AS seed_tier
        FROM fnaics fn
        JOIN nodes n ON fn.naics = n.naics
        LEFT JOIN fpsc fp ON fp.uei = fn.uei AND fp.psc = n.psc""")

    con.execute(f"""CREATE TABLE final AS
        WITH r AS (
          SELECT c.uei AS subawardee_uei, d.nm AS legal_business_name,
            coalesce(nd.n_declared_naics,0) AS n_declared_naics, coalesce(pd.n_declared_psc,0) AS n_declared_psc,
            (pd.n_declared_psc IS NOT NULL) AS has_declared_psc,
            c.combo_id AS dst_combo, c.naics AS dst_naics, c.psc AS dst_psc,
            c.naics_description AS dst_naics_description, c.psc_description AS dst_psc_description,
            c.seed_tier,
            round((CASE WHEN c.seed_tier='naics+psc' THEN 2.0 ELSE 1.0 END) * ln(1 + c.n_primes), 4) AS score,
            c.n_primes AS lane_n_primes, c.n_subawardees AS lane_n_subawardees,
            c.median_subaward_amt AS lane_median_amt, c.total_subaward_amt AS lane_total_amt,
            coalesce(tp.top_primes, []::VARCHAR[]) AS top_primes,
            row_number() OVER (PARTITION BY c.uei
              ORDER BY (c.seed_tier='naics+psc') DESC, c.n_primes DESC, c.total_subaward_amt DESC) AS rank
          FROM cand c
          JOIN dsbs d ON c.uei = d.uei
          LEFT JOIN ndecl nd ON c.uei = nd.uei
          LEFT JOIN pdecl pd ON c.uei = pd.uei
          LEFT JOIN topp tp ON c.combo_id = tp.combo
        )
        SELECT * FROM r WHERE rank <= {TOP_N}""")
    n_rows = con.execute("SELECT count(*) FROM final").fetchone()[0]
    n_cov = con.execute("SELECT count(DISTINCT subawardee_uei) FROM final").fetchone()[0]
    n_t1 = con.execute("SELECT count(DISTINCT subawardee_uei) FROM final WHERE seed_tier='naics+psc'").fetchone()[0]
    print(f"DSBS total={n_dsbs:,} · cold (no sub history)={n_cold:,} · cold w/ declared PSC={n_psc_firms:,}")
    print(f"cold firms with >=1 lane={n_cov:,} ({100*n_cov/n_cold:.1f}% of cold) · with a naics+psc (both-declared) lane={n_t1:,}")
    print(f"rows (<= {TOP_N}/firm)={n_rows:,}")

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
        print("DEMO — a cold-start DSBS with declared PSC (naics+psc tier lanes surface first)")
        print("=" * 80)
        firm = con.execute("""SELECT subawardee_uei FROM final WHERE seed_tier='naics+psc'
            GROUP BY 1 HAVING count(*) BETWEEN 3 AND 8 ORDER BY 1 LIMIT 1""").fetchone()[0]
        nm, ndn, ndp = con.execute(f"SELECT any_value(legal_business_name), max(n_declared_naics), max(n_declared_psc) FROM final WHERE subawardee_uei='{firm}'").fetchone()
        print(f"  firm {firm} ({nm}) — declares {ndn} NAICS, {ndp} PSC\n  RECOMMENDED:")
        for r in con.execute(f"""SELECT rank, dst_combo, seed_tier, dst_naics_description, dst_psc_description,
            lane_n_primes, round(lane_median_amt) med, top_primes FROM final WHERE subawardee_uei='{firm}' ORDER BY rank""").fetchall():
            tp = ", ".join(list(r[7])[:3]) if r[7] else "-"
            print(f"   #{r[0]} {r[1]:<13} [{r[2]:<10}] primes={r[5]:<4} ${int(r[6]):>8,}  {str(r[3])[:22]:<22} | {str(r[4])[:28]}")
            print(f"        buyers: {tp[:88]}")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
