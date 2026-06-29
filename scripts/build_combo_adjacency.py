#!/usr/bin/env python3
"""Empirical NAICS+PSC combo-adjacency model — data-driven "equivalent lanes" for subawardees.

WHY
    Per-firm capability adjacency was previously a prefix heuristic (group by NAICS-4 ×
    PSC-family). That is arbitrary and width-sensitive. This builds the *empirical* version:
    treat each distinct (prime_naics, prime_psc) combo as a node, and connect two combos when
    the SAME subawardee has won work in both — "firms that win X also win Y" (market basket /
    collaborative-filtering over subawardee portfolios). The adjacency is learned from the
    full population once, then any firm's "equivalent lanes" is a neighbor lookup on the combos
    it already holds.

SOURCE (Gen-3 SoR, read-only)
    s3://data-sink/active/subaward_naics_psc/   (one row per FFATA subaward line; carries
    subawardee_uei + prime_awardee_uei + prime NAICS + joined prime PSC + amount)

NODE SET
    The 1,391 (NAICS,PSC) combos that ACTUALLY get subcontracted — the set where sub demand
    exists (vs ~28,600 combos in the full prime-contract universe). This is deliberate: a sub-
    rostering adjacency should live in the space where subcontracting actually happens.

TARGETS (Gen-3 native Lance v2.1, full-snapshot overwrite — derived rollup, not append SoR)
    s3://data-sink/active/subaward_combo_nodes/   one row per combo (1,391) — demand stats
    s3://data-sink/active/subaward_combo_edges/   directed edges (combo->combo) — co-occurrence

EDGE METRICS (per directed src->dst, computed over distinct subawardees)
    n_src     firms holding src                       deg(src)
    n_dst     firms holding dst                       deg(dst)
    n_both    firms holding BOTH                       co-occurrence count
    support   n_both / N_firms                         joint frequency
    confidence n_both / n_src                          P(dst | src)  — directional, drives recos
    lift      (n_both * N_firms) / (n_src * n_dst)      >1 ⇒ positive association (popularity-corrected)
    jaccard   n_both / (n_src + n_dst - n_both)         symmetric similarity
    All edges are kept (even n_both=1); consumers threshold on n_both / lift / confidence.

INDEXES
    nodes: BTREE combo_id, naics, psc
    edges: BTREE src_combo, dst_combo

    doppler run -- python3 scripts/build_combo_adjacency.py            # build + index + verify
    doppler run -- python3 scripts/build_combo_adjacency.py --demo     # build + worked examples
Read-only source; two Lance writes. Doppler core-x/prd.
"""
from __future__ import annotations
import os, sys
import duckdb, lance

SRC   = "s3://data-sink/active/subaward_naics_psc/"
NODES = "s3://data-sink/active/subaward_combo_nodes/"
EDGES = "s3://data-sink/active/subaward_combo_edges/"


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

    ds = lance.dataset(SRC, storage_options=opt)
    con.register("_e", ds.scanner(columns=[
        "subawardee_uei", "prime_awardee_uei", "prime_naics_code", "prime_psc_code",
        "prime_naics_description", "prime_psc_description", "subaward_amount"]).to_reader())
    # analytic grain: a subaward "win" with both codes present
    con.execute("""CREATE TABLE a AS SELECT
        upper(trim(subawardee_uei))            AS uei,
        upper(trim(prime_awardee_uei))         AS prime_uei,
        prime_naics_code || '|' || prime_psc_code AS combo,
        prime_naics_code                       AS naics,
        prime_psc_code                         AS psc,
        prime_naics_description                AS naics_desc,
        prime_psc_description                  AS psc_desc,
        coalesce(subaward_amount, 0)           AS amt
        FROM _e
        WHERE subawardee_uei IS NOT NULL AND trim(subawardee_uei) <> ''
          AND prime_naics_code IS NOT NULL AND trim(prime_naics_code) <> ''
          AND prime_psc_code  IS NOT NULL AND trim(prime_psc_code)  <> ''""")
    n_rows = con.execute("SELECT count(*) FROM a").fetchone()[0]
    n_firms = con.execute("SELECT count(DISTINCT uei) FROM a").fetchone()[0]

    # ── NODES: one row per combo, with demand stats + modal descriptions ──────────────
    con.execute("""CREATE TABLE nodes AS SELECT
        combo AS combo_id, naics, psc,
        any_value(naics_desc) AS naics_description,
        mode(psc_desc)        AS psc_description,
        count(DISTINCT uei)       AS n_subawardees,
        count(DISTINCT prime_uei) AS n_primes,
        count(*)                  AS n_subaward_lines,
        round(sum(amt))           AS total_subaward_amt,
        round(median(amt))        AS median_subaward_amt
        FROM a GROUP BY combo, naics, psc""")
    n_nodes = con.execute("SELECT count(*) FROM nodes").fetchone()[0]

    # ── EDGES: combo co-occurrence over distinct subawardee portfolios ────────────────
    con.execute("CREATE TABLE fc AS SELECT DISTINCT uei, combo FROM a")
    con.execute("CREATE TABLE deg AS SELECT combo, count(DISTINCT uei) n_firms FROM fc GROUP BY 1")
    # undirected co-occurrence (c1<c2), then mirror to directed
    con.execute("""CREATE TABLE co AS SELECT f1.combo c1, f2.combo c2, count(*) n_both
        FROM fc f1 JOIN fc f2 ON f1.uei = f2.uei AND f1.combo < f2.combo
        GROUP BY 1, 2""")
    con.execute(f"""CREATE TABLE edges AS
        WITH d(c1, c2, n_both) AS (
            SELECT c1, c2, n_both FROM co
            UNION ALL
            SELECT c2, c1, n_both FROM co
        )
        SELECT
            d.c1 AS src_combo, d.c2 AS dst_combo,
            s.n_firms AS n_src, t.n_firms AS n_dst, d.n_both,
            round(d.n_both::DOUBLE / {n_firms}, 8)                              AS support,
            round(d.n_both::DOUBLE / s.n_firms, 6)                              AS confidence,
            round((d.n_both::DOUBLE * {n_firms}) / (s.n_firms * t.n_firms), 4)  AS lift,
            round(d.n_both::DOUBLE / (s.n_firms + t.n_firms - d.n_both), 6)     AS jaccard
        FROM d JOIN deg s ON d.c1 = s.combo JOIN deg t ON d.c2 = t.combo""")
    n_edges = con.execute("SELECT count(*) FROM edges").fetchone()[0]

    print(f"source rows={n_rows:,} · distinct subawardees={n_firms:,}")
    print(f"NODES: {n_nodes:,} combos  ->  {NODES}")
    print(f"EDGES: {n_edges:,} directed combo->combo  ->  {EDGES}")

    # ── write durable Lance + scalar indexes ──────────────────────────────────────────
    for tbl, uri, idx in [
        ("nodes", NODES, [("combo_id", "BTREE"), ("naics", "BTREE"), ("psc", "BTREE")]),
        ("edges", EDGES, [("src_combo", "BTREE"), ("dst_combo", "BTREE")]),
    ]:
        lance.write_dataset(con.execute(f"SELECT * FROM {tbl}").to_arrow_table(), uri,
                            mode="overwrite", data_storage_version="2.1",
                            max_rows_per_file=1_048_576, storage_options=opt)
        d = lance.dataset(uri, storage_options=opt)
        for col, it in idx:
            try:
                d.create_scalar_index(col, index_type=it, replace=True)
            except Exception as exc:  # noqa: BLE001 — index miss must not fail a good load
                print(f"  index {col} skipped: {exc}")
        print(f"  wrote {uri} ({d.count_rows():,} rows, {len(idx)} indexes)")

    if demo:
        print("\n" + "=" * 78)
        print("DEMO 1 — top empirical neighbors of 561210|M1JZ (Facilities Support · Operation of Bldgs)")
        print("=" * 78)
        rows = con.execute("""SELECT e.dst_combo, n.naics_description, n.psc_description,
            e.n_both, e.confidence, e.lift, round(n.median_subaward_amt) med_amt
            FROM edges e JOIN nodes n ON e.dst_combo = n.combo_id
            WHERE e.src_combo = '561210|M1JZ' AND e.n_both >= 3
            ORDER BY e.lift DESC LIMIT 10""").fetchall()
        for r in rows:
            print(f"  {r[0]:<14} lift={r[5]:>6} conf={r[4]:<7} co={r[3]:<4} ${int(r[6]):>9,}  {str(r[1])[:26]:<26} | {str(r[2])[:34]}")

        print("\n" + "=" * 78)
        print("DEMO 2 — recommended expansion lanes for one firm (its combos' neighbors, not already held)")
        print("=" * 78)
        firm = con.execute("""SELECT uei FROM (
            SELECT uei, count(DISTINCT combo) c FROM a GROUP BY 1 HAVING c BETWEEN 2 AND 4) ORDER BY uei LIMIT 1""").fetchone()[0]
        held = [r[0] for r in con.execute(f"SELECT DISTINCT combo FROM a WHERE uei='{firm}'").fetchall()]
        print(f"  firm {firm} holds: {held}")
        inlist = "(" + ",".join("'" + c + "'" for c in held) + ")"
        rec = con.execute(f"""SELECT e.dst_combo, any_value(n.naics_description) nd, any_value(n.psc_description) pd,
            round(sum(e.lift * e.confidence), 3) score, max(e.n_both) co, round(any_value(n.median_subaward_amt)) med, any_value(n.n_primes) primes
            FROM edges e JOIN nodes n ON e.dst_combo = n.combo_id
            WHERE e.src_combo IN {inlist} AND e.dst_combo NOT IN {inlist} AND e.n_both >= 2
            GROUP BY e.dst_combo ORDER BY score DESC LIMIT 8""").fetchall()
        for r in rec:
            print(f"  -> {r[0]:<14} score={r[3]:<7} co={r[4]:<3} primes={r[6]:<4} ${int(r[5]):>8,}  {str(r[1])[:24]:<24} | {str(r[2])[:32]}")

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
