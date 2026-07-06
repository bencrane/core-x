#!/usr/bin/env python3
"""gtm capability inference — L2 cooccurrence matrix + the two per-entity projections.

Derived EVIDENCE only, ALL PRE-WEIGHTING: raw counts and sums, no weight, no score, no
rank, no threshold — consumers weight at read time. Rebuilt-and-swapped snapshots.

VERB VOCABULARY (contractual, used verbatim in the market registry):
  registered_*   SAM claims (not built here)
  primed_in_*    demonstrated as prime (a prime-side code lane)
  subbed_under_* the PRIME award's codes on subawards the firm delivered under — where
                 the firm worked as a sub, NEVER a claim of what it can prime
  inferred_*     cooccurrence evidence from both-sider firms, not a demonstration

PARAM SET v1 (every editorial decision, in one place):
  - substrate: gtm_entity_code_lanes (1 row per (uei, side, code_type, code); lifetime
    money per lane). Universes derive from LANE SIDE PRESENCE — a firm is a both-sider
    here iff it has >=1 sub lane AND >=1 prime lane (code-bearing awards only; this is
    deliberately the code-pairable universe, not the rollup's prime_and_sub flag, which
    also counts code-less awards that cannot pair).
  - matrix pairs are SAME-TYPE only in v1 (naics->naics, psc->psc). (naics,psc)
    same-award combo siblings are pinned for v2, not built here.
  - supporting_bothsider_firm_ct on the projections is the PAIR-SUM: Σ over the firm's
    matched subbed_under (resp. primed_in) codes of that pair's cooccurring_firm_ct.
    A both-sider supporting via k shared codes is counted k times. It is NOT a
    distinct-firm union: the exact union-distinct aggregation was measured infeasible
    at this grain (the naics slice alone overflowed a 44 GiB spill ceiling; psc is ~6x
    larger). Pair-sum is deterministic raw evidence; weight at read.
  - projections EXCLUDE demonstrated codes (a firm's inferred_primeable set excludes
    codes it already primes in; the subbable mirror excludes codes already subbed
    under) — inference only where there is no demonstration.

TARGETS (all overwrite):
  s3://data-sink/active/gtm_subbed_under_to_primed_in_cooccurrence/
      grain (subbed_under_code_type, subbed_under_code, primed_in_code_type,
             primed_in_code); BTREE subbed_under_code + primed_in_code,
      BITMAP subbed_under_code_type + primed_in_code_type
  s3://data-sink/active/gtm_entity_inferred_primeable_codes/
      grain (uei, code_type, code); universe = every uei with >=1 subbed_under lane;
      BTREE uei + code
  s3://data-sink/active/gtm_entity_inferred_subbable_codes/
      grain (uei, code_type, code); universe = every uei with >=1 primed_in lane;
      BTREE uei + code

    LANCE_BYPASS_SPILLING=true doppler run -p core-x -c prd -- python3 \
        scripts/build_gtm_capability_inference.py --as-of YYYY-MM-DD

Read-only sources; three Lance writes. Doppler core-x/prd.
"""
from __future__ import annotations
import argparse
import os
import sys
from datetime import date

import duckdb
import lance

A = "s3://data-sink/active"
MATRIX_OUT = f"{A}/gtm_subbed_under_to_primed_in_cooccurrence/"
PRIMEABLE_OUT = f"{A}/gtm_entity_inferred_primeable_codes/"
SUBBABLE_OUT = f"{A}/gtm_entity_inferred_subbable_codes/"
PARAM_SET_ID = "v1"
# uei-hash buckets per code_type keep each projection join+group within memory+spill
# bounds (psc fan-out is ~6x naics).
BUCKETS = {"naics": 4, "psc": 16}


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", default=date.today().isoformat())
    args = ap.parse_args()
    as_of = args.as_of

    opt = so()
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")

    lanes_ds = lance.dataset(f"{A}/gtm_entity_code_lanes/", storage_options=opt)
    built_from = f"gtm_entity_code_lanes:v{lanes_ds.version}"
    print(f"sources: {built_from}  as_of={as_of}", flush=True)

    reader = lanes_ds.scanner(
        columns=["uei", "side", "code_type", "code", "obl_lifetime"],
        filter="uei IS NOT NULL AND code IS NOT NULL").to_reader()
    con.register("_l", reader)
    con.execute("CREATE TABLE lanes AS SELECT * FROM _l")
    con.unregister("_l")
    print(f"lanes landed: {con.execute('SELECT COUNT(*) FROM lanes').fetchone()[0]:,}", flush=True)

    # ── universes (lane side presence) ────────────────────────────────────────────────────
    sub_only, prime_only, both = con.execute("""
      WITH u AS (SELECT uei, MAX(side='sub')::INT AS s, MAX(side='prime')::INT AS p
                 FROM lanes GROUP BY 1)
      SELECT SUM(s*(1-p)), SUM(p*(1-s)), SUM(s*p) FROM u
    """).fetchone()
    print(f"universes: sub_only={sub_only:,}  prime_only={prime_only:,}  both_sider={both:,}",
          flush=True)
    con.execute("""CREATE TABLE both_u AS
                   SELECT uei FROM lanes WHERE side='sub'
                   INTERSECT SELECT uei FROM lanes WHERE side='prime'""")
    con.execute("""CREATE TABLE s_both AS SELECT l.uei, l.code_type, l.code, l.obl_lifetime
                   FROM lanes l JOIN both_u USING (uei) WHERE side='sub'""")
    con.execute("""CREATE TABLE p_both AS SELECT l.uei, l.code_type, l.code, l.obl_lifetime
                   FROM lanes l JOIN both_u USING (uei) WHERE side='prime'""")

    # ── output 1: the cooccurrence matrix (same-type pairs over both-siders) ──────────────
    con.execute(f"""
    CREATE TABLE matrix AS
    WITH pairs AS (
      SELECT s.code_type AS subbed_under_code_type, s.code AS subbed_under_code,
             p.code_type AS primed_in_code_type, p.code AS primed_in_code,
             COUNT(DISTINCT s.uei)  AS cooccurring_firm_ct,
             SUM(s.obl_lifetime)    AS cooccurring_sub_amt_lifetime,
             SUM(p.obl_lifetime)    AS cooccurring_prime_obl_lifetime
      FROM s_both s
      JOIN p_both p ON s.uei = p.uei AND s.code_type = p.code_type
      GROUP BY 1, 2, 3, 4
    ),
    sub_denom AS (
      SELECT code_type, code, COUNT(DISTINCT uei) AS subbed_under_firm_ct
      FROM s_both GROUP BY 1, 2
    ),
    prime_denom AS (
      SELECT code_type, code, COUNT(DISTINCT uei) AS primed_in_firm_ct
      FROM p_both GROUP BY 1, 2
    )
    SELECT pr.subbed_under_code_type, pr.subbed_under_code,
           pr.primed_in_code_type, pr.primed_in_code,
           pr.cooccurring_firm_ct,
           sd.subbed_under_firm_ct,
           pd.primed_in_firm_ct,
           pr.cooccurring_sub_amt_lifetime,
           pr.cooccurring_prime_obl_lifetime,
           DATE '{as_of}' AS as_of, '{built_from}' AS built_from_version,
           '{PARAM_SET_ID}' AS param_set_id
    FROM pairs pr
    JOIN sub_denom sd ON pr.subbed_under_code_type = sd.code_type
                      AND pr.subbed_under_code = sd.code
    JOIN prime_denom pd ON pr.primed_in_code_type = pd.code_type
                        AND pr.primed_in_code = pd.code
    """)
    n_matrix = con.execute("SELECT COUNT(*) FROM matrix").fetchone()[0]
    by_type = con.execute(
        "SELECT subbed_under_code_type, COUNT(*) FROM matrix GROUP BY 1 ORDER BY 1").fetchall()
    print(f"matrix cells: {n_matrix:,}  by type: {by_type}", flush=True)

    # ── output 2/3: per-entity projections (bucketed join+group; pair-sum support) ───────
    def build_projection(name: str, from_side: str, matrix_from: str, matrix_to: str,
                         matched_ct_col: str, amt_col: str, exclude_side: str) -> str:
        con.execute(f"""
        CREATE TABLE {name} (uei VARCHAR, code_type VARCHAR, code VARCHAR,
                             supporting_bothsider_firm_ct BIGINT,
                             {matched_ct_col} BIGINT, {amt_col} DOUBLE)
        """)
        for ct, nb in BUCKETS.items():
            for b in range(nb):
                con.execute(f"""
                INSERT INTO {name}
                SELECT f.uei, m.{matrix_to}_code_type AS code_type, m.{matrix_to}_code AS code,
                       SUM(m.cooccurring_firm_ct) AS supporting_bothsider_firm_ct,
                       COUNT(*)                   AS {matched_ct_col},
                       SUM(f.obl_lifetime)        AS {amt_col}
                FROM (SELECT uei, code, obl_lifetime FROM lanes
                      WHERE side = '{from_side}' AND code_type = '{ct}'
                        AND hash(uei) % {nb} = {b}) f
                JOIN matrix m ON m.{matrix_from}_code_type = '{ct}'
                             AND m.{matrix_from}_code = f.code
                GROUP BY 1, 2, 3
                """)
            print(f"{name}: {ct} buckets done "
                  f"(rows so far {con.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0]:,})",
                  flush=True)
        # inference only where there is no demonstration: drop codes already on the
        # excluded (demonstrated) side for that firm.
        con.execute(f"""
        CREATE TABLE {name}_final AS
        SELECT t.*, DATE '{as_of}' AS as_of, '{built_from}' AS built_from_version,
               '{PARAM_SET_ID}' AS param_set_id
        FROM {name} t
        ANTI JOIN (SELECT uei, code_type, code FROM lanes WHERE side = '{exclude_side}') d
          USING (uei, code_type, code)
        """)
        con.execute(f"DROP TABLE {name}")
        return f"{name}_final"

    primeable = build_projection(
        "primeable", from_side="sub", matrix_from="subbed_under", matrix_to="primed_in",
        matched_ct_col="matched_subbed_under_code_ct", amt_col="sub_amt_via_matched_codes",
        exclude_side="prime")
    n_primeable = con.execute(f"SELECT COUNT(*) FROM {primeable}").fetchone()[0]
    print(f"inferred_primeable rows: {n_primeable:,}", flush=True)

    subbable = build_projection(
        "subbable", from_side="prime", matrix_from="primed_in", matrix_to="subbed_under",
        matched_ct_col="matched_primed_in_code_ct", amt_col="prime_obl_via_matched_codes",
        exclude_side="sub")
    n_subbable = con.execute(f"SELECT COUNT(*) FROM {subbable}").fetchone()[0]
    print(f"inferred_subbable rows: {n_subbable:,}", flush=True)

    # ── invariants (fail-closed before any write) ─────────────────────────────────────────
    dup_m = con.execute("""SELECT COUNT(*) FROM (
        SELECT subbed_under_code_type, subbed_under_code, primed_in_code_type, primed_in_code
        FROM matrix GROUP BY 1,2,3,4 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup_m == 0, f"matrix grain not unique: {dup_m} dups"
    # projection uniqueness holds by construction (per-(type,bucket) GROUP BY over
    # hash-disjoint uei slices, then a row-preserving anti-join); verify it directly on a
    # deterministic 1/64 uei-hash slice of each (a full 100M-row dup-check group-by is
    # exactly the aggregation that cannot fit this machine).
    for t in (primeable, subbable):
        dup = con.execute(f"""SELECT COUNT(*) FROM (
            SELECT uei, code_type, code FROM {t} WHERE hash(uei) % 64 = 7
            GROUP BY 1,2,3 HAVING COUNT(*) > 1)""").fetchone()[0]
        assert dup == 0, f"{t} grain not unique on verification slice: {dup} dups"
        neg = con.execute(f"""SELECT COUNT(*) FROM {t}
            WHERE supporting_bothsider_firm_ct <= 0""").fetchone()[0]
        assert neg == 0, f"{t}: non-positive support counts: {neg}"
    ct_bad = con.execute("SELECT COUNT(*) FROM matrix WHERE cooccurring_firm_ct <= 0 "
                         "OR subbed_under_firm_ct <= 0 OR primed_in_firm_ct <= 0").fetchone()[0]
    assert ct_bad == 0, f"matrix: non-positive counts: {ct_bad}"
    same_type = con.execute("SELECT COUNT(*) FROM matrix "
                            "WHERE subbed_under_code_type != primed_in_code_type").fetchone()[0]
    assert same_type == 0, f"v1 is same-type pairs only; {same_type} cross-type rows"
    print("invariants OK", flush=True)

    # ── write + index ─────────────────────────────────────────────────────────────────────
    for table, out, indexes in (
        ("matrix", MATRIX_OUT, [("subbed_under_code", "BTREE"), ("primed_in_code", "BTREE"),
                                ("subbed_under_code_type", "BITMAP"),
                                ("primed_in_code_type", "BITMAP")]),
        (primeable, PRIMEABLE_OUT, [("uei", "BTREE"), ("code", "BTREE")]),
        (subbable, SUBBABLE_OUT, [("uei", "BTREE"), ("code", "BTREE")]),
    ):
        res = con.execute(f"SELECT * FROM {table}")
        reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
        lance.write_dataset(reader, out, mode="overwrite", storage_options=opt)
        ds = lance.dataset(out, storage_options=opt)
        for col, idx in indexes:
            ds.create_scalar_index(col, idx)
        print(f"wrote {out}  v{ds.version}  rows={ds.count_rows():,}  "
              f"indexes={[c for c, _ in indexes]}", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
