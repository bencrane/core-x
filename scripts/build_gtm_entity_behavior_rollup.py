#!/usr/bin/env python3
"""gtm_entity_behavior_rollup + gtm_entity_code_lanes — L2 audience projections (one build, two grains).

Derived BEHAVIOR only — deterministic arithmetic over the two canonical award spines. No facts
copied from the entity layer (join gtm_sam_entities / sam_master_entities on uei at query time),
no vendor data, no editorial scores. Rebuilt-and-swapped snapshot, never appended.

  gtm_entity_behavior_rollup   1 row per uei          window money, recency, active posture,
                                                      breadth summary, role flags, provenance
  gtm_entity_code_lanes        1 row per              per-code money for "in these NAICS/PSC"
                               (uei, side,            audience cuts; side = prime|sub
                               code_type, code)       (sub codes are the prime award's, inherited)

PARAM SET v1 (every editorial decision, in one place):
  - population: every uei seen in either spine (recipient_uei / subawardee_uei NOT NULL)
  - prime universe: category='contract' OR category IS NULL (263k NULL-category rows probed
    2026-07-05: all carry uei+piid, $126.8B — real contracts missing the label). category='idv'
    EXCLUDED (vehicle parents; ceiling/double-count risk). Sub universe: all rows (probed 100%
    sub-contract/procurement).
  - windows: 12/24/36/60 months back from --as-of, by action_date (award grain: the window
    means "awarded in window", not transaction-level outlay timing — the spine has no txn grain)
  - SUB REPORTING LAG: FSRS subaward reporting trails the award, sometimes badly. Sub-side
    short windows (12/24mo) are FLOORS, not truth — reliable sub recency is 60mo/lifetime.
    Columns stay populated (they are the objective sum of what is reported); interpretation
    caveat lives here.
  - ACTIVE POSTURE DEFERRED (v1): the spine's period_of_performance_current_end_date has a
    hard ceiling at build-date-1 and potential_end_date is 100% NULL (probed 2026-07-05) —
    active_award_ct / active_obl / earliest_pop_end / pop_expiring_180d_ct cannot be truthfully
    derived. Restore them when the L1 substrate is fixed (see the PoP end-date task).
  - flags at the reliable window per side: is_prime_24mo (FPDS reports fast) /
    is_sub_60mo (lag physics above); prime_and_sub = lifetime both-sider (teaming signal)
  - lanes: prime naics/psc from the award row; sub naics/psc from prime_award_* inline codes;
    NULL codes emit no lane row

TARGET: s3://data-sink/active/gtm_entity_behavior_rollup/   (overwrite; BTREE uei)
        s3://data-sink/active/gtm_entity_code_lanes/        (overwrite; BTREE uei, code ·
                                                             BITMAP side, code_type)

    LANCE_BYPASS_SPILLING=true doppler run -- python3 \
        scripts/build_gtm_entity_behavior_rollup.py --as-of YYYY-MM-DD

Read-only sources; two Lance writes. Doppler core-x/prd.
"""
from __future__ import annotations
import argparse
import os
import sys
from datetime import date

import duckdb
import lance

A = "s3://data-sink/active"
ROLLUP_OUT = f"{A}/gtm_entity_behavior_rollup/"
LANES_OUT = f"{A}/gtm_entity_code_lanes/"
PARAM_SET_ID = "v1"


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
    con.execute("SET memory_limit='14GB'; SET threads TO 4;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")

    # ── land the two narrow projections (Lance scanners are one-shot: stream each into a
    # DuckDB table under its own name, then every aggregate reads the DuckDB copy) ──────────
    prime_ds = lance.dataset(f"{A}/usaspending_award_canonical/", storage_options=opt)
    sub_ds = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    built_from = (f"usaspending_award_canonical:v{prime_ds.version}"
                  f"|usaspending_subaward_canonical:v{sub_ds.version}")
    print(f"sources: {built_from}  as_of={as_of}", flush=True)

    prime_reader = prime_ds.scanner(
        columns=["recipient_uei", "total_obligation", "action_date", "naics_code",
                 "product_or_service_code", "awarding_agency_code"],
        filter="(category = 'contract' OR category IS NULL) AND recipient_uei IS NOT NULL",
    ).to_reader()
    con.register("_pr", prime_reader)
    con.execute("CREATE TABLE prime AS SELECT * FROM _pr")
    con.unregister("_pr")
    print(f"prime landed: {con.execute('SELECT COUNT(*) FROM prime').fetchone()[0]:,}", flush=True)

    sub_reader = sub_ds.scanner(
        columns=["subawardee_uei", "subaward_amount", "subaward_action_date",
                 "prime_award_naics_code", "prime_award_product_or_service_code"],
        filter="subawardee_uei IS NOT NULL",
    ).to_reader()
    con.register("_sr", sub_reader)
    con.execute("CREATE TABLE sub AS SELECT * FROM _sr")
    con.unregister("_sr")
    print(f"sub landed: {con.execute('SELECT COUNT(*) FROM sub').fetchone()[0]:,}", flush=True)

    w = {n: f"(DATE '{as_of}' - INTERVAL {n} MONTH)" for n in (12, 24, 36, 60)}
    today = f"DATE '{as_of}'"

    # ── rollup: 1 row per uei ────────────────────────────────────────────────────────────────
    con.execute(f"""
    CREATE TABLE rollup AS
    WITH p AS (
      SELECT recipient_uei AS uei,
             SUM(CASE WHEN action_date >= {w[12]} THEN total_obligation ELSE 0 END) AS prime_obl_12mo,
             SUM(CASE WHEN action_date >= {w[24]} THEN total_obligation ELSE 0 END) AS prime_obl_24mo,
             SUM(CASE WHEN action_date >= {w[36]} THEN total_obligation ELSE 0 END) AS prime_obl_36mo,
             SUM(CASE WHEN action_date >= {w[60]} THEN total_obligation ELSE 0 END) AS prime_obl_60mo,
             SUM(total_obligation)                                                  AS prime_obl_lifetime,
             COUNT(*) FILTER (action_date >= {w[24]})                               AS prime_award_ct_24mo,
             COUNT(*) FILTER (action_date >= {w[60]})                               AS prime_award_ct_60mo,
             COUNT(*)                                                               AS prime_award_ct_lifetime,
             MIN(action_date)                                                       AS p_first_action,
             MAX(action_date)                                                       AS p_last_action,
             COUNT(DISTINCT naics_code)                                             AS distinct_naics_ct,
             COUNT(DISTINCT product_or_service_code)                                AS distinct_psc_ct,
             COUNT(DISTINCT awarding_agency_code)                                   AS distinct_agency_ct
      FROM prime GROUP BY 1
    ),
    top_naics AS (
      SELECT uei, naics_code AS top_naics FROM (
        SELECT recipient_uei AS uei, naics_code,
               ROW_NUMBER() OVER (PARTITION BY recipient_uei
                                  ORDER BY SUM(total_obligation) DESC NULLS LAST, naics_code) AS rn
        FROM prime WHERE naics_code IS NOT NULL GROUP BY 1, 2
      ) WHERE rn = 1
    ),
    top_agency AS (
      SELECT uei, awarding_agency_code AS top_agency_code FROM (
        SELECT recipient_uei AS uei, awarding_agency_code,
               ROW_NUMBER() OVER (PARTITION BY recipient_uei
                                  ORDER BY SUM(total_obligation) DESC NULLS LAST, awarding_agency_code) AS rn
        FROM prime WHERE awarding_agency_code IS NOT NULL GROUP BY 1, 2
      ) WHERE rn = 1
    ),
    s AS (
      SELECT subawardee_uei AS uei,
             SUM(CASE WHEN subaward_action_date >= {w[24]} THEN subaward_amount ELSE 0 END) AS sub_amt_24mo,
             SUM(CASE WHEN subaward_action_date >= {w[60]} THEN subaward_amount ELSE 0 END) AS sub_amt_60mo,
             SUM(subaward_amount)                                                           AS sub_amt_lifetime,
             COUNT(*)                                                                       AS sub_ct_lifetime,
             COUNT(*) FILTER (subaward_action_date >= {w[60]})                              AS sub_ct_60mo,
             MIN(subaward_action_date)                                                      AS s_first_action,
             MAX(subaward_action_date)                                                      AS s_last_action
      FROM sub GROUP BY 1
    )
    SELECT COALESCE(p.uei, s.uei)                                        AS uei,
           COALESCE(p.prime_obl_12mo, 0)                                 AS prime_obl_12mo,
           COALESCE(p.prime_obl_24mo, 0)                                 AS prime_obl_24mo,
           COALESCE(p.prime_obl_36mo, 0)                                 AS prime_obl_36mo,
           COALESCE(p.prime_obl_60mo, 0)                                 AS prime_obl_60mo,
           COALESCE(p.prime_obl_lifetime, 0)                             AS prime_obl_lifetime,
           COALESCE(p.prime_award_ct_24mo, 0)                            AS prime_award_ct_24mo,
           COALESCE(p.prime_award_ct_60mo, 0)                            AS prime_award_ct_60mo,
           COALESCE(p.prime_award_ct_lifetime, 0)                        AS prime_award_ct_lifetime,
           COALESCE(s.sub_amt_24mo, 0)                                   AS sub_amt_24mo,
           COALESCE(s.sub_amt_60mo, 0)                                   AS sub_amt_60mo,
           COALESCE(s.sub_amt_lifetime, 0)                               AS sub_amt_lifetime,
           COALESCE(s.sub_ct_lifetime, 0)                                AS sub_ct_lifetime,
           LEAST(COALESCE(p.p_first_action, s.s_first_action),
                 COALESCE(s.s_first_action, p.p_first_action))           AS first_action_date,
           GREATEST(COALESCE(p.p_last_action, s.s_last_action),
                    COALESCE(s.s_last_action, p.p_last_action))          AS last_action_date,
           DATEDIFF('day',
                    GREATEST(COALESCE(p.p_last_action, s.s_last_action),
                             COALESCE(s.s_last_action, p.p_last_action)),
                    {today})                                             AS days_since_last_action,
           COALESCE(p.distinct_naics_ct, 0)                              AS distinct_naics_ct,
           COALESCE(p.distinct_psc_ct, 0)                                AS distinct_psc_ct,
           COALESCE(p.distinct_agency_ct, 0)                             AS distinct_agency_ct,
           tn.top_naics                                                  AS top_naics,
           ta.top_agency_code                                            AS top_agency_code,
           (COALESCE(p.prime_award_ct_24mo, 0) > 0)                      AS is_prime_24mo,
           (COALESCE(s.sub_ct_60mo, 0) > 0)                              AS is_sub_60mo,
           (COALESCE(p.prime_award_ct_lifetime, 0) > 0
            AND COALESCE(s.sub_ct_lifetime, 0) > 0)                      AS prime_and_sub,
           {today}                                                       AS as_of,
           '{built_from}'                                                AS built_from_version,
           '{PARAM_SET_ID}'                                              AS param_set_id
    FROM p
    FULL OUTER JOIN s ON p.uei = s.uei
    LEFT JOIN top_naics tn ON COALESCE(p.uei, s.uei) = tn.uei
    LEFT JOIN top_agency ta ON COALESCE(p.uei, s.uei) = ta.uei
    """)
    n_rollup = con.execute("SELECT COUNT(*) FROM rollup").fetchone()[0]
    print(f"rollup rows: {n_rollup:,}", flush=True)

    # ── lanes: 1 row per (uei, side, code_type, code) ────────────────────────────────────────
    con.execute(f"""
    CREATE TABLE lanes AS
    SELECT recipient_uei AS uei, 'prime' AS side, 'naics' AS code_type, naics_code AS code,
           SUM(CASE WHEN action_date >= {w[12]} THEN total_obligation ELSE 0 END) AS obl_12mo,
           SUM(CASE WHEN action_date >= {w[24]} THEN total_obligation ELSE 0 END) AS obl_24mo,
           SUM(CASE WHEN action_date >= {w[60]} THEN total_obligation ELSE 0 END) AS obl_60mo,
           SUM(total_obligation) AS obl_lifetime,
           COUNT(*) AS action_ct, MAX(action_date) AS last_action_date
    FROM prime WHERE naics_code IS NOT NULL GROUP BY 1, 4
    UNION ALL
    SELECT recipient_uei, 'prime', 'psc', product_or_service_code,
           SUM(CASE WHEN action_date >= {w[12]} THEN total_obligation ELSE 0 END),
           SUM(CASE WHEN action_date >= {w[24]} THEN total_obligation ELSE 0 END),
           SUM(CASE WHEN action_date >= {w[60]} THEN total_obligation ELSE 0 END),
           SUM(total_obligation), COUNT(*), MAX(action_date)
    FROM prime WHERE product_or_service_code IS NOT NULL GROUP BY 1, 4
    UNION ALL
    SELECT subawardee_uei, 'sub', 'naics', prime_award_naics_code,
           SUM(CASE WHEN subaward_action_date >= {w[12]} THEN subaward_amount ELSE 0 END),
           SUM(CASE WHEN subaward_action_date >= {w[24]} THEN subaward_amount ELSE 0 END),
           SUM(CASE WHEN subaward_action_date >= {w[60]} THEN subaward_amount ELSE 0 END),
           SUM(subaward_amount), COUNT(*), MAX(subaward_action_date)
    FROM sub WHERE prime_award_naics_code IS NOT NULL GROUP BY 1, 4
    UNION ALL
    SELECT subawardee_uei, 'sub', 'psc', prime_award_product_or_service_code,
           SUM(CASE WHEN subaward_action_date >= {w[12]} THEN subaward_amount ELSE 0 END),
           SUM(CASE WHEN subaward_action_date >= {w[24]} THEN subaward_amount ELSE 0 END),
           SUM(CASE WHEN subaward_action_date >= {w[60]} THEN subaward_amount ELSE 0 END),
           SUM(subaward_amount), COUNT(*), MAX(subaward_action_date)
    FROM sub WHERE prime_award_product_or_service_code IS NOT NULL GROUP BY 1, 4
    """)
    n_lanes = con.execute("SELECT COUNT(*) FROM lanes").fetchone()[0]
    print(f"lanes rows: {n_lanes:,}", flush=True)

    # ── invariants (fail-closed before any write) ────────────────────────────────────────────
    dup = con.execute("SELECT COUNT(*) FROM (SELECT uei FROM rollup GROUP BY 1 HAVING COUNT(*) > 1)").fetchone()[0]
    assert dup == 0, f"rollup uei not unique: {dup} dups"
    dup_l = con.execute(
        "SELECT COUNT(*) FROM (SELECT uei, side, code_type, code FROM lanes GROUP BY 1,2,3,4 HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    assert dup_l == 0, f"lanes key not unique: {dup_l} dups"
    src_sum, roll_sum = con.execute(
        "SELECT (SELECT SUM(total_obligation) FROM prime), (SELECT SUM(prime_obl_lifetime) FROM rollup)"
    ).fetchone()
    # $100 band on a ~$5.8T sum: float64 group-vs-global summation-order noise is ~$2 at this
    # magnitude; real loss (a dropped uei group) is >= thousands. Relative ~2e-11.
    assert abs(src_sum - roll_sum) < 100.0, f"obligation conservation failed: {src_sum} vs {roll_sum}"
    print(f"invariants OK  (prime obl conserved: ${src_sum/1e12:.3f}T)", flush=True)

    # ── write + index ────────────────────────────────────────────────────────────────────────
    for table, out, indexes in (
        ("rollup", ROLLUP_OUT, [("uei", "BTREE")]),
        ("lanes", LANES_OUT, [("uei", "BTREE"), ("code", "BTREE"),
                              ("side", "BITMAP"), ("code_type", "BITMAP")]),
    ):
        res = con.execute(f"SELECT * FROM {table}")
        reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
        lance.write_dataset(reader, out, mode="overwrite", storage_options=opt)
        ds = lance.dataset(out, storage_options=opt)
        for col, idx in indexes:
            ds.create_scalar_index(col, idx)
        print(f"wrote {out}  v{ds.version}  rows={ds.count_rows():,}  indexes={[c for c, _ in indexes]}",
              flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
