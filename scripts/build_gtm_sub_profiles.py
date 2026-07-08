#!/usr/bin/env python3
"""gtm_sub_profiles — one row per subawardee: the peer-set + percentile dimensions.

SoR  s3://data-sink/active/gtm_sub_profiles/
     (1 row / sub_uei over the FULL FSRS canonical spine; Lance; derived,
      snapshot-overwrite; BTREE on uei)

WHY
Peer-set construction (pre-call brief Act 3 / blob schema §2, frozen in
docs/reference/SUB_UNIVERSE_BLOB_SCHEMA_AND_NODE_GRAMMAR.md) needs, for EVERY
subawardee at once: deal band (p20–p80 + median chunk), performance states,
lane breadth, buyer count/concentration, and 5y trajectory. No existing mart
carries these at sub grain: gtm_sub_combo_lanes has lanes but no chunks/states;
gtm_entity_behavior_rollup has $ windows but no band/states/trajectory;
gtm_prime_sub_pairs has edges but no percentiles. This mart closes exactly that
set — nothing speculative.

FIELDS (all computed over the sub's OWN subaward record)
  deal band       median/p20/p80 chunk, lifetime + 60mo — the pinned p20–p80
                  band comparator. Chunk stats run over raw signed line amounts
                  (NET, de-obligations included — same doctrine as the farm-out
                  mart).
  pop_states      DISTINCT sub place-of-performance states, $-ordered desc
                  (LIST<VARCHAR>) + top_pop_state — the ≥1-shared-state peer
                  predicate.
  breadth         n_lanes_lifetime (distinct non-null NAICS×PSC combos),
                  n_distinct_primes_lifetime / _60mo.
  concentration   top_buyer_share_lifetime_pct + top_buyer_uei.
  trajectory      cagr_5y_pct over complete-calendar-year sums (first→last of
                  the trailing 5 complete years with activity); NULL with fewer
                  than 2 active years in that window or a non-positive base —
                  a one-year record has no trajectory (null ≠ 0).

NULL SEMANTICS: unknown ≠ zero. Percentiles/CAGR are null where the record
cannot support them; pop_states is empty-list only when no US state is stamped.

WINDOWS rolling, anchored at build-time as_of (12/24/60mo + lifetime) — same
staleness model as the sibling GTM marts; rerun to refresh.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_sub_profiles.py [--verify]
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import lance

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

A = "s3://data-sink/active"
OUT = f"{A}/gtm_sub_profiles/"
PARAM_SET_ID = "v1"
BTREE = ["uei"]


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def _cx():
    con = duckdb.connect()
    con.execute("SET memory_limit='20GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")
    return con


def build() -> int:
    opt = so()
    today = date.today()
    as_of = today.isoformat()
    w12 = (today - timedelta(days=365)).isoformat()
    w24 = (today - timedelta(days=730)).isoformat()
    w60 = (today - timedelta(days=1826)).isoformat()
    # trailing 5 COMPLETE calendar years for the trajectory basis
    y_hi = today.year - 1
    y_lo = y_hi - 4
    con = _cx()

    se = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    con.register("_se", se.scanner(
        columns=["subawardee_uei", "prime_awardee_uei", "subaward_amount",
                 "subaward_action_date", "prime_award_naics_code",
                 "prime_award_product_or_service_code",
                 "subaward_primary_place_of_performance_state_code"],
        filter="subawardee_uei IS NOT NULL").to_reader())
    con.execute("CREATE TABLE edges AS SELECT * FROM _se")

    con.execute(f"""CREATE TABLE base AS
        SELECT subawardee_uei AS uei,
               SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w12}') AS sub_amt_12mo,
               SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w24}') AS sub_amt_24mo,
               SUM(subaward_amount) FILTER (subaward_action_date >= DATE '{w60}') AS sub_amt_60mo,
               SUM(subaward_amount) AS sub_amt_lifetime,
               COUNT(*) FILTER (subaward_action_date >= DATE '{w24}') AS n_subawards_24mo,
               COUNT(*) AS n_subawards_lifetime,
               COUNT(DISTINCT prime_awardee_uei) FILTER (subaward_action_date >= DATE '{w60}') AS n_distinct_primes_60mo,
               COUNT(DISTINCT prime_awardee_uei) AS n_distinct_primes_lifetime,
               COUNT(DISTINCT (prime_award_naics_code, prime_award_product_or_service_code))
                   FILTER (prime_award_naics_code IS NOT NULL AND prime_award_product_or_service_code IS NOT NULL)
                   AS n_lanes_lifetime,
               MEDIAN(subaward_amount) AS median_chunk_lifetime,
               QUANTILE_CONT(subaward_amount, 0.20) AS p20_chunk_lifetime,
               QUANTILE_CONT(subaward_amount, 0.80) AS p80_chunk_lifetime,
               MEDIAN(subaward_amount) FILTER (subaward_action_date >= DATE '{w60}') AS median_chunk_60mo,
               QUANTILE_CONT(subaward_amount, 0.20) FILTER (subaward_action_date >= DATE '{w60}') AS p20_chunk_60mo,
               QUANTILE_CONT(subaward_amount, 0.80) FILTER (subaward_action_date >= DATE '{w60}') AS p80_chunk_60mo,
               MIN(subaward_action_date) AS first_action_date,
               MAX(subaward_action_date) AS last_action_date
        FROM edges GROUP BY 1""")

    # top-buyer concentration (lifetime $ share of the single largest prime)
    con.execute("""CREATE TABLE conc AS
        SELECT uei, top_buyer_uei,
               CASE WHEN total > 0 THEN ROUND(100.0 * top_amt / total, 2) END AS top_buyer_share_lifetime_pct
        FROM (SELECT subawardee_uei AS uei,
                     arg_max(prime_awardee_uei, amt) AS top_buyer_uei,
                     MAX(amt) AS top_amt, SUM(amt) AS total
              FROM (SELECT subawardee_uei, prime_awardee_uei,
                           SUM(subaward_amount) AS amt
                    FROM edges GROUP BY 1, 2)
              GROUP BY 1)""")

    # performance states, $-ordered desc
    con.execute("""CREATE TABLE popst AS
        SELECT uei, LIST(state ORDER BY amt DESC) AS pop_states,
               arg_max(state, amt) AS top_pop_state
        FROM (SELECT subawardee_uei AS uei,
                     TRIM(subaward_primary_place_of_performance_state_code) AS state,
                     SUM(subaward_amount) AS amt
              FROM edges
              WHERE TRIM(COALESCE(subaward_primary_place_of_performance_state_code, '')) <> ''
              GROUP BY 1, 2)
        GROUP BY 1""")

    # 5y trajectory over complete calendar years: CAGR from first->last active
    # year within [y_lo, y_hi]; needs >=2 active years and a positive base.
    con.execute(f"""CREATE TABLE traj AS
        SELECT uei,
               CASE WHEN n_years >= 2 AND first_amt > 0 AND span > 0
                    THEN ROUND(100.0 * (POW(last_amt / first_amt, 1.0 / span) - 1), 2)
               END AS cagr_5y_pct,
               n_years AS cagr_n_years
        FROM (SELECT uei, COUNT(*) AS n_years,
                     arg_min(amt, yr) AS first_amt, arg_max(amt, yr) AS last_amt,
                     MAX(yr) - MIN(yr) AS span
              FROM (SELECT subawardee_uei AS uei,
                           YEAR(subaward_action_date) AS yr, SUM(subaward_amount) AS amt
                    FROM edges
                    WHERE YEAR(subaward_action_date) BETWEEN {y_lo} AND {y_hi}
                    GROUP BY 1, 2)
              WHERE amt > 0
              GROUP BY 1)""")

    con.execute("""CREATE TABLE profiles AS
        SELECT b.*, c.top_buyer_uei, c.top_buyer_share_lifetime_pct,
               p.pop_states, p.top_pop_state, t.cagr_5y_pct, t.cagr_n_years
        FROM base b
        LEFT JOIN conc c USING (uei)
        LEFT JOIN popst p USING (uei)
        LEFT JOIN traj t USING (uei)""")
    n = con.execute("SELECT COUNT(*) FROM profiles").fetchone()[0]
    print(f"sub profiles: {n:,} rows", flush=True)

    res = con.execute(f"""SELECT *, DATE '{as_of}' AS as_of,
        'usaspending_subaward_canonical:v{se.version}' AS built_from_version,
        '{PARAM_SET_ID}' AS param_set_id FROM profiles""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE], storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    """Spot-verify one sub against a direct spine pass: lifetime $, median,
    band ordering, top-buyer share, state ordering, and null-not-zero CAGR."""
    import statistics
    opt = so()
    ds = lance.dataset(OUT, storage_options=opt)
    # pick a mid-size probe from the mart itself (deterministic: heaviest
    # lifetime $ among subs with 20..200 lifetime subawards)
    t = ds.scanner(columns=["uei", "sub_amt_lifetime", "n_subawards_lifetime"]).to_table().to_pylist()
    cands = [r for r in t if r["n_subawards_lifetime"] and 20 <= r["n_subawards_lifetime"] <= 200]
    probe = max(cands, key=lambda r: float(r["sub_amt_lifetime"] or 0))["uei"]
    mart = ds.scanner(filter=f"uei = '{probe}'").to_table().to_pylist()[0]

    se = lance.dataset(f"{A}/usaspending_subaward_canonical/", storage_options=opt)
    raw = se.scanner(filter=f"subawardee_uei = '{probe}'",
                     columns=["prime_awardee_uei", "subaward_amount",
                              "subaward_primary_place_of_performance_state_code"]).to_table().to_pylist()
    amts = [float(r["subaward_amount"] or 0) for r in raw]
    if abs(sum(amts) - float(mart["sub_amt_lifetime"])) > 0.01:
        print(f"FAIL lifetime $: raw {sum(amts):,.2f} vs mart {mart['sub_amt_lifetime']:,.2f}")
        return 1
    if abs(statistics.median(amts) - float(mart["median_chunk_lifetime"])) > 0.01:
        print("FAIL median")
        return 1
    if not (float(mart["p20_chunk_lifetime"]) <= float(mart["median_chunk_lifetime"])
            <= float(mart["p80_chunk_lifetime"])):
        print("FAIL band ordering")
        return 1
    by_prime: dict = {}
    for r in raw:
        by_prime[r["prime_awardee_uei"]] = by_prime.get(r["prime_awardee_uei"], 0.0) + float(r["subaward_amount"] or 0)
    share = round(100.0 * max(by_prime.values()) / sum(by_prime.values()), 2)
    if abs(share - float(mart["top_buyer_share_lifetime_pct"])) > 0.05:
        print(f"FAIL top-buyer share: raw {share} vs mart {mart['top_buyer_share_lifetime_pct']}")
        return 1
    by_state: dict = {}
    for r in raw:
        st = (r["subaward_primary_place_of_performance_state_code"] or "").strip()
        if st:
            by_state[st] = by_state.get(st, 0.0) + float(r["subaward_amount"] or 0)
    want = [s for s, _ in sorted(by_state.items(), key=lambda kv: -kv[1])]
    if list(mart["pop_states"]) != want:
        print(f"FAIL pop_states order: mart {mart['pop_states']} vs raw {want}")
        return 1
    # null-not-zero: single-active-year subs must carry NULL CAGR
    one_yr = ds.scanner(filter="cagr_n_years = 1", columns=["cagr_5y_pct"]).to_table().to_pylist()
    if any(r["cagr_5y_pct"] is not None for r in one_yr[:1000]):
        print("FAIL: 1-year sub carries non-null CAGR")
        return 1
    print(f"verify OK: {probe} — lifetime $, median, band, top-buyer share, "
          f"state order exact; 1-year CAGR null holds (n={len(one_yr):,})")
    return 0


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
