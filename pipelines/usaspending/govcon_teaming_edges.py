"""GOVCON TEAMING EDGES (Phase 0, plan item 5) — 5-year prime↔sub teaming graph.

Build plan: docs/plans/GOVCON_SCOPE_PROCESSING_AND_GTM_QUERY_BUILD_PLAN.md (Phase-0 teaming-edges
table is the schema authority; the frozen pyarrow schema lives in
pipelines/sam_gov/govcon_gtm_schemas.py and is asserted before every write).

Grain: (prime_uei, sub_uei) — one edge per prime/sub pair, OVERWRITE rebuild per window.
Substrate: the 5-year ``usaspending/subaward_search`` corpus restricted to the
``subawardee_work_profile`` UEI universe, UNIONED with the 90-day API-fresh subaward feed (the
bulk mirror LAGS the fresh feed — the just-won subawards that defined the target set would
otherwise be missing), deduped to true subaward grain (sub_uei, award_key, subaward_number) —
the plan-§1 "130,011 distinct subawards" basis. The dedup count is measured and logged every
build; rebuild this dataset immediately after any ``subawardee_work_profile`` rebuild.

Aggregates per edge (frozen schema, exact): latest-reported prime/sub names, edge_dollars_5y
(sum), edge_count_5y (count), distinct_awards_5y, first/last action dates, top_naics (mode over
edge subawards). BTREE on BOTH UEI columns after the build.

Scan results are window-cached to parquet in SCRATCH (same resume pattern as
subawardee_work_profile.py) so a crashed build never re-scans subaward_search.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      python3 pipelines/usaspending/govcon_teaming_edges.py <build|verify> [years]
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from pipelines.sam_gov.govcon_gtm_schemas import (  # noqa: E402
    TEAMING_EDGES_URI,
    assert_schema,
    teaming_edges_schema,
)

# ─────────────────────────── constants ───────────────────────────

FEED = "govcon_teaming_edges"

PROFILE_URI = os.environ.get(
    "SUBAWARDEE_WORK_PROFILE_URI",
    "s3://data-sink/active/subawardee_work_profile",
).rstrip("/") + "/"

SUB_FRESH_URI = os.environ.get(
    "USASPENDING_API_SUBAWARD_FRESH_URI",
    "s3://data-sink/active/usaspending_api_fresh/contract_subaward",
).rstrip("/") + "/"

# SUBAWARD role now reads the reconciled contract-subaward canonical (BULK∪FRESH, one row per
# (prime_award_unique_key, subaward_number)) instead of hand-rolling the subaward_search∪fresh union.
# Contract-only by construction; subaward_amount sentinels ($39T mis-key) already nulled on-spine.
SUBAWARD_CANONICAL_URI = "s3://data-sink/active/usaspending_subaward_canonical/"

DEFAULT_YEARS = 5
BATCH = 4000  # UEIs per index-pushdown scan
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 250_000
SCRATCH = os.environ.get("GTE_SCRATCH", "/tmp/govcon_teaming_edges")
INDEX_COLS = ["prime_uei", "sub_uei"]


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so():
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
        if os.environ.get("R2_ACCOUNT_ID") else None)
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _inlist(vals):
    return ",".join("'" + v.replace("'", "''") + "'" for v in vals)


def _batched(seq, n):
    return [seq[i:i + n] for i in range(0, len(seq), n)]


# ─────────────────────────── build ───────────────────────────


def build(years=DEFAULT_YEARS):
    import duckdb
    import lance
    import pyarrow as pa

    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    os.makedirs(SCRATCH, exist_ok=True)
    today = dt.datetime.now(dt.timezone.utc).date()
    cutoff = (today - dt.timedelta(days=365 * years)).isoformat()
    run_id = f"teaming_edges_{started.strftime('%Y%m%dT%H%M%SZ')}"

    schema = teaming_edges_schema()
    # Schema-drift gate BEFORE any work: the frozen schema is the law.
    assert_schema(TEAMING_EDGES_URI, schema, so)

    con = duckdb.connect(":memory:")
    try:
        log(f"build: {years}y window -> {cutoff}  run_id={run_id}")

        # ── 0. entity universe = subawardee_work_profile UEIs (the profile cohort) ──
        profile_ds = lance.dataset(PROFILE_URI, storage_options=so)
        con.register("prof_u_src", profile_ds.scanner(columns=["subawardee_uei"]).to_table())
        con.execute("""CREATE TABLE prof_u AS
          SELECT DISTINCT trim(subawardee_uei) AS uei FROM prof_u_src
          WHERE nullif(trim(subawardee_uei), '') IS NOT NULL;""")
        con.unregister("prof_u_src")
        n_prof = con.execute("SELECT count(*) FROM prof_u").fetchone()[0]
        log(f"  profile universe: {n_prof} subawardee UEIs")

        # ── 1. the reconciled contract-subaward canonical (already BULK∪FRESH-deduped to one row per
        # (prime_award_unique_key, subaward_number)), restricted to the profile cohort. Replaces the
        # hand-rolled subaward_search scan + parquet cache + per-uei index-pushdown batching with a
        # single fast scan; the fresh top-up (§2) closes the canonical's rebuild-cadence gap. ──
        canon = lance.dataset(SUBAWARD_CANONICAL_URI, storage_options=so)
        con.register("cn", canon.scanner(columns=[
            "subawardee_uei", "subawardee_name", "prime_awardee_uei", "prime_awardee_name",
            "prime_award_unique_key", "subaward_number", "subaward_amount",
            "subaward_action_date", "prime_award_naics_code"],
            filter=f"subaward_action_date >= DATE '{cutoff}'").to_reader())
        con.execute("""CREATE TABLE sb_search AS
          SELECT trim(subawardee_uei)                     AS sub_uei,
                 nullif(trim(subawardee_name), '')         AS sub_name,
                 nullif(trim(prime_awardee_uei), '')       AS prime_uei,
                 nullif(trim(prime_awardee_name), '')      AS prime_name,
                 trim(prime_award_unique_key)              AS award_key,
                 nullif(trim(subaward_number), '')         AS subnum,
                 subaward_amount                           AS amt,
                 subaward_action_date                      AS adt,
                 nullif(trim(prime_award_naics_code), '')  AS naics
          FROM cn
          WHERE trim(subawardee_uei) IN (SELECT uei FROM prof_u);""")
        con.unregister("cn")

        # ── 2. 90-day API-fresh feed (tops up the canonical past its rebuild cadence) ──
        fresh_ds = lance.dataset(SUB_FRESH_URI, storage_options=so)
        con.register("rf", fresh_ds.scanner(columns=[
            "subawardee_uei", "subawardee_name", "prime_awardee_uei", "prime_awardee_name",
            "prime_award_unique_key", "subaward_number", "subaward_amount",
            "subaward_action_date", "prime_award_naics_code"]).to_reader())
        con.execute(f"""CREATE TABLE fresh AS
          SELECT trim(subawardee_uei)                          AS sub_uei,
                 nullif(trim(subawardee_name), '')             AS sub_name,
                 nullif(trim(prime_awardee_uei), '')           AS prime_uei,
                 nullif(trim(prime_awardee_name), '')          AS prime_name,
                 trim(prime_award_unique_key)                  AS award_key,
                 nullif(trim(subaward_number), '')             AS subnum,
                 TRY_CAST(subaward_amount AS DOUBLE)           AS amt,
                 TRY_CAST(subaward_action_date AS DATE)        AS adt,
                 nullif(trim(prime_award_naics_code), '')      AS naics
          FROM rf
          WHERE nullif(trim(subawardee_uei), '') IS NOT NULL
            AND TRY_CAST(subaward_action_date AS DATE) >= DATE '{cutoff}'""")
        con.unregister("rf")

        # ── 3. union + dedup to true subaward grain — the plan-§1 distinct-subawards basis ──
        # Canonical rows (pr=0) outrank fresh re-pulls; within a source, latest revision wins.
        con.execute("""CREATE TABLE sb AS
          WITH u AS (
            SELECT *, 0 AS pr FROM sb_search
            UNION ALL
            SELECT *, 1 AS pr FROM fresh),
          r AS (SELECT *, row_number() OVER (
                  PARTITION BY sub_uei, award_key, subnum
                  ORDER BY pr ASC, adt DESC NULLS LAST) rn
                FROM u)
          SELECT sub_uei, sub_name, prime_uei, prime_name, award_key, subnum, amt, adt, naics
          FROM r WHERE rn = 1""")
        distinct_subawards = con.execute("SELECT count(*) FROM sb").fetchone()[0]
        log(f"  distinct subawards (deduped basis): {distinct_subawards} "
            f"(plan §1 cites ~130,011)")

        # ── 4. aggregate to (prime_uei, sub_uei) edges ──
        # top_naics = mode over edge subawards (DuckDB mode() ignores NULLs; deterministic
        # tie-break by frequency then lexicographic via the explicit roll-up).
        con.execute("""CREATE TABLE edges AS
          WITH e AS (
            SELECT * FROM sb WHERE prime_uei IS NOT NULL AND sub_uei IS NOT NULL),
          naics_roll AS (
            SELECT prime_uei, sub_uei, naics, count(*) c
            FROM e WHERE naics IS NOT NULL GROUP BY 1, 2, 3),
          naics_top AS (
            SELECT prime_uei, sub_uei, naics AS top_naics,
                   row_number() OVER (PARTITION BY prime_uei, sub_uei
                                      ORDER BY c DESC, naics ASC) rn
            FROM naics_roll)
          SELECT
            e.prime_uei,
            e.sub_uei,
            arg_max(e.prime_name, e.adt)          AS prime_name,
            arg_max(e.sub_name, e.adt)            AS sub_name,
            sum(e.amt)                            AS edge_dollars_5y,
            count(*)                              AS edge_count_5y,
            count(DISTINCT e.award_key)           AS distinct_awards_5y,
            min(e.adt)                            AS first_action_date,
            max(e.adt)                            AS last_action_date,
            any_value(nt.top_naics)               AS top_naics
          FROM e
          LEFT JOIN naics_top nt
            ON nt.prime_uei = e.prime_uei AND nt.sub_uei = e.sub_uei AND nt.rn = 1
          GROUP BY e.prime_uei, e.sub_uei""")

        n_edges, n_primes, n_subs = con.execute(
            "SELECT count(*), count(DISTINCT prime_uei), count(DISTINCT sub_uei) FROM edges"
        ).fetchone()
        grain_dupes = con.execute(
            "SELECT count(*) - count(DISTINCT (prime_uei, sub_uei)) FROM edges").fetchone()[0]
        if grain_dupes:
            raise RuntimeError(f"edge grain violation: {grain_dupes} duplicate (prime_uei, sub_uei) rows")
        if n_edges == 0:
            raise RuntimeError("0 edges assembled — refusing to overwrite")
        log(f"  edges: {n_edges} distinct (prime_uei, sub_uei); "
            f"{n_primes} primes, {n_subs} subs; grain assert PASS")

        # ── 5. cast to the frozen schema EXACTLY, assert, overwrite, index ──
        raw = con.execute(f"""SELECT
            prime_uei, sub_uei, prime_name, sub_name,
            CAST(edge_dollars_5y AS DOUBLE)        AS edge_dollars_5y,
            CAST(edge_count_5y AS INTEGER)         AS edge_count_5y,
            CAST(distinct_awards_5y AS INTEGER)    AS distinct_awards_5y,
            first_action_date, last_action_date, top_naics,
            '{run_id}'                             AS run_id,
            TIMESTAMPTZ '{started.isoformat()}'    AS built_at
          FROM edges""").arrow()
        if isinstance(raw, pa.RecordBatchReader):
            raw = raw.read_all()
        out = raw.cast(schema)
        if not out.schema.equals(schema, check_metadata=False):
            raise RuntimeError("assembled table schema != frozen teaming_edges_schema")

        assert_schema(TEAMING_EDGES_URI, schema, so)  # re-assert immediately before write
        lance.write_dataset(out, TEAMING_EDGES_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        ds = lance.dataset(TEAMING_EDGES_URI, storage_options=so)
        for c in INDEX_COLS:
            ds.create_scalar_index(c, index_type="BTREE")
            log(f"  BTREE {c}")
        assert_schema(TEAMING_EDGES_URI, schema, so)

        log(json.dumps({
            "status": "built", "rows": n_edges, "distinct_prime_uei": n_primes,
            "distinct_sub_uei": n_subs, "distinct_subawards_basis": distinct_subawards,
            "run_id": run_id, "uri": TEAMING_EDGES_URI}))
        log(f"DONE edges: {n_edges} rows -> {TEAMING_EDGES_URI}")
    finally:
        con.close()


# ─────────────────────────── verify ───────────────────────────


def verify():
    import lance

    so = _r2_so()
    schema = teaming_edges_schema()
    assert_schema(TEAMING_EDGES_URI, schema, so)
    ds = lance.dataset(TEAMING_EDGES_URI, storage_options=so)
    tbl = ds.scanner(columns=["prime_uei", "sub_uei"]).to_table()
    rows = tbl.num_rows
    primes = len(set(tbl.column("prime_uei").to_pylist()))
    subs = len(set(tbl.column("sub_uei").to_pylist()))
    pairs = len({(p, s) for p, s in zip(tbl.column("prime_uei").to_pylist(),
                                        tbl.column("sub_uei").to_pylist())})
    try:
        idx = [getattr(i, "name", i.get("name") if isinstance(i, dict) else str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    print(json.dumps({
        "uri": TEAMING_EDGES_URI, "rows": rows, "distinct_prime_uei": primes,
        "distinct_sub_uei": subs, "grain_unique": pairs == rows,
        "schema": "PASS", "indices": idx}, indent=2))
    if pairs != rows:
        sys.exit(1)


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    a2 = int(sys.argv[2]) if len(sys.argv) > 2 else None
    if cmd == "build":
        build(years=a2 or DEFAULT_YEARS)
    elif cmd == "verify":
        verify()
    else:
        print(f"unknown command: {cmd} (build|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
