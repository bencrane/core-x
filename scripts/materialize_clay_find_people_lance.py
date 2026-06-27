#!/usr/bin/env python3
"""Rehydrate the active/clay_find_people/ Lance SoR from HQX gtm.clay_find_people.

WHY THIS EXISTS
    gtm.clay_find_people (HQX Postgres) is the Clay/DEX people LANDING table; the Gen-3
    system of record is its Lance mirror at s3://data-sink/active/clay_find_people/.
    Every Clay re-pull appends net-new rows to the landing table (deduped Postgres-side on
    the record_id PK), leaving the Lance stale. This worker re-projects the FULL current
    landing table 1:1 into Lance so the SoR reflects every landed person. The original
    in-session materializer was never committed; this is its durable, schema-pinned form.

SEMANTICS — FULL OVERWRITE (idempotent, non-destructive)
    The landing table is the deduped union (record_id is its PK), so Postgres ⊇ Lance and a
    straight projection is exactly correct — no partial-append edge cases. lance.write_dataset
    mode="overwrite" is MVCC: it commits a NEW dataset version and RETAINS prior versions
    (rollback-able), so this is not an in-place rewrite. Re-running is a safe no-op modulo a
    fresh materialized_at stamp.

TRANSFORM IS 100% DUCKDB
    HQX Postgres is ATTACHed READ_ONLY via the DuckDB postgres scanner; the projection is
    streamed (fetch_record_batch) so the ~450 MB raw_payload column never fully lands in RAM,
    each batch is cast to the EXACT target Arrow schema (jsonb raw_payload -> string,
    large_string -> string, timestamps -> us/UTC), NOT-NULL columns are guarded loudly, and
    Lance is written straight to R2. The 4 BTREE + 3 Bitmap scalar indices are rebuilt
    (overwrite drops them) so pushdown filtering survives the refresh.

SCHEMA (28 fields — reproduced verbatim from the live dataset, NOT inferred)
    27 source columns of gtm.clay_find_people + materialized_at (one uniform now() stamp).

RUN
    doppler run -p core-x -c prd -- uv run --no-project \
        --with boto3 --with pylance --with duckdb --with 'psycopg[binary]' \
        python3 scripts/materialize_clay_find_people_lance.py

    # read-only preflight (counts + schema parity, no write):
    ... python3 scripts/materialize_clay_find_people_lance.py --dry-run
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

# Pin the fleet-standard in-memory sort path for index builds. Lance's external
# (spilling) sorter runs in a tiny reservation pool and trips "Resources exhausted"
# on string-column BTREE builds even at this row count; the in-memory path handles
# 818k rows trivially. Must be set before lance touches the sorter.
os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

# ── Coordinates ──────────────────────────────────────────────────────────────
TARGET_URI = os.environ.get("CLAY_FIND_PEOPLE_LANCE_URI",
                            "s3://data-sink/active/clay_find_people/")
SOURCE_TABLE = "hqx.gtm.clay_find_people"          # DuckDB-qualified (attached db `hqx`)
DATA_STORAGE_VERSION = "2.1"                        # fleet default (02_lancedb_storage.md §2.3)
MAX_ROWS_PER_FILE = 1_048_576                       # Lance default — 818k rows -> 1 fragment
STREAM_ROWS_PER_BATCH = 100_000                     # out-of-core: ~one batch resident at a time

# Scalar index plan — verbatim from the live dataset's committed indices.
BTREE_COLS = ["record_id", "person_id", "linkedin_url_norm", "domain_norm"]
BITMAP_COLS = ["loc_country_iso", "loc_state", "loc_region"]

# NOT-NULL contract (guarded per batch). materialized_at is generated non-null below.
NOT_NULL = ["record_id", "person_id", "linkedin_url_raw", "linkedin_url_norm",
            "source", "raw_payload", "landed_at"]


# ── Exact target Arrow schema (field order + types + nullability) ────────────
def _target_schema():
    import pyarrow as pa
    ts = pa.timestamp("us", tz="UTC")
    f = pa.field
    return pa.schema([
        f("record_id", pa.string(), nullable=False),
        f("person_id", pa.string(), nullable=False),
        f("linkedin_url_raw", pa.string(), nullable=False),
        f("linkedin_url_norm", pa.string(), nullable=False),
        f("domain_norm", pa.string(), nullable=True),
        f("source", pa.string(), nullable=False),
        f("raw_payload", pa.string(), nullable=False),
        f("landed_at", ts, nullable=False),
        f("domain_raw", pa.string(), nullable=True),
        f("full_name", pa.string(), nullable=True),
        f("first_name", pa.string(), nullable=True),
        f("last_name", pa.string(), nullable=True),
        f("location_name", pa.string(), nullable=True),
        f("follower_count", pa.int64(), nullable=True),
        f("company_table_id", pa.string(), nullable=True),
        f("company_record_id", pa.string(), nullable=True),
        f("matched_job_title", pa.string(), nullable=True),
        f("matched_company_name", pa.string(), nullable=True),
        f("matched_start_date", pa.string(), nullable=True),
        f("loc_city", pa.string(), nullable=True),
        f("loc_state", pa.string(), nullable=True),
        f("loc_region", pa.string(), nullable=True),
        f("loc_country", pa.string(), nullable=True),
        f("loc_country_iso", pa.string(), nullable=True),
        f("latest_experience_title", pa.string(), nullable=True),
        f("latest_experience_company", pa.string(), nullable=True),
        f("latest_experience_start_date", pa.string(), nullable=True),
        f("materialized_at", ts, nullable=False),
    ])


# Projection: every source column in target order; jsonb -> VARCHAR; uniform run stamp.
# now() is evaluated once per statement, so materialized_at is constant across all batches.
def _projection_sql() -> str:
    return f"""
SELECT
    record_id, person_id, linkedin_url_raw, linkedin_url_norm, domain_norm, source,
    CAST(raw_payload AS VARCHAR)              AS raw_payload,
    landed_at,
    domain_raw, full_name, first_name, last_name, location_name,
    follower_count,
    company_table_id, company_record_id, matched_job_title, matched_company_name,
    matched_start_date, loc_city, loc_state, loc_region, loc_country, loc_country_iso,
    latest_experience_title, latest_experience_company, latest_experience_start_date,
    CAST(now() AS TIMESTAMP WITH TIME ZONE)  AS materialized_at
FROM {SOURCE_TABLE}
"""


# ── R2 / Postgres plumbing ───────────────────────────────────────────────────
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID (r2-credentials / core-x prd).")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _source_dsn() -> str:
    """HQX pooled (Supavisor session-mode :5432) DSN with TLS enforced. Session mode is
    required for the long single-cursor scan; transaction mode (:6543) breaks it."""
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set (core-x prd).")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _attach_source(con) -> None:
    con.execute("INSTALL postgres; LOAD postgres;")
    # HARD-CAP postgres connections to ONE. Supavisor session mode caps at pool_size=15
    # shared across every client; the scanner otherwise opens one libpq connection per
    # thread and trips EMAXCONNSESSION. A single serial connection is plenty — the read is
    # network-bound, not CPU-bound — and is the lightest possible footprint on the pool.
    con.execute("SET pg_connection_limit=1;")
    con.execute("SET threads TO 1;")
    # Spill config — out-of-core safety for the projection/scan (the mandate, not a hope).
    con.execute("SET memory_limit='6GB';")
    os.makedirs("/tmp/duckdb_clay_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duckdb_clay_spill';")
    dsn = _source_dsn().replace("'", "''")  # escape for the SQL string literal (never logged)
    con.execute(f"ATTACH '{dsn}' AS hqx (TYPE postgres, READ_ONLY);")


def _source_count(con) -> int:
    return con.sql(f"SELECT count(*) FROM {SOURCE_TABLE}").fetchone()[0]


# ── Streaming write ──────────────────────────────────────────────────────────
def _cast_batches(con, target):
    """Yield record batches cast to the exact target schema, guarding NOT-NULL columns.
    Streamed from DuckDB so peak memory is ~one batch, never the whole 818k-row table."""
    import pyarrow as pa

    names = [f.name for f in target]
    reader = con.sql(_projection_sql()).fetch_record_batch(STREAM_ROWS_PER_BATCH)
    for rb in reader:
        t = pa.Table.from_batches([rb]).select(names).cast(target)
        for c in NOT_NULL:
            nulls = t.column(c).null_count
            if nulls:
                raise ValueError(
                    f"schema contract violation: {c} declared NOT NULL but {nulls} null(s)")
        for b in t.to_batches():
            yield b


def _write(con, so: dict) -> None:
    import lance
    import pyarrow as pa

    target = _target_schema()
    reader = pa.RecordBatchReader.from_batches(target, _cast_batches(con, target))
    lance.write_dataset(
        reader,
        TARGET_URI,
        mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        storage_options=so,
    )


def _build_indexes(so: dict) -> list[str]:
    import lance
    ds = lance.dataset(TARGET_URI, storage_options=so)
    built: list[str] = []
    for col in BTREE_COLS:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        built.append(f"{col}:BTREE")
        print(f"  ✓ BTREE  {col}")
    for col in BITMAP_COLS:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        built.append(f"{col}:BITMAP")
        print(f"  ✓ BITMAP {col}")
    return built


def _verify(so: dict, expected_rows: int) -> None:
    import lance
    ds = lance.dataset(TARGET_URI, storage_options=so)
    n = ds.count_rows()
    idx_names = {ix.get("name") if isinstance(ix, dict) else getattr(ix, "name", str(ix))
                 for ix in ds.list_indices()}
    want_idx = {f"{c}_idx" for c in (BTREE_COLS + BITMAP_COLS)}
    missing = sorted(want_idx - idx_names)
    print(f"\n=== verify ===\n  rows        : {n:,} (expected {expected_rows:,})")
    print(f"  version     : {ds.version}")
    print(f"  indices     : {sorted(idx_names)}")
    if n != expected_rows:
        raise RuntimeError(f"row-count mismatch: Lance {n:,} != Postgres {expected_rows:,}")
    if missing:
        raise RuntimeError(f"missing indices after rebuild: {missing}")
    # Pushdown probe — the record_id BTREE must answer a point lookup.
    probe = ds.scanner(columns=["record_id"],
                       filter="record_id IS NOT NULL", limit=1).to_table().num_rows
    print(f"  probe       : record_id lookup -> {probe} row")
    print("  OK — Lance ≡ Postgres, all indices present.")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="Counts + schema parity only; write nothing.")
    ap.add_argument("--reindex-only", action="store_true",
                    help="Skip the overwrite; (re)build the scalar indices on the current "
                         "Lance version and verify. Use to finish a run whose data write "
                         "committed but whose index build failed.")
    args = ap.parse_args(argv)

    import duckdb

    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    con = duckdb.connect(":memory:")
    try:
        _attach_source(con)
        pg_n = _source_count(con)
        print(f"source {SOURCE_TABLE}: {pg_n:,} rows")

        if args.dry_run:
            import lance
            try:
                live = lance.dataset(TARGET_URI, storage_options=so)
                print(f"live Lance: {live.count_rows():,} rows · v{live.version} "
                      f"(stale by {pg_n - live.count_rows():,})")
                live_fields = [(f.name, str(f.type), f.nullable) for f in live.schema]
                want_fields = [(f.name, str(f.type), f.nullable) for f in _target_schema()]
                print("schema parity (target == live):",
                      "MATCH" if live_fields == want_fields else "DRIFT")
                if live_fields != want_fields:
                    for w, l in zip(want_fields, live_fields):
                        if w != l:
                            print(f"   target={w}  live={l}")
            except Exception as exc:  # noqa: BLE001
                print(f"live Lance not readable: {exc}")
            print("dry-run: no write.")
            return 0

        if args.reindex_only:
            print("reindex-only: skipping write; (re)building indices on current version…")
        else:
            print(f"writing overwrite → {TARGET_URI} (streamed, {STREAM_ROWS_PER_BATCH:,}/batch)")
            _write(con, so)
            print("rebuilding scalar indices…")
        built = _build_indexes(so)
        _verify(so, pg_n)
        elapsed = (dt.datetime.now(dt.timezone.utc) - started).total_seconds()
        verb = "reindexed" if args.reindex_only else "rehydrated"
        print(f"\n{verb} {pg_n:,} rows · indices={len(built)} · {elapsed:.0f}s")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
