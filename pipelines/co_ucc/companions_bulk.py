"""Compute worker — Colorado UCC companion datasets (debtors / secured parties / collateral).

Part of the ``co-ucc-companions`` Modal app. Endpoint-less; spawned by the Universal
Dispatcher (core/modal_dispatcher.py), the only proxy-authed endpoint in the fleet.
Clean-room data plane: no Iceberg, no Polaris — DuckDB does 100% of the transform,
Lance is written straight to R2.

WHY THIS WORKER EXISTS
    The CO UCC filing-transaction ledger (pipelines/co_ucc/transactions_bulk.py →
    s3://data-sink/active/co_ucc_transactions/) carries NO party / collateral strings.
    Those companion records live in the legacy Gen-2 lake, the Apache Polaris/Iceberg
    warehouse rooted at s3://dex-raw-landing-zone/ (POLARIS_WAREHOUSE_BASE_LOCATION).
    This worker MIGRATES the three CO companion streams out of that Gen-2 plane and
    materializes them as native Gen-3 Lance datasets in the active data sink — no
    multi-bucket crosswalk view, no catalog round-trip. The Gen-2 bucket and the
    Gen-3 data-sink bucket are two buckets in the SAME R2 account, so one credential
    set reads the source and writes the target.

SOURCE (Gen-2 parquet streams — verified by structural probe, snapshot 2026-05-08):
    s3://dex-raw-landing-zone/ucc/state=CO/stream={debtors,secured_parties,collateral}/
        snapshot=<YYYY-MM-DD>/data.parquet
    Each companion row references its filing by ``fileid`` (numeric-looking VARCHAR,
    e.g. "545806"). Probe result: 100.00% of distinct companion ``fileid`` values
    resolve into the CO filings grain — debtors 1,985,901 rows / 1,576,918 filings;
    secured_parties 2,055,777 / 1,885,973; collateral 1,682,948 / 1,350,593.

TARGET (Gen-3 system of record — native Lance v2.1, full-snapshot overwrite):
    s3://data-sink/active/ucc_co_debtors/
    s3://data-sink/active/ucc_co_secured_parties/
    s3://data-sink/active/ucc_co_collateral/

KEY ALIGNMENT (directive — schema parity with the ledger's primary key):
    The legacy filing identifier ``fileid`` is renamed to ``file_id`` at the transform
    boundary so every companion table shares the ledger's join column verbatim. ``file_id``
    is STRICT VARCHAR (numeric form kept as text — leading-zero / format safety, exactly
    as the ledger keeps it). A BTREE scalar index on ``file_id`` is built and committed on
    all three tables — the directive's hard deliverable — giving sub-second file_id joins
    back to co_ucc_transactions. Load-bearing resolution keys (normalized party name / zip,
    org / last name) additionally get BTREE indices per ARCHITECTURE.md §4, mirroring the
    CA UCC companion tables, so these tables can serve the name/zip resolution joins they
    exist for without an immediate follow-up reindex.

Data plane (one Modal invocation; three independent datasets):
    Gen-2 parquet stream
      → boto3 download → /tmp                                  (Python: I/O only)
      → DuckDB read_parquet → project / rename / cast          (100% in SQL)
      → Arrow table → con.sql(...).to_arrow_table()
      → lance.write_dataset(s3://data-sink/active/ucc_co_<x>/, v2.1, overwrite)
      → BTREE on file_id (+ resolution keys) / BITMAP on categoricals

Control plane (Trigger v4 durable callback): the worker accepts ``trigger_callback_url``
and, on terminal state (success OR failure), (1) writes the run row to
``ops.co_ucc_companions_runs`` via psycopg and (2) POSTs a FLAT JSON body to that url to
wake the suspended Trigger run. No ``{"data": ...}`` envelope.

    modal run    pipelines/co_ucc/companions_bulk.py::init_ops    # create ops table
    modal run    pipelines/co_ucc/companions_bulk.py::run         # execute the migration (manual)
    modal run    pipelines/co_ucc/companions_bulk.py::run --only debtors   # one stream
    modal run    pipelines/co_ucc/companions_bulk.py::reindex_only         # rebuild indexes
    modal deploy pipelines/co_ucc/companions_bulk.py              # make dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

# Gen-2 source lake (Polaris warehouse bucket) and Gen-3 target sink — same R2 account.
# SPENT MIGRATION: the CO UCC companion streams were materialized to Gen-3
# (ucc_co_{debtors,secured_parties,collateral}) and the Gen-2 source bucket
# `dex-raw-landing-zone` was PURGED 2026-06-07. The source no longer exists; this
# default is retained only for historical reference. Re-running `run` requires an
# explicit UCC_CO_SOURCE_BUCKET pointing at a live source.
SOURCE_BUCKET = os.environ.get("UCC_CO_SOURCE_BUCKET", "dex-raw-landing-zone")
TARGET_BUCKET = "data-sink"
SOURCE_STREAM_PREFIX = "ucc/state=CO/stream={stream}/"  # snapshot=<date>/data.parquet under here

SCRATCH_DIR = "/tmp"
FEED = "co_ucc_companions"

# The three companion datasets. Each: source stream name → Gen-3 Lance URI (env-overridable).
# Target names preserve the Gen-2 ``ucc_co`` namespace under the Gen-3 active tier, per directive.
_ACTIVE = "s3://data-sink/active"
DATASETS = ("debtors", "secured_parties", "collateral")
DATASET_URI = {
    "debtors": os.environ.get("UCC_CO_DEBTORS_URI", f"{_ACTIVE}/ucc_co_debtors/"),
    "secured_parties": os.environ.get("UCC_CO_SECURED_PARTIES_URI", f"{_ACTIVE}/ucc_co_secured_parties/"),
    "collateral": os.environ.get("UCC_CO_COLLATERAL_URI", f"{_ACTIVE}/ucc_co_collateral/"),
}

# Lance fragment sizing — fleet defaults (02_lancedb_storage.md §2.3).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3  # 90 GiB — Lance's documented default; never shatters these sets
# Net-new datasets → pin the current Lance default.
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan. file_id BTREE is the directive's hard requirement (primary join key
# back to co_ucc_transactions). The remaining BTREEs are load-bearing resolution keys
# (ARCHITECTURE.md §4); BITMAPs are low-cardinality categoricals filtered frequently.
INDEXES: dict[str, dict[str, list[str]]] = {
    "debtors": {
        "BTREE": ["file_id", "party_name_normalized", "organization_name", "last_name", "party_zip5"],
        "BITMAP": ["action_type", "record_status", "state"],
    },
    "secured_parties": {
        "BTREE": ["file_id", "party_name_normalized", "organization_name", "last_name", "party_zip5"],
        "BITMAP": ["action_type", "record_status", "state"],
    },
    "collateral": {
        "BTREE": ["file_id"],
        "BITMAP": ["action_type", "record_status", "farm_product_flag", "county"],
    },
}

# ── ops.co_ucc_companions_runs DDL. Verbatim mirror of the canonical .sql sibling.
# Applied by the `init_ops` entrypoint and (defensively) before each terminal write. ──
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.co_ucc_companions_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,
    source_bucket  text        NOT NULL,
    snapshot_date  date,
    datasets       jsonb       NOT NULL,
    rows_total     bigint      NOT NULL DEFAULT 0,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS co_ucc_companions_runs_feed_idx        ON ops.co_ucc_companions_runs (feed);
CREATE INDEX IF NOT EXISTS co_ucc_companions_runs_status_idx      ON ops.co_ucc_companions_runs (status);
CREATE INDEX IF NOT EXISTS co_ucc_companions_runs_snapshot_idx    ON ops.co_ucc_companions_runs (snapshot_date);
CREATE INDEX IF NOT EXISTS co_ucc_companions_runs_recorded_at_idx ON ops.co_ucc_companions_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",       # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",           # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",          # R2 read (Gen-2) + write plumbing
    "requests>=2.32",       # Trigger callback
    "psycopg[binary]>=3.2",  # terminal state → ops.co_ucc_companions_runs
).env(
    # BTREE index builds sort the column; Lance's spill-to-disk sorter under-sizes its
    # DataFusion pool and OOMs on high-cardinality string columns (party_name_normalized
    # over ~2M rows) even in a 16 GiB container (lance#2650). Force the in-memory path.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("co-ucc-companions", image=image)


# ── DuckDB projections. read_parquet (lowercase source cols) → defensive nullif/trim.
# Every identifier is STRICT VARCHAR (no numeric cast — leading-zero / format safety).
# fileid → file_id is the only rename that carries join semantics. ─────────────────────
def _lit(s: str) -> str:
    return s.replace("'", "''")


def _prov(source_file: str, snapshot_date: str) -> str:
    """Provenance trailer appended to every projection."""
    return (
        f"    CAST('{_lit(snapshot_date)}' AS DATE) AS snapshot_date,\n"
        f"    '{_lit(source_file)}' AS source_file,\n"
        "    now() AS ingested_at"
    )


def _sql_debtors(path: str, source_file: str, snapshot_date: str) -> str:
    return f"""
SELECT
    nullif(trim("fileid"), '')                  AS file_id,
    nullif(trim("debtorid"), '')                AS debtor_id,
    nullif(trim("actiontypecode"), '')          AS action_type_code,
    nullif(trim("actiontype"), '')              AS action_type,
    nullif(trim("recordstatuscode"), '')        AS record_status_code,
    nullif(trim("recordstatus"), '')            AS record_status,
    nullif(trim("organizationname"), '')        AS organization_name,
    nullif(trim("lastname"), '')                AS last_name,
    nullif(trim("firstname"), '')               AS first_name,
    nullif(trim("middlename"), '')              AS middle_name,
    nullif(trim("organizationjurisdiction"), '') AS organization_jurisdiction,
    nullif(trim("organizationtype"), '')        AS organization_type,
    nullif(trim("organizationid"), '')          AS organization_id,
    nullif(trim("debtordescription"), '')       AS debtor_description,
    nullif(trim("efsuniqueid"), '')             AS efs_unique_id,
    nullif(trim("address1"), '')                AS address1,
    nullif(trim("address2"), '')                AS address2,
    nullif(trim("city"), '')                    AS city,
    nullif(trim("state"), '')                   AS state,
    nullif(trim("zipcode"), '')                 AS zipcode,
    nullif(trim("country"), '')                 AS country,
    nullif(trim("party_name_normalized"), '')   AS party_name_normalized,
    nullif(trim("party_zip5"), '')              AS party_zip5,
    nullif(trim("party_state_normalized"), '')  AS party_state_normalized,
    nullif(trim("party_role"), '')              AS party_role,
{_prov(source_file, snapshot_date)}
FROM read_parquet('{_lit(path)}')
"""


def _sql_secured_parties(path: str, source_file: str, snapshot_date: str) -> str:
    # Note: source uses the abbreviated ``recordstatuscd`` (not ``recordstatuscode``).
    return f"""
SELECT
    nullif(trim("fileid"), '')                  AS file_id,
    nullif(trim("spid"), '')                    AS secured_party_id,
    nullif(trim("actiontypecode"), '')          AS action_type_code,
    nullif(trim("actiontype"), '')              AS action_type,
    nullif(trim("recordstatuscd"), '')          AS record_status_code,
    nullif(trim("recordstatus"), '')            AS record_status,
    nullif(trim("organizationname"), '')        AS organization_name,
    nullif(trim("lastname"), '')                AS last_name,
    nullif(trim("firstname"), '')               AS first_name,
    nullif(trim("middlename"), '')              AS middle_name,
    nullif(trim("assignor"), '')                AS assignor,
    nullif(trim("address1"), '')                AS address1,
    nullif(trim("address2"), '')                AS address2,
    nullif(trim("city"), '')                    AS city,
    nullif(trim("state"), '')                   AS state,
    nullif(trim("zipcode"), '')                 AS zipcode,
    nullif(trim("country"), '')                 AS country,
    nullif(trim("party_name_normalized"), '')   AS party_name_normalized,
    nullif(trim("party_zip5"), '')              AS party_zip5,
    nullif(trim("party_state_normalized"), '')  AS party_state_normalized,
    nullif(trim("party_role"), '')              AS party_role,
{_prov(source_file, snapshot_date)}
FROM read_parquet('{_lit(path)}')
"""


def _sql_collateral(path: str, source_file: str, snapshot_date: str) -> str:
    return f"""
SELECT
    nullif(trim("fileid"), '')                        AS file_id,
    nullif(trim("collateralid"), '')                  AS collateral_id,
    nullif(trim("actiontypecode"), '')                AS action_type_code,
    nullif(trim("actiontype"), '')                    AS action_type,
    nullif(trim("recordstatuscode"), '')              AS record_status_code,
    nullif(trim("recordstatus"), '')                  AS record_status,
    nullif(trim("collateraldescription"), '')         AS collateral_description,
    nullif(trim("additionalcollateraldescription"), '') AS additional_collateral_description,
    nullif(trim("collateral_description_normalized"), '') AS collateral_description_normalized,
    nullif(trim("farmproductflag"), '')               AS farm_product_flag,
    nullif(trim("farmproductallyears"), '')           AS farm_product_all_years,
    nullif(trim("farmproductstartyear"), '')          AS farm_product_start_year,
    nullif(trim("farmproductendyear"), '')            AS farm_product_end_year,
    nullif(trim("actionreferenceid"), '')             AS action_reference_id,
    nullif(trim("countyid"), '')                      AS county_id,
    nullif(trim("county"), '')                        AS county,
{_prov(source_file, snapshot_date)}
FROM read_parquet('{_lit(path)}')
"""


SQL_BUILDER = {
    "debtors": _sql_debtors,
    "secured_parties": _sql_secured_parties,
    "collateral": _sql_collateral,
}


# ── R2 / object-store plumbing (mirrors the ledger worker verbatim) ────────────────────
def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret. One
    credential set reads the Gen-2 bucket and writes the Gen-3 bucket (same account)."""
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _s3_client():
    """boto3 S3 client for R2. Forces checksum behaviour to ``when_required`` (botocore's
    default flexible-checksum validation does not match R2's semantics)."""
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


def _discover_snapshot_key(s3, stream: str, snapshot: str = "") -> tuple[str, str]:
    """Resolve (object_key, snapshot_date) for one stream. With an explicit snapshot
    (YYYY-MM-DD) it targets that partition; otherwise it lists snapshot= partitions and
    picks the most recent. Fails loud if the stream or its data.parquet is absent."""
    base = SOURCE_STREAM_PREFIX.format(stream=stream)
    if snapshot:
        key = f"{base}snapshot={snapshot}/data.parquet"
        s3.head_object(Bucket=SOURCE_BUCKET, Key=key)  # raises if missing
        return key, snapshot
    resp = s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=base, Delimiter="/")
    parts = []
    for p in resp.get("CommonPrefixes", []):
        seg = p["Prefix"].rstrip("/").rsplit("/", 1)[-1]
        if seg.startswith("snapshot="):
            parts.append(seg.split("=", 1)[1])
    if not parts:
        raise RuntimeError(f"No snapshot= partitions under s3://{SOURCE_BUCKET}/{base}")
    latest = max(parts)
    key = f"{base}snapshot={latest}/data.parquet"
    s3.head_object(Bucket=SOURCE_BUCKET, Key=key)
    return key, latest


def _download(s3, key: str, out_path: str) -> int:
    s3.download_file(SOURCE_BUCKET, key, out_path)
    return os.path.getsize(out_path)


def _write_lance(table, uri: str, so: dict) -> None:
    import lance

    lance.write_dataset(
        table,
        uri,
        mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )


def _create_indexes(ds_name: str, so: dict) -> list[dict]:
    """Build BTREE + BITMAP scalar indexes for one dataset across all fragments.
    create_scalar_index defaults to replace=True → idempotent. file_id (the directive's
    mandated index) is first in the BTREE list. Best-effort per index but logged loudly."""
    import lance

    ds = lance.dataset(DATASET_URI[ds_name], storage_options=so)
    out: list[dict] = []
    for itype in ("BTREE", "BITMAP"):
        for col in INDEXES[ds_name].get(itype, []):
            try:
                ds.create_scalar_index(col, index_type=itype)
                print(f"  {itype:6s} ✓ {ds_name}.{col}")
                out.append({"col": col, "type": itype, "ok": True})
            except Exception as exc:  # noqa: BLE001 — an index miss must not fail a good load
                print(f"  {itype:6s} ✗ {ds_name}.{col}: {exc}")
                out.append({"col": col, "type": itype, "ok": False, "error": str(exc)})
    return out


def _committed_index_names(ds_name: str, so: dict) -> list[str]:
    import lance

    ds = lance.dataset(DATASET_URI[ds_name], storage_options=so)
    names = []
    for ix in ds.list_indices():
        names.append(ix.get("name", str(ix)) if isinstance(ix, dict)
                     else getattr(ix, "name", str(ix)))
    return sorted(names)


# ── Terminal state + callback ──────────────────────────────────────────────────────────
def _record_run(source_bucket, snapshot_date, datasets, rows_total,
                status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.co_ucc_companions_runs (psycopg). Best-effort: never let an
    audit-write failure crash an otherwise-good migration."""
    import psycopg
    from psycopg.types.json import Jsonb

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.co_ucc_companions_runs
                    (feed, source_bucket, snapshot_date, datasets, rows_total,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, source_bucket, snapshot_date, Jsonb(datasets), rows_total,
                 status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the migration
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint url. FLAT JSON body — no
    ``{"data": ...}`` envelope. A few retries for delivery reliability."""
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import time

    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}")
                return
            print(f"Callback attempt {i + 1} non-2xx: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def ingest_co_ucc_companions(
    only: str = "",
    snapshot: str = "",
    trigger_callback_url: str | None = None,
) -> dict:
    """Migrate the CO UCC companion streams Gen-2 → Gen-3. For each of debtors /
    secured_parties / collateral: download the Gen-2 parquet → DuckDB project/rename/cast
    → Lance overwrite → BTREE(file_id …)+BITMAP indexes. Then record ops.* state and wake
    the Trigger run. ``only`` restricts to one stream; ``snapshot`` pins a partition.
    Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt
    import gc

    import duckdb
    import lance  # noqa: F401 — imported so a missing dep fails early, not mid-write

    started_at = dt.datetime.now(dt.timezone.utc)
    targets = [only] if only else list(DATASETS)
    for t in targets:
        if t not in DATASET_URI:
            raise ValueError(f"unknown stream {t!r}; expected one of {list(DATASETS)}")

    counts: dict[str, int] = {}
    index_status: dict[str, list] = {}
    snapshot_date: str | None = snapshot or None
    status = "error"
    error: str | None = None

    try:
        so = _r2_storage_options()
        s3 = _s3_client()

        for ds_name in targets:
            print(f"\n=== {ds_name} ===")
            key, snap = _discover_snapshot_key(s3, ds_name, snapshot)
            snapshot_date = snap  # all streams share one snapshot; last wins (identical)
            source_file = key.rsplit("/", 1)[-1]
            local = os.path.join(SCRATCH_DIR, f"co_ucc_{ds_name}.parquet")
            nbytes = _download(s3, key, local)
            print(f"  downloaded s3://{SOURCE_BUCKET}/{key} ({nbytes/(1024**2):.1f} MiB)")

            con = duckdb.connect(":memory:")
            try:
                con.execute("PRAGMA threads=4;")
                sql = SQL_BUILDER[ds_name](local, source_file, snap)
                table = con.sql(sql).to_arrow_table()
            finally:
                con.close()
            counts[ds_name] = table.num_rows
            print(f"  transformed {table.num_rows:,} rows ({table.num_columns} cols) → {DATASET_URI[ds_name]}")

            _write_lance(table, DATASET_URI[ds_name], so)
            print(f"  wrote Lance (overwrite, v{DATA_STORAGE_VERSION})")
            index_status[ds_name] = _create_indexes(ds_name, so)

            try:
                os.remove(local)
            except OSError:
                pass
            del table
            gc.collect()

        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        rows_total = int(sum(counts.values()))
        _record_run(SOURCE_BUCKET, snapshot_date, counts, rows_total,
                    status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "feed": FEED, "source_bucket": SOURCE_BUCKET,
             "snapshot_date": snapshot_date, "rows_total": rows_total, "datasets": counts},
        )

    if status != "success":
        raise RuntimeError(f"co_ucc companions migration failed: {error}")
    return {"feed": FEED, "source_bucket": SOURCE_BUCKET, "snapshot_date": snapshot_date,
            "rows_total": int(sum(counts.values())), "datasets": counts,
            "indexes": index_status, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 45,
    memory=16384,
    cpu=4.0,
)
def reindex(only: str = "") -> dict:
    """(Re)build scalar indexes on already-written companion datasets without
    re-migrating. Idempotent (create_scalar_index defaults to replace=True)."""
    so = _r2_storage_options()
    targets = [only] if only else list(DATASETS)
    out = {}
    for ds_name in targets:
        if ds_name not in INDEXES:
            continue
        print(f"=== reindex {ds_name} ===")
        _create_indexes(ds_name, so)
        out[ds_name] = _committed_index_names(ds_name, so)
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.co_ucc_companions_runs (idempotent). Mirrors the canonical .sql."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name='co_ucc_companions_runs'
            ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    print(f"ops.co_ucc_companions_runs ready — columns: {cols}")
    return {"table": "ops.co_ucc_companions_runs", "columns": cols}


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.co_ucc_companions_runs DDL."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def run(only: str = "", snapshot: str = "") -> None:
    """Manual migration (no Trigger callback). ops.* write still fires."""
    import json

    print(json.dumps(
        ingest_co_ucc_companions.remote(only=only, snapshot=snapshot, trigger_callback_url=None),
        indent=2, default=str,
    ))


@app.local_entrypoint()
def reindex_only(only: str = "") -> None:
    """Rebuild indexes on the existing datasets (no re-migration)."""
    import json

    print(json.dumps(reindex.remote(only=only), indent=2, default=str))
