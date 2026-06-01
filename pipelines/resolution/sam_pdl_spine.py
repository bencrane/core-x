"""Compute worker — SAM.gov <-> PDL identity-resolution spine (domain bridge).

Part of the ``resolution-spines`` Modal app. Endpoint-less; spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local
entrypoints. Builds a system-of-record BRIDGE that links federal SAM.gov entity
registrations to PDL company records on the normalized web domain — the vector
the reconnaissance pass validated (~60% of SAM's distinct domains intersect PDL).

Data plane (clean-room — DuckDB does 100% of the transform):
    R2 Lance: active/entity_registrations/  (SAM)  ──┐
    R2 Lance: active/pdl_companies/         (PDL)  ──┤  DuckDB norm_host/is_domain
      → project ids + normalized domain (100% in SQL) │  + is_platform stoplist
      → inner join on normalized_domain               │  + PDL fan-out cap
      → Arrow table                                   ┘
      → lance.write_dataset(LOCAL /tmp, v2.1, overwrite) + BTREE indexes
      → boto3 publish (wipe + upload) → s3://data-sink/active/bridge_sam_pdl/

Transform logic is the single source of truth in ``spine_sql.py`` (shared with
the local runner so the two can never drift). Design decisions encoded there,
each surfaced for the review gate:
  * Carries BOTH ``uei`` (v2 layout) and ``duns`` (legacy/DUNS-era layout). Only
    ~12% of SAM rows have a ``uei``; keying on it alone would forfeit the entire
    pre-UEI corpus (~5.1M domain-matched legacy rows). A row enters the bridge
    with ``uei OR duns``.
  * ALL SAM fields (uei, duns, name, status, entity_url) are derived WIDTH-AWARE
    from ``pipe_fields`` off ``field_count`` (120 vs 142). The flat uei / name
    columns and ``format_family`` are NOT trusted: ``_classify()``-by-filename
    mislabels some 142/v2-layout files as legacy_v1, misprojecting those flat
    columns. Width is the reliable layout key. (Details in spine_sql.)
  * PDL fan-out cap (default 25) discards aggregator / link-in-bio / franchise
    domains PDL maps to thousands of companies (pure cross-product noise). PDL
    averages 1.01 companies/domain, so the cap is near-lossless on real signal.
  * Grain = DISTINCT on the projected columns: every distinct entity<->company
    linkage is preserved (no rollup of distinct UEIs/DUNS), byte-identical
    monthly-snapshot duplicates are dropped (clean Metabase lookup).

Why LOCAL stage + boto3 publish (NOT a direct Lance write to R2): mirrors
pipelines/pdl_companies/free_company_dataset.py — Lance's direct R2 write trips
R2's uniform-part multipart rule once a BTREE page_data.lance file is large
enough; staging locally (no multipart) and publishing via boto3/s3transfer
(uniform parts) is R2-compliant.

    modal deploy pipelines/resolution/sam_pdl_spine.py
    modal run    pipelines/resolution/sam_pdl_spine.py::initdb      # create ops.* table
    modal run    pipelines/resolution/sam_pdl_spine.py::run         # build + publish bridge
    modal run    pipelines/resolution/sam_pdl_spine.py::run --fanout-cap 25
    modal run    pipelines/resolution/sam_pdl_spine.py::show_ledger
"""

from __future__ import annotations

import os

import modal

from spine_sql import (
    BRIDGE_COLUMNS, DEFAULT_FANOUT_CAP, INDEX_COLUMNS, MACROS,
    bridge_select_sql, pdl_normalize_sql, sam_normalize_sql,
)

BUCKET = "data-sink"
SAM_PREFIX = "active/entity_registrations/"
PDL_PREFIX = "active/pdl_companies/"
DATASET_PREFIX = "active/bridge_sam_pdl/"          # system-of-record bridge tier
DATASET_URI = os.environ.get("BRIDGE_SAM_PDL_LANCE_URI", f"s3://{BUCKET}/{DATASET_PREFIX}")
SAM_URI = f"s3://{BUCKET}/{SAM_PREFIX}"
PDL_URI = f"s3://{BUCKET}/{PDL_PREFIX}"
SCRATCH_DIR = "/tmp/spine"
LOCAL_DATASET = os.path.join(SCRATCH_DIR, "bridge_lance")
FEED = "sam_pdl_spine"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576

# Width-driven derivation reads only the raw pipe array + its length. The flat
# uei / legal_business_name columns are NOT trusted: the SAM worker's
# _classify()-by-filename mislabels some 142/v2-layout files as legacy_v1, which
# misprojects those flat columns (UEI -> duns, a date -> name). spine_sql derives
# every SAM field width-aware off field_count, which is the reliable layout key.
SAM_COLS = ["pipe_fields", "field_count"]
PDL_COLS = ["pdl_company_id", "company_name", "domain"]

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ops.sam_pdl_spine_runs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            TEXT        NOT NULL,
    dataset_uri     TEXT        NOT NULL,
    sam_uri         TEXT        NOT NULL,
    pdl_uri         TEXT        NOT NULL,
    fanout_cap      INTEGER,
    entity_url_flat BOOLEAN,
    rows_processed  BIGINT,
    distinct_uei    BIGINT,
    distinct_duns   BIGINT,
    distinct_pdl    BIGINT,
    status          TEXT        NOT NULL,
    error           TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ NOT NULL
)
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "boto3>=1.35",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env(
    # BTREE training sorts the indexed column; bypass Lance's bounded spill-to-disk
    # ExternalSorter (mirrors pipelines/pdl_companies). The bridge is far smaller
    # than PDL, but uei / pdl_company_id / duns are still multi-hundred-k sorts.
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("spine_sql")


app = modal.App("resolution-spines", image=image)


def _r2_storage_options() -> dict[str, str]:
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


def _create_indexes(local_path: str) -> list[str]:
    """BTREE scalar indexes on the bridge resolution keys (local — no R2
    multipart). Per-column try/except: an index miss must not fail the build."""
    import lance

    ds = lance.dataset(local_path)
    built: list[str] = []
    for col in INDEX_COLUMNS:
        try:
            ds.create_scalar_index(col, "BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001 — an index miss must not fail the load
            print(f"  WARN: BTREE index on {col} failed: {exc}")
    return built


def _replace_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Idempotent publish: wipe the R2 prefix, then upload the local Lance
    dataset via boto3 (uniform-part multipart → R2-compliant)."""
    to_del = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            to_del.append({"Key": o["Key"]})
            if len(to_del) == 1000:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})
                to_del = []
    if to_del:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})

    uploaded = 0
    for root, _, files in os.walk(local_dir):
        for fn in files:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            s3.upload_file(lp, BUCKET, prefix + rel)
            uploaded += 1
    return uploaded


def _detect_entity_url(sam_ds) -> bool:
    return "entity_url" in sam_ds.schema.names


def _record_run(dataset_uri, fanout_cap, entity_url_flat, rows, distinct_uei,
                distinct_duns, distinct_pdl, status, error, started_at,
                completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.sam_pdl_spine_runs
                    (feed, dataset_uri, sam_uri, pdl_uri, fanout_cap, entity_url_flat,
                     rows_processed, distinct_uei, distinct_duns, distinct_pdl,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, dataset_uri, SAM_URI, PDL_URI, fanout_cap, entity_url_flat,
                 rows, distinct_uei, distinct_duns, distinct_pdl,
                 status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
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


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def init_schema() -> dict:
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
        cur.execute(_CREATE_TABLE_SQL)
        conn.commit()
    print("ops.sam_pdl_spine_runs ready")
    return {"status": "ok", "table": "ops.sam_pdl_spine_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=524288,  # Modal's explicit floor (512 GiB); staging is small
)
def build_spine(trigger_callback_url: str | None = None,
                fanout_cap: int = DEFAULT_FANOUT_CAP) -> dict:
    """Read SAM + PDL Lance from R2 → DuckDB normalize/join → Lance overwrite on
    LOCAL disk → BTREE index → boto3 publish to R2. Records ops.* + wakes Trigger.
    Re-raises on failure so the Modal call is marked failed."""
    import datetime as dt
    import shutil

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    rows = 0
    distinct_uei = distinct_duns = distinct_pdl = None
    entity_url_flat = None
    status = "error"
    error: str | None = None
    built: list[str] = []

    try:
        so = _r2_storage_options()
        os.makedirs(SCRATCH_DIR, exist_ok=True)
        shutil.rmtree(LOCAL_DATASET, ignore_errors=True)

        sam_ds = lance.dataset(SAM_URI, storage_options=so)
        pdl_ds = lance.dataset(PDL_URI, storage_options=so)
        entity_url_flat = _detect_entity_url(sam_ds)
        print(f"SAM flat entity_url column present: {entity_url_flat}; deriving "
              f"ALL SAM fields width-aware from pipe_fields (format_family/flat "
              f"uei+name are NOT trusted — see spine_sql).")

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            con.execute("SET memory_limit='24GB';")
            con.execute(MACROS)

            con.register("sam_src", sam_ds.scanner(columns=SAM_COLS, batch_size=65536).to_reader())
            con.execute(f"CREATE TABLE sam_v AS {sam_normalize_sql('sam_src')}")
            con.register("pdl_src", pdl_ds.scanner(columns=PDL_COLS, batch_size=131072).to_reader())
            con.execute(f"CREATE TABLE pdl_v AS {pdl_normalize_sql('pdl_src')}")
            con.execute("CREATE TABLE pdl_fan AS SELECT nd, count(*) AS pdl_n FROM pdl_v GROUP BY nd")

            table = con.execute(
                bridge_select_sql("sam_v", "pdl_v", "pdl_fan", fanout_cap, distinct=True)
            ).to_arrow_table()
            rows = table.num_rows
            con.register("bridge", table)
            distinct_uei = con.execute("SELECT count(DISTINCT uei) FROM bridge WHERE uei IS NOT NULL").fetchone()[0]
            distinct_duns = con.execute("SELECT count(DISTINCT duns) FROM bridge WHERE duns IS NOT NULL").fetchone()[0]
            distinct_pdl = con.execute("SELECT count(DISTINCT pdl_company_id) FROM bridge").fetchone()[0]
            print(f"  bridge rows={rows:,} distinct_uei={distinct_uei:,} "
                  f"distinct_duns={distinct_duns:,} distinct_pdl={distinct_pdl:,}")
        finally:
            con.close()

        lance.write_dataset(
            table, LOCAL_DATASET,
            mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
        )
        built = _create_indexes(LOCAL_DATASET)
        uploaded = _replace_r2_prefix(s3=_s3_client(), prefix=DATASET_PREFIX, local_dir=LOCAL_DATASET)
        print(f"Published {uploaded} files → {DATASET_URI}")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(DATASET_URI, fanout_cap, entity_url_flat, int(rows),
                    distinct_uei, distinct_duns, distinct_pdl, status, error,
                    started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": FEED,
             "dataset_uri": DATASET_URI, "distinct_uei": distinct_uei,
             "distinct_pdl": distinct_pdl, "fanout_cap": fanout_cap},
        )

    if status != "success":
        raise RuntimeError(f"sam_pdl_spine build failed: {error}")
    return {"feed": FEED, "rows_processed": int(rows), "distinct_uei": distinct_uei,
            "distinct_duns": distinct_duns, "distinct_pdl": distinct_pdl,
            "dataset_uri": DATASET_URI, "columns": BRIDGE_COLUMNS,
            "indices": built, "fanout_cap": fanout_cap, "status": status}


@app.local_entrypoint()
def initdb() -> None:
    import json

    print(json.dumps(init_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def run(fanout_cap: int = DEFAULT_FANOUT_CAP) -> None:
    import json

    print(json.dumps(build_spine.remote(trigger_callback_url=None, fanout_cap=fanout_cap),
                     indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 5) -> None:
    import json

    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, feed, rows_processed, distinct_uei, distinct_duns, distinct_pdl, "
            "fanout_cap, entity_url_flat, status, started_at, completed_at "
            "FROM ops.sam_pdl_spine_runs ORDER BY id DESC LIMIT %s", (limit,))
        cols = [d[0] for d in cur.description]
        print(json.dumps([dict(zip(cols, r)) for r in cur.fetchall()], indent=2, default=str))
