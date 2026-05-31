"""Compute worker — Colorado SoS bulk business-entity registry ingest.

Part of the ``co-sos-entities`` Modal app. One endpoint-less function, spawned by
the Universal Dispatcher (core/modal_dispatcher.py) or driven by the local
``backfill`` entrypoint. This worker has no web endpoint. Clean-room data plane:
no Iceberg, no Polaris — DuckDB does 100% of the transform, Lance is written
straight to R2.

Snapshot model (approved): the date-stamped CSV is a COMPLETE point-in-time
snapshot of the entire CO registry (every entity at its current status), not a
delta. ``mode="overwrite"`` is canonical — each refresh rebuilds the active table
wholesale, and Lance's immutable-manifest MVCC retains prior versions for free
snapshot time-travel (cleanup_old_versions bounds R2 growth). No partitioned
time-series tier.

Data plane (one Modal invocation per snapshot):
    R2 landing CSV (already dropped manually — no Socrata fetch phase yet)
      → boto3 download to /tmp                          (Python: I/O only, no parse)
      → DuckDB read_csv(all_varchar) → 35-col project / cast   (100% in SQL)
      → Arrow table  → con.sql(...).to_arrow_table()
      → lance.write_dataset(s3://data-sink/active/co_sos/, v2.1, overwrite)
      → BTREE + BITMAP scalar indexes on the load-bearing keys

Encoding: probed clean ASCII/UTF-8 (zero 0x80-0xFF bytes, no BOM, no NUL, no
embedded newlines, RFC-4180 doubled-quote). UNLIKE the SAM/PPP feeds this needs
NO cp1252 transcode — Python streams raw bytes, DuckDB reads utf-8 directly.

Identifier handling: ``entityid`` is 100% unique, all-numeric, YYYY-prefixed
(e.g. 20251665680), 10-11 digits, zero leading zeros TODAY. DuckDB auto-sniff
classifies it BIGINT — that is the trap. It is forced to VARCHAR (plain
nullif/trim, never TRY_CAST) to preserve the year-prefix semantics, stay
join-stable across datasets, and survive any future leading-zero / format drift.

Control plane (Trigger v4 durable callback): the worker accepts
``trigger_callback_url`` and, on terminal state (success OR failure), (1) writes
the run row to ``ops.co_sos_entity_runs`` via psycopg and (2) POSTs a FLAT JSON
body ``{status, rows, feed, snapshot_date, dataset_uri}`` to that url to wake the
suspended Trigger run. No ``{"data": ...}`` envelope.

    modal deploy pipelines/co_sos/entities_bulk.py
    modal run    pipelines/co_sos/entities_bulk.py::backfill            # default landing key
    modal run    pipelines/co_sos/entities_bulk.py::backfill --key landing/co_sos/<file>.csv
    modal run    pipelines/co_sos/entities_bulk.py::reindex             # rebuild indexes only
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
LANDING_PREFIX = "landing/co_sos/"
# Lance system-of-record tier (NOT the landing/raw zone). Exact path per directive.
DATASET_URI = os.environ.get("CO_SOS_LANCE_URI", "s3://data-sink/active/co_sos/")
SCRATCH_DIR = "/tmp"
FEED = "co_sos_entities"

# The snapshot the operator landed manually. snapshot_date is decoded from the
# `20260531` stamp in the filename (overridable via the snapshot_date kwarg).
DEFAULT_KEY = "landing/co_sos/Business_Entities_in_Colorado_20260531.csv"

# Lance fragment sizing (directive constraints).
#   max_rows_per_file = 1048576 — exact. 3.06M rows → 3 fragments.
#   max_bytes_per_file: the directive wrote `90 * 10243`, annotated "(90 GiB)".
#   The only reading that equals 90 GiB is `90 * 1024**3` (= 96,636,764,160),
#   Lance's documented default. Confirmed correct by the operator. A literal
#   `90 * 10243` (~900 KB) would shatter the dataset into ~900 fragments.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new dataset → pin the current Lance default (per 02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan (approved). BTREE for high-cardinality load-bearing resolution
# keys (equality + range); BITMAP for low-cardinality categoricals filtered often.
#   jurisdiction_of_formation: 1,372 distinct + dirty (mixes 'DE' and 'Delaware')
#     → BTREE for the medium cardinality; raw strings indexed as-is (no normalize).
#   agent_organization_name: approved scope expansion for registered-agent cluster
#     analysis (691k non-null, high cardinality) → BTREE.
CO_SOS_BTREE_INDEXES = [
    "entity_id",                  # unique YYYY-prefixed PK
    "entity_name",                # ~3.06M-distinct resolution / lookup key
    "jurisdiction_of_formation",  # 1,372 distinct, dirty, frequent domestic/foreign filter
    "agent_organization_name",    # registered-agent cluster analysis
]
CO_SOS_BITMAP_INDEXES = [
    "entity_status",   # 15 categoricals
    "entity_type",     # 33 categoricals
    "principal_state",  # 74 categoricals; primary geo filter
]

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",       # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",           # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "boto3>=1.35",          # R2 landing read
    "requests>=2.32",       # Trigger callback
    "psycopg[binary]>=3.2",  # ops.* terminal state
).env(
    # BTREE scalar-index builds sort the column. Lance's spill-to-disk sorter
    # uses a small bounded DataFusion memory pool that OOMs on high-cardinality
    # string columns (here entity_name ~3.06M rows + agent_organization_name).
    # Force the in-memory sort path (uses the container's 16 GiB) so every index
    # builds. See lance-format/lance#2650.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("co-sos-entities", image=image)

# 34 source columns → snake_case Lance schema, in CSV header order. Every value is
# VARCHAR via nullif(trim()) — the source carries no numeric columns, and entity_id
# is held VARCHAR DELIBERATELY (never TRY_CAST). zipcodes / codes keep any leading
# zeros. The source header misspells `jurisdictonofformation`; the projection
# corrects it to `jurisdiction_of_formation`. No VARIANT anywhere.
_STR_COLS = [
    ("entityid", "entity_id"),
    ("entityname", "entity_name"),
    ("principaladdress1", "principal_address_1"),
    ("principaladdress2", "principal_address_2"),
    ("principalcity", "principal_city"),
    ("principalstate", "principal_state"),
    ("principalzipcode", "principal_zip"),
    ("principalcountry", "principal_country"),
    ("mailingaddress1", "mailing_address_1"),
    ("mailingaddress2", "mailing_address_2"),
    ("mailingcity", "mailing_city"),
    ("mailingstate", "mailing_state"),
    ("mailingzipcode", "mailing_zip"),
    ("mailingcountry", "mailing_country"),
    ("entitystatus", "entity_status"),
    ("jurisdictonofformation", "jurisdiction_of_formation"),  # source misspelling fixed
    ("entitytype", "entity_type"),
    ("agentfirstname", "agent_first_name"),
    ("agentmiddlename", "agent_middle_name"),
    ("agentlastname", "agent_last_name"),
    ("agentsuffix", "agent_suffix"),
    ("agentorganizationname", "agent_organization_name"),
    ("agentprincipaladdress1", "agent_principal_address_1"),
    ("agentprincipaladdress2", "agent_principal_address_2"),
    ("agentprincipalcity", "agent_principal_city"),
    ("agentprincipalstate", "agent_principal_state"),
    ("agentprincipalzipcode", "agent_principal_zip"),
    ("agentprincipalcountry", "agent_principal_country"),
    ("agentmailingaddress1", "agent_mailing_address_1"),
    ("agentmailingaddress2", "agent_mailing_address_2"),
    ("agentmailingcity", "agent_mailing_city"),
    ("agentmailingstate", "agent_mailing_state"),
    ("agentmailingzipcode", "agent_mailing_zip"),
    ("agentmailingcountry", "agent_mailing_country"),
]
# entityformdate: MM/DD/YYYY, zero-padded (probed min_len == max_len == 10),
# range 1869-2026, 100% populated. TRY_STRPTIME → DATE; blank/garbage → NULL.
_DATE_COLS = [
    ("entityformdate", "entity_form_date"),
]


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the Modal secret.
    AWS-style creds + explicit endpoint + region 'auto'. Endpoint supplied
    directly (R2_ENDPOINT) or derived from R2_ACCOUNT_ID."""
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
    """boto3 S3 client for R2. Forces checksum behaviour to ``when_required``:
    botocore's default flexible-checksum validation does not match R2's
    semantics and otherwise raises on get/put."""
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


def _snapshot_from_key(key: str) -> str | None:
    """Decode the YYYYMMDD stamp from the filename, e.g.
    Business_Entities_in_Colorado_20260531.csv → 2026-05-31."""
    import re

    m = re.search(r"(\d{4})(\d{2})(\d{2})", key.rsplit("/", 1)[-1])
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None


def _build_sql(csv_path: str, source_file: str, snapshot_date: str) -> str:
    """100% of the transform. read_csv(all_varchar=true) → defensive projection →
    snake_case. The file is clean UTF-8 so no encoding clause is needed. All
    interpolated values are repo-controlled (our /tmp path, registry filename,
    fixed literals) — no user input — and single-quote-escaped regardless."""

    def lit(s: str) -> str:
        return s.replace("'", "''")

    def strcol(src: str, dst: str) -> str:
        return f'nullif(trim("{src}"), \'\') AS {dst}'

    def datecol(src: str, dst: str) -> str:
        # CO form dates are zero-padded MM/DD/YYYY; TRY_STRPTIME→DATE, blank → NULL.
        return (
            f"TRY_CAST(TRY_STRPTIME(nullif(trim(\"{src}\"), ''), '%m/%d/%Y') AS DATE) AS {dst}"
        )

    projections = (
        [strcol(s, d) for s, d in _STR_COLS]
        + [datecol(s, d) for s, d in _DATE_COLS]
    )
    projection_sql = ",\n    ".join(projections)
    return f"""
WITH raw AS (
    SELECT *
    FROM read_csv(
        '{lit(csv_path)}',
        all_varchar = true,
        header = true,
        sample_size = -1,
        ignore_errors = false
    )
)
SELECT
    {projection_sql},
    '{lit(source_file)}' AS source_file,
    CAST('{lit(snapshot_date)}' AS DATE) AS snapshot_date,
    now() AS ingested_at
FROM raw
"""


def _create_indexes(so: dict) -> None:
    """Build the BTREE + BITMAP scalar indexes on the active dataset.
    create_scalar_index defaults to replace=True, so re-running rebuilds cleanly.
    An index miss is logged but never fails the load — the data write is the
    critical artifact and indexes are idempotently rebuildable via `reindex`."""
    import lance

    ds = lance.dataset(DATASET_URI, storage_options=so)
    for col in CO_SOS_BTREE_INDEXES:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"WARN: BTREE index on {col} failed: {exc}")
    for col in CO_SOS_BITMAP_INDEXES:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"WARN: BITMAP index on {col} failed: {exc}")


def _list_committed_indices(ds) -> list:
    """Best-effort read of committed scalar indices (name/type/fields). Tolerant
    of pylance return-shape drift (dict vs object, list_indices vs list_indexes)."""
    for attr in ("list_indices", "list_indexes"):
        fn = getattr(ds, attr, None)
        if fn is None:
            continue
        try:
            out = []
            for ix in fn():
                if isinstance(ix, dict):
                    out.append({k: ix.get(k) for k in ("name", "type", "fields")})
                else:
                    out.append({
                        "name": getattr(ix, "name", None),
                        "type": str(getattr(ix, "type", None)),
                        "fields": getattr(ix, "fields", None),
                    })
            return out
        except Exception as exc:  # noqa: BLE001
            return [{"error": f"{attr}: {exc}"}]
    return [{"error": "no list_indices/list_indexes method on dataset"}]


def _record_run(source_file, snapshot_date, dataset_uri, rows, status, error,
                started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.co_sos_entity_runs
                    (source_file, snapshot_date, dataset_uri, rows_processed,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (source_file, snapshot_date, dataset_uri, rows, status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url. The whole body
    becomes result.output — NO API key, NO {"data": ...} envelope."""
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
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def ingest_co_sos(
    key: str = DEFAULT_KEY,
    snapshot_date: str | None = None,
    trigger_callback_url: str | None = None,
) -> dict:
    """Download the landed CSV → DuckDB project/cast → Arrow → Lance overwrite →
    BTREE/BITMAP index, then record ops.* state and wake Trigger. Re-raises on
    failure so the Modal call is marked failed."""
    import datetime as dt
    import os.path

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    filename = key.rsplit("/", 1)[-1]
    snap = snapshot_date or _snapshot_from_key(key) or started_at.date().isoformat()
    rows = 0
    status = "error"
    error: str | None = None

    try:
        so = _r2_storage_options()
        s3 = _s3_client()

        scratch = os.path.join(SCRATCH_DIR, filename)
        s3.download_file(BUCKET, key, scratch)  # I/O only — clean UTF-8, no transcode

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            table = con.sql(_build_sql(scratch, filename, snap)).to_arrow_table()
        finally:
            con.close()
        rows = table.num_rows

        lance.write_dataset(
            table,
            DATASET_URI,
            mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        print(f"Wrote {rows:,} rows → {DATASET_URI} (snapshot {snap})")
        _create_indexes(so)
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(filename, snap, DATASET_URI, int(rows), status, error,
                    started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": FEED,
             "snapshot_date": snap, "dataset_uri": DATASET_URI},
        )

    if status != "success":
        raise RuntimeError(f"CO SoS ingest failed for {key}: {error}")
    return {"feed": FEED, "source_file": filename, "rows_processed": int(rows),
            "snapshot_date": snap, "dataset_uri": DATASET_URI, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 45,
    memory=16384,
    cpu=4.0,
)
def reindex_co_sos() -> dict:
    """(Re)build the BTREE + BITMAP scalar indexes on the already-written dataset
    without re-ingesting. Idempotent (replace=True). LANCE_BYPASS_SPILLING is
    baked into the image so the high-cardinality BTREE builds never OOM."""
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    rows = ds.count_rows()
    print(f"Reindexing {DATASET_URI} — {rows:,} rows")
    _create_indexes(so)
    ds = lance.dataset(DATASET_URI, storage_options=so)  # reopen → read committed set
    committed = _list_committed_indices(ds)
    print(f"Committed indices: {committed}")
    return {
        "dataset": DATASET_URI, "rows": rows,
        "btree": CO_SOS_BTREE_INDEXES, "bitmap": CO_SOS_BITMAP_INDEXES,
        "committed_indices": committed,
    }


@app.local_entrypoint()
def backfill(key: str = DEFAULT_KEY, snapshot_date: str = "") -> None:
    """Manual ingest of one landed snapshot (no callback). ops.* write still fires
    if the hqx-postgres secret is attached."""
    import json

    print(f"Ingesting s3://{BUCKET}/{key} → {DATASET_URI}")
    result = ingest_co_sos.remote(
        key=key, snapshot_date=(snapshot_date or None), trigger_callback_url=None
    )
    print(json.dumps(result, indent=2, default=str))


@app.local_entrypoint()
def reindex() -> None:
    """Rebuild the scalar indexes on the existing dataset (no re-ingest)."""
    import json

    print(json.dumps(reindex_co_sos.remote(), indent=2, default=str))
