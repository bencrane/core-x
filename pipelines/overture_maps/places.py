"""Compute worker — Overture Maps Foundation "Places" spatial bulk ingest.

Part of the ``overture-maps-pipelines`` Modal app. Endpoint-less; spawned by the
Universal Dispatcher (core/modal_dispatcher.py) or driven by the local
entrypoints. A full point-in-time snapshot of the US subset of Overture Places.

Source (public, anonymous — NO credentials):
    s3://overturemaps-us-west-2/release/<YYYY-MM-DD.N>/theme=places/type=place/*.parquet
    GeoParquet; ``geometry`` is WKB (BLOB) per the GeoParquet spec. The worker
    resolves the LATEST release automatically (anonymous ListObjects), so the
    monthly Trigger cron always lands the current snapshot; a ``release`` kwarg
    overrides for pinned backfills.

Data plane (clean-room — DuckDB does 100% of the transform):
    public S3 GeoParquet
      → DuckDB httpfs anonymous read_parquet (range-request partial reads)     (read)
      → spatial ST_X/ST_Y(ST_GeomFromWKB(geometry)) flatten + addresses[1] unpack
        + WHERE addresses[1].country='US'  (100% in SQL)                       (transform)
      → Arrow (to_arrow_table; reader fallback at scale)
      → lance.write_dataset(LOCAL /tmp, v2.1, overwrite) + BTREE/BITMAP indexes (persist)
      → boto3 publish (wipe + upload) → s3://data-sink/active/overture_places/  (publish)

CRITICAL — geometry is flattened, never persisted as WKB. ST_X/ST_Y operate on a
GEOMETRY; Overture's ``geometry`` column reads back as WKB BLOB, so it is decoded
once via ST_GeomFromWKB in the ``geo`` CTE and only the longitude/latitude floats
reach Arrow/Lance. The raw WKB blob is dropped at the transform boundary.

Why LOCAL stage + boto3 publish (NOT a direct Lance write to R2): Lance's direct
write to R2 trips R2's multipart rule — "all non-trailing parts must have the same
length" (400 InvalidPart) — once a near-unique scalar-index page_data.lance file
is large enough to force object_store's adaptive multipart to ESCALATE part size
mid-upload (AWS S3 tolerates unequal non-trailing parts; R2 does not). At the
US-Places row count the BTREE on the unique ``id`` string + near-unique
longitude/latitude doubles cross that threshold. So the whole dataset (data +
indices) is built on local disk (no multipart) and published once via boto3
(s3transfer = uniform parts → R2-compliant). Mirrors
pipelines/pdl_companies/free_company_dataset.py, which solved the identical
constraint at 35.4M rows.

Index plan (operator-authorized 2026-06-01):
    BTREE  : id, longitude, latitude   (high-cardinality resolution / range keys)
    BITMAP : region                    (≤~60 US ISO-3166-2 codes — low cardinality)

    modal deploy pipelines/overture_maps/places.py
    modal run    pipelines/overture_maps/places.py::initdb       # create ops.* table
    modal run    pipelines/overture_maps/places.py::run          # ingest latest release → Lance
    modal run    pipelines/overture_maps/places.py::run --release 2026-05-21.0   # pin a release
    modal run    pipelines/overture_maps/places.py::reindex      # rebuild scalar indexes only
    modal run    pipelines/overture_maps/places.py::show_ledger  # print ops ledger
"""

from __future__ import annotations

import os

import modal

# ── System-of-record (R2) ──────────────────────────────────────────────────
BUCKET = "data-sink"
DATASET_PREFIX = "active/overture_places/"
DATASET_URI = os.environ.get("OVERTURE_PLACES_LANCE_URI", f"s3://{BUCKET}/{DATASET_PREFIX}")
SCRATCH_DIR = "/tmp/overture"
LOCAL_DATASET = os.path.join(SCRATCH_DIR, "places_lance")  # local Lance staging (pre-publish)
FEED = "overture_places"

# ── Upstream (public AWS S3 — anonymous) ───────────────────────────────────
OVERTURE_BUCKET = "overturemaps-us-west-2"
OVERTURE_REGION = "us-west-2"
OVERTURE_PLACES_GLOB = "s3://overturemaps-us-west-2/release/{rel}/theme=places/type=place/*.parquet"

# ── Lance fragment sizing (exact; mirrors pdl_companies / the Lance defaults) ─
#   max_rows_per_file = 1048576 — the Lance default.
#   max_bytes_per_file = 90 GiB — 90 * 1024**3 (NOT 90*10243); the literal
#   90 GiB ceiling, so the snapshot is not shattered into micro-fragments.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
# Net-new dataset → pin the current Lance default (per 02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"
# Streaming fallback batch (pre-authorized): one Arrow RecordBatch per 1,048,576
# rows if the full materialization hits a catchable allocation error.
STREAM_BATCH_ROWS = 1048576

# ── Scalar index plan (operator-authorized; §8-B of the approved plan) ──────
# BTREE for high-cardinality load-bearing resolution / range keys; BITMAP for
# the low-cardinality categorical. The geometry is flattened to lon/lat floats,
# so the per-axis BTREEs serve bounding-box range predicates without a WKB column.
OVERTURE_BTREE_INDEXES = [
    "id",         # GERS id — unique resolution key
    "longitude",  # flattened ST_X — bbox range predicates
    "latitude",   # flattened ST_Y — bbox range predicates
]
OVERTURE_BITMAP_INDEXES = [
    "region",     # US ISO-3166-2 subdivision (~57 distinct) — low cardinality
]

# Terminal-state ledger. CREATE SCHEMA + CREATE TABLE run as SEPARATE statements
# (psycopg sends one command per execute()). Mirrors the ops.* contract
# (ARCHITECTURE.md §5); adds distinct_ids + published_{files,bytes} + release_tag
# + write_path as integrity / storage-audit signals.
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ops.overture_places_runs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            TEXT        NOT NULL,
    dataset_uri     TEXT        NOT NULL,
    release_tag     TEXT,
    snapshot_date   DATE,
    rows_processed  BIGINT,
    distinct_ids    BIGINT,
    published_files BIGINT,
    published_bytes BIGINT,
    write_path      TEXT,
    status          TEXT        NOT NULL,
    error           TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ NOT NULL
)
"""

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
        "lancedb>=0.15",
        "pylance>=7",            # provides `import lance`; lancedb does not re-export it
        "pyarrow>=17",
        "boto3>=1.35",           # anonymous Overture list + R2 publish (uniform-part multipart)
        "requests>=2.32",        # Trigger waitpoint callback
        "psycopg[binary]>=3.2",  # ops.* terminal state
    )
    # Bake the DuckDB extensions into the image so the worker only LOADs at
    # runtime (no per-run download, no runtime-install flakiness). The build
    # duckdb wheel == the runtime wheel, so the version-specific extension
    # binaries match.
    .run_commands(
        "python -c \"import duckdb; duckdb.connect().execute('INSTALL httpfs; INSTALL spatial;')\""
    )
    .env(
        # BTREE training sorts the column; bypass Lance's bounded spill-to-disk
        # ExternalSorter, whose memory accounting under-sizes the pool and
        # exhausts on multi-million-row columns. With 32 GiB the unique ``id``
        # string + lon/lat doubles sort fully in-memory. See lance-format/lance#2650.
        {"LANCE_BYPASS_SPILLING": "true"}
    )
)

app = modal.App("overture-maps-pipelines", image=image)


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the r2-credentials
    Modal secret. AWS-style creds + explicit endpoint + region 'auto'. Used by the
    boto3 R2 client; the Lance write/read is LOCAL (no storage_options)."""
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
    """boto3 S3 client for R2 (signed). Forces checksum behaviour to
    ``when_required``: botocore's default flexible-checksum validation does not
    match R2's semantics and otherwise raises on get/put."""
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


def _anon_s3_client():
    """Anonymous (UNSIGNED) boto3 S3 client for the PUBLIC Overture bucket on AWS.
    Used only to enumerate release prefixes (I/O = listing). The Parquet read
    itself is DuckDB httpfs."""
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    return boto3.client("s3", region_name=OVERTURE_REGION,
                        config=Config(signature_version=UNSIGNED))


def _resolve_latest_release(explicit: str | None) -> str:
    """Resolve the Overture release tag. An explicit ``release`` kwarg wins;
    otherwise enumerate ``release/`` prefixes on the public bucket (one cheap
    delimited ListObjects) and pick the lexicographic max — ``YYYY-MM-DD.N`` sorts
    chronologically. This is the monthly-refresh auto-pickup."""
    if explicit:
        return explicit.strip().strip("/")
    s3 = _anon_s3_client()
    releases: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=OVERTURE_BUCKET, Prefix="release/", Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            tag = cp["Prefix"].split("/")[1] if "/" in cp["Prefix"] else ""
            if tag:
                releases.append(tag)
    if not releases:
        raise RuntimeError("No Overture releases found under s3://overturemaps-us-west-2/release/")
    return max(releases)


def _detect_geometry_decode(con, read_glob: str) -> str:
    """Return the SQL expression yielding a GEOMETRY from the source ``geometry``
    column. Overture stores it as WKB BLOB (GeoParquet) → ST_GeomFromWKB decode.
    If the installed spatial build already auto-types it as GEOMETRY, use it
    directly. Metadata-only probe: DESCRIBE on LIMIT 0 reads the Parquet footer,
    not rows — no data materialization."""
    desc = con.execute(
        "DESCRIBE SELECT geometry FROM read_parquet(?) LIMIT 0", [read_glob]
    ).fetchall()
    col_type = (desc[0][1] if desc else "BLOB").upper()
    return "geometry" if "GEOMETRY" in col_type else "ST_GeomFromWKB(geometry)"


def _build_sql(geom_expr: str) -> str:
    """100% of the transform. Anonymous read_parquet over the resolved release →
    decode geometry ONCE (geo CTE) + US filter pushed to the scan → flatten
    ST_X/ST_Y to lon/lat floats + unpack addresses[1]/names/categories →
    snake_case projection. The WKB ``geometry`` is NEVER projected past the geo
    CTE — only the float coordinates land. ``geom_expr`` is repo-controlled
    (probe output, not user input); the read path and dates are bound positionally."""
    return f"""
WITH raw AS (
    SELECT * FROM read_parquet(?)
),
geo AS (
    SELECT
        id,
        {geom_expr} AS geom,        -- decode WKB→GEOMETRY once; never persisted
        addresses,
        names,
        categories,
        confidence
    FROM raw
    WHERE addresses[1].country = 'US'   -- ISO 3166-1 alpha-2; predicate pushed to scan
)
SELECT
    nullif(trim(id), '')                     AS id,
    ST_X(geom)                               AS longitude,   -- flattened float
    ST_Y(geom)                               AS latitude,    -- flattened float
    nullif(trim(addresses[1].region), '')    AS region,       -- US ISO-3166-2 subdivision
    nullif(trim(addresses[1].country), '')   AS country,      -- constant 'US' (provenance)
    nullif(trim(names.primary), '')          AS name,         -- entity-resolution key
    nullif(trim(categories.primary), '')     AS category,     -- POI category slug
    TRY_CAST(confidence AS DOUBLE)           AS confidence,    -- Overture 0..1 quality score
    CAST(? AS DATE)                          AS snapshot_date,
    CAST(? AS VARCHAR)                       AS release_tag,
    now()                                    AS ingested_at
FROM geo
"""


def _create_indexes(local_path: str) -> list[str]:
    """Build BTREE + BITMAP scalar indexes on the LOCAL dataset (no storage_options
    — local writes avoid R2's multipart part-size rule). replace=True → idempotent.
    Per-column try/except: an index miss must not fail an otherwise-good load."""
    import lance

    ds = lance.dataset(local_path)
    built: list[str] = []
    for col in OVERTURE_BTREE_INDEXES:
        try:
            ds.create_scalar_index(col, "BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001 — an index miss must not fail the load
            print(f"  WARN: BTREE index on {col} failed: {exc}")
    for col in OVERTURE_BITMAP_INDEXES:
        try:
            ds.create_scalar_index(col, "BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN: BITMAP index on {col} failed: {exc}")
    return built


def _list_committed_indices(ds) -> list:
    """Best-effort read of committed scalar indices. Tolerant of pylance
    return-shape drift (dict vs object, list_indices vs list_indexes)."""
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


def _replace_r2_prefix(s3, prefix: str, local_dir: str) -> tuple[int, int]:
    """Idempotent publish: wipe the R2 prefix, then upload the local Lance dataset
    (boto3/s3transfer = uniform-part multipart, R2-compliant). Returns
    (files_uploaded, bytes_uploaded). Mirrors pdl_companies."""
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
    uploaded_bytes = 0
    for root, _, files in os.walk(local_dir):
        for fn in files:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            s3.upload_file(lp, BUCKET, prefix + rel)
            uploaded += 1
            uploaded_bytes += os.path.getsize(lp)
    return uploaded, uploaded_bytes


def _download_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Stage the committed R2 dataset back to local disk (for an in-place reindex
    without re-ingesting). Returns files downloaded."""
    import shutil

    shutil.rmtree(local_dir, ignore_errors=True)
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            rel = o["Key"][len(prefix):]
            if not rel:  # prefix placeholder
                continue
            lp = os.path.join(local_dir, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(lp), exist_ok=True)
            s3.download_file(BUCKET, o["Key"], lp)
            n += 1
    return n


def _record_run(dataset_uri, release_tag, snapshot_date, rows, distinct_ids,
                published_files, published_bytes, write_path, status, error,
                started_at, completed_at) -> None:
    """Terminal run row → ops.overture_places_runs (psycopg). Best-effort: never
    let an audit-write failure crash an otherwise-good ingest."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.overture_places_runs
                    (feed, dataset_uri, release_tag, snapshot_date, rows_processed,
                     distinct_ids, published_files, published_bytes, write_path,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, dataset_uri, release_tag, snapshot_date, rows, distinct_ids,
                 published_files, published_bytes, write_path, status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST the RAW terminal payload to the Trigger waitpoint url. The whole body
    becomes result.output — NO API key, NO {"data": ...} envelope (flat)."""
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
    """Create the ops schema + overture_places_runs ledger (idempotent)."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS ops;")
        cur.execute(_CREATE_TABLE_SQL)
        conn.commit()
    print("ops.overture_places_runs ready")
    return {"status": "ok", "table": "ops.overture_places_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90,
    memory=32768,        # 32 GiB — in-memory BTREE sort of unique id + lon/lat (LANCE_BYPASS_SPILLING)
    cpu=8.0,
    ephemeral_disk=524288,  # Modal's explicit floor (512 GiB); local Lance staging needs the disk
)
def ingest_overture_places(release: str | None = None,
                           trigger_callback_url: str | None = None) -> dict:
    """Resolve latest release → DuckDB anonymous read + spatial transform → Arrow →
    Lance overwrite on LOCAL disk → BTREE/BITMAP index locally → boto3 publish to R2
    (uniform-part multipart). Full to_arrow_table() materialization in the 32 GiB
    container; pre-authorized streaming to_arrow_reader fallback on a catchable
    allocation error. Records ops.* state, wakes the Trigger run. Re-raises on
    failure so the Modal call is marked failed."""
    import datetime as dt
    import shutil

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    snapshot_date = started_at.date().isoformat()
    rows = 0
    distinct_ids: int | None = None
    published_files = 0
    published_bytes = 0
    write_path = "materialize"
    status = "error"
    error: str | None = None
    release_tag: str | None = None
    built: list[str] = []

    try:
        release_tag = _resolve_latest_release(release)
        read_glob = OVERTURE_PLACES_GLOB.format(rel=release_tag)
        print(f"Overture release: {release_tag}")
        print(f"Reading: {read_glob}")

        os.makedirs(SCRATCH_DIR, exist_ok=True)
        shutil.rmtree(LOCAL_DATASET, ignore_errors=True)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute("SET enable_progress_bar=false;")
            con.execute("LOAD httpfs;")
            con.execute("LOAD spatial;")
            con.execute(f"SET s3_region='{OVERTURE_REGION}';")  # anonymous public read

            geom_expr = _detect_geometry_decode(con, read_glob)
            print(f"  geometry decode: ST_X/ST_Y({geom_expr})")
            sql = _build_sql(geom_expr)
            params = [read_glob, snapshot_date, release_tag]

            try:
                # PRIMARY — full materialization in the 32 GiB container.
                table = con.execute(sql, params).to_arrow_table()
                rows = table.num_rows
                # Build-time integrity check: exact DISTINCT on the GERS id
                # (zero-copy replacement scan over the Arrow table).
                con.register("proj", table)
                distinct_ids = con.execute("SELECT count(DISTINCT id) FROM proj").fetchone()[0]
                con.unregister("proj")
                print(f"  materialized {rows:,} US rows; distinct id = {distinct_ids:,}")
                # LOCAL write (no storage_options) — avoids R2 multipart rule.
                lance.write_dataset(
                    table,
                    LOCAL_DATASET,
                    mode="overwrite",
                    data_storage_version=DATA_STORAGE_VERSION,
                    max_rows_per_file=MAX_ROWS_PER_FILE,
                    max_bytes_per_file=MAX_BYTES_PER_FILE,
                )
            except (MemoryError, duckdb.OutOfMemoryException) as mem_exc:
                # FALLBACK (pre-authorized) — stream RecordBatches; flat RAM.
                # distinct-id is skipped here to preserve bounded memory.
                write_path = "stream"
                print(f"  materialization hit {type(mem_exc).__name__}; "
                      f"falling back to streaming to_arrow_reader: {mem_exc}")
                con.close()
                con = duckdb.connect(":memory:")
                con.execute("PRAGMA threads=8;")
                con.execute("SET enable_progress_bar=false;")
                con.execute("LOAD httpfs;")
                con.execute("LOAD spatial;")
                con.execute(f"SET s3_region='{OVERTURE_REGION}';")
                reader = con.execute(sql, params).to_arrow_reader(STREAM_BATCH_ROWS)
                lance.write_dataset(
                    reader,
                    LOCAL_DATASET,
                    schema=reader.schema,   # REQUIRED for a reader/iterator source
                    mode="overwrite",
                    data_storage_version=DATA_STORAGE_VERSION,
                    max_rows_per_file=MAX_ROWS_PER_FILE,
                    max_bytes_per_file=MAX_BYTES_PER_FILE,
                )
        finally:
            con.close()

        if write_path == "stream":
            rows = lance.dataset(LOCAL_DATASET).count_rows()
            print(f"  streamed {rows:,} rows (distinct-id check skipped on stream path)")

        # Index on LOCAL disk (no R2 multipart), then publish.
        built = _create_indexes(LOCAL_DATASET)
        s3 = _s3_client()
        published_files, published_bytes = _replace_r2_prefix(s3, DATASET_PREFIX, LOCAL_DATASET)
        print(f"Published {published_files} files ({published_bytes:,} bytes) → {DATASET_URI}")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(DATASET_URI, release_tag, snapshot_date, int(rows), distinct_ids,
                    published_files, published_bytes, write_path, status, error,
                    started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": FEED,
             "dataset_uri": DATASET_URI, "release_tag": release_tag,
             "snapshot_date": snapshot_date, "distinct_ids": distinct_ids,
             "published_files": published_files, "published_bytes": published_bytes,
             "write_path": write_path},
        )

    if status != "success":
        raise RuntimeError(f"overture_places ingest failed (release={release_tag}): {error}")
    return {"feed": FEED, "release_tag": release_tag, "rows_processed": int(rows),
            "distinct_ids": distinct_ids, "published_files": published_files,
            "published_bytes": published_bytes, "write_path": write_path,
            "dataset_uri": DATASET_URI, "snapshot_date": snapshot_date,
            "indices": built, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 90,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=524288,
)
def reindex_overture_places() -> dict:
    """(Re)build the scalar indexes without re-ingesting: stage the committed R2
    dataset to local disk, index locally (no R2 multipart), publish back via
    boto3. Idempotent (replace=True)."""
    import lance

    s3 = _s3_client()
    staged = _download_r2_prefix(s3, DATASET_PREFIX, LOCAL_DATASET)
    print(f"Staged {staged} files from {DATASET_URI} → {LOCAL_DATASET}")
    built = _create_indexes(LOCAL_DATASET)
    published_files, published_bytes = _replace_r2_prefix(s3, DATASET_PREFIX, LOCAL_DATASET)
    print(f"Published {published_files} files ({published_bytes:,} bytes) → {DATASET_URI}")
    ds = lance.dataset(LOCAL_DATASET)
    committed = _list_committed_indices(ds)
    print(f"Committed indices: {committed}")
    return {"dataset": DATASET_URI, "built": built, "committed_indices": committed,
            "published_files": published_files, "published_bytes": published_bytes}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 5) -> list:
    """Read the most recent ops.overture_places_runs rows."""
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, feed, dataset_uri, release_tag, snapshot_date, "
            "rows_processed, distinct_ids, published_files, published_bytes, "
            "write_path, status, error, started_at, completed_at "
            "FROM ops.overture_places_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


@app.local_entrypoint()
def initdb() -> None:
    """Create the ops ledger table (run once before the first ingest)."""
    import json

    print(json.dumps(init_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def run(release: str | None = None) -> None:
    """Ingest the latest (or pinned) Overture release → Lance (manual ops path; no
    Trigger callback)."""
    import json

    print(json.dumps(ingest_overture_places.remote(release=release, trigger_callback_url=None),
                     indent=2, default=str))


@app.local_entrypoint()
def reindex() -> None:
    """Rebuild the scalar indexes on the existing dataset (no re-ingest)."""
    import json

    print(json.dumps(reindex_overture_places.remote(), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 5) -> None:
    """Print the most recent ops ledger rows."""
    import json

    print(json.dumps(ledger.remote(limit), indent=2, default=str))
