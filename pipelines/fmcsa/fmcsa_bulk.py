"""Compute worker — FMCSA bulk feed ingestion (clean-room rebuild).

Part of the ``fmcsa-pipelines`` Modal app. Spawned by the Universal Dispatcher
(core/modal_dispatcher.py) one invocation per feed, by the Trigger.dev cron tasks
(src/trigger/fmcsa_daily.ts, fmcsa_monthly.ts), or driven directly by the
``backfill`` local entrypoint. This worker has no web endpoint.

This replaces the legacy ``data-engine-x-fmcsa-ingest`` Modal app, whose 15-minute
heartbeat (``*/15 * * * *``) polled a ``ops.fmcsa_feed_schedule_config`` table to
decide which feeds were "due" — a 2,976-probe/day single-point-of-failure that
silently halted all 31 feeds whenever the config table flipped ``enabled=false``.
Cadence here belongs to Trigger.dev v4 exclusively; this worker just ingests one
feed when asked.

Source reality (confirmed by live metadata + byte peek, 2026-05-31):
    data.transportation.gov datasets are one of two asset types, and the type
    dictates BOTH how we fetch AND how we parse:

      - ``tabular`` (census, oos) → bulk CSV export at
        /api/views/{id}/rows.csv?accessType=DOWNLOAD. Has a HEADER row; columns
        are projected BY NAME. Carries a clean DOT_NUMBER (USDOT).

      - ``file``/blob (carrier, auth_hist, revocation, insurance, boc3) →
        /api/views/{id}/files/{blobId} (blobId resolved from metadata). A
        HEADERLESS, comma-delimited, double-quoted .txt with a POSITIONAL layout.
        Confirmed layout: position 0 = full docket ('MC000675'); position 1 =
        zero-padded USDOT ('00124159') for carrier/auth_hist/revocation/boc3 — so
        these DO carry USDOT (insurance is the exception: its position 1 is the
        insurance type). DuckDB names headerless columns column0..N OR (when the
        column count ≥ 10) zero-pads to column00..NN, so we resolve the actual
        names via DESCRIBE and reference the key positions by index — never a
        hard-coded "column0".

    The legacy prototype 403'd because it ran the paginated SODA /resource/ API
    against ``file`` assets. We never touch the SODA query API: we resolve the
    asset type from metadata and pull the bulk export/blob. No auth, no app token
    (stale tokens actively cause 403s).

    The "Daily Difference" feeds are full SNAPSHOTS, not deltas (≈100% day-over-day
    key overlap upstream). The active layer holds the LATEST full snapshot per feed
    (overwrite) — this honours D-2 ("store the full daily snapshot, don't derive
    diffs at ingest") while avoiding the legacy gotcha of accumulating ~100%-
    redundant daily copies indefinitely. snapshot_date is stamped on every row.
    The immutable per-day history lives in the landing/ raw zone (one dated object
    per feed per run); the active Lance dataset is point-in-time latest.

Resolution keys (the FMCSA↔SAM federal-contracts bridge joins on these — top
priority, must never carry wrong values):
  - ``carrier_dot``    — USDOT, normalized to an unpadded integer string so it
                         joins across feeds and into the SAM bridge ('00124159'
                         → '124159'; '00000000' → NULL). Populated from DOT_NUMBER
                         (tabular) and from position 1 (carrier/auth_hist/
                         revocation/boc3); NULL for insurance (no USDOT).
  - ``carrier_docket`` — full MC/MX/FF docket from position 0 for blob feeds; NULL
                         for tabular feeds (no single docket column).
  Every source field is ALSO retained losslessly, so a Phase-2 reconciliation can
  map further columns without re-ingesting.

Data plane (clean-room — DuckDB does 100% of the transform):
    DOT source → requests stream (retried)   → /tmp/<feed>.<ext>      (Python: I/O)
      → boto3 upload → R2 landing (raw, byte-faithful provenance)      (Python: I/O)
      → DuckDB read_csv(all_varchar=true) → keys + provenance          (100% SQL)
      → Arrow         → con.sql(...).to_arrow_table()
      → lance.write_dataset(LOCAL /tmp) + BTREE indexes
      → boto3 mirror  → s3://data-sink/active/fmcsa/<feed>/            (Python: I/O)

    R2 NOTE: Lance's native object-store writer emits variable-size multipart
    chunks, which S3 accepts but Cloudflare R2 rejects ("All non-trailing parts
    must have the same length", InvalidPart). So Lance is written to LOCAL scratch
    and the dataset directory is mirrored to R2 with boto3 (uniform 8 MiB parts,
    R2-accepted). DuckDB still does 100% of the transform; Lance is still the
    format and R2 the system of record — only the write transport changes.

    to_arrow_table() is the only Arrow export (the forbidden fetch_arrow_table()
    appears nowhere). Phase-3 >5M-row feeds will batch-read via a record_batch
    reader into the same local-stage writer.

Idempotency: full-snapshot overwrite per feed (the active dataset is replaced each
run). ``ops.fmcsa_feed_runs`` upserts on (feed, snapshot_date) — snapshot_date is
NOT NULL, closing the legacy "NULL feed_date defeats dedup" hole that
double-ingested 4 feeds in production.

    modal run    pipelines/fmcsa/fmcsa_bulk.py                 # Phase-1 backfill (parallel)
    modal run    pipelines/fmcsa/fmcsa_bulk.py --dry-run       # print the feed plan
    modal run    pipelines/fmcsa/fmcsa_bulk.py --only census   # one feed
    modal deploy pipelines/fmcsa/fmcsa_bulk.py
"""

from __future__ import annotations

import os

import modal

BASE = "https://data.transportation.gov"
BUCKET = "data-sink"
LANDING_PREFIX = "landing/fmcsa"
# Lance system-of-record tier (one dataset per feed, D-3). Active layer, not landing.
ACTIVE_PREFIX = os.environ.get("FMCSA_ACTIVE_PREFIX", "s3://data-sink/active/fmcsa")
SCRATCH_DIR = "/tmp"
LANCE_STAGE = "/tmp/lance_active"

# Lance fragment sizing.
# NOTE: the directive wrote `max_bytes_per_file=90 * 10243`. That is read here as
# `90 * 1024**3` (90 GiB) — Lance's documented default, confirmed by the operator.
# A literal `90 * 10243` (~900 KB) would shatter each multi-GB feed into thousands
# of fragments and wreck read performance.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

# Phase-1 feed registry. ``heavy`` routes a feed to the 32 GiB worker (D-6). For
# tabular feeds, ``tabular_dot_col`` is the normalize_names() column carrying USDOT
# (its presence also marks the feed as tabular for index selection). For file
# feeds, ``file_docket_idx`` / ``file_usdot_idx`` are positional key indices
# (None → that key absent). Asset KIND is resolved at runtime from metadata.
FEEDS: dict[str, dict] = {
    "carrier":    {"view_id": "6qg9-x4f8", "name": "Carrier",             "cadence": "daily", "heavy": False, "file_docket_idx": 0, "file_usdot_idx": 1},
    "census":     {"view_id": "az4n-8mr2", "name": "Company Census File", "cadence": "daily", "heavy": True,  "tabular_dot_col": "dot_number"},
    "auth_hist":  {"view_id": "sn3k-dnx7", "name": "AuthHist",            "cadence": "daily", "heavy": False, "file_docket_idx": 0, "file_usdot_idx": 1},
    "revocation": {"view_id": "pivg-szje", "name": "Revocation",          "cadence": "daily", "heavy": False, "file_docket_idx": 0, "file_usdot_idx": 1},
    "insurance":  {"view_id": "mzmm-6xep", "name": "Insur",               "cadence": "daily", "heavy": False, "file_docket_idx": 0, "file_usdot_idx": None},
    "boc3":       {"view_id": "fb8g-ngam", "name": "BOC3",                "cadence": "daily", "heavy": False, "file_docket_idx": 0, "file_usdot_idx": 1},
    "oos":        {"view_id": "p2mt-9ige", "name": "Out of Service",      "cadence": "daily", "heavy": False, "tabular_dot_col": "dot_number"},
}

PHASE1_FEEDS = list(FEEDS.keys())

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=0.19",         # provides `import lance`
    "pyarrow>=17",
    "boto3>=1.35",           # R2 landing upload/download + active-layer mirror
    "requests>=2.32",        # DOT source fetch
    "psycopg[binary]>=3.2",  # ops.* terminal state
)

# Domain-grouped app, isolated per ARCHITECTURE.md §3.
app = modal.App("fmcsa-pipelines", image=image)


# --------------------------------------------------------------------------- #
# R2 / S3
# --------------------------------------------------------------------------- #
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
    """boto3 S3 client for R2. Forces checksum behaviour to ``when_required``:
    botocore's default flexible-checksum validation does not match R2's semantics
    and otherwise raises FlexibleChecksumError on download_file/upload_file."""
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


def _active_location(feed: str) -> tuple[str, str]:
    """(bucket, key_prefix) for a feed's active Lance dataset, parsed from
    ACTIVE_PREFIX (an s3:// URI)."""
    rest = ACTIVE_PREFIX.replace("s3://", "").strip("/")
    parts = rest.split("/", 1)
    bucket = parts[0]
    base = parts[1] if len(parts) > 1 else ""
    key_prefix = "/".join(p for p in (base, feed) if p)
    return bucket, key_prefix


def _dataset_uri(feed: str) -> str:
    return f"{ACTIVE_PREFIX}/{feed}/"


# --------------------------------------------------------------------------- #
# Source resolution + fetch  (D-4: metadata-resolve → assetType branch)
# --------------------------------------------------------------------------- #
def _resolve_asset(view_id: str) -> dict:
    """Resolve a data.transportation.gov dataset to a concrete download URL + kind.

    ``file``/blob assets resolve to the blob (headerless positional .txt);
    everything else is a tabular CSV export (header row). This is the 403-proof
    path: we never hit the paginated SODA API."""
    import requests

    r = requests.get(f"{BASE}/api/views/{view_id}.json", timeout=60)
    r.raise_for_status()
    m = r.json()
    blob_id = m.get("blobId")
    if blob_id:
        return {"kind": "file", "url": f"{BASE}/api/views/{view_id}/files/{blob_id}",
                "name": m.get("name"), "updated_at": m.get("rowsUpdatedAt")}
    return {"kind": "tabular", "url": f"{BASE}/api/views/{view_id}/rows.csv?accessType=DOWNLOAD",
            "name": m.get("name"), "updated_at": m.get("rowsUpdatedAt")}


def _download(url: str, dest: str, attempts: int = 5) -> int:
    """Stream a source URL to local scratch, retrying transient connection breaks.

    The large bulk exports (census ~330 MB) occasionally drop mid-stream with
    IncompleteRead/ChunkedEncodingError from data.transportation.gov; each attempt
    re-downloads from scratch with backoff. Verifies the written size against
    Content-Length when the server advertises one (and is not gzip-encoded), so a
    silently truncated body is a failure, not a short success. Returns bytes."""
    import time

    import requests

    backoff = (5, 15, 45, 120)
    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            total = 0
            with requests.get(url, stream=True, timeout=(30, 1800)) as r:
                r.raise_for_status()
                declared = r.headers.get("Content-Length")
                gzipped = "gzip" in r.headers.get("Content-Encoding", "").lower()
                expected = int(declared) if (declared and not gzipped) else None
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8 << 20):
                        if chunk:
                            f.write(chunk)
                            total += len(chunk)
            if expected is not None and total != expected:
                raise OSError(f"truncated download: got {total} bytes, expected {expected}")
            return total
        except Exception as exc:  # noqa: BLE001 — retry transient network failures
            last_exc = exc
            if i < attempts - 1:
                wait = backoff[min(i, len(backoff) - 1)]
                print(f"download attempt {i + 1}/{attempts} failed ({exc}); retrying in {wait}s")
                time.sleep(wait)
    raise RuntimeError(f"download failed after {attempts} attempts: {last_exc}")


# --------------------------------------------------------------------------- #
# DuckDB projection — branches on asset kind
# --------------------------------------------------------------------------- #
def _read_csv_clause(kind: str, path: str) -> str:
    def lit(s: str) -> str:
        return s.replace("'", "''")

    if kind == "tabular":
        return (f"read_csv('{lit(path)}', all_varchar=true, header=true, "
                f"normalize_names=true, sample_size=-1, ignore_errors=false)")
    return (f"read_csv('{lit(path)}', all_varchar=true, header=false, "
            f"null_padding=true, sample_size=-1, ignore_errors=true)")


def _dot_norm(col: str) -> str:
    # Canonical USDOT: strip zero-padding and drop the 0 / '00000000' sentinel, so
    # a blob USDOT ('00124159') joins the census DOT_NUMBER ('124159') and the SAM
    # bridge key resolves across feeds.
    return f"CASE WHEN TRY_CAST({col} AS BIGINT) > 0 THEN CAST(TRY_CAST({col} AS BIGINT) AS VARCHAR) END"


def _provenance_sql(feed: str, view_id: str, snapshot_date: str,
                    source_updated_at: str | None) -> str:
    def lit(s: str) -> str:
        return s.replace("'", "''")

    updated_sql = (f"TIMESTAMP '{lit(source_updated_at)}'"
                   if source_updated_at else "CAST(NULL AS TIMESTAMP)")
    return (f"'{lit(feed)}' AS source_feed,\n"
            f"    '{lit(view_id)}' AS source_view_id,\n"
            f"    DATE '{lit(snapshot_date)}' AS snapshot_date,\n"
            f"    {updated_sql} AS source_updated_at,\n"
            f"    now() AS ingested_at")


def _build_sql(feed: str, kind: str, scratch_path: str, snapshot_date: str,
               view_id: str, source_updated_at: str | None,
               pos_cols: list[str] | None) -> str:
    """Project the source CSV: derive canonical resolution keys, retain every
    source column losslessly as VARCHAR, append provenance.

    tabular → header=true, project DOT_NUMBER by name (carrier_dot), docket NULL.
    file    → header=false, positional; keys referenced via the actual DESCRIBE'd
              column names in ``pos_cols`` (robust to DuckDB's column-name padding)."""
    cfg = FEEDS[feed]
    prov = _provenance_sql(feed, view_id, snapshot_date, source_updated_at)
    src = _read_csv_clause(kind, scratch_path)

    if kind == "tabular":
        dot_col = cfg.get("tabular_dot_col", "dot_number")
        return f"""
SELECT
    {_dot_norm(dot_col)} AS carrier_dot,
    CAST(NULL AS VARCHAR) AS carrier_docket,
    *,
    {prov}
FROM {src}
"""

    cols = pos_cols or []
    di = cfg.get("file_docket_idx", 0)
    ui = cfg.get("file_usdot_idx")
    docket_expr = (f"nullif(trim({cols[di]}), '')"
                   if di is not None and di < len(cols) else "CAST(NULL AS VARCHAR)")
    dot_expr = (_dot_norm(cols[ui])
                if ui is not None and ui < len(cols) else "CAST(NULL AS VARCHAR)")
    return f"""
SELECT
    {dot_expr} AS carrier_dot,
    {docket_expr} AS carrier_docket,
    *,
    {prov}
FROM {src}
"""


# --------------------------------------------------------------------------- #
# Lance write — local stage + boto3 mirror (R2 multipart-uniform), then index
# --------------------------------------------------------------------------- #
def _index_cols(feed: str, kind: str) -> list[str]:
    cfg = FEEDS[feed]
    if kind == "tabular":
        return ["carrier_dot"]
    return ["carrier_docket"] + (["carrier_dot"] if cfg.get("file_usdot_idx") is not None else [])


def _clear_r2_prefix(s3, bucket: str, key_prefix: str) -> int:
    deleted = 0
    batch: list[dict] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key_prefix + "/"):
        for o in page.get("Contents", []):
            batch.append({"Key": o["Key"]})
            if len(batch) == 1000:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
                deleted += len(batch)
                batch = []
    if batch:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
        deleted += len(batch)
    return deleted


def _upload_dir_to_r2(s3, local_dir: str, bucket: str, key_prefix: str) -> int:
    count = 0
    for root, _dirs, files in os.walk(local_dir):
        for fn in files:
            full = os.path.join(root, fn)
            rel = os.path.relpath(full, local_dir)
            s3.upload_file(full, bucket, f"{key_prefix}/{rel}")
            count += 1
    return count


def _write_lance_staged(con, sql: str, feed: str, kind: str) -> int:
    """Write the projection to a LOCAL Lance dataset (+ BTREE indexes), then mirror
    the directory to R2 with boto3 (uniform multipart parts that R2 accepts).
    Overwrite semantics: the R2 active prefix is cleared first. Returns rows."""
    import shutil

    import lance

    local_dir = os.path.join(LANCE_STAGE, feed)
    shutil.rmtree(local_dir, ignore_errors=True)
    os.makedirs(local_dir, exist_ok=True)

    table = con.sql(sql).to_arrow_table()
    rows = table.num_rows

    lance.write_dataset(table, local_dir, mode="overwrite",
                        data_storage_version="2.0",
                        max_rows_per_file=MAX_ROWS_PER_FILE,
                        max_bytes_per_file=MAX_BYTES_PER_FILE)

    ds = lance.dataset(local_dir)
    for col in _index_cols(feed, kind):
        try:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"Created BTREE index on {feed}.{col} (local)")
        except Exception as exc:  # noqa: BLE001
            print(f"WARN: BTREE index on {feed}.{col} failed (non-fatal): {exc}")

    s3 = _s3_client()
    bucket, key_prefix = _active_location(feed)
    cleared = _clear_r2_prefix(s3, bucket, key_prefix)
    uploaded = _upload_dir_to_r2(s3, local_dir, bucket, key_prefix)
    print(f"R2 mirror {feed}: cleared {cleared} old objects, uploaded {uploaded} files → {key_prefix}/")

    shutil.rmtree(local_dir, ignore_errors=True)
    return rows


# --------------------------------------------------------------------------- #
# ops ledger + Trigger callback
# --------------------------------------------------------------------------- #
# psycopg3 execute() runs one statement at a time — keep these separate.
OPS_DDL = (
    "CREATE SCHEMA IF NOT EXISTS ops",
    """
    CREATE TABLE IF NOT EXISTS ops.fmcsa_feed_runs (
        id                bigserial PRIMARY KEY,
        feed              text        NOT NULL,
        view_id           text        NOT NULL,
        snapshot_date     date        NOT NULL,
        asset_kind        text,
        rows_processed    bigint      NOT NULL DEFAULT 0,
        bytes_landed      bigint,
        landing_key       text,
        source_updated_at timestamptz,
        status            text        NOT NULL,
        error             text,
        started_at        timestamptz NOT NULL,
        completed_at      timestamptz,
        UNIQUE (feed, snapshot_date)
    )
    """,
)


def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record_run(*, feed, view_id, snapshot_date, asset_kind, rows, bytes_landed,
                landing_key, source_updated_at, status, error, started_at, completed_at) -> None:
    """Upsert terminal state. ON CONFLICT (feed, snapshot_date) so a same-day
    re-run overwrites rather than duplicating — snapshot_date is NOT NULL by
    construction, so the dedup key never collapses."""
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.fmcsa_feed_runs
                    (feed, view_id, snapshot_date, asset_kind, rows_processed, bytes_landed,
                     landing_key, source_updated_at, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (feed, snapshot_date) DO UPDATE SET
                    view_id = EXCLUDED.view_id,
                    asset_kind = EXCLUDED.asset_kind,
                    rows_processed = EXCLUDED.rows_processed,
                    bytes_landed = EXCLUDED.bytes_landed,
                    landing_key = EXCLUDED.landing_key,
                    source_updated_at = EXCLUDED.source_updated_at,
                    status = EXCLUDED.status,
                    error = EXCLUDED.error,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at
                """,
                (feed, view_id, snapshot_date, asset_kind, rows, bytes_landed, landing_key,
                 source_updated_at, status, error, started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.* write failed: {exc}")
    finally:
        conn.close()


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


# --------------------------------------------------------------------------- #
# Core ingest
# --------------------------------------------------------------------------- #
def _run_ingest(feed: str, snapshot_date: str | None,
                trigger_callback_url: str | None) -> dict:
    import datetime as dt
    import os.path

    import duckdb

    if feed not in FEEDS:
        raise ValueError(f"Unknown feed '{feed}'. Known: {sorted(FEEDS)}")
    cfg = FEEDS[feed]
    view_id = cfg["view_id"]
    started_at = dt.datetime.now(dt.timezone.utc)
    snap = snapshot_date or started_at.strftime("%Y-%m-%d")

    rows = 0
    bytes_landed = 0
    landing_key = None
    source_updated_at = None
    kind = None
    status = "error"
    error: str | None = None

    try:
        asset = _resolve_asset(view_id)
        kind = asset["kind"]
        src_updated = asset.get("updated_at")
        if src_updated:
            source_updated_at = dt.datetime.fromtimestamp(int(src_updated), dt.timezone.utc).isoformat()

        ext = "csv" if kind == "tabular" else "txt"
        scratch = os.path.join(SCRATCH_DIR, f"{feed}.{ext}")
        bytes_landed = _download(asset["url"], scratch)

        # Land raw bytes to R2 first (durable, byte-faithful provenance).
        s3 = _s3_client()
        landing_key = f"{LANDING_PREFIX}/{feed}/{snap}/{view_id}.{ext}"
        s3.upload_file(scratch, BUCKET, landing_key)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=4;")
            pos_cols = None
            if kind == "file":
                # Resolve actual positional column names (DuckDB pads column0..N to
                # column00..NN once the count ≥ 10) so key positions bind correctly.
                desc = con.execute(f"DESCRIBE SELECT * FROM {_read_csv_clause(kind, scratch)}").fetchall()
                pos_cols = [row[0] for row in desc]
            sql = _build_sql(feed, kind, scratch, snap, view_id, source_updated_at, pos_cols)
            rows = _write_lance_staged(con, sql, feed, kind)
        finally:
            con.close()

        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(feed=feed, view_id=view_id, snapshot_date=snap, asset_kind=kind,
                    rows=int(rows), bytes_landed=int(bytes_landed), landing_key=landing_key,
                    source_updated_at=source_updated_at, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "rows": int(rows), "feed": feed, "snapshot_date": snap},
        )

    if status != "success":
        raise RuntimeError(f"FMCSA ingest failed for {feed} ({snap}): {error}")
    return {"feed": feed, "snapshot_date": snap, "rows_processed": int(rows),
            "asset_kind": kind, "bytes_landed": int(bytes_landed),
            "landing_key": landing_key, "status": status}


# --------------------------------------------------------------------------- #
# Modal functions — standard (16 GiB) and heavy (32 GiB, wide/tall feeds, D-6)
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60,
    memory=16384,
    cpu=4.0,
)
def ingest_fmcsa_feed(feed: str, snapshot_date: str | None = None,
                      trigger_callback_url: str | None = None) -> dict:
    """Standard memory tier (16 GiB)."""
    return _run_ingest(feed, snapshot_date, trigger_callback_url)


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=2 * 60 * 60,
    memory=32768,
    cpu=8.0,
)
def ingest_fmcsa_feed_xl(feed: str, snapshot_date: str | None = None,
                         trigger_callback_url: str | None = None) -> dict:
    """Heavy memory tier (32 GiB) for wide/tall feeds (e.g. census, 147 cols)."""
    return _run_ingest(feed, snapshot_date, trigger_callback_url)


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_datasets(feeds: list[str] | None = None) -> dict:
    """Read-back proof: open each feed's Lance dataset from R2 and count rows +
    non-null resolution keys. Authoritative success check — reads what actually
    landed, independent of the write path's return value."""
    import lance

    so = _r2_storage_options()
    out: dict[str, dict] = {}
    for feed in (feeds or PHASE1_FEEDS):
        uri = _dataset_uri(feed)
        try:
            ds = lance.dataset(uri, storage_options=so)
            n = ds.count_rows()
            dot = ds.count_rows(filter="carrier_dot IS NOT NULL")
            dock = ds.count_rows(filter="carrier_docket IS NOT NULL")
            out[feed] = {"rows": n, "carrier_dot_non_null": dot,
                         "carrier_docket_non_null": dock, "status": "ok"}
        except Exception as exc:  # noqa: BLE001
            out[feed] = {"rows": 0, "status": f"missing/error: {str(exc)[:160]}"}
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops_table() -> str:
    """Create ops.fmcsa_feed_runs (idempotent). Run once before the first ingest."""
    conn = _pg_connect()
    if conn is None:
        return "skipped: HQX_DB_URL_POOLED not set"
    try:
        with conn, conn.cursor() as cur:
            for stmt in OPS_DDL:
                cur.execute(stmt)
        return "ops.fmcsa_feed_runs ready"
    finally:
        conn.close()


def route_for(feed: str) -> str:
    """Function name the dispatcher should target for a feed (size-based, D-6)."""
    return "ingest_fmcsa_feed_xl" if FEEDS[feed].get("heavy") else "ingest_fmcsa_feed"


# --------------------------------------------------------------------------- #
# Manual backfill entrypoint
# --------------------------------------------------------------------------- #
@app.local_entrypoint()
def backfill(only: str = "", snapshot_date: str = "", dry_run: bool = False) -> None:
    """Phase-1 backfill. Feeds write independent Lance datasets, so they run in
    PARALLEL via Modal .map fan-out. ``--dry-run`` prints the plan and creates the
    ops table without ingesting; ``--only`` filters."""
    feeds = [f for f in PHASE1_FEEDS if (not only or only in f)]
    print(f"FMCSA Phase-1 plan ({len(feeds)} feeds):")
    for f in feeds:
        c = FEEDS[f]
        print(f"  {f:12s} {c['view_id']}  heavy={c.get('heavy', False)!s:5s}  → {route_for(f)}")

    print("\nEnsuring ops.fmcsa_feed_runs …")
    print("  ", init_ops_table.remote())
    if dry_run:
        return

    snap = snapshot_date or None
    std = [f for f in feeds if route_for(f) == "ingest_fmcsa_feed"]
    xl = [f for f in feeds if route_for(f) == "ingest_fmcsa_feed_xl"]
    kw = {"snapshot_date": snap, "trigger_callback_url": None}

    # return_exceptions=True: one feed failing must not abort the whole backfill.
    paired: list[tuple[str, object]] = []
    if std:
        paired += list(zip(std, ingest_fmcsa_feed.map(std, kwargs=kw, return_exceptions=True)))
    if xl:
        paired += list(zip(xl, ingest_fmcsa_feed_xl.map(xl, kwargs=kw, return_exceptions=True)))

    print("\n=== FMCSA Phase-1 backfill results ===")
    total = 0
    failed = 0
    for feed, r in paired:
        if isinstance(r, Exception):
            failed += 1
            print(f"  {feed:12s} FAILED  {type(r).__name__}: {str(r)[:160]}")
            continue
        n = int(r.get("rows_processed", 0))
        total += n
        print(f"  {feed:12s} rows={n:>9,}  kind={r.get('asset_kind')}  status={r.get('status')}")
    print(f"  {'TOTAL':12s} rows={total:>9,}  ({len(paired) - failed}/{len(paired)} feeds ok)")

    # Read-back proof straight from the Lance datasets in R2.
    print("\n=== Lance read-back (verify_datasets) ===")
    verify = verify_datasets.remote([f for f, _ in paired])
    for feed in [f for f, _ in paired]:
        v = verify.get(feed, {})
        print(f"  {feed:12s} rows={v.get('rows', 0):>9,}  dot_nn={v.get('carrier_dot_non_null', 0):>9,}"
              f"  dock_nn={v.get('carrier_docket_non_null', 0):>9,}  {v.get('status')}")
