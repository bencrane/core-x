"""One-shot structural optimization of the committed Overture Places Lance SoR.

RE-INGEST-FREE: reads s3://data-sink/active/overture_places/ back from R2 (no
Overture re-pull), applies the v2 transform (Hilbert sort/spatial key + region
normalize + confidence→float32 + constant-column demotion to schema metadata),
rewrites SORTED by (region, hilbert), rebuilds the v2 scalar-index set, then
republishes to the SAME URI — guarded by a pre-wipe R2 backup and a HARD
build-verify gate before any publish.

Mutates the SoR. Re-run-safe (aborts pre-mutation as a clean no-op if the SoR is
already v2), ledgered (ops.overture_places_runs, write_path='optimize'), reversible
(server-side R2 backup + restore-on-failure).

    modal run pipelines/overture_maps/optimize.py::dryrun    # build+verify LOCAL only, NO mutation
    modal run pipelines/overture_maps/optimize.py::apply     # backup → publish → verify → ledger
"""
from __future__ import annotations

import os

import modal

from pipelines.overture_maps._transform import (
    HILBERT_BOUNDS_TAG,
    OPTIMIZED_BITMAP_INDEXES,
    OPTIMIZED_BTREE_INDEXES,
    SCHEMA_VERSION,
    projection_sql,
)

# ── System-of-record (R2) ──────────────────────────────────────────────────
BUCKET = "data-sink"
DATASET_PREFIX = "active/overture_places/"
DATASET_URI = f"s3://{BUCKET}/{DATASET_PREFIX}"
SCRATCH_DIR = "/tmp/overture_opt"
LOCAL_OUT = os.path.join(SCRATCH_DIR, "out_lance")
FEED = "overture_places"

# Source baseline from the 2026-06-06 diagnostic — assert no drift before mutating.
SRC_ROWS_EXPECTED = 16_273_123

# Lance fragment sizing — identical to the ingest (Lance defaults / 90 GiB ceiling).
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
STREAM_BATCH_ROWS = 1_048_576

# Source columns to read back (flat; constants captured separately for metadata).
SOURCE_COLUMNS = [
    "id", "longitude", "latitude", "region",
    "locality", "postcode", "name", "category", "confidence",
]


class AlreadyV2(Exception):
    """Raised when the SoR is already overture_places.v2 — migration is a no-op.
    Caught by optimize_overture_places and reported as a clean, non-error no-op so a
    retry harness treats 'already migrated' as success, never as failure."""

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",
        "lancedb>=0.15",
        "pylance>=7",
        "pyarrow>=17",
        "boto3>=1.35",
        "psycopg[binary]>=3.2",
    )
    .run_commands(
        "python -c \"import duckdb; duckdb.connect().execute('INSTALL httpfs; INSTALL spatial;')\""
    )
    .env({"LANCE_BYPASS_SPILLING": "true"})
    # Mount the local package so `from pipelines.overture_maps._transform import …`
    # resolves in the container. (Modal automounts imported local modules in current
    # versions; this is the explicit, deterministic form.)
    .add_local_python_source("pipelines")
)

app = modal.App("overture-maps-optimize", image=image)


# ── R2 helpers (self-contained; mirror pipelines/overture_maps/places.py) ────
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
        "aws_endpoint": endpoint,
        "aws_region": "auto",
        "aws_virtual_hosted_style_request": "false",
    }


def _lance_storage_options() -> dict[str, str]:
    # object_store keys for Lance reads/writes against R2 (path-style).
    return _r2_storage_options()


def _s3_client():
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client("s3", endpoint_url=so["aws_endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"],
                        region_name="auto", config=cfg)


def _list_keys(s3, prefix: str) -> list[str]:
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            keys.append(o["Key"])
    return keys


def _backup_r2_prefix(s3, src_prefix: str, bak_prefix: str) -> int:
    """Server-side CopyObject every object src→bak (no egress). Returns count."""
    n = 0
    for key in _list_keys(s3, src_prefix):
        rel = key[len(src_prefix):]
        if not rel:
            continue
        s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key},
                       Key=bak_prefix + rel)
        n += 1
    return n


def _wipe_prefix(s3, prefix: str) -> None:
    batch = []
    for key in _list_keys(s3, prefix):
        batch.append({"Key": key})
        if len(batch) == 1000:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})
            batch = []
    if batch:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})


def _upload_dir(s3, prefix: str, local_dir: str) -> tuple[int, int]:
    files = bytes_ = 0
    for root, _, fnames in os.walk(local_dir):
        for fn in fnames:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            s3.upload_file(lp, BUCKET, prefix + rel)
            files += 1
            bytes_ += os.path.getsize(lp)
    return files, bytes_


def _restore_r2_prefix(s3, bak_prefix: str, dst_prefix: str) -> int:
    """Roll back: wipe dst, copy bak→dst server-side."""
    _wipe_prefix(s3, dst_prefix)
    n = 0
    for key in _list_keys(s3, bak_prefix):
        rel = key[len(bak_prefix):]
        if not rel:
            continue
        s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key},
                       Key=dst_prefix + rel)
        n += 1
    return n


def _record_run(dataset_uri, release_tag, snapshot_date, rows, distinct_ids,
                published_files, published_bytes, write_path, status, error,
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
                INSERT INTO ops.overture_places_runs
                    (feed, dataset_uri, release_tag, snapshot_date, rows_processed,
                     distinct_ids, published_files, published_bytes, write_path,
                     status, error, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (FEED, dataset_uri, release_tag, snapshot_date, rows, distinct_ids,
                 published_files, published_bytes, write_path, status, error,
                 started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the migration
        print(f"WARN: ops.* write failed: {exc}")


# ── index build + verification ──────────────────────────────────────────────
def _build_indexes(ds) -> list[str]:
    built = []
    for col in OPTIMIZED_BTREE_INDEXES:
        ds.create_scalar_index(col, "BTREE", replace=True)
        built.append(f"BTREE:{col}")
        print(f"  BTREE  ✓ {col}")
    for col in OPTIMIZED_BITMAP_INDEXES:
        ds.create_scalar_index(col, "BITMAP", replace=True)
        built.append(f"BITMAP:{col}")
        print(f"  BITMAP ✓ {col}")
    return built


def _index_names(ds) -> set[str]:
    out = set()
    for ix in ds.list_indices():
        cols = ix.get("fields") if isinstance(ix, dict) else getattr(ix, "fields", None)
        if cols:
            out.update(cols)
    return out


def _verify_local(local_path: str, expected_rows: int) -> dict:
    """HARD pre-publish gate. Raises on any failure → SoR is never touched."""
    import lance

    ds = lance.dataset(local_path)
    rows = ds.count_rows()
    fields = {f.name: str(f.type) for f in ds.schema}
    meta = {k.decode(): v.decode() for k, v in (ds.schema.metadata or {}).items()}
    idx_cols = _index_names(ds)

    expect_fields = {
        "id": "string", "longitude": "double", "latitude": "double",
        "hilbert": "uint32", "region": "string", "locality": "string",
        "postcode": "string", "name": "string", "category": "string",
        "confidence": "float",
    }
    expect_idx = set(OPTIMIZED_BTREE_INDEXES) | set(OPTIMIZED_BITMAP_INDEXES)
    expect_meta = {"country", "release_tag", "snapshot_date", "ingested_at", "schema_version"}

    problems = []
    if rows != expected_rows:
        problems.append(f"row count {rows} != expected {expected_rows}")
    if fields != expect_fields:
        problems.append(f"schema mismatch: got {fields}")
    if not expect_idx.issubset(idx_cols):
        problems.append(f"missing indices: {expect_idx - idx_cols}")
    if {"longitude", "latitude"} & idx_cols:
        problems.append(f"stale lon/lat BTREE present: {idx_cols}")
    if not expect_meta.issubset(set(meta)):
        problems.append(f"missing metadata keys: {expect_meta - set(meta)}")

    # pushdown smoke test on the new spatial key
    plan = ds.scanner(filter="hilbert >= 0 AND hilbert <= 4294967295",
                      columns=["id"]).explain_plan(True)
    if "ScalarIndexQuery" not in plan:
        problems.append("hilbert range did not use ScalarIndexQuery")

    if problems:
        raise RuntimeError("LOCAL VERIFY FAILED:\n  - " + "\n  - ".join(problems))
    return {"rows": rows, "fields": fields, "metadata": meta, "indexed_cols": sorted(idx_cols)}


def _transform_and_build(con_threads: int = 8) -> dict:
    """Read SoR → transform (sorted) → write local Lance → build v2 indexes →
    LOCAL verify. No R2 mutation. Returns build report."""
    import shutil

    import duckdb
    import lance

    so = _lance_storage_options()
    src = lance.dataset(DATASET_URI, storage_options=so)

    # Idempotency guard: a v2 SoR has 'hilbert' and has dropped the provenance
    # columns. Detect that and abort as a clean no-op BEFORE any read/transform —
    # re-running after a (rare) failed auto-restore must not crash on a missing column.
    field_names = {f.name for f in src.schema}
    if "hilbert" in field_names and not {"country", "release_tag"}.issubset(field_names):
        sv = (src.schema.metadata or {}).get(b"schema_version", b"").decode() or "v2"
        raise AlreadyV2(sv)

    src_rows = src.count_rows()
    print(f"Source rows: {src_rows:,}")
    if src_rows != SRC_ROWS_EXPECTED:
        raise RuntimeError(
            f"Source row drift: {src_rows} != baseline {SRC_ROWS_EXPECTED}. "
            "Re-run the diagnostic and update SRC_ROWS_EXPECTED before optimizing."
        )

    # capture constant provenance (one row — these are cardinality-1 columns)
    prov_tbl = src.scanner(
        columns=["country", "release_tag", "snapshot_date", "ingested_at"], limit=1
    ).to_table()
    prov = {c: prov_tbl.column(c)[0].as_py() for c in prov_tbl.column_names}
    release_tag = str(prov.get("release_tag"))
    snapshot_date = str(prov.get("snapshot_date"))
    metadata = {
        "country": str(prov.get("country")),
        "release_tag": release_tag,
        "snapshot_date": snapshot_date,
        "ingested_at": str(prov.get("ingested_at")),
        "schema_version": SCHEMA_VERSION,
        "sort_order": "region,hilbert",
        "hilbert_bounds": HILBERT_BOUNDS_TAG,
    }
    print(f"Captured provenance → schema metadata: {metadata}")

    os.makedirs(SCRATCH_DIR, exist_ok=True)
    shutil.rmtree(LOCAL_OUT, ignore_errors=True)

    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads={con_threads};")
    con.execute("SET enable_progress_bar=false;")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET memory_limit='24GB';")
    con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
    con.execute("LOAD spatial;")

    reader = src.scanner(columns=SOURCE_COLUMNS).to_reader()
    con.register("src", reader)
    sql = projection_sql("src")

    distinct_ids = None
    write_path = "materialize"
    try:
        table = con.execute(sql).to_arrow_table()
        table = table.replace_schema_metadata(
            {k.encode(): v.encode() for k, v in metadata.items()}
        )
        out_rows = table.num_rows
        con.register("proj", table)
        distinct_ids = con.execute("SELECT count(DISTINCT id) FROM proj").fetchone()[0]
        con.unregister("proj")
        if out_rows != src_rows:
            raise RuntimeError(f"row-preservation violated: {out_rows} != {src_rows}")
        if distinct_ids != out_rows:
            raise RuntimeError(f"id no longer unique: distinct {distinct_ids} != rows {out_rows}")
        print(f"  transformed {out_rows:,} rows; distinct id = {distinct_ids:,}")
        lance.write_dataset(
            table, LOCAL_OUT, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
        )
    except (MemoryError, duckdb.OutOfMemoryException) as exc:
        write_path = "stream"
        print(f"  materialize hit {type(exc).__name__}; streaming fallback: {exc}")
        con.close()
        con = duckdb.connect(":memory:")
        con.execute(f"PRAGMA threads={con_threads};")
        con.execute("SET enable_progress_bar=false;")
        con.execute("SET preserve_insertion_order=false;")
        con.execute("SET memory_limit='24GB';")
        con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
        con.execute("LOAD spatial;")
        reader = src.scanner(columns=SOURCE_COLUMNS).to_reader()
        con.register("src", reader)
        rdr = con.execute(sql).to_arrow_reader(STREAM_BATCH_ROWS)
        # NOTE: lance.write_dataset drops the schema= kwarg's metadata for a
        # RecordBatchReader source (it takes the data schema from the reader). Write
        # with the reader's own schema, then set metadata on the committed dataset.
        lance.write_dataset(
            rdr, LOCAL_OUT, schema=rdr.schema, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
        )
        lance.dataset(LOCAL_OUT).update_schema_metadata(dict(metadata))
    finally:
        con.close()

    if write_path == "stream":
        out_rows = lance.dataset(LOCAL_OUT).count_rows()
        if out_rows != src_rows:
            raise RuntimeError(f"row-preservation violated (stream): {out_rows} != {src_rows}")

    ds_out = lance.dataset(LOCAL_OUT)
    built = _build_indexes(ds_out)
    report = _verify_local(LOCAL_OUT, src_rows)
    report.update({"built": built, "write_path": write_path,
                   "release_tag": release_tag, "snapshot_date": snapshot_date,
                   "distinct_ids": distinct_ids, "src_rows": src_rows})
    print(f"LOCAL build+verify OK: {report}")
    return report


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90, memory=32768, cpu=8.0, ephemeral_disk=524288,
)
def optimize_overture_places(apply: bool = False) -> dict:
    """dryrun (apply=False): build+verify LOCAL only, NO mutation.
    apply=True: + R2 backup → publish → post-publish verify (restore-on-fail) → ledger."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    try:
        report = _transform_and_build()
    except AlreadyV2 as exc:
        msg = f"SoR is already {exc}; migration is a no-op (nothing to do)."
        print(msg)
        return {"mode": "noop", "already_v2": True, "schema_version": str(exc),
                "mutated": False, "note": msg}

    if not apply:
        return {"mode": "dryrun", "mutated": False, **report}

    s3 = _s3_client()
    ts = started_at.strftime("%Y%m%dT%H%M%SZ")
    bak_prefix = f"active/overture_places__bak_{report['release_tag']}_{ts}/"
    status, error = "error", None
    published_files = published_bytes = 0
    try:
        n_bak = _backup_r2_prefix(s3, DATASET_PREFIX, bak_prefix)
        print(f"Backed up {n_bak} objects → s3://{BUCKET}/{bak_prefix}")

        _wipe_prefix(s3, DATASET_PREFIX)
        published_files, published_bytes = _upload_dir(s3, DATASET_PREFIX, LOCAL_OUT)
        print(f"Published {published_files} files ({published_bytes:,} B) → {DATASET_URI}")

        # post-publish verify against R2; restore on any failure
        pub = lance.dataset(DATASET_URI, storage_options=_lance_storage_options())
        pub_rows = pub.count_rows()
        pub_idx = _index_names(pub)
        n_region = pub.scanner(filter="region = 'CA'", columns=["id"]).to_table().num_rows
        ok = (pub_rows == report["src_rows"]
              and set(OPTIMIZED_BTREE_INDEXES + OPTIMIZED_BITMAP_INDEXES).issubset(pub_idx)
              and n_region > 0)
        if not ok:
            raise RuntimeError(
                f"POST-PUBLISH VERIFY FAILED: rows={pub_rows} idx={sorted(pub_idx)} ca_rows={n_region}"
            )
        status = "success"
        print(f"Post-publish verify OK: rows={pub_rows:,} CA={n_region:,} idx={sorted(pub_idx)}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"FAILURE: {error} — attempting rollback from {bak_prefix}")
        try:
            n_res = _restore_r2_prefix(s3, bak_prefix, DATASET_PREFIX)
            print(f"ROLLBACK: restored {n_res} objects from backup; SoR returned to pre-optimize state.")
        except Exception as rexc:  # noqa: BLE001
            print(f"CRITICAL: rollback FAILED: {rexc}. Backup intact at s3://{BUCKET}/{bak_prefix}")
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(DATASET_URI, report["release_tag"], report["snapshot_date"],
                    int(report["src_rows"]), report.get("distinct_ids"),
                    published_files, published_bytes, "optimize", status, error,
                    started_at, completed_at)

    return {"mode": "apply", "mutated": True, "backup_prefix": bak_prefix,
            "published_files": published_files, "published_bytes": published_bytes, **report}


@app.local_entrypoint()
def dryrun() -> None:
    import json
    print(json.dumps(optimize_overture_places.remote(apply=False), indent=2, default=str))


@app.local_entrypoint()
def apply() -> None:
    import json
    print(json.dumps(optimize_overture_places.remote(apply=True), indent=2, default=str))
