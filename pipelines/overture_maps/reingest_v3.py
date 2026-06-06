"""One-shot v3 RE-INGEST of the Overture Places SoR with deterministic resolution keys.

WHY A RE-INGEST (not an in-place transform): the v2 SoR dropped websites/phones/
freeform/taxonomy at ingest — they exist ONLY in the Overture source, so they cannot be
recovered from the committed dataset (optimize.py reads the SoR and is structurally
incapable of adding them). This worker re-reads the Overture source PINNED to the SAME
release the live SoR was built from (2026-05-20.0) so v3 is a true row-superset of v2
(same release, same US filter), adds registrable `domain` + canonical `phone` + raw
`street` + `taxonomy`, BTREE-indexes the two deterministic keys (domain, phone), and
republishes to the SAME URI.

It OVERWRITES the SoR, so it carries optimize.py's full safety harness: build → LOCAL
HARD verify gate → server-side R2 backup of the current v2 → wipe+publish → post-publish
verify → restore-on-failure → ops ledger. dryrun/apply split. Re-run-safe (clean no-op
if the SoR is already v3).

    modal run pipelines/overture_maps/reingest_v3.py::dryrun   # build+verify LOCAL only, NO mutation
    modal run pipelines/overture_maps/reingest_v3.py::apply    # backup -> publish -> verify -> ledger
"""
from __future__ import annotations

import os

import modal

from pipelines.overture_maps._transform import (
    HILBERT_BOUNDS_TAG,
    OPTIMIZED_BITMAP_INDEXES,
    OPTIMIZED_BTREE_INDEXES,
    SCHEMA_VERSION,
    domain_sql,
    phone_sql,
    projection_sql,
)

# ── System-of-record (R2) ──────────────────────────────────────────────────
BUCKET = "data-sink"
DATASET_PREFIX = "active/overture_places/"
DATASET_URI = f"s3://{BUCKET}/{DATASET_PREFIX}"
SCRATCH_DIR = "/tmp/overture_v3"
LOCAL_OUT = os.path.join(SCRATCH_DIR, "out_lance")
FEED = "overture_places"

# ── Upstream (public AWS S3 — anonymous) ───────────────────────────────────
OVERTURE_BUCKET = "overturemaps-us-west-2"
OVERTURE_REGION = "us-west-2"
OVERTURE_PLACES_GLOB = "s3://overturemaps-us-west-2/release/{rel}/theme=places/type=place/*.parquet"

# Pin to the EXACT release the live v2 SoR was built from (its schema metadata
# release_tag), so v3 is a true superset of v2 — same rows, no vintage drift.
PINNED_RELEASE = "2026-05-20.0"
# Row baseline from the 2026-06-06 diagnostic — assert no drift before publishing.
SRC_ROWS_EXPECTED = 16_273_123

# Lance fragment sizing — identical to the ingest / optimize.
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
STREAM_BATCH_ROWS = 1_048_576


class AlreadyV3(Exception):
    """Raised when the SoR is already overture_places.v3 — re-ingest is a no-op."""


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
    .add_local_python_source("pipelines")
)

app = modal.App("overture-maps-reingest-v3", image=image)


# ── R2 helpers (self-contained; mirror optimize.py) ──────────────────────────
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


def _anon_s3_client():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    return boto3.client("s3", region_name=OVERTURE_REGION,
                        config=Config(signature_version=UNSIGNED))


def _list_keys(s3, prefix: str) -> list[str]:
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            keys.append(o["Key"])
    return keys


def _backup_r2_prefix(s3, src_prefix: str, bak_prefix: str) -> int:
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
    except Exception as exc:  # noqa: BLE001 — audit must not mask the re-ingest
        print(f"WARN: ops.* write failed: {exc}")


def _detect_geometry_decode(con, read_glob: str) -> str:
    """Overture geometry is WKB BLOB in older releases, native GEOMETRY in newer ones.
    DESCRIBE the footer (LIMIT 0, no rows read) and pick the decode accordingly."""
    desc = con.execute(
        "DESCRIBE SELECT geometry FROM read_parquet(?) LIMIT 0", [read_glob]
    ).fetchall()
    col_type = (desc[0][1] if desc else "BLOB").upper()
    return "geometry" if "GEOMETRY" in col_type else "ST_GeomFromWKB(geometry)"


def _build_sql(geom_expr: str) -> str:
    """Overture source -> v3 schema. Anonymous read_parquet over the PINNED release;
    decode geometry once (geo CTE) + US filter pushed to scan; flatten ST_X/ST_Y +
    unpack addresses[1]/names/categories/taxonomy + normalize domain/phone/street
    (flat CTE); shared v3 projection. WKB never leaves the geo CTE. geom_expr is
    repo-controlled (probe output); only the read path binds positionally (one ?)."""
    return f"""
WITH raw AS (
    SELECT * FROM read_parquet(?)
),
geo AS (
    SELECT
        id,
        {geom_expr} AS geom,
        addresses,
        names,
        categories,
        taxonomy,
        websites,
        phones,
        confidence
    FROM raw
    WHERE addresses[1].country = 'US'
),
flat AS (
    SELECT
        nullif(trim(id), '')                     AS id,
        ST_X(geom)                               AS longitude,
        ST_Y(geom)                               AS latitude,
        nullif(trim(addresses[1].region), '')    AS region,
        nullif(trim(addresses[1].locality), '')  AS locality,
        nullif(trim(addresses[1].postcode), '')  AS postcode,
        nullif(trim(names.primary), '')          AS name,
        nullif(trim(categories.primary), '')     AS category,
        nullif(trim(taxonomy.primary), '')       AS taxonomy,
        TRY_CAST(confidence AS DOUBLE)           AS confidence,
        {domain_sql('websites[1]')}              AS domain,
        {phone_sql('phones[1]')}                 AS phone,
        nullif(trim(addresses[1].freeform), '')  AS street
    FROM geo
)
{projection_sql("flat")}
"""


# ── index build + verification ──────────────────────────────────────────────
def _build_indexes(ds) -> list[str]:
    built = []
    for col in OPTIMIZED_BTREE_INDEXES:
        ds.create_scalar_index(col, "BTREE", replace=True)
        built.append(f"BTREE:{col}")
        print(f"  BTREE  ok {col}")
    for col in OPTIMIZED_BITMAP_INDEXES:
        ds.create_scalar_index(col, "BITMAP", replace=True)
        built.append(f"BITMAP:{col}")
        print(f"  BITMAP ok {col}")
    return built


def _index_names(ds) -> set:
    out = set()
    for ix in ds.list_indices():
        cols = ix.get("fields") if isinstance(ix, dict) else getattr(ix, "fields", None)
        if cols:
            out.update(cols)
    return out


def _verify_local(local_path: str, expected_rows: int) -> dict:
    """HARD pre-publish gate. Raises on any failure -> SoR is never touched."""
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
        "taxonomy": "string", "confidence": "float",
        "domain": "string", "phone": "string", "street": "string",
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
    if meta.get("schema_version") != SCHEMA_VERSION:
        problems.append(f"schema_version {meta.get('schema_version')} != {SCHEMA_VERSION}")

    # coverage gate — the whole point of v3. Floors set ~3pp under measured
    # one-file coverage (domain 81.6 / phone 91.8 / street 96.6) for full-run slack.
    cov = ds.scanner(columns=["domain", "phone", "street"]).to_table()
    n = cov.num_rows or 1
    dom_pct = 100.0 * (n - cov.column("domain").null_count) / n
    pho_pct = 100.0 * (n - cov.column("phone").null_count) / n
    st_pct = 100.0 * (n - cov.column("street").null_count) / n
    if dom_pct < 78.0:
        problems.append(f"domain coverage {dom_pct:.2f}% < 78% floor")
    if pho_pct < 88.0:
        problems.append(f"phone coverage {pho_pct:.2f}% < 88% floor")
    if st_pct < 93.0:
        problems.append(f"street coverage {st_pct:.2f}% < 93% floor")

    # pushdown smoke tests on the two new resolution keys
    for col in ("domain", "phone"):
        real = ds.scanner(columns=[col], filter=f"{col} IS NOT NULL", limit=1).to_table()
        if real.num_rows == 0:
            problems.append(f"{col} has zero non-null values")
            continue
        val = real.column(0)[0].as_py().replace("'", "''")
        plan = ds.scanner(filter=f"{col} = '{val}'", columns=["id"]).explain_plan(True)
        if "ScalarIndexQuery" not in plan or f"{col}_idx" not in plan:
            problems.append(f"{col} '=' did not use ScalarIndexQuery@{col}_idx")

    if problems:
        raise RuntimeError("LOCAL VERIFY FAILED:\n  - " + "\n  - ".join(problems))
    return {"rows": rows, "fields": fields, "metadata": meta,
            "indexed_cols": sorted(idx_cols),
            "coverage": {"domain_pct": round(dom_pct, 2), "phone_pct": round(pho_pct, 2),
                         "street_pct": round(st_pct, 2)}}


def _transform_and_build(con_threads: int = 8) -> dict:
    """Read Overture source (pinned release) -> v3 transform (sorted) -> local Lance ->
    indices -> LOCAL verify. No R2 mutation. Returns build report."""
    import datetime as dt
    import shutil

    import duckdb
    import lance

    # Idempotency guard: a v3 SoR already carries the resolution keys + schema_version.
    so = _lance_storage_options()
    try:
        cur = lance.dataset(DATASET_URI, storage_options=so)
        field_names = {f.name for f in cur.schema}
        sv = (cur.schema.metadata or {}).get(b"schema_version", b"").decode()
        if {"domain", "phone"}.issubset(field_names) and sv == SCHEMA_VERSION:
            raise AlreadyV3(sv)
    except AlreadyV3:
        raise
    except Exception as exc:  # noqa: BLE001 — a missing/unreadable SoR is fine for a rebuild
        print(f"NOTE: could not open current SoR for idempotency check ({exc}); proceeding.")

    read_glob = OVERTURE_PLACES_GLOB.format(rel=PINNED_RELEASE)
    snapshot_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
    ingested_at = dt.datetime.now(dt.timezone.utc).isoformat()
    metadata = {
        "country": "US",
        "release_tag": PINNED_RELEASE,
        "snapshot_date": snapshot_date,
        "ingested_at": ingested_at,
        "schema_version": SCHEMA_VERSION,
        "sort_order": "region,hilbert",
        "hilbert_bounds": HILBERT_BOUNDS_TAG,
    }
    print(f"Re-ingest pinned release {PINNED_RELEASE}; reading {read_glob}")

    os.makedirs(SCRATCH_DIR, exist_ok=True)
    shutil.rmtree(LOCAL_OUT, ignore_errors=True)

    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads={con_threads};")
    con.execute("SET enable_progress_bar=false;")
    con.execute("SET preserve_insertion_order=false;")
    con.execute("SET memory_limit='24GB';")
    con.execute(f"SET temp_directory='{SCRATCH_DIR}/duckdb_spill';")
    con.execute("LOAD httpfs;")
    con.execute("LOAD spatial;")
    con.execute(f"SET s3_region='{OVERTURE_REGION}';")

    geom_expr = _detect_geometry_decode(con, read_glob)
    print(f"  geometry decode: ST_X/ST_Y({geom_expr})")
    sql = _build_sql(geom_expr)
    params = [read_glob]

    distinct_ids = None
    write_path = "materialize"
    try:
        table = con.execute(sql, params).to_arrow_table()
        table = table.replace_schema_metadata(
            {k.encode(): v.encode() for k, v in metadata.items()}
        )
        out_rows = table.num_rows
        con.register("proj", table)
        distinct_ids = con.execute("SELECT count(DISTINCT id) FROM proj").fetchone()[0]
        con.unregister("proj")
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
        con.execute("LOAD httpfs;")
        con.execute("LOAD spatial;")
        con.execute(f"SET s3_region='{OVERTURE_REGION}';")
        rdr = con.execute(sql, params).to_arrow_reader(STREAM_BATCH_ROWS)
        lance.write_dataset(
            rdr, LOCAL_OUT, schema=rdr.schema, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
        )
        lance.dataset(LOCAL_OUT).update_schema_metadata(dict(metadata))
    finally:
        con.close()

    out_rows = lance.dataset(LOCAL_OUT).count_rows()
    # Row-superset gate: pinned to the v2 release, v3 must reproduce the v2 row count.
    if out_rows != SRC_ROWS_EXPECTED:
        raise RuntimeError(
            f"row drift: v3 produced {out_rows} != v2 baseline {SRC_ROWS_EXPECTED} "
            f"for pinned release {PINNED_RELEASE}. Re-confirm the baseline before publishing."
        )

    ds_out = lance.dataset(LOCAL_OUT)
    built = _build_indexes(ds_out)
    report = _verify_local(LOCAL_OUT, SRC_ROWS_EXPECTED)
    report.update({"built": built, "write_path": write_path,
                   "release_tag": PINNED_RELEASE, "snapshot_date": snapshot_date,
                   "distinct_ids": distinct_ids, "src_rows": out_rows})
    print(f"LOCAL build+verify OK: {report}")
    return report


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120, memory=32768, cpu=8.0, ephemeral_disk=524288,
)
def reingest_overture_places_v3(apply: bool = False) -> dict:
    """dryrun (apply=False): build+verify LOCAL only, NO mutation.
    apply=True: + R2 backup of current v2 -> publish -> post-publish verify
    (restore-on-fail) -> ledger."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    try:
        report = _transform_and_build()
    except AlreadyV3 as exc:
        msg = f"SoR is already {exc}; re-ingest is a no-op (nothing to do)."
        print(msg)
        return {"mode": "noop", "already_v3": True, "schema_version": str(exc),
                "mutated": False, "note": msg}

    if not apply:
        return {"mode": "dryrun", "mutated": False, **report}

    s3 = _s3_client()
    ts = started_at.strftime("%Y%m%dT%H%M%SZ")
    bak_prefix = f"active/overture_places__bak_v3_{report['release_tag']}_{ts}/"
    status, error = "error", None
    published_files = published_bytes = 0
    try:
        n_bak = _backup_r2_prefix(s3, DATASET_PREFIX, bak_prefix)
        print(f"Backed up {n_bak} objects (current v2) -> s3://{BUCKET}/{bak_prefix}")

        _wipe_prefix(s3, DATASET_PREFIX)
        published_files, published_bytes = _upload_dir(s3, DATASET_PREFIX, LOCAL_OUT)
        print(f"Published {published_files} files ({published_bytes:,} B) -> {DATASET_URI}")

        # post-publish verify against R2; restore on any failure
        pub = lance.dataset(DATASET_URI, storage_options=_lance_storage_options())
        pub_rows = pub.count_rows()
        pub_idx = _index_names(pub)
        rd = pub.scanner(columns=["domain"], filter="domain IS NOT NULL", limit=1).to_table()
        dom_val = rd.column(0)[0].as_py().replace("'", "''") if rd.num_rows else None
        dom_hits = (pub.scanner(filter=f"domain = '{dom_val}'", columns=["id"]).to_table().num_rows
                    if dom_val else 0)
        ok = (pub_rows == report["src_rows"]
              and set(OPTIMIZED_BTREE_INDEXES + OPTIMIZED_BITMAP_INDEXES).issubset(pub_idx)
              and dom_val is not None and dom_hits > 0)
        if not ok:
            raise RuntimeError(
                f"POST-PUBLISH VERIFY FAILED: rows={pub_rows} idx={sorted(pub_idx)} "
                f"domain={dom_val!r} domain_hits={dom_hits}"
            )
        status = "success"
        print(f"Post-publish verify OK: rows={pub_rows:,} domain_resolves={dom_hits} idx={sorted(pub_idx)}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"FAILURE: {error} — attempting rollback from {bak_prefix}")
        try:
            n_res = _restore_r2_prefix(s3, bak_prefix, DATASET_PREFIX)
            print(f"ROLLBACK: restored {n_res} objects; SoR returned to the pre-v3 (v2) state.")
        except Exception as rexc:  # noqa: BLE001
            print(f"CRITICAL: rollback FAILED: {rexc}. Backup intact at s3://{BUCKET}/{bak_prefix}")
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(DATASET_URI, report["release_tag"], report["snapshot_date"],
                    int(report["src_rows"]), report.get("distinct_ids"),
                    published_files, published_bytes, "reingest_v3", status, error,
                    started_at, completed_at)

    return {"mode": "apply", "mutated": True, "backup_prefix": bak_prefix,
            "published_files": published_files, "published_bytes": published_bytes, **report}


@app.local_entrypoint()
def dryrun() -> None:
    import json
    print(json.dumps(reingest_overture_places_v3.remote(apply=False), indent=2, default=str))


@app.local_entrypoint()
def apply() -> None:
    import json
    print(json.dumps(reingest_overture_places_v3.remote(apply=True), indent=2, default=str))
