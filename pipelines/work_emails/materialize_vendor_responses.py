"""Compute worker — Gen-3 materialization of the work-email PROVIDER-RESPONSE payloads.

The finding-provider raw payloads, copied out of Postgres VERBATIM, one row per contact
(1:1 with ops.email_resolutions). A contact carries whichever provider raws were attempted
as separate columns — icypeas_raw / leadmagic_raw / blitz_email_raw — exactly as stored
upstream. Supplied emails (no finder) never reach ops.email_resolutions, so they simply
do not appear here. No reshaping: the Postgres row IS the Lance row.

SOURCE (live hq-x Postgres, read-only via the DuckDB postgres scanner):
    ops.email_resolutions  (one row per contact_id; provider raws as jsonb columns)
    DSN is HQX_DB_URL_POOLED; DuckDB ATTACHes READ_ONLY. Run-state → ops.work_emails_runs
    (feed='work_email_vendor_responses').

RAW PRESERVATION (the hard constraint). icypeas_raw / leadmagic_raw / blitz_email_raw are
copied via CAST(<col> AS VARCHAR) — the WHOLE jsonb blob as lossless JSON text, never
re-parsed, re-emitted, normalized, or split. The thin typed columns (source_vendor /
source_tier / certainty) are the already-derived ops.* columns carried alongside, never a
replacement for the raw.

TARGET (Gen-3 system of record — native Lance v2.1):
    s3://data-sink/active/work_email_vendor_responses/

GRAIN / PK. contact_id (1:1 with the SoR).

REFRESH. ``run`` = full-snapshot overwrite (captures inserts + re-resolution updates).
``append`` = watermark on resolved_at; merge_insert on contact_id (update + insert).

INDEXES:
    BTREE  : contact_id (PK)
    BITMAP : source_vendor   (icypeas | leadmagic | blitz)

    modal run    pipelines/work_emails/materialize_vendor_responses.py::init_ops
    modal run    pipelines/work_emails/materialize_vendor_responses.py::run           # full overwrite
    modal run    pipelines/work_emails/materialize_vendor_responses.py::append_only   # watermark append
    modal run    pipelines/work_emails/materialize_vendor_responses.py::reindex_only
    modal run    pipelines/work_emails/materialize_vendor_responses.py::verify_only
    modal deploy pipelines/work_emails/materialize_vendor_responses.py
"""

from __future__ import annotations

import os

import modal

_ACTIVE = "s3://data-sink/active"
DATASET = "work_email_vendor_responses"
DATASET_URI = os.environ.get("WORK_EMAIL_VENDOR_RESPONSES_URI", f"{_ACTIVE}/{DATASET}/")

FEED = "work_email_vendor_responses"
SOURCE_DB = "hqx:ops.email_resolutions"  # provenance label recorded in ops.*

MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
READ_BATCH_ROWS = 25000  # rows carry verbatim provider blobs → smaller batches

INDEXES: dict[str, list[str]] = {
    "BTREE": ["contact_id", "person_id"],
    "BITMAP": ["source_vendor"],
}

# Straight 1:1 projection of ops.email_resolutions. Raw provider columns → JSON text (lossless).
# person_id mirrors contact_id (same value); it has no source column, so _sql aliases it.
_COLS = [
    "contact_id", "person_id", "email", "source_vendor", "source_tier", "certainty",
    "company_domain", "person_linkedin_url",
    "icypeas_raw", "leadmagic_raw", "blitz_email_raw",
    "batch_label", "resolved_at",
]
_NOT_NULL = {"contact_id", "person_id", "resolved_at"}
_INT_COLS = {"source_tier"}
_JSON_COLS = {"icypeas_raw", "leadmagic_raw", "blitz_email_raw"}  # whole-blob VERBATIM via CAST AS VARCHAR
_ALIAS_COLS = {"person_id": "contact_id"}  # projected as <source> AS <col> (no own source column)

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.work_emails_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,
    source_db      text        NOT NULL,
    datasets       jsonb       NOT NULL,
    mode           text        NOT NULL DEFAULT 'overwrite',
    rows_total     bigint      NOT NULL DEFAULT 0,
    rows_source    bigint      NOT NULL DEFAULT 0,
    rows_added     bigint      NOT NULL DEFAULT 0,
    watermark      timestamptz,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS work_emails_runs_feed_idx        ON ops.work_emails_runs (feed);
CREATE INDEX IF NOT EXISTS work_emails_runs_status_idx      ON ops.work_emails_runs (status);
CREATE INDEX IF NOT EXISTS work_emails_runs_recorded_at_idx ON ops.work_emails_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "psycopg[binary]>=3.2",
    "requests>=2.32",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("work-email-vendor-responses-materializer", image=image)


def _schema():
    import pyarrow as pa

    def f(name, typ):
        return pa.field(name, typ, nullable=name not in _NOT_NULL)

    ts = pa.timestamp("us", tz="UTC")
    cols = []
    for name in _COLS:
        if name == "resolved_at":
            cols.append(f(name, ts))
        elif name in _INT_COLS:
            cols.append(f(name, pa.int64()))
        else:
            cols.append(f(name, pa.string()))  # incl. the three *_raw blobs (jsonb → JSON string, lossless)
    cols.append(pa.field("materialized_at", ts, nullable=False))  # lineage
    return pa.schema(cols)


def _sql(where: str = "") -> str:
    """Straight 1:1 projection. The three provider raw jsonb columns → VARCHAR (lossless JSON
    text, whole blob, never split). Alias columns (person_id) project <source> AS <col>.
    Column order matches _schema()."""
    def _p(c: str) -> str:
        if c in _ALIAS_COLS:
            return f"{_ALIAS_COLS[c]} AS {c}"
        return f"CAST({c} AS VARCHAR) AS {c}" if c in _JSON_COLS else c

    proj = ",\n        ".join(_p(c) for c in _COLS)
    return f"""
        SELECT
        {proj},
        now() AS materialized_at
        FROM hqx.ops.email_resolutions
        {where}
    """


# ── DSN + R2 plumbing (fleet-standard; mirrors materialize_email_verifications.py) ─
def _hqx_dsn() -> str:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres Modal secret.")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the r2-credentials secret.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _attach(con, dsn: str) -> None:
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute("INSTALL json; LOAD json;")
    con.execute("PRAGMA threads=4;")
    con.execute(f"ATTACH '{dsn.replace(chr(39), chr(39) * 2)}' AS hqx (TYPE postgres, READ_ONLY);")


def _casted_reader(con, schema, where: str = ""):
    import pyarrow as pa

    rbr = con.sql(_sql(where)).fetch_record_batch(READ_BATCH_ROWS)

    def _gen():
        for batch in rbr:
            yield pa.Table.from_batches([batch]).cast(schema).to_batches()[0]

    return pa.RecordBatchReader.from_batches(schema, _gen())


def _create_indexes(so: dict) -> list[dict]:
    import lance

    ds = lance.dataset(DATASET_URI, storage_options=so)
    out: list[dict] = []
    for index_type, cols in INDEXES.items():
        for col in cols:
            try:
                ds.create_scalar_index(col, index_type=index_type)
                print(f"  {index_type:<6} ✓ {DATASET}.{col}")
                out.append({"col": col, "type": index_type, "ok": True})
            except Exception as exc:  # noqa: BLE001
                print(f"  {index_type:<6} ✗ {DATASET}.{col}: {exc}")
                out.append({"col": col, "type": index_type, "ok": False, "error": str(exc)})
    return out


def _committed_index_names(so: dict) -> list[str]:
    import lance

    ds = lance.dataset(DATASET_URI, storage_options=so)
    names = []
    for ix in ds.list_indices():
        names.append(ix.get("name", str(ix)) if isinstance(ix, dict) else getattr(ix, "name", str(ix)))
    return sorted(names)


def _record_run(mode, rows_total, rows_source, rows_added, watermark, status, error,
                started_at, completed_at) -> None:
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
                INSERT INTO ops.work_emails_runs
                    (feed, source_db, datasets, mode, rows_total, rows_source, rows_added,
                     watermark, status, error, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (FEED, SOURCE_DB, Jsonb({DATASET: rows_total}), mode, rows_total, rows_source,
                 rows_added, watermark, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* state write failed: {exc}")


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


_SECRETS = [modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")]


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=8192, cpu=2.0)
def ingest_vendor_responses(trigger_callback_url: str | None = None) -> dict:
    """Full-snapshot overwrite: ATTACH READ_ONLY, project ops.email_resolutions 1:1 → Arrow
    (cast) → Lance overwrite → indexes → ops.* + callback."""
    import datetime as dt

    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    rows_total = rows_source = 0
    status, error = "error", None
    try:
        so = _r2_storage_options()
        con = duckdb.connect(":memory:")
        try:
            _attach(con, _hqx_dsn())
            rows_source = con.sql("SELECT count(*) FROM hqx.ops.email_resolutions").fetchone()[0]
            print(f"source rows: {rows_source:,}")
            reader = _casted_reader(con, _schema())
            lance.write_dataset(
                reader, DATASET_URI, mode="overwrite",
                data_storage_version=DATA_STORAGE_VERSION,
                max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                storage_options=so,
            )
            rows_total = lance.dataset(DATASET_URI, storage_options=so).count_rows()
            print(f"wrote Lance (overwrite, v{DATA_STORAGE_VERSION}) — {rows_total:,} rows")
            _create_indexes(so)
        finally:
            con.close()
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("overwrite", rows_total, rows_source, rows_total, None, status, error,
                    started_at, completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "feed": FEED, "source_db": SOURCE_DB, "mode": "overwrite",
                        "rows_total": rows_total, "rows_source": rows_source,
                        "datasets": {DATASET: rows_total}})
    if status != "success":
        raise RuntimeError(f"work_email_vendor_responses materialization failed: {error}")
    return {"feed": FEED, "mode": "overwrite", "rows_total": rows_total,
            "rows_source": rows_source, "status": status}


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=8192, cpu=2.0)
def append_vendor_responses(trigger_callback_url: str | None = None) -> dict:
    """Incremental: watermark = max(resolved_at) already in Lance; pull rows strictly newer;
    merge_insert on contact_id (update + insert) — one atomic Lance commit, no standalone delete."""
    import datetime as dt

    import duckdb
    import lance
    import pyarrow.compute as pc

    started_at = dt.datetime.now(dt.timezone.utc)
    rows_total = rows_source = rows_added = 0
    watermark = None
    status, error = "error", None
    try:
        so = _r2_storage_options()
        ds = lance.dataset(DATASET_URI, storage_options=so)
        before = ds.count_rows()
        wm = pc.max(ds.to_table(columns=["resolved_at"]).column("resolved_at")).as_py()
        watermark = wm
        if wm is None:
            raise RuntimeError("existing dataset has no resolved_at watermark; run a full overwrite first.")
        where = f"WHERE resolved_at > TIMESTAMPTZ '{wm.isoformat()}'"
        con = duckdb.connect(":memory:")
        try:
            _attach(con, _hqx_dsn())
            rows_source = con.sql(
                f"SELECT count(*) FROM hqx.ops.email_resolutions {where}").fetchone()[0]
            print(f"new/updated rows since {wm.isoformat()}: {rows_source:,}")
            if rows_source:
                new_tbl = con.sql(_sql(where)).to_arrow_table().cast(_schema())
                (ds.merge_insert("contact_id")
                   .when_matched_update_all()
                   .when_not_matched_insert_all()
                   .execute(new_tbl))
        finally:
            con.close()
        rows_total = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        rows_added = rows_total - before
        print(f"merged {rows_source:,} (net new {rows_added:,}) → {rows_total:,} rows")
        if rows_source:
            _create_indexes(so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("append", rows_total, rows_source, rows_added, watermark, status, error,
                    started_at, completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "feed": FEED, "source_db": SOURCE_DB, "mode": "append",
                        "rows_total": rows_total, "rows_added": rows_added})
    if status != "success":
        raise RuntimeError(f"work_email_vendor_responses append failed: {error}")
    return {"feed": FEED, "mode": "append", "rows_added": rows_added, "rows_total": rows_total,
            "rows_source": rows_source, "status": status}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15, memory=8192, cpu=4.0)
def reindex() -> dict:
    so = _r2_storage_options()
    print(f"=== reindex {DATASET} ===")
    _create_indexes(so)
    return {DATASET: _committed_index_names(so)}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10, memory=8192)
def verify() -> dict:
    """Read-back: row count, contact_id uniqueness invariant, schema, indexes, BTREE probe."""
    import pyarrow.compute as pc

    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()
    keys = ds.to_table(columns=["contact_id"])
    distinct_key = pc.count_distinct(keys.column("contact_id")).as_py()
    unique_ok = (n == distinct_key)

    sample = next((v for v in keys.column("contact_id").to_pylist() if v), None)
    probe = ds.scanner(columns=["contact_id"],
                       filter=f"contact_id = '{sample}'").to_table().num_rows if sample else -1
    out = {
        "uri": DATASET_URI, "rows": n, "distinct_contact_id": distinct_key,
        "unique_invariant_ok": unique_ok, "schema": [f.name for f in ds.schema],
        "indexes": _committed_index_names(so), f"probe_contact_id={sample!r}": probe,
    }
    print(f"{DATASET}: {n:,} rows · distinct(contact_id)={distinct_key:,} · unique_ok={unique_ok}")
    print(f"  indexes={out['indexes']}")
    if not unique_ok:
        raise RuntimeError(f"uniqueness invariant FAILED: rows={n} != distinct(contact_id)={distinct_key}")
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name='work_emails_runs' ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    print(f"ops.work_emails_runs ready — columns: {cols}")
    return {"table": "ops.work_emails_runs", "columns": cols}


@app.local_entrypoint()
def init_ops() -> None:
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def run() -> None:
    import json
    print(json.dumps(ingest_vendor_responses.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def append_only() -> None:
    import json
    print(json.dumps(append_vendor_responses.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def reindex_only() -> None:
    import json
    print(json.dumps(reindex.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_only() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
