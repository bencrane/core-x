"""Compute worker — Gen-3 materialization of the MillionVerifier VALIDATION payloads.

MillionVerifier is the universal validator — it runs on EVERY work email. This table holds
its verdict per contact, with the full MV payload copied out of Postgres VERBATIM, one row
per contact (1:1, latest-wins). ``mv_raw`` is the WHOLE MillionVerifier response array (the
1- or 2-element list, the unknown-retry call included) stored intact as one column — readers
parse it if they want the individual responses. No reshaping: the Postgres row IS the Lance row.

SOURCE (live hq-x Postgres, read-only via the DuckDB postgres scanner) — mv_raw from BOTH
work-email SoRs, deduplicated MOST-RECENT-WINS per contact_id:
    ops.email_resolutions   (source_table='email_resolutions')   — provider-found + MV
    ops.email_verifications (source_table='email_verifications')  — supplied + MV
    DSN is HQX_DB_URL_POOLED; DuckDB ATTACHes READ_ONLY. Run-state → ops.work_emails_runs
    (feed='work_email_mv_validations').

RAW PRESERVATION (the hard constraint). ``mv_raw`` is copied via CAST(mv_raw AS VARCHAR) —
the whole jsonb array as lossless JSON text, never re-parsed, re-emitted, normalized, or
split element-by-element. The thin pulled-out columns (mv_resultcode/result/quality/subresult)
are the already-derived ops.* columns carried alongside, never a replacement for the raw.

TARGET (Gen-3 system of record — native Lance v2.1):
    s3://data-sink/active/work_email_mv_validations/

GRAIN / PK. contact_id (1:1, latest-wins across both SoRs).

REFRESH. ``run`` = full-snapshot overwrite (inserts + re-verification updates). ``append`` =
watermark on resolved_at; merge_insert on contact_id (update + insert).

INDEXES:
    BTREE  : contact_id (PK), email
    BITMAP : verification_status, mv_resultcode, source_table

    modal run    pipelines/work_emails/materialize_mv_validations.py::init_ops
    modal run    pipelines/work_emails/materialize_mv_validations.py::run           # full overwrite
    modal run    pipelines/work_emails/materialize_mv_validations.py::append_only   # watermark append
    modal run    pipelines/work_emails/materialize_mv_validations.py::reindex_only
    modal run    pipelines/work_emails/materialize_mv_validations.py::verify_only
    modal deploy pipelines/work_emails/materialize_mv_validations.py
"""

from __future__ import annotations

import os

import modal

_ACTIVE = "s3://data-sink/active"
DATASET = "work_email_mv_validations"
DATASET_URI = os.environ.get("WORK_EMAIL_MV_VALIDATIONS_URI", f"{_ACTIVE}/{DATASET}/")

FEED = "work_email_mv_validations"
SOURCE_DB = "hqx:ops.email_resolutions+email_verifications"  # provenance label recorded in ops.*

MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
READ_BATCH_ROWS = 50000

INDEXES: dict[str, list[str]] = {
    "BTREE": ["contact_id", "person_id", "email"],
    "BITMAP": ["verification_status", "mv_resultcode", "source_table"],
}

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

app = modal.App("work-email-mv-validations-materializer", image=image)


def _schema():
    import pyarrow as pa

    ts = pa.timestamp("us", tz="UTC")
    return pa.schema([
        pa.field("contact_id",          pa.string(), nullable=False),  # PK · BTREE
        pa.field("person_id",           pa.string(), nullable=False),  # == contact_id · BTREE
        pa.field("email",               pa.string(), nullable=True),   # validated email (verbatim) · BTREE
        pa.field("verification_status", pa.string(), nullable=True),   # BITMAP
        pa.field("mv_resultcode",       pa.int64(),  nullable=True),   # BITMAP · derived pull-out
        pa.field("mv_result",           pa.string(), nullable=True),
        pa.field("mv_quality",          pa.string(), nullable=True),
        pa.field("mv_subresult",        pa.string(), nullable=True),
        pa.field("mv_raw",              pa.string(), nullable=True),   # WHOLE MV response array, VERBATIM (JSON text)
        pa.field("source_table",        pa.string(), nullable=False),  # BITMAP · email_resolutions|email_verifications
        pa.field("company_domain",      pa.string(), nullable=True),
        pa.field("batch_label",         pa.string(), nullable=True),
        pa.field("resolved_at",         ts,          nullable=False),  # watermark
        pa.field("materialized_at",     ts,          nullable=False),  # lineage
    ])


def _sql(where: str = "") -> str:
    """UNION mv_raw from both SoRs → most-recent-wins per contact_id. mv_raw → VARCHAR (whole
    blob, lossless, never split). ``where`` (append watermark) injected into BOTH arms.
    Column order matches _schema()."""
    return f"""
    WITH unified AS (
        SELECT
            contact_id, email, verification_status,
            mv_resultcode, mv_result, mv_quality, mv_subresult,
            CAST(mv_raw AS VARCHAR) AS mv_raw,
            'email_resolutions' AS source_table,
            company_domain, batch_label, resolved_at
        FROM hqx.ops.email_resolutions
        {where}
        UNION ALL
        SELECT
            contact_id, email, verification_status,
            mv_resultcode, mv_result, mv_quality, mv_subresult,
            CAST(mv_raw AS VARCHAR) AS mv_raw,
            'email_verifications' AS source_table,
            company_domain, batch_label, resolved_at
        FROM hqx.ops.email_verifications
        {where}
    ),
    ranked AS (
        SELECT *, row_number() OVER (
            PARTITION BY contact_id ORDER BY resolved_at DESC NULLS LAST
        ) AS rn
        FROM unified
    )
    SELECT
        contact_id, contact_id AS person_id, email, verification_status,
        mv_resultcode, mv_result, mv_quality, mv_subresult,
        mv_raw, source_table, company_domain, batch_label, resolved_at,
        now() AS materialized_at
    FROM ranked
    WHERE rn = 1
    """


def _count_sql(where: str = "") -> str:
    return f"""
    SELECT count(DISTINCT contact_id) FROM (
        SELECT contact_id FROM hqx.ops.email_resolutions {where}
        UNION ALL
        SELECT contact_id FROM hqx.ops.email_verifications {where}
    )
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
def ingest_mv_validations(trigger_callback_url: str | None = None) -> dict:
    """Full-snapshot overwrite: ATTACH READ_ONLY, UNION both SoRs (most-recent-wins per contact_id),
    mv_raw whole-blob → Arrow (cast) → Lance overwrite → indexes → ops.* + callback."""
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
            rows_source = con.sql(_count_sql()).fetchone()[0]
            print(f"source distinct contacts: {rows_source:,}")
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
        raise RuntimeError(f"work_email_mv_validations materialization failed: {error}")
    return {"feed": FEED, "mode": "overwrite", "rows_total": rows_total,
            "rows_source": rows_source, "status": status}


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=8192, cpu=2.0)
def append_mv_validations(trigger_callback_url: str | None = None) -> dict:
    """Incremental: watermark = max(resolved_at) already in Lance; pull rows strictly newer from
    BOTH arms; merge_insert on contact_id (update + insert) — one atomic Lance commit."""
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
            rows_source = con.sql(_count_sql(where)).fetchone()[0]
            print(f"new/updated contacts since {wm.isoformat()}: {rows_source:,}")
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
        raise RuntimeError(f"work_email_mv_validations append failed: {error}")
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
    print(json.dumps(ingest_mv_validations.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def append_only() -> None:
    import json
    print(json.dumps(append_mv_validations.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def reindex_only() -> None:
    import json
    print(json.dumps(reindex.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_only() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
