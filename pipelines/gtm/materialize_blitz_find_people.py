"""Materialize ops.blitz_find_people (HQX Postgres) → native Gen-3 Lance SoR.

Sibling of ``materialize_clay_find_people.py`` — the Blitz Find People parallel to Clay Find
People. Faithful, lossless 1:1 projection: ATTACH hq-x Postgres READ_ONLY, stream
``ops.blitz_find_people`` → Arrow (cast to contract) → Lance. NO values are altered or
interpreted — ``raw_payload`` (the verbatim Blitz person object) rides through as lossless JSON
text. Overwrite CREATES the dataset; thereafter ``append_only`` merge_inserts on ``record_id``
so existing rows are never overwritten — only net-new (person × company) records land.

SOURCE  ops.blitz_find_people  (record_id = md5(person_linkedin_norm | company_domain), PK).
TARGET  s3://data-sink/active/blitz_find_people/  (native Lance v2.1).
    BTREE  : record_id (PK), person_linkedin_norm (join → people/phone), company_domain (bridge → companies)
    BITMAP : loc_country_iso

    modal deploy pipelines/gtm/materialize_blitz_find_people.py
    modal run    pipelines/gtm/materialize_blitz_find_people.py::run           # full overwrite (creates)
    modal run    pipelines/gtm/materialize_blitz_find_people.py::append_only   # incremental watermark append
    modal run    pipelines/gtm/materialize_blitz_find_people.py::reindex_only
    modal run    pipelines/gtm/materialize_blitz_find_people.py::verify_only
"""
from __future__ import annotations

import os

import modal

_ACTIVE = "s3://data-sink/active"
DATASET = "blitz_find_people"
DATASET_URI = os.environ.get("BLITZ_FIND_PEOPLE_URI", f"{_ACTIVE}/{DATASET}/")
FEED = "blitz_find_people"
SOURCE_DB = "hqx:ops.blitz_find_people"

MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
READ_BATCH_ROWS = 50000

INDEXES: dict[str, list[str]] = {
    "BTREE": ["record_id", "person_linkedin_norm", "company_domain"],
    "BITMAP": ["loc_country_iso"],
}

# 1:1 projection of ops.blitz_find_people (column order verbatim).
_COLS = [
    "record_id", "person_linkedin_url", "person_linkedin_norm", "company_domain",
    "company_linkedin_url", "first_name", "last_name", "full_name", "headline",
    "loc_city", "loc_state", "loc_country_iso", "loc_continent", "raw_payload",
    "source", "batch_label", "landed_at",
]
_NOT_NULL = {"record_id", "person_linkedin_url", "person_linkedin_norm", "raw_payload", "source", "landed_at"}

# Dedicated materialization ledger — DISTINCT from ops.blitz_find_people_runs (owned by the
# find-people worker), to avoid a schema clash on a shared table name.
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.blitz_find_people_mat_runs (
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
CREATE INDEX IF NOT EXISTS blitz_find_people_mat_runs_feed_idx        ON ops.blitz_find_people_mat_runs (feed);
CREATE INDEX IF NOT EXISTS blitz_find_people_mat_runs_recorded_at_idx ON ops.blitz_find_people_mat_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
    "psycopg[binary]>=3.2", "requests>=2.32",
).env({"LANCE_BYPASS_SPILLING": "true"})
app = modal.App("blitz-find-people-materialize", image=image)
_SECRETS = [modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")]


def _schema():
    import pyarrow as pa
    ts = pa.timestamp("us", tz="UTC")
    def f(name, typ): return pa.field(name, typ, nullable=name not in _NOT_NULL)
    cols = [f(c, ts) if c == "landed_at" else f(c, pa.string()) for c in _COLS]
    cols.append(pa.field("materialized_at", ts, nullable=False))
    return pa.schema(cols)


def _sql(where: str = "") -> str:
    proj = ",\n        ".join(
        "CAST(raw_payload AS VARCHAR) AS raw_payload" if c == "raw_payload" else c for c in _COLS)
    return f"SELECT\n        {proj},\n        now() AS materialized_at\n        FROM hqx.ops.blitz_find_people\n        {where}"


def _hqx_dsn() -> str:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres Modal secret.")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT"); account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id: endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint: raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _attach(con, dsn: str) -> None:
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute("INSTALL json; LOAD json;")
    con.execute("PRAGMA threads=4;")
    con.execute(f"ATTACH '{dsn.replace(chr(39), chr(39)*2)}' AS hqx (TYPE postgres, READ_ONLY);")


def _casted_reader(con, schema, where: str = ""):
    import pyarrow as pa
    rbr = con.sql(_sql(where)).fetch_record_batch(READ_BATCH_ROWS)
    def _gen():
        for batch in rbr:
            yield pa.Table.from_batches([batch]).cast(schema).to_batches()[0]
    return pa.RecordBatchReader.from_batches(schema, _gen())


def _create_indexes(so: dict) -> list[dict]:
    import lance
    ds = lance.dataset(DATASET_URI, storage_options=so); out = []
    for index_type, cols in INDEXES.items():
        for col in cols:
            try:
                ds.create_scalar_index(col, index_type=index_type)
                print(f"  {index_type:<6} ✓ {DATASET}.{col}"); out.append({"col": col, "type": index_type, "ok": True})
            except Exception as exc:  # noqa: BLE001
                print(f"  {index_type:<6} ✗ {DATASET}.{col}: {exc}"); out.append({"col": col, "type": index_type, "ok": False, "error": str(exc)})
    return out


def _committed_index_names(so: dict) -> list[str]:
    import lance
    ds = lance.dataset(DATASET_URI, storage_options=so)
    return sorted(ix.get("name", str(ix)) if isinstance(ix, dict) else getattr(ix, "name", str(ix)) for ix in ds.list_indices())


def _record_run(mode, rows_total, rows_source, rows_added, watermark, status, error, started_at, completed_at) -> None:
    import psycopg
    from psycopg.types.json import Jsonb
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn: print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write."); return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute("""INSERT INTO ops.blitz_find_people_mat_runs
                (feed, source_db, datasets, mode, rows_total, rows_source, rows_added, watermark, status, error, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (FEED, SOURCE_DB, Jsonb({DATASET: rows_total}), mode, rows_total, rows_source, rows_added, watermark, status, error, started_at, completed_at))
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    if not url: print("No trigger_callback_url (manual run); skipping callback."); return
    import time, requests
    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300: print(f"Callback delivered: {payload}"); return
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i+1} failed: {exc}")
        time.sleep(2 * (i + 1))


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=16384, cpu=4.0)
def ingest_blitz_find_people(trigger_callback_url: str | None = None) -> dict:
    """Full-snapshot overwrite: ops.blitz_find_people → Lance overwrite → indexes → ledger."""
    import datetime as dt
    import duckdb, lance
    started_at = dt.datetime.now(dt.timezone.utc); rows_total = rows_source = 0; status, error = "error", None
    try:
        so = _r2_storage_options(); con = duckdb.connect(":memory:")
        try:
            _attach(con, _hqx_dsn())
            rows_source = con.sql("SELECT count(*) FROM hqx.ops.blitz_find_people").fetchone()[0]
            print(f"source rows: {rows_source:,}")
            reader = _casted_reader(con, _schema())
            lance.write_dataset(reader, DATASET_URI, mode="overwrite", data_storage_version=DATA_STORAGE_VERSION,
                                max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE, storage_options=so)
            rows_total = lance.dataset(DATASET_URI, storage_options=so).count_rows()
            print(f"wrote Lance (overwrite, v{DATA_STORAGE_VERSION}) — {rows_total:,} rows")
            _create_indexes(so)
        finally:
            con.close()
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc); status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("overwrite", rows_total, rows_source, rows_total, None, status, error, started_at, completed_at)
        _post_callback(trigger_callback_url, {"status": status, "feed": FEED, "mode": "overwrite", "rows_total": rows_total, "rows_source": rows_source})
    if status != "success": raise RuntimeError(f"blitz_find_people materialization failed: {error}")
    return {"feed": FEED, "mode": "overwrite", "rows_total": rows_total, "rows_source": rows_source, "status": status}


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=16384, cpu=4.0)
def append_blitz_find_people(trigger_callback_url: str | None = None) -> dict:
    """Incremental: watermark = max(landed_at) in Lance; pull newer; merge_insert on record_id
    (existing rows never overwritten — only net-new record_ids land)."""
    import datetime as dt
    import duckdb, lance
    import pyarrow.compute as pc
    started_at = dt.datetime.now(dt.timezone.utc); rows_total = rows_source = rows_added = 0; watermark = None; status, error = "error", None
    try:
        so = _r2_storage_options(); ds = lance.dataset(DATASET_URI, storage_options=so); before = ds.count_rows()
        wm = pc.max(ds.to_table(columns=["landed_at"]).column("landed_at")).as_py(); watermark = wm
        if wm is None: raise RuntimeError("existing dataset has no landed_at watermark; run a full overwrite first.")
        where = f"WHERE landed_at > TIMESTAMPTZ '{wm.isoformat()}'"
        con = duckdb.connect(":memory:")
        try:
            _attach(con, _hqx_dsn())
            rows_source = con.sql(f"SELECT count(*) FROM hqx.ops.blitz_find_people {where}").fetchone()[0]
            print(f"new rows since {wm.isoformat()}: {rows_source:,}")
            if rows_source:
                new_tbl = con.sql(_sql(where)).to_arrow_table().cast(_schema())
                ds.merge_insert("record_id").when_not_matched_insert_all().execute(new_tbl)
        finally:
            con.close()
        rows_total = lance.dataset(DATASET_URI, storage_options=so).count_rows(); rows_added = rows_total - before
        print(f"appended {rows_added:,} → {rows_total:,} rows")
        if rows_added: _create_indexes(so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc); status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run("append", rows_total, rows_source, rows_added, watermark, status, error, started_at, completed_at)
        _post_callback(trigger_callback_url, {"status": status, "feed": FEED, "mode": "append", "rows_total": rows_total, "rows_added": rows_added})
    if status != "success": raise RuntimeError(f"blitz_find_people append failed: {error}")
    return {"feed": FEED, "mode": "append", "rows_added": rows_added, "rows_total": rows_total, "rows_source": rows_source, "status": status}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15, memory=8192, cpu=4.0)
def reindex() -> dict:
    so = _r2_storage_options(); print(f"=== reindex {DATASET} ==="); _create_indexes(so)
    return {DATASET: _committed_index_names(so)}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10, memory=8192)
def verify() -> dict:
    import pyarrow.compute as pc
    import lance
    so = _r2_storage_options(); ds = lance.dataset(DATASET_URI, storage_options=so); n = ds.count_rows()
    keys = ds.to_table(columns=["record_id"]); distinct = pc.count_distinct(keys.column("record_id")).as_py()
    unique_ok = (n == distinct)
    out = {"uri": DATASET_URI, "rows": n, "distinct_record_id": distinct, "unique_invariant_ok": unique_ok,
           "schema": [f.name for f in ds.schema], "indexes": _committed_index_names(so)}
    print(f"{DATASET}: {n:,} rows · distinct(record_id)={distinct:,} · unique_ok={unique_ok}")
    print(f"  indexes={out['indexes']}")
    if not unique_ok: raise RuntimeError(f"uniqueness invariant FAILED: rows={n} != distinct(record_id)={distinct}")
    return out


@app.local_entrypoint()
def run() -> None:
    import json
    print(json.dumps(ingest_blitz_find_people.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def append_only() -> None:
    import json
    print(json.dumps(append_blitz_find_people.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def reindex_only() -> None:
    import json
    print(json.dumps(reindex.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_only() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
