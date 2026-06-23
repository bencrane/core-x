"""Compute worker — Gen-3 materialization of the Clay Enrich Company raw landing grain.

Sibling of ``materialize_clay_find_companies.py``. Part of the ``clay-enrich-companies`` Modal app.
Endpoint-less; spawned by the Universal Dispatcher (core/modal_dispatcher.py) or run manually.
Clean-room data plane: DuckDB does 100% of the read/derive, Lance is written straight to R2.
No catalog round-trip.

WHY THIS WORKER EXISTS
    Clay *Enrich Company* dossier records land append-only in ``gtm.clay_enrich_companies`` (HQX
    Postgres) via the edge_api ``/api/v1/clay/enrich-companies/land`` endpoint — Postgres is the raw
    vault and stays the landing target (this worker does NOT auto-wire into that path). That surface
    is RAW-ONLY: raw_payload (jsonb) verbatim + the single drift-proof connect key
    ``company_linkedin_url`` (= raw_payload->>'url'); the payload is NEVER exploded. This worker
    mirrors the table into a STANDALONE native Gen-3 Lance dataset and derives the lake's canonical
    company join key ``linkedin_slug`` from that URL — so the dossier is JOIN-able to its siblings
    (clay_find_companies / clay_find_people / firmographics_blitz, all indexed on linkedin_slug)
    without round-tripping Postgres. The payload itself is carried losslessly as a JSON string and
    is NOT unfurled here either.

DISTINCT from ``materialize_clay_find_companies.py``: that mirrors the Clay *Find Companies*
discovery grain (verbatim + exploded typed columns). THIS mirrors the *Enrich Company* dossier
(raw_payload only). Different upstream table, different Lance dataset.

SOURCE (live hq-x Postgres, read-only via the DuckDB postgres scanner):
    gtm.clay_enrich_companies  (record_id is a true PK — append-only history; no dedup needed)
    The DSN is HQX_DB_URL_POOLED (the ``hqx-postgres`` Modal secret, Supavisor SESSION mode :5432);
    DuckDB ATTACHes it READ_ONLY. Source AND control-plane are the same DB — run-state is written
    back to ops.clay_enrich_companies_runs.

TARGET (Gen-3 system of record — native Lance v2.1):
    s3://data-sink/active/clay_enrich_companies/

GRAIN / PK. ``record_id`` (sha256 of the canonical raw_payload, computed by the lander) is the PK
and is unique upstream, so the projection is a straight 1:1 copy — no dedup. APPEND-ONLY HISTORY:
a refreshed Clay payload for the same company lands as a NEW record_id; ``company_linkedin_url`` and
the derived ``linkedin_slug`` are NON-unique bridge keys (a company accumulates a timestamped
history). Current dossier per company = latest landed_at per linkedin_slug, downstream.

DERIVED KEY (canonical, from core.web_norm — the single rule definition, never re-inlined):
    linkedin_slug   linkedin_slug(company_linkedin_url) — the company-page slug (company/<slug>),
                    scheme/www/trailing-slash/case folded away. FK → clay_find_companies.linkedin_slug
                    / clay_find_people.linkedin_slug / firmographics_blitz.linkedin_slug.

``raw_payload`` (jsonb) is carried losslessly as a JSON-string column (full mirror); NOT exploded.

REFRESH. ``run`` does a full-snapshot overwrite. ``append`` is the incremental, manual-only path:
read max(landed_at) already in Lance, pull rows newer than that watermark, and merge_insert on
record_id (idempotent — only net-new record_ids land). No cron, no callback wiring into the landing
endpoint.

INDEXES:
    BTREE  : record_id (PK), company_linkedin_url, linkedin_slug

    modal run    pipelines/gtm/materialize_clay_enrich_companies.py::init_ops      # create ops table (HQX)
    modal run    pipelines/gtm/materialize_clay_enrich_companies.py::run           # full overwrite (manual)
    modal run    pipelines/gtm/materialize_clay_enrich_companies.py::append_only   # incremental watermark append (manual)
    modal run    pipelines/gtm/materialize_clay_enrich_companies.py::reindex_only  # rebuild indexes
    modal run    pipelines/gtm/materialize_clay_enrich_companies.py::verify_only   # read-back assertions
    modal deploy pipelines/gtm/materialize_clay_enrich_companies.py                # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

from core.web_norm import linkedin_slug

# Gen-3 target sink (active tier). Net-new dataset lands directly here.
_ACTIVE = "s3://data-sink/active"
DATASET = "clay_enrich_companies"
DATASET_URI = os.environ.get("CLAY_ENRICH_COMPANIES_URI", f"{_ACTIVE}/{DATASET}/")

FEED = "clay_enrich_companies"
SOURCE_DB = "hqx:gtm.clay_enrich_companies"  # provenance label recorded in ops.*

# Lance fragment sizing — fleet defaults (02_lancedb_storage.md §2.3).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
READ_BATCH_ROWS = 50000  # DuckDB → Arrow streaming batch (out-of-core: never buffer the full table)

# Scalar index plan. BTREE only — raw-only grain has no low-cardinality projected columns.
INDEXES: dict[str, list[str]] = {
    "BTREE": ["record_id", "company_linkedin_url", "linkedin_slug"],
}

# Verbatim passthrough columns (mirror gtm.clay_enrich_companies). jsonb → VARCHAR (lossless).
_PASSTHROUGH = ["record_id", "company_linkedin_url", "raw_payload", "source", "landed_at"]
_JSONB = {"raw_payload"}

# Final schema/output column order (PK + connect key + derived bridge first, then payload + lineage).
_COLS = [
    "record_id", "company_linkedin_url", "linkedin_slug", "raw_payload", "source",
    "landed_at", "materialized_at",
]
_NOT_NULL = {"record_id", "raw_payload", "source", "landed_at", "materialized_at"}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.clay_enrich_companies_runs (
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

CREATE INDEX IF NOT EXISTS clay_enrich_companies_runs_feed_idx        ON ops.clay_enrich_companies_runs (feed);
CREATE INDEX IF NOT EXISTS clay_enrich_companies_runs_status_idx      ON ops.clay_enrich_companies_runs (status);
CREATE INDEX IF NOT EXISTS clay_enrich_companies_runs_recorded_at_idx ON ops.clay_enrich_companies_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "psycopg[binary]>=3.2",
    "requests>=2.32",
).env({"LANCE_BYPASS_SPILLING": "true"}).add_local_python_source("core.web_norm")

app = modal.App("clay-enrich-companies", image=image)


def _schema():
    import pyarrow as pa

    def f(name, typ):
        return pa.field(name, typ, nullable=name not in _NOT_NULL)

    ts = pa.timestamp("us", tz="UTC")
    cols = []
    for name in _COLS:
        if name in ("landed_at", "materialized_at"):
            cols.append(f(name, ts))
        else:
            cols.append(f(name, pa.string()))  # incl. jsonb → JSON string, lossless
    return pa.schema(cols)


def _sql(where: str = "") -> str:
    """Straight 1:1 projection + the canonical linkedin_slug bridge key. jsonb → VARCHAR (lossless
    JSON text). The slug builder is interpolated from core.web_norm — NEVER re-inlined."""
    base = ",\n        ".join(
        f"CAST({c} AS VARCHAR) AS {c}" if c in _JSONB else c for c in _PASSTHROUGH
    )
    return f"""
    SELECT
        {base},
        {linkedin_slug("company_linkedin_url")}  AS linkedin_slug,
        now()                                    AS materialized_at
    FROM hqx.gtm.clay_enrich_companies
    {where}
    """


# ── DSN + R2 plumbing (fleet-standard; mirrors materialize_clay_find_companies.py) ───────────
def _hqx_dsn() -> str:
    """hq-x Postgres DSN — SESSION pooler (:5432), SSL enforced. The DuckDB postgres scanner needs
    session-scoped state (multi-statement, cursors), so NOT the transaction pooler (:6543)."""
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
    # threads=1 → the postgres scanner opens a SINGLE backend connection. The hq-x Supavisor
    # SESSION pool (:5432) is capped at 15 clients and is shared with the live edge_api landing
    # service; a 4-way parallel scan reliably trips EMAXCONNSESSION. A single-connection streaming
    # read fits the pool.
    con.execute("PRAGMA threads=1;")
    con.execute(f"ATTACH '{dsn.replace(chr(39), chr(39) * 2)}' AS hqx (TYPE postgres, READ_ONLY);")


def _casted_reader(con, schema, where: str = ""):
    """Stream DuckDB → Arrow in batches, casting each to the exact contract. Out-of-core:
    the full table is never resident — lance.write_dataset pulls batches lazily."""
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
            except Exception as exc:  # noqa: BLE001 — an index miss must not fail a good load
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
                INSERT INTO ops.clay_enrich_companies_runs
                    (feed, source_db, datasets, mode, rows_total, rows_source, rows_added,
                     watermark, status, error, started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (FEED, SOURCE_DB, Jsonb({DATASET: rows_total}), mode, rows_total, rows_source,
                 rows_added, watermark, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the migration
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


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=16384, cpu=4.0)
def ingest_clay_enrich_companies(trigger_callback_url: str | None = None) -> dict:
    """Full-snapshot overwrite: ATTACH hq-x Postgres READ_ONLY, stream gtm.clay_enrich_companies →
    Arrow (derive linkedin_slug + cast to contract) → Lance overwrite → BTREE indexes →
    ops.* state + callback."""
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
            rows_source = con.sql("SELECT count(*) FROM hqx.gtm.clay_enrich_companies").fetchone()[0]
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
        raise RuntimeError(f"clay_enrich_companies materialization failed: {error}")
    return {"feed": FEED, "mode": "overwrite", "rows_total": rows_total,
            "rows_source": rows_source, "status": status}


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=16384, cpu=4.0)
def append_clay_enrich_companies(trigger_callback_url: str | None = None) -> dict:
    """Incremental, manual-only: watermark = max(landed_at) already in Lance; pull rows strictly
    newer; merge_insert on record_id (idempotent — only net-new record_ids land)."""
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
        wm = pc.max(ds.to_table(columns=["landed_at"]).column("landed_at")).as_py()
        watermark = wm
        if wm is None:
            raise RuntimeError("existing dataset has no landed_at watermark; run a full overwrite first.")
        where = f"WHERE landed_at > TIMESTAMPTZ '{wm.isoformat()}'"
        con = duckdb.connect(":memory:")
        try:
            _attach(con, _hqx_dsn())
            rows_source = con.sql(
                f"SELECT count(*) FROM hqx.gtm.clay_enrich_companies {where}").fetchone()[0]
            print(f"new rows since {wm.isoformat()}: {rows_source:,}")
            if rows_source:
                new_tbl = con.sql(_sql(where)).to_arrow_table().cast(_schema())
                ds.merge_insert("record_id").when_not_matched_insert_all().execute(new_tbl)
        finally:
            con.close()
        rows_total = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        rows_added = rows_total - before
        print(f"appended {rows_added:,} → {rows_total:,} rows")
        if rows_added:
            _create_indexes(so)  # refresh indexes to cover new fragments
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
        raise RuntimeError(f"clay_enrich_companies append failed: {error}")
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
    """Read-back: row count, record_id uniqueness invariant, schema, indexes, BTREE probe,
    derived-key coverage."""
    import pyarrow.compute as pc

    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()
    keys = ds.to_table(columns=["record_id", "company_linkedin_url", "linkedin_slug"])
    distinct_record = pc.count_distinct(keys.column("record_id")).as_py()
    unique_ok = (n == distinct_record)
    url = keys.column("company_linkedin_url")
    sl = keys.column("linkedin_slug")
    company_linkedin_url_nonnull = n - url.null_count
    linkedin_slug_nonnull = n - sl.null_count
    distinct_linkedin_slug = pc.count_distinct(sl).as_py()

    sample = next((v for v in keys.column("record_id").to_pylist() if v), None)
    probe = ds.scanner(columns=["record_id"],
                       filter=f"record_id = '{sample}'").to_table().num_rows if sample else -1
    out = {
        "uri": DATASET_URI, "rows": n, "distinct_record_id": distinct_record,
        "unique_invariant_ok": unique_ok,
        "company_linkedin_url_nonnull": company_linkedin_url_nonnull,
        "linkedin_slug_nonnull": linkedin_slug_nonnull, "distinct_linkedin_slug": distinct_linkedin_slug,
        "schema": [f.name for f in ds.schema], "indexes": _committed_index_names(so),
        f"probe_record_id={sample!r}": probe,
    }
    print(f"{DATASET}: {n:,} rows · distinct(record_id)={distinct_record:,} · unique_ok={unique_ok}")
    print(f"  company_linkedin_url: {company_linkedin_url_nonnull:,} non-null · "
          f"linkedin_slug: {linkedin_slug_nonnull:,} non-null ({distinct_linkedin_slug:,} distinct)")
    print(f"  indexes={out['indexes']}")
    print(f"  probe record_id={sample!r} → {probe} row(s)")
    if not unique_ok:
        raise RuntimeError(f"uniqueness invariant FAILED: rows={n} != distinct(record_id)={distinct_record}")
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
            WHERE table_schema='ops' AND table_name='clay_enrich_companies_runs' ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    print(f"ops.clay_enrich_companies_runs ready — columns: {cols}")
    return {"table": "ops.clay_enrich_companies_runs", "columns": cols}


@app.local_entrypoint()
def init_ops() -> None:
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def run() -> None:
    import json
    print(json.dumps(ingest_clay_enrich_companies.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def append_only() -> None:
    import json
    print(json.dumps(append_clay_enrich_companies.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def reindex_only() -> None:
    import json
    print(json.dumps(reindex.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_only() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
