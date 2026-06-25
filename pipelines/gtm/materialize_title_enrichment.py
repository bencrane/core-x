"""Compute worker — Gen-3 materialization of the job-title enrichment grain as a
PERSON-keyed serving master (latest classification wins per person).

The hot read-path dimension: one current normalized title per person, joinable 1:1 to
active/people on the LinkedIn URL, indexed so the CRM/audience tools can FILTER people by
their enriched title (normalized_level / normalized_function / confidence) with index
pushdown. NO raw payloads live here — the verbatim landed body is the append-only SoR
(gtm.title_enrichment, jsonb raw_payload). This master is a convenience projection ON TOP of
that table, never a replacement.

WHY THIS WORKER EXISTS
    gtm.title_enrichment (HQX Postgres) is the raw vault: PERSON × title-classification,
    APPEND-ONLY HISTORY — a re-classification of the same person lands a NEW row, and the
    reader takes the latest by landed_at. That history grain is correct for the vault but is
    the wrong shape for a people join (a person with N classifications would fan a people row
    out N×). This worker mirrors the table into a STANDALONE native Gen-3 Lance dataset,
    COLLAPSED most-recent-wins per person, so the title grain is queryable in the R2 lake and
    JOINABLE on person_linkedin_url to active/people without round-tripping Postgres. The
    gtm_mcp serving registry auto-discovers active/title_enrichment/ as a flat root, so it is
    referenceable as ``title_enrichment`` in the cross-dataset ``query`` tool the moment it lands.

SOURCE (live hq-x Postgres, read-only via the DuckDB postgres scanner):
    gtm.title_enrichment  — PERSON-grain append-only history (PK record_id). The DSN is
    HQX_DB_URL_POOLED (the ``hqx-postgres`` Modal secret, Supavisor SESSION mode :5432);
    DuckDB ATTACHes it READ_ONLY. Source AND control-plane are the same DB — run-state is
    written back to ops.title_enrichment_lance_runs.

TARGET (Gen-3 system of record — native Lance v2.1):
    s3://data-sink/active/title_enrichment/

GRAIN / PK. ``person_linkedin_url_norm`` (1 row / person). The normalized key is the SAME
normalization gtm.title_enrichment folds into record_id (lower → strip scheme → strip leading
``www.`` → strip trailing slash → NULL if emptied), so the master key is deterministic and
stable. Among a person's history the row with the greatest ``landed_at`` wins (``record_id``
DESC breaks ties). Rows with no person_linkedin_url are TITLE-grain, unjoinable to people by
construction, and are EXCLUDED from this serving master — they stay in the gtm.title_enrichment
SoR (the stateless /titles/normalize path), never lost. ``person_linkedin_url`` is carried
VERBATIM (the winning row's value) alongside the normalized key, so a caller can join people on
either the raw fleet-convention key OR the robust normalized bridge.

REFRESH. ``run`` does a full-snapshot overwrite (re-collapses the whole history → latest per
person; captures new persons AND re-classifications). ``append`` is the incremental path:
watermark = max(landed_at) already in Lance; pull rows strictly newer, collapse most-recent-wins
WITHIN that batch per person, and merge_insert on person_linkedin_url_norm with update + insert
(a newer classification UPDATES the person's master row; anything > watermark is by definition
newer than the master, so update is always correct). No cron, no callback wiring into a landing
rail — manual / dispatcher-spawned.

INDEXES:
    BTREE  : person_linkedin_url_norm (PK · bridge → active/people), person_linkedin_url
             (verbatim raw-join convention), record_id (lineage → the chosen SoR row),
             title_norm (title bridge/dedup key)
    BITMAP : normalized_level, normalized_function, confidence, model, source
             (categorical filter accelerators — the whole point of "filter people by title")

    modal run    pipelines/gtm/materialize_title_enrichment.py::init_ops      # create ops table (HQX)
    modal run    pipelines/gtm/materialize_title_enrichment.py::run           # full overwrite (initial / rebuild)
    modal run    pipelines/gtm/materialize_title_enrichment.py::append_only   # incremental watermark append
    modal run    pipelines/gtm/materialize_title_enrichment.py::reindex_only  # rebuild indexes
    modal run    pipelines/gtm/materialize_title_enrichment.py::verify_only   # read-back assertions
    modal deploy pipelines/gtm/materialize_title_enrichment.py               # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

# Gen-3 target sink (active tier). Net-new dataset lands directly here.
_ACTIVE = "s3://data-sink/active"
DATASET = "title_enrichment"
DATASET_URI = os.environ.get("TITLE_ENRICHMENT_URI", f"{_ACTIVE}/{DATASET}/")

FEED = "title_enrichment"
SOURCE_DB = "hqx:gtm.title_enrichment"  # provenance label recorded in ops.*

# Lance fragment sizing — fleet defaults (02_lancedb_storage.md §2.3).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
READ_BATCH_ROWS = 50000  # DuckDB → Arrow streaming batch (out-of-core: never buffer the full table)

# Scalar index plan.
INDEXES: dict[str, list[str]] = {
    "BTREE": ["person_linkedin_url_norm", "person_linkedin_url", "record_id", "title_norm"],
    "BITMAP": ["normalized_level", "normalized_function", "confidence", "model", "source"],
}

# person_linkedin_url → canonical person key. EXACTLY the normalization gtm.title_enrichment
# folds into record_id: lower → strip scheme → strip leading www. → strip trailing slash →
# NULL if emptied. Raw Python string keeps the regex backslash literal for DuckDB (RE2).
_PLI_NORM = (
    r"nullif(rtrim(regexp_replace(regexp_replace(lower(trim(person_linkedin_url)), "
    r"'^https?://', ''), '^www\.', ''), '/'), '')"
)

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.title_enrichment_lance_runs (
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

CREATE INDEX IF NOT EXISTS title_enrichment_lance_runs_feed_idx        ON ops.title_enrichment_lance_runs (feed);
CREATE INDEX IF NOT EXISTS title_enrichment_lance_runs_status_idx      ON ops.title_enrichment_lance_runs (status);
CREATE INDEX IF NOT EXISTS title_enrichment_lance_runs_recorded_at_idx ON ops.title_enrichment_lance_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "psycopg[binary]>=3.2",
    "requests>=2.32",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("title-enrichment-materializer", image=image)


def _schema():
    import pyarrow as pa

    ts = pa.timestamp("us", tz="UTC")
    return pa.schema([
        pa.field("person_linkedin_url_norm", pa.string(), nullable=False),  # PK · bridge → active/people · BTREE
        pa.field("person_linkedin_url",      pa.string(), nullable=False),  # verbatim (winning row) · BTREE
        pa.field("record_id",                pa.string(), nullable=False),  # lineage → gtm.title_enrichment · BTREE
        pa.field("raw_job_title",            pa.string(), nullable=False),  # verbatim as sent
        pa.field("title_norm",               pa.string(), nullable=False),  # title bridge/dedup key · BTREE
        pa.field("normalized_job_title",     pa.string(), nullable=True),   # plain passthrough
        pa.field("normalized_level",         pa.string(), nullable=True),   # BITMAP (filter)
        pa.field("normalized_function",      pa.string(), nullable=True),   # BITMAP (filter)
        pa.field("confidence",               pa.string(), nullable=True),   # BITMAP (filter)
        pa.field("model",                    pa.string(), nullable=True),   # BITMAP (lineage filter)
        pa.field("reasoning",                pa.string(), nullable=True),   # free text — carried, NOT indexed
        pa.field("source",                   pa.string(), nullable=False),  # BITMAP
        pa.field("landed_at",                ts,          nullable=False),  # winning row's landed_at · watermark
        pa.field("materialized_at",          ts,          nullable=False),  # lineage
    ])


def _sql(where: str = "") -> str:
    """Collapse the PERSON-grain append-only history → most-recent-wins per person. The
    normalized LinkedIn key (the same one folded into record_id) is the partition; greatest
    landed_at wins (record_id DESC breaks ties). Rows with no normalizable person_linkedin_url
    are dropped (title-grain, unjoinable to people). ``where`` is injected raw (the append
    scopes to landed_at > watermark). Column order matches _schema() exactly."""
    return f"""
    WITH base AS (
        SELECT
            {_PLI_NORM} AS person_linkedin_url_norm,
            person_linkedin_url,
            record_id, raw_job_title, title_norm, normalized_job_title,
            normalized_level, normalized_function, confidence, model, reasoning,
            source, landed_at
        FROM hqx.gtm.title_enrichment
        {where}
    ),
    ranked AS (
        SELECT *,
            row_number() OVER (
                PARTITION BY person_linkedin_url_norm
                ORDER BY landed_at DESC, record_id DESC
            ) AS rn
        FROM base
        WHERE person_linkedin_url_norm IS NOT NULL
    )
    SELECT
        person_linkedin_url_norm,
        person_linkedin_url,
        record_id, raw_job_title, title_norm, normalized_job_title,
        normalized_level, normalized_function, confidence, model, reasoning,
        source, landed_at,
        now() AS materialized_at
    FROM ranked
    WHERE rn = 1
    """


def _count_sql(where: str = "") -> str:
    """Distinct joinable persons (non-null normalized LinkedIn key) in scope."""
    return f"""
    SELECT count(DISTINCT {_PLI_NORM})
    FROM hqx.gtm.title_enrichment
    {where}
    """


# ── DSN + R2 plumbing (fleet-standard; mirrors materialize_phone_resolutions.py) ─
def _hqx_dsn() -> str:
    """hq-x Postgres DSN — SESSION pooler (:5432), SSL enforced. The DuckDB postgres scanner
    needs session-scoped state (multi-statement, cursors), so NOT the transaction pooler (:6543)."""
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
                INSERT INTO ops.title_enrichment_lance_runs
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


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=8192, cpu=2.0)
def ingest_title_enrichment(trigger_callback_url: str | None = None) -> dict:
    """Full-snapshot overwrite (initial hydrate / intentional rebuild): ATTACH hq-x Postgres
    READ_ONLY, collapse gtm.title_enrichment history → latest-per-person → Arrow (cast to
    contract) → Lance overwrite → BTREE+BITMAP indexes → ops.* state + callback. For routine
    refresh use append."""
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
            print(f"source distinct persons (joinable): {rows_source:,}")
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
        raise RuntimeError(f"title_enrichment materialization failed: {error}")
    return {"feed": FEED, "mode": "overwrite", "rows_total": rows_total,
            "rows_source": rows_source, "status": status}


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=8192, cpu=2.0)
def append_title_enrichment(trigger_callback_url: str | None = None) -> dict:
    """Incremental latest-wins: watermark = max(landed_at) already in Lance; pull rows strictly
    newer, collapse most-recent-wins per person WITHIN that batch, merge_insert on
    person_linkedin_url_norm with update + insert. Anything > watermark is by construction newer
    than the person's existing master row, so the update is always the correct newer classification."""
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
            rows_source = con.sql(_count_sql(where)).fetchone()[0]
            print(f"new joinable persons since {wm.isoformat()}: {rows_source:,}")
            if rows_source:
                new_tbl = con.sql(_sql(where)).to_arrow_table().cast(_schema())
                (ds.merge_insert("person_linkedin_url_norm")
                   .when_matched_update_all()
                   .when_not_matched_insert_all()
                   .execute(new_tbl))
        finally:
            con.close()
        rows_total = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        rows_added = rows_total - before
        print(f"merged batch → {rows_total:,} rows ({rows_added:+,} net)")
        if rows_source:
            _create_indexes(so)  # refresh indexes to cover new/rewritten fragments
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
        raise RuntimeError(f"title_enrichment append failed: {error}")
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
    """Read-back: row count, person-key uniqueness invariant (PK == distinct), filter-dimension
    cardinality, schema, indexes, BTREE probe."""
    import pyarrow.compute as pc

    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()
    keys = ds.to_table(columns=["person_linkedin_url_norm", "normalized_function", "normalized_level"])
    pk = keys.column("person_linkedin_url_norm")
    distinct_person = pc.count_distinct(pk).as_py()
    unique_ok = (n == distinct_person) and (pk.null_count == 0)
    distinct_function = pc.count_distinct(keys.column("normalized_function")).as_py()
    distinct_level = pc.count_distinct(keys.column("normalized_level")).as_py()

    sample = next((v for v in pk.to_pylist() if v), None)
    probe = ds.scanner(columns=["person_linkedin_url_norm"],
                       filter=f"person_linkedin_url_norm = '{sample}'").to_table().num_rows if sample else -1
    out = {
        "uri": DATASET_URI, "rows": n, "distinct_person_linkedin_url_norm": distinct_person,
        "unique_invariant_ok": unique_ok,
        "distinct_normalized_function": distinct_function,
        "distinct_normalized_level": distinct_level,
        "schema": [f.name for f in ds.schema], "indexes": _committed_index_names(so),
        f"probe_person_linkedin_url_norm={sample!r}": probe,
    }
    print(f"{DATASET}: {n:,} rows · distinct(person_linkedin_url_norm)={distinct_person:,} · unique_ok={unique_ok}")
    print(f"  filter dims: normalized_function={distinct_function} distinct · normalized_level={distinct_level} distinct")
    print(f"  indexes={out['indexes']}")
    print(f"  probe person_linkedin_url_norm={sample!r} → {probe} row(s)")
    if not unique_ok:
        raise RuntimeError(f"uniqueness invariant FAILED: rows={n} != distinct(person_key)={distinct_person} "
                           f"(or PK has nulls: {pk.null_count})")
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
            WHERE table_schema='ops' AND table_name='title_enrichment_lance_runs' ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    print(f"ops.title_enrichment_lance_runs ready — columns: {cols}")
    return {"table": "ops.title_enrichment_lance_runs", "columns": cols}


@app.local_entrypoint()
def init_ops() -> None:
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def run() -> None:
    import json
    print(json.dumps(ingest_title_enrichment.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def append_only() -> None:
    import json
    print(json.dumps(append_title_enrichment.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def reindex_only() -> None:
    import json
    print(json.dumps(reindex.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_only() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
