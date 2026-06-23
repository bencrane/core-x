"""Compute worker — Gen-3 materialization of the Clay Find Companies raw landing grain.

Sibling of ``materialize_clay_find_people.py``. Part of the ``clay-find-companies`` Modal app.
Endpoint-less; spawned by the Universal Dispatcher (core/modal_dispatcher.py) or run manually.
Clean-room data plane: DuckDB does 100% of the read/cast/derive, Lance is written straight to R2.
No catalog round-trip.

WHY THIS WORKER EXISTS
    Clay Find Companies records land append-only in ``gtm.clay_find_companies`` (HQX Postgres) via
    the edge_api ``/api/v1/clay/find-companies/land`` endpoint — Postgres is the raw vault and stays
    the landing target (this worker does NOT auto-wire into that path). The landing surface does ZERO
    semantic normalization (linkedin_url / domain stored VERBATIM, by design). This worker mirrors the
    table into a STANDALONE native Gen-3 Lance dataset AND derives the canonical bridge keys
    (``domain_norm`` / ``linkedin_slug`` / ``is_generic_domain``) so the company grain is queryable +
    JOIN-able in the R2 lake (→ firmographics_blitz / clay_find_people / equipment_matchmaking)
    without round-tripping Postgres.

SOURCE (live hq-x Postgres, read-only via the DuckDB postgres scanner):
    gtm.clay_find_companies  (201,550 rows as of 2026-06-23; record_id is a true PK — no dedup)
    The DSN is HQX_DB_URL_POOLED (the ``hqx-postgres`` Modal secret, Supavisor SESSION mode :5432);
    DuckDB ATTACHes it READ_ONLY. Source AND control-plane are the same DB — run-state is written
    back to ops.clay_find_companies_runs.

TARGET (Gen-3 system of record — native Lance v2.1):
    s3://data-sink/active/clay_find_companies/

GRAIN / PK. ``record_id`` (sha256 of the verbatim company_key — linkedin_url → domain →
linkedin_company_id → clay_company_id, degrading) is the PK and is unique upstream (201,550 rows =
201,550 distinct), so the projection is a straight 1:1 copy — no most-recent-wins dedup. Because the
landing key is the RAW string, trailing-slash / ``www.`` / case variants of the same company land as
DISTINCT rows; collapsing those is a downstream concern and is NOT done here — ``domain_norm`` /
``linkedin_slug`` are derived bridge keys (non-unique, nullable), never the PK.

DERIVED BRIDGE KEYS (canonical, from core.web_norm — the single rule definition, never re-inlined):
    domain_norm        normalized_domain(_bare_host(COALESCE(domain, domain_resolved)))
                       — prefer the verbatim landed domain, fall back to Clay's resolved domain.
                       FK → firmographics_blitz.domain_norm / equipment_matchmaking.domain_norm.
    linkedin_slug      linkedin_slug(linkedin_url) — the company-page slug (company/<slug>).
    is_generic_domain  BITMAP flag — 1 when domain_norm is a shared webmail/social/marketplace host
                       that must NOT be used as a 1:1 join key. Consumers filter, not re-derive.

``raw_payload`` (jsonb) + the 3 structured jsonb columns (industries / structured_locations /
derived_datapoints) are carried losslessly as JSON-string columns (full mirror); the rest are the
typed projection.

REFRESH. ``run`` does a full-snapshot overwrite. ``append`` is the incremental, manual-only path:
read max(landed_at) already in Lance, pull rows newer than that watermark, and merge_insert on
record_id (idempotent — only net-new record_ids land). No cron, no callback wiring into the landing
endpoint.

INDEXES:
    BTREE  : record_id (PK), domain_norm, linkedin_slug, linkedin_company_id
    BITMAP : is_generic_domain, domain_is_live, hq_country_iso, hq_state, hq_region

    modal run    pipelines/gtm/materialize_clay_find_companies.py::init_ops      # create ops table (HQX)
    modal run    pipelines/gtm/materialize_clay_find_companies.py::run           # full overwrite (manual)
    modal run    pipelines/gtm/materialize_clay_find_companies.py::append_only   # incremental watermark append (manual)
    modal run    pipelines/gtm/materialize_clay_find_companies.py::reindex_only  # rebuild indexes
    modal run    pipelines/gtm/materialize_clay_find_companies.py::verify_only   # read-back assertions
    modal deploy pipelines/gtm/materialize_clay_find_companies.py                # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

from core.web_norm import _bare_host, is_generic_domain, linkedin_slug, normalized_domain

# Gen-3 target sink (active tier). Net-new dataset lands directly here.
_ACTIVE = "s3://data-sink/active"
DATASET = "clay_find_companies"
DATASET_URI = os.environ.get("CLAY_FIND_COMPANIES_URI", f"{_ACTIVE}/{DATASET}/")

FEED = "clay_find_companies"
SOURCE_DB = "hqx:gtm.clay_find_companies"  # provenance label recorded in ops.*

# Lance fragment sizing — fleet defaults (02_lancedb_storage.md §2.3).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"
READ_BATCH_ROWS = 50000  # DuckDB → Arrow streaming batch (out-of-core: never buffer the full table)

# Scalar index plan.
INDEXES: dict[str, list[str]] = {
    "BTREE": ["record_id", "domain_norm", "linkedin_slug", "linkedin_company_id"],
    "BITMAP": ["is_generic_domain", "domain_is_live", "hq_country_iso", "hq_state", "hq_region"],
}

# Verbatim passthrough columns (mirror gtm.clay_find_companies). jsonb → VARCHAR (lossless).
_PASSTHROUGH = [
    "record_id", "clay_company_id", "linkedin_company_id", "name", "size", "company_type",
    "industry", "country", "location", "description", "annual_revenue",
    "total_funding_amount_range_usd", "domain_resolved", "domain_is_live", "domain_redirects",
    "hq_city", "hq_state", "hq_region", "hq_country_iso", "hq_postal_code",
    "derived_business_stage", "derived_scale_scope", "derived_pattern_tags", "derived_description",
    "industries", "structured_locations", "derived_datapoints", "source", "raw_payload",
    "landed_at", "linkedin_url", "domain",
]
_JSONB = {"industries", "structured_locations", "derived_datapoints", "raw_payload"}
_BIGINT = {"clay_company_id", "linkedin_company_id"}
_BOOL = {"domain_is_live", "domain_redirects"}

# Derived columns appended to the passthrough projection.
_DERIVED = ["domain_norm", "linkedin_slug", "is_generic_domain", "materialized_at"]

# Final schema/output column order (PK + derived bridges first, then payload).
_COLS = [
    "record_id", "domain_norm", "linkedin_slug", "linkedin_url", "domain", "domain_resolved",
    "is_generic_domain", "clay_company_id", "linkedin_company_id", "name", "size", "company_type",
    "industry", "country", "location", "description", "annual_revenue",
    "total_funding_amount_range_usd", "domain_is_live", "domain_redirects",
    "hq_city", "hq_state", "hq_region", "hq_country_iso", "hq_postal_code",
    "derived_business_stage", "derived_scale_scope", "derived_pattern_tags", "derived_description",
    "industries", "structured_locations", "derived_datapoints", "source", "raw_payload",
    "landed_at", "materialized_at",
]
# Only the structurally-guaranteed columns are NOT NULL. linkedin_url is currently 100% populated but
# the landing key can degrade to domain/ids, so it is left nullable for future appends.
_NOT_NULL = {"record_id", "source", "raw_payload", "landed_at", "materialized_at"}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.clay_find_companies_runs (
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

CREATE INDEX IF NOT EXISTS clay_find_companies_runs_feed_idx        ON ops.clay_find_companies_runs (feed);
CREATE INDEX IF NOT EXISTS clay_find_companies_runs_status_idx      ON ops.clay_find_companies_runs (status);
CREATE INDEX IF NOT EXISTS clay_find_companies_runs_recorded_at_idx ON ops.clay_find_companies_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "psycopg[binary]>=3.2",
    "requests>=2.32",
).env({"LANCE_BYPASS_SPILLING": "true"}).add_local_python_source("core.web_norm")

app = modal.App("clay-find-companies", image=image)


def _schema():
    import pyarrow as pa

    def f(name, typ):
        return pa.field(name, typ, nullable=name not in _NOT_NULL)

    ts = pa.timestamp("us", tz="UTC")
    cols = []
    for name in _COLS:
        if name in ("landed_at", "materialized_at"):
            cols.append(f(name, ts))
        elif name in _BIGINT:
            cols.append(f(name, pa.int64()))
        elif name in _BOOL or name == "is_generic_domain":
            cols.append(f(name, pa.bool_()))
        else:
            cols.append(f(name, pa.string()))  # incl. jsonb → JSON string, lossless
    return pa.schema(cols)


def _sql(where: str = "") -> str:
    """Straight 1:1 projection + derived canonical bridge keys. jsonb → VARCHAR (lossless JSON text).
    Three-stage CTE so each heavy regex runs once and is_generic_domain reads the fully-materialized
    domain_norm column (no forward-alias). Canonical builders interpolated from core.web_norm —
    NEVER re-inlined."""
    base = ",\n        ".join(
        f"CAST({c} AS VARCHAR) AS {c}" if c in _JSONB else c for c in _PASSTHROUGH
    )
    # prefer the verbatim landed domain, fall back to Clay's resolved domain
    domain_expr = "COALESCE(nullif(trim(domain), ''), nullif(trim(domain_resolved), ''))"
    passthrough = ",\n        ".join(_PASSTHROUGH)
    # Final projection MUST be emitted in exact _COLS (schema) order — .cast() matches by position.
    def _final(col: str) -> str:
        if col == "is_generic_domain":
            return f"{is_generic_domain('domain_norm')} AS is_generic_domain"
        if col == "materialized_at":
            return "now() AS materialized_at"
        return col  # passthrough, domain_norm, linkedin_slug all exist in the keyed CTE
    final = ",\n        ".join(_final(c) for c in _COLS)
    return f"""
    WITH norm AS (
        SELECT
        {base},
        {_bare_host(domain_expr)}        AS _host,
        {linkedin_slug("linkedin_url")}  AS linkedin_slug
        FROM hqx.gtm.clay_find_companies
        {where}
    ),
    keyed AS (
        SELECT
        {passthrough},
        linkedin_slug,
        {normalized_domain("_host")}     AS domain_norm
        FROM norm
    )
    SELECT
        {final}
    FROM keyed
    """


# ── DSN + R2 plumbing (fleet-standard; mirrors materialize_clay_find_people.py) ───────────
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
    # read fits the pool and is ample for this 201k-row grain.
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
                INSERT INTO ops.clay_find_companies_runs
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
def ingest_clay_find_companies(trigger_callback_url: str | None = None) -> dict:
    """Full-snapshot overwrite: ATTACH hq-x Postgres READ_ONLY, stream gtm.clay_find_companies →
    Arrow (derive bridge keys + cast to contract) → Lance overwrite → BTREE+BITMAP indexes →
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
            rows_source = con.sql("SELECT count(*) FROM hqx.gtm.clay_find_companies").fetchone()[0]
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
        raise RuntimeError(f"clay_find_companies materialization failed: {error}")
    return {"feed": FEED, "mode": "overwrite", "rows_total": rows_total,
            "rows_source": rows_source, "status": status}


@app.function(secrets=_SECRETS, timeout=60 * 30, memory=16384, cpu=4.0)
def append_clay_find_companies(trigger_callback_url: str | None = None) -> dict:
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
                f"SELECT count(*) FROM hqx.gtm.clay_find_companies {where}").fetchone()[0]
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
        raise RuntimeError(f"clay_find_companies append failed: {error}")
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
    keys = ds.to_table(columns=["record_id", "domain_norm", "linkedin_slug"])
    distinct_record = pc.count_distinct(keys.column("record_id")).as_py()
    unique_ok = (n == distinct_record)
    dn = keys.column("domain_norm")
    sl = keys.column("linkedin_slug")
    domain_norm_nonnull = n - dn.null_count
    distinct_domain_norm = pc.count_distinct(dn).as_py()
    linkedin_slug_nonnull = n - sl.null_count

    sample = next((v for v in keys.column("record_id").to_pylist() if v), None)
    probe = ds.scanner(columns=["record_id"],
                       filter=f"record_id = '{sample}'").to_table().num_rows if sample else -1
    out = {
        "uri": DATASET_URI, "rows": n, "distinct_record_id": distinct_record,
        "unique_invariant_ok": unique_ok,
        "domain_norm_nonnull": domain_norm_nonnull, "distinct_domain_norm": distinct_domain_norm,
        "linkedin_slug_nonnull": linkedin_slug_nonnull,
        "schema": [f.name for f in ds.schema], "indexes": _committed_index_names(so),
        f"probe_record_id={sample!r}": probe,
    }
    print(f"{DATASET}: {n:,} rows · distinct(record_id)={distinct_record:,} · unique_ok={unique_ok}")
    print(f"  domain_norm: {domain_norm_nonnull:,} non-null ({distinct_domain_norm:,} distinct) · "
          f"linkedin_slug: {linkedin_slug_nonnull:,} non-null")
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
            WHERE table_schema='ops' AND table_name='clay_find_companies_runs' ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    print(f"ops.clay_find_companies_runs ready — columns: {cols}")
    return {"table": "ops.clay_find_companies_runs", "columns": cols}


@app.local_entrypoint()
def init_ops() -> None:
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def run() -> None:
    import json
    print(json.dumps(ingest_clay_find_companies.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def append_only() -> None:
    import json
    print(json.dumps(append_clay_find_companies.remote(trigger_callback_url=None), indent=2, default=str))


@app.local_entrypoint()
def reindex_only() -> None:
    import json
    print(json.dumps(reindex.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_only() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
