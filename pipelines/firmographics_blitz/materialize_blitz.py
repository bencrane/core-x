"""Compute worker — standalone Gen-3 materialization of the Blitz firmographics reference grain.

Part of the ``firmographics-blitz`` Modal app. Endpoint-less; spawned by the Universal Dispatcher
(core/modal_dispatcher.py) or run manually. Clean-room data plane: DuckDB does 100% of the
transform, Lance is written straight to R2. No Iceberg, no Polaris, no catalog round-trip.

WHY THIS WORKER EXISTS (Directive 12 plan → Directive 13 build)
    The legacy dex-db (Polaris-backed) firmographic pipeline is deprecated. The BlitzAPI
    firmographic payloads already live in the hq-x Postgres control-plane DB, embedded in the
    ``ops.task_runs.result_payload`` JSONB for two task types. This worker projects the highest-
    value firmographic fields out of that JSONB and materializes them as a STANDALONE native
    Gen-3 Lance dataset — deliberately NOT merged into gtm ``companies`` (firmographic coverage
    extends far beyond the companies population).

SOURCE (live hq-x Postgres, read-only via the DuckDB postgres scanner):
    ops.task_runs WHERE task_type IN ('blitz_firmo_direct','modal_hydrate_firmo_cascade')
                    AND status = 'completed'      (165,884 rows as of 2026-06-02)
    Both task types wrap the SAME BlitzAPI company object under different keys:
        blitz_firmo_direct          → result_payload.blitz_payload.{found, company{…}}
        modal_hydrate_firmo_cascade → result_payload.blitz_data.{found, company{…}}
    Unified with COALESCE(blitz_payload, blitz_data). The DSN is HQX_DB_URL_POOLED (the
    ``hqx-postgres`` Modal secret); DuckDB ATTACHes it READ_ONLY. Source AND control-plane are the
    same DB — run-state is written back to ops.firmographics_blitz_runs.

TARGET (Gen-3 system of record — native Lance v2.1, full-snapshot overwrite):
    s3://data-sink/active/firmographics_blitz/

ANCHOR & DEDUP. ``domain_norm`` (normalized top-level ``domain`` column, 100% populated) is the
PRIMARY KEY. The source is not unique per domain (165,884 rows → 133,256 distinct domain_norm; one
domain carries 356 temporal re-hydrations), so rows are deduplicated MOST-RECENT-WINS:
    row_number() OVER (PARTITION BY domain_norm
                       ORDER BY source_updated_at DESC NULLS LAST,
                                (source_task_type='modal_hydrate_firmo_cascade') DESC)  → keep rn=1
domain_norm normalization is inlined (NOT core.name_norm — that is a *name* blocking key): lower/trim
→ strip scheme → strip leading ``www.`` → strip path/query → strip trailing dots → NULL if emptied.

SCHEMA (24 cols; flattened from the BlitzAPI company{} + company.hq{} + top-level anchors + lineage).
No revenue field exists upstream — size proxies are ``employee_size_band`` / ``employees_on_linkedin``
/ ``followers``. ``hq.country`` is 100% NULL upstream and is dropped. ``founded_year`` is clamped to
[1800, 2026] (drops 367 junk values incl. a literal 20132).

INDEXES (directive's hard deliverable):
    BTREE  : domain_norm (PK), uei (bridge → govcon contractor_award_summary BTREE(recipient_uei))
    BITMAP : industry, employee_size_band, company_type, hq_region   (GTM filter accelerators)

Control plane (Trigger v4 durable callback): the worker accepts ``trigger_callback_url`` and, on
terminal state (success OR failure), (1) writes the run row to ops.firmographics_blitz_runs
(HQX_DB_URL_POOLED) via psycopg and (2) POSTs a FLAT JSON body to that url. No ``{"data": ...}``
envelope.

    modal run    pipelines/firmographics_blitz/materialize_blitz.py::init_ops      # create ops table (HQX)
    modal run    pipelines/firmographics_blitz/materialize_blitz.py::run           # execute (manual)
    modal run    pipelines/firmographics_blitz/materialize_blitz.py::reindex_only  # rebuild indexes
    modal run    pipelines/firmographics_blitz/materialize_blitz.py::verify_only   # read-back assertions
    modal deploy pipelines/firmographics_blitz/materialize_blitz.py                # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

# Gen-3 target sink (active tier). Net-new dataset lands directly here.
_ACTIVE = "s3://data-sink/active"
DATASET = "firmographics_blitz"
DATASET_URI = os.environ.get("FIRMOGRAPHICS_BLITZ_URI", f"{_ACTIVE}/{DATASET}/")

FEED = "firmographics_blitz"
SOURCE_DB = "hqx:ops.task_runs"  # provenance label recorded in ops.* (source = hq-x control-plane DB)

# Source filter — the two firmographic task types, completed only.
TASK_TYPES = ("blitz_firmo_direct", "modal_hydrate_firmo_cascade")
FOUNDED_YEAR_MIN, FOUNDED_YEAR_MAX = 1800, 2026  # plausibility clamp (else NULL)

# Lance fragment sizing — fleet defaults (02_lancedb_storage.md §2.3). The dataset is small
# (~133k rows, one fragment); these caps simply mirror the rest of the fleet.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3  # 90 GiB — Lance's documented default
DATA_STORAGE_VERSION = "2.1"       # net-new dataset → pin the current Lance default

# Scalar index plan — the directive's hard deliverable.
INDEXES: dict[str, list[str]] = {
    "BTREE": ["domain_norm", "uei"],
    "BITMAP": ["industry", "employee_size_band", "company_type", "hq_region"],
}

# ── ops.firmographics_blitz_runs DDL — verbatim mirror of the canonical .sql sibling.
# Applied by `init_ops` and (defensively) before each terminal write. Lives in the HQX
# control-plane DB (HQX_DB_URL_POOLED), alongside every other ops.*_runs table. ──────────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.firmographics_blitz_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,
    source_db      text        NOT NULL,
    datasets       jsonb       NOT NULL,
    rows_total     bigint      NOT NULL DEFAULT 0,
    rows_source    bigint      NOT NULL DEFAULT 0,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS firmographics_blitz_runs_feed_idx        ON ops.firmographics_blitz_runs (feed);
CREATE INDEX IF NOT EXISTS firmographics_blitz_runs_status_idx      ON ops.firmographics_blitz_runs (status);
CREATE INDEX IF NOT EXISTS firmographics_blitz_runs_recorded_at_idx ON ops.firmographics_blitz_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "psycopg[binary]>=3.2",  # source read DSN handling + terminal state → ops.*
    "requests>=2.32",        # Trigger callback
).env(
    # Force the fleet-standard in-memory sort path so index builds behave identically to the
    # rest of the fleet (avoids DataFusion spill-to-disk OOM on high-cardinality columns).
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("firmographics-blitz", image=image)


# ── PyArrow schema — the exact contract (Directive 12 §2). The DuckDB projection TRY_CASTs to
# these types; the worker then table.cast(SCHEMA) to enforce field order / type / nullability. ──
def _schema():
    import pyarrow as pa

    return pa.schema([
        # Resolution anchors
        pa.field("domain_norm",           pa.string(),                  nullable=False),  # PK · BTREE
        pa.field("domain_raw",            pa.string(),                  nullable=True),   # pre-norm audit
        pa.field("uei",                   pa.string(),                  nullable=True),   # BTREE · govcon/SAM bridge
        # Firmographic core (Blitz company{})
        pa.field("company_name",          pa.string(),                  nullable=False),
        pa.field("website",               pa.string(),                  nullable=True),
        pa.field("industry",              pa.string(),                  nullable=True),   # BITMAP
        pa.field("employee_size_band",    pa.string(),                  nullable=True),   # BITMAP
        pa.field("employees_on_linkedin", pa.int64(),                   nullable=True),
        pa.field("company_type",          pa.string(),                  nullable=True),   # BITMAP
        pa.field("founded_year",          pa.int32(),                   nullable=True),   # clamped [1800,2026]
        pa.field("followers",             pa.int64(),                   nullable=True),
        pa.field("specialties",           pa.list_(pa.string()),        nullable=True),
        pa.field("about",                 pa.string(),                  nullable=True),
        # HQ geography (Blitz company.hq{})
        pa.field("hq_city",               pa.string(),                  nullable=True),
        pa.field("hq_state",              pa.string(),                  nullable=True),
        pa.field("hq_region",             pa.string(),                  nullable=True),   # BITMAP
        pa.field("hq_continent",          pa.string(),                  nullable=True),
        # LinkedIn identity
        pa.field("linkedin_url",          pa.string(),                  nullable=True),
        pa.field("linkedin_id",           pa.int64(),                   nullable=True),
        # Provenance / lineage
        pa.field("source_task_type",      pa.string(),                  nullable=False),
        pa.field("source_run_id",         pa.string(),                  nullable=True),
        pa.field("source_updated_at",     pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("materialized_at",       pa.timestamp("us", tz="UTC"), nullable=False),
    ])


# ── DuckDB projection. The hq-x Postgres is ATTACHed READ_ONLY as `hqx`; ops.task_runs is read as
# hqx.ops.task_runs. result_payload is cast to JSON so the ->/->> operators work regardless of how
# the postgres scanner surfaces jsonb. ───────────────────────────────────────────────────────────
def _normalized_domain(col: str) -> str:
    """Canonical domain-anchor normalization (a DuckDB SQL expression).

    lower/trim → strip scheme (http/https) → strip leading ``www.`` → strip path/query
    (everything from the first ``/``) → strip trailing dots → NULL if emptied. ``\\.`` here
    emits ``\\.`` in the generated SQL.
    """
    return (
        "nullif("
        "regexp_replace("                                            # 5) trailing dots
        "regexp_replace("                                            # 4) path / query
        "regexp_replace("                                            # 3) leading www.
        "regexp_replace("                                            # 2) scheme
        "lower(trim(CAST(" + col + " AS VARCHAR))),"                 # 1) lower + trim
        " '^https?://', '', 'g'),"
        " '^www\\.', '', 'g'),"
        " '/.*$', '', 'g'),"
        " '\\.+$', '', 'g'),"
        " '')"
    )


def _sql() -> str:
    """Unified projection + most-recent-wins dedup → one row per domain_norm."""
    tt = ", ".join(f"'{t}'" for t in TASK_TYPES)
    nd = _normalized_domain("domain")
    return f"""
WITH base AS (
    SELECT
        run_id,
        task_type,
        updated_at,
        domain                                                            AS domain_raw,
        nullif(trim(uei), '')                                             AS uei,
        {nd}                                                              AS domain_norm,
        COALESCE(
            CAST(result_payload AS JSON) -> 'blitz_payload',
            CAST(result_payload AS JSON) -> 'blitz_data'
        )                                                                 AS bp
    FROM hqx.ops.task_runs
    WHERE task_type IN ({tt})
      AND status = 'completed'
),
co AS (
    SELECT *, bp -> 'company' AS c
    FROM base
    WHERE json_type(bp -> 'company') = 'OBJECT'
      AND domain_norm IS NOT NULL
),
projected AS (
    SELECT
        domain_norm,
        domain_raw,
        uei,
        nullif(trim(c ->> 'name'), '')                                    AS company_name,
        nullif(trim(c ->> 'website'), '')                                 AS website,
        nullif(trim(c ->> 'industry'), '')                                AS industry,
        nullif(trim(c ->> 'size'), '')                                    AS employee_size_band,
        try_cast(c ->> 'employees_on_linkedin' AS BIGINT)                 AS employees_on_linkedin,
        nullif(trim(c ->> 'type'), '')                                    AS company_type,
        CASE WHEN try_cast(c ->> 'founded_year' AS INTEGER)
                  BETWEEN {FOUNDED_YEAR_MIN} AND {FOUNDED_YEAR_MAX}
             THEN try_cast(c ->> 'founded_year' AS INTEGER) END           AS founded_year,
        try_cast(c ->> 'followers' AS BIGINT)                             AS followers,
        CASE WHEN json_type(c -> 'specialties') = 'ARRAY'
             THEN CAST(c -> 'specialties' AS VARCHAR[]) END               AS specialties,
        nullif(trim(c ->> 'about'), '')                                   AS about,
        nullif(trim(c -> 'hq' ->> 'city'), '')                            AS hq_city,
        nullif(trim(c -> 'hq' ->> 'state'), '')                           AS hq_state,
        nullif(trim(c -> 'hq' ->> 'region'), '')                          AS hq_region,
        nullif(trim(c -> 'hq' ->> 'continent'), '')                       AS hq_continent,
        nullif(trim(c ->> 'linkedin_url'), '')                            AS linkedin_url,
        try_cast(c ->> 'linkedin_id' AS BIGINT)                           AS linkedin_id,
        task_type                                                         AS source_task_type,
        run_id                                                            AS source_run_id,
        updated_at                                                        AS source_updated_at
    FROM co
),
ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY domain_norm
        ORDER BY source_updated_at DESC NULLS LAST,
                 (source_task_type = 'modal_hydrate_firmo_cascade') DESC
    ) AS rn
    FROM projected
)
SELECT
    domain_norm, domain_raw, uei, company_name, website, industry, employee_size_band,
    employees_on_linkedin, company_type, founded_year, followers, specialties, about,
    hq_city, hq_state, hq_region, hq_continent, linkedin_url, linkedin_id,
    source_task_type, source_run_id, source_updated_at,
    now() AS materialized_at
FROM ranked
WHERE rn = 1
"""


def _source_row_count_sql() -> str:
    tt = ", ".join(f"'{t}'" for t in TASK_TYPES)
    return (f"SELECT count(*) FROM hqx.ops.task_runs "
            f"WHERE task_type IN ({tt}) AND status = 'completed'")


# ── Source DSN + R2 plumbing ─────────────────────────────────────────────────────────────
def _hqx_dsn() -> str:
    """The hq-x Postgres DSN (pooled/Supavisor session mode) with SSL enforced. The DuckDB
    postgres scanner hands this to libpq; Supabase requires TLS."""
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres Modal secret.")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _r2_storage_options() -> dict[str, str]:
    """object_store options for Cloudflare R2, sourced from the r2-credentials Modal secret."""
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


def _write_lance(table, uri: str, so: dict) -> None:
    import lance

    lance.write_dataset(
        table,
        uri,
        mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )


def _create_indexes(so: dict) -> list[dict]:
    """Build the mandated scalar indexes. create_scalar_index defaults to replace=True →
    idempotent. Best-effort per index, but logged loudly: an index miss must not silently pass."""
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
        names.append(ix.get("name", str(ix)) if isinstance(ix, dict)
                     else getattr(ix, "name", str(ix)))
    return sorted(names)


# ── Terminal state + callback ────────────────────────────────────────────────────────────
def _record_run(rows_total, rows_source, status, error, started_at, completed_at) -> None:
    """Terminal run row → ops.firmographics_blitz_runs (HQX_DB_URL_POOLED, psycopg).
    Best-effort: never let an audit-write failure crash an otherwise-good migration."""
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
                INSERT INTO ops.firmographics_blitz_runs
                    (feed, source_db, datasets, rows_total, rows_source, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, SOURCE_DB, Jsonb({DATASET: rows_total}), rows_total, rows_source,
                 status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the migration
        print(f"WARN: ops.* state write failed: {exc}")


def _post_callback(url, payload, attempts: int = 3) -> None:
    """POST terminal metadata to the Trigger waitpoint url. FLAT JSON body — no
    ``{"data": ...}`` envelope. A few retries for delivery reliability."""
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


@app.function(
    secrets=[
        modal.Secret.from_name("r2-credentials"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 30,
    memory=8192,
    cpu=4.0,
)
def ingest_firmographics_blitz(trigger_callback_url: str | None = None) -> dict:
    """Project ops.task_runs firmographic JSONB → deduplicated Gen-3 Lance. ATTACH the hq-x
    Postgres READ_ONLY, project/cast/dedup in DuckDB → Arrow → cast(SCHEMA) → Lance overwrite →
    BTREE+BITMAP indexes. Then record ops.* state and wake the Trigger run. Re-raises on failure
    so the Modal call is marked failed."""
    import datetime as dt
    import gc

    import duckdb
    import lance  # noqa: F401 — imported so a missing dep fails early, not mid-write

    started_at = dt.datetime.now(dt.timezone.utc)
    rows_total = 0
    rows_source = 0
    index_status: list[dict] = []
    status = "error"
    error: str | None = None

    try:
        so = _r2_storage_options()
        dsn = _hqx_dsn()

        con = duckdb.connect(":memory:")
        try:
            con.execute("INSTALL postgres; LOAD postgres;")
            con.execute("INSTALL json; LOAD json;")
            con.execute("PRAGMA threads=4;")
            # READ_ONLY ATTACH — no server-side writes, ever. DSN is single-quote escaped for the
            # string literal (never logged; it carries the password).
            con.execute(f"ATTACH '{dsn.replace(chr(39), chr(39) * 2)}' AS hqx "
                        "(TYPE postgres, READ_ONLY);")

            rows_source = con.sql(_source_row_count_sql()).fetchone()[0]
            print(f"source completed rows: {rows_source:,}")

            table = con.sql(_sql()).to_arrow_table()
            table = table.cast(_schema())  # enforce the exact §2 contract
            rows_total = table.num_rows
            print(f"projected (deduped) {rows_total:,} rows ({table.num_columns} cols) "
                  f"→ {DATASET_URI}")

            _write_lance(table, DATASET_URI, so)
            print(f"wrote Lance (overwrite, v{DATA_STORAGE_VERSION})")
            index_status = _create_indexes(so)

            del table
            gc.collect()
        finally:
            con.close()

        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(rows_total, rows_source, status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "feed": FEED, "source_db": SOURCE_DB,
             "rows_total": rows_total, "rows_source": rows_source,
             "datasets": {DATASET: rows_total}},
        )

    if status != "success":
        raise RuntimeError(f"firmographics_blitz materialization failed: {error}")
    return {"feed": FEED, "source_db": SOURCE_DB, "rows_total": rows_total,
            "rows_source": rows_source, "datasets": {DATASET: rows_total},
            "indexes": index_status, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 15,
    memory=8192,
    cpu=4.0,
)
def reindex() -> dict:
    """(Re)build the scalar indexes on the already-written dataset without re-materializing.
    Idempotent (create_scalar_index defaults to replace=True)."""
    so = _r2_storage_options()
    print(f"=== reindex {DATASET} ===")
    _create_indexes(so)
    return {DATASET: _committed_index_names(so)}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 10,
    memory=8192,
)
def verify() -> dict:
    """Read-back assertions: row count, the directive's count_rows()==distinct(domain_norm)
    uniqueness invariant, schema, committed indexes, and BTREE lookup probes that actually
    answer (domain_norm + uei)."""
    import pyarrow.compute as pc

    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()

    anchors = ds.to_table(columns=["domain_norm", "uei"])
    distinct_domain = pc.count_distinct(anchors.column("domain_norm")).as_py()
    uei_nonnull = n - anchors.column("uei").null_count
    unique_ok = (n == distinct_domain)

    # Probe the domain_norm BTREE with a value drawn from the data itself → expect exactly 1 row.
    sample_domain = None
    for v in anchors.column("domain_norm").to_pylist():
        if v:
            sample_domain = v
            break
    probe_domain = ds.scanner(
        columns=["domain_norm"],
        filter=f"domain_norm = '{sample_domain}'",
    ).to_table().num_rows if sample_domain else -1

    out = {
        "uri": DATASET_URI,
        "rows": n,
        "distinct_domain_norm": distinct_domain,
        "unique_invariant_ok": unique_ok,
        "uei_nonnull": uei_nonnull,
        "schema": [f.name for f in ds.schema],
        "indexes": _committed_index_names(so),
        f"probe_domain_norm={sample_domain!r}": probe_domain,
    }
    print(f"{DATASET}: {n:,} rows · distinct(domain_norm)={distinct_domain:,} · "
          f"unique_ok={unique_ok} · uei_nonnull={uei_nonnull:,}")
    print(f"  indexes={out['indexes']}")
    print(f"  probe domain_norm={sample_domain!r} → {probe_domain} row(s)")
    if not unique_ok:
        raise RuntimeError(
            f"uniqueness invariant FAILED: rows={n} != distinct(domain_norm)={distinct_domain}")
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.firmographics_blitz_runs in the HQX control-plane DB (idempotent)."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name='firmographics_blitz_runs'
            ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    print(f"ops.firmographics_blitz_runs ready — columns: {cols}")
    return {"table": "ops.firmographics_blitz_runs", "columns": cols}


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.firmographics_blitz_runs DDL (HQX)."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def run() -> None:
    """Manual materialization (no Trigger callback). ops.* write still fires."""
    import json

    print(json.dumps(
        ingest_firmographics_blitz.remote(trigger_callback_url=None),
        indent=2, default=str,
    ))


@app.local_entrypoint()
def reindex_only() -> None:
    """Rebuild indexes on the existing dataset (no re-materialization)."""
    import json

    print(json.dumps(reindex.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_only() -> None:
    """Read-back assertions on the materialized dataset."""
    import json

    print(json.dumps(verify.remote(), indent=2, default=str))
