"""Compute worker — standalone Gen-3 materialization of the LeadMagic firmographics reference grain.

STAGE 2 of 2. The ``firmographics-leadmagic-capture`` worker
(pipelines/firmographics_leadmagic/find_company_leadmagic.py) CAPTURES verbatim LeadMagic
/company-search payloads into ops.firmographics_leadmagic_capture. This worker projects the
highest-value firmographic fields out of that JSONB and materializes them as a STANDALONE native
Gen-3 Lance dataset — the LeadMagic sibling of firmographics_blitz, deliberately NOT merged into
gtm ``companies`` or firmographics_blitz (each vendor is its own SoR; a downstream crosswalk unions
them). Endpoint-less; spawned by the Universal Dispatcher (core/modal_dispatcher.py) or run
manually. DuckDB does 100% of the transform, Lance is written straight to R2 — no catalog.

SOURCE (live hq-x Postgres, read-only via the DuckDB postgres scanner):
    ops.firmographics_leadmagic_capture WHERE company_status = 'found'
    The verbatim /company-search response is in ``leadmagic_raw`` (loosely typed upstream — the
    LeadMagic OpenAPI only guarantees ``credits_consumed`` on the 200 body), so every firmographic
    field is read DEFENSIVELY across camel/snake variants and the JSONB stays the SoR. The DSN is
    HQX_DB_URL_POOLED (the ``hqx-postgres`` Modal secret); DuckDB ATTACHes it READ_ONLY.

TARGET (Gen-3 system of record — native Lance v2.1, full-snapshot overwrite):
    s3://data-sink/active/firmographics_leadmagic/

ANCHOR & DEDUP. ``company_key`` (string, non-null by construction) is the PRIMARY KEY: LeadMagic
``companyId`` when present, else ``domain:<domain_norm>``, else ``li:<linkedin_slug>``, else
``ent:<entity_id>`` — so a hit always has a stable identity even when the loosely-typed response
drops ``companyId``. Rows are deduplicated MOST-RECENT-WINS by ``source_captured_at``. ``domain_norm``
and ``linkedin_slug`` are the cross-plane BRIDGE keys, derived via core.web_norm (the fleet's single
source of truth for web-identity blocking keys — NOT re-inlined), so they exact-join firmographics_blitz
/ gtm companies / the SAM domain spine. The per-INPUT (domain|linkedin)→company mapping for every
enriched entity is preserved upstream in ops.firmographics_leadmagic_capture (this Lance grain is the
company, not the input).

INDEXES (hard deliverable):
    BTREE  : company_key (PK), company_id, domain_norm, linkedin_slug
    BITMAP : industry, employee_range, hq_country, hq_state   (GTM filter accelerators)

Control plane (Trigger v4 durable callback): on terminal state (success OR failure) the worker
(1) writes ops.firmographics_leadmagic_runs (HQX_DB_URL_POOLED) via psycopg and (2) POSTs a FLAT
JSON body to ``trigger_callback_url``.

    modal run    pipelines/firmographics_leadmagic/materialize_leadmagic.py::init_ops
    modal run    pipelines/firmographics_leadmagic/materialize_leadmagic.py::run
    modal run    pipelines/firmographics_leadmagic/materialize_leadmagic.py::reindex_only
    modal run    pipelines/firmographics_leadmagic/materialize_leadmagic.py::verify_only
    modal deploy pipelines/firmographics_leadmagic/materialize_leadmagic.py
"""

from __future__ import annotations

import os

import modal

from core.web_norm import _bare_host, linkedin_slug, normalized_domain

# Gen-3 target sink (active tier). Net-new dataset lands directly here.
_ACTIVE = "s3://data-sink/active"
DATASET = "firmographics_leadmagic"
DATASET_URI = os.environ.get("FIRMOGRAPHICS_LEADMAGIC_URI", f"{_ACTIVE}/{DATASET}/")

FEED = "firmographics_leadmagic"
SOURCE_DB = "hqx:ops.firmographics_leadmagic_capture"
FOUNDED_YEAR_MIN, FOUNDED_YEAR_MAX = 1800, 2026  # plausibility clamp (else NULL)

# Lance fragment sizing — fleet defaults (02_lancedb_storage.md §2.3).
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan — the hard deliverable.
INDEXES: dict[str, list[str]] = {
    "BTREE": ["company_key", "company_id", "domain_norm", "linkedin_slug"],
    "BITMAP": ["industry", "employee_range", "hq_country", "hq_state"],
}

# ── ops.firmographics_leadmagic_runs DDL — verbatim mirror of the canonical .sql sibling (the
# materialize ledger only; the capture worker owns the capture table + finder ledger). ──────────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.firmographics_leadmagic_runs (
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
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_runs_feed_idx        ON ops.firmographics_leadmagic_runs (feed);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_runs_status_idx      ON ops.firmographics_leadmagic_runs (status);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_runs_recorded_at_idx ON ops.firmographics_leadmagic_runs (recorded_at DESC);
"""

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",
        "lancedb>=0.15",
        "pylance>=7",
        "pyarrow>=17",
        "psycopg[binary]>=3.2",
        "requests>=2.32",
    )
    .env({"LANCE_BYPASS_SPILLING": "true"})
    .add_local_python_source("core.web_norm")  # ship the canonical web-identity key builders
)

app = modal.App("firmographics-leadmagic", image=image)


# ── PyArrow schema — the exact contract. The DuckDB projection TRY_CASTs to these types; the
# worker then table.cast(SCHEMA) to enforce field order / type / nullability. ───────────────────
def _schema():
    import pyarrow as pa

    return pa.schema([
        # Resolution anchors
        pa.field("company_key",       pa.string(),                  nullable=False),  # PK · BTREE
        pa.field("company_id",        pa.int64(),                   nullable=True),   # BTREE · LeadMagic native id
        pa.field("domain_norm",       pa.string(),                  nullable=True),   # BTREE · cross-plane bridge
        pa.field("linkedin_slug",     pa.string(),                  nullable=True),   # BTREE · cross-plane bridge
        pa.field("linkedin_url",      pa.string(),                  nullable=True),
        # Firmographic core (LeadMagic company{})
        pa.field("company_name",      pa.string(),                  nullable=True),
        pa.field("industry",          pa.string(),                  nullable=True),   # BITMAP
        pa.field("employee_count",    pa.int64(),                   nullable=True),
        pa.field("employee_range",    pa.string(),                  nullable=True),   # BITMAP
        pa.field("revenue",           pa.string(),                  nullable=True),
        pa.field("founded_year",      pa.int32(),                   nullable=True),   # clamped [1800,2026]
        pa.field("follower_count",    pa.int64(),                   nullable=True),
        pa.field("description",       pa.string(),                  nullable=True),
        pa.field("specialties",       pa.list_(pa.string()),        nullable=True),
        pa.field("competitors",       pa.list_(pa.string()),        nullable=True),
        # HQ geography
        pa.field("hq_city",           pa.string(),                  nullable=True),
        pa.field("hq_state",          pa.string(),                  nullable=True),   # BITMAP
        pa.field("hq_country",        pa.string(),                  nullable=True),   # BITMAP
        # Socials
        pa.field("twitter_url",       pa.string(),                  nullable=True),
        pa.field("facebook_url",      pa.string(),                  nullable=True),
        pa.field("logo_url",          pa.string(),                  nullable=True),
        # Provenance / lineage
        pa.field("source_entity_id",  pa.string(),                  nullable=True),
        pa.field("source_captured_at", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("materialized_at",   pa.timestamp("us", tz="UTC"), nullable=False),
    ])


def _sql() -> str:
    """Defensive projection + most-recent-wins dedup → one row per company_key. Firmographic
    fields are read across camel/snake key variants because the /company-search 200 body is not
    enumerated in LeadMagic's OpenAPI. ``lr`` is the verbatim response cast to JSON."""
    bare = _bare_host("input_domain")                       # bare host, computed ONCE as _bh
    dom = normalized_domain("_bh")                          # gated registrable domain over _bh
    li = linkedin_slug("COALESCE(b2b_profile_url, input_linkedin_url)")
    return f"""
WITH src AS (
    SELECT entity_id, company_id, b2b_profile_url, input_domain, input_linkedin_url,
           input_company_name, captured_at,
           CAST(leadmagic_raw AS JSON) AS lr
    FROM hqx.ops.firmographics_leadmagic_capture
    WHERE company_status = 'found'
),
norm AS (
    SELECT *, {bare} AS _bh, {li} AS linkedin_slug,
           COALESCE(b2b_profile_url, input_linkedin_url) AS linkedin_url
    FROM src
),
norm2 AS (
    SELECT *, {dom} AS domain_norm FROM norm
),
projected AS (
    SELECT
        COALESCE(
            CAST(company_id AS VARCHAR),
            'domain:' || domain_norm,
            'li:'     || linkedin_slug,
            'ent:'    || entity_id
        )                                                                 AS company_key,
        company_id,
        domain_norm,
        linkedin_slug,
        linkedin_url,
        nullif(trim(COALESCE(lr ->> 'companyName', lr ->> 'company_name',
                             lr ->> 'name', input_company_name)), '')     AS company_name,
        nullif(trim(lr ->> 'industry'), '')                               AS industry,
        try_cast(COALESCE(lr ->> 'employeeCount', lr ->> 'employee_count',
                          lr ->> 'employees') AS BIGINT)                  AS employee_count,
        nullif(trim(COALESCE(lr ->> 'employeeRange', lr ->> 'employee_range',
                             lr ->> 'size')), '')                         AS employee_range,
        nullif(trim(COALESCE(lr ->> 'revenue', lr ->> 'formattedRevenue',
                             lr ->> 'annualRevenue')), '')                AS revenue,
        CASE WHEN try_cast(COALESCE(lr ->> 'founded', lr ->> 'foundedYear',
                                    lr ->> 'founded_year') AS INTEGER)
                  BETWEEN {FOUNDED_YEAR_MIN} AND {FOUNDED_YEAR_MAX}
             THEN try_cast(COALESCE(lr ->> 'founded', lr ->> 'foundedYear',
                                    lr ->> 'founded_year') AS INTEGER) END AS founded_year,
        try_cast(COALESCE(lr ->> 'followerCount', lr ->> 'followers',
                          lr ->> 'follower_count') AS BIGINT)             AS follower_count,
        nullif(trim(COALESCE(lr ->> 'description', lr ->> 'about')), '')  AS description,
        CASE WHEN json_type(lr -> 'specialties') = 'ARRAY'
             THEN CAST(lr -> 'specialties' AS VARCHAR[]) END              AS specialties,
        CASE
            WHEN json_type(lr -> 'competitors') = 'ARRAY'
                THEN CAST(lr -> 'competitors' AS VARCHAR[])
            WHEN json_type(lr -> 'topCompetitors') = 'ARRAY'
                THEN CAST(lr -> 'topCompetitors' AS VARCHAR[])
        END                                                               AS competitors,
        nullif(trim(COALESCE(lr -> 'headquarters' ->> 'city', lr ->> 'city')), '')       AS hq_city,
        nullif(trim(COALESCE(lr -> 'headquarters' ->> 'state', lr ->> 'state')), '')      AS hq_state,
        nullif(trim(COALESCE(lr -> 'headquarters' ->> 'country', lr ->> 'country')), '')  AS hq_country,
        nullif(trim(COALESCE(lr ->> 'twitter_url', lr ->> 'twitterUrl')), '')  AS twitter_url,
        nullif(trim(COALESCE(lr ->> 'facebook_url', lr ->> 'facebookUrl')), '') AS facebook_url,
        nullif(trim(COALESCE(lr ->> 'logo_url', lr ->> 'logoUrl', lr ->> 'logo')), '') AS logo_url,
        entity_id                                                         AS source_entity_id,
        captured_at                                                       AS source_captured_at
    FROM norm2
),
ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY company_key
        ORDER BY source_captured_at DESC NULLS LAST
    ) AS rn
    FROM projected
)
SELECT
    company_key, company_id, domain_norm, linkedin_slug, linkedin_url, company_name, industry,
    employee_count, employee_range, revenue, founded_year, follower_count, description,
    specialties, competitors, hq_city, hq_state, hq_country, twitter_url, facebook_url, logo_url,
    source_entity_id, source_captured_at,
    now() AS materialized_at
FROM ranked
WHERE rn = 1
"""


def _source_row_count_sql() -> str:
    return ("SELECT count(*) FROM hqx.ops.firmographics_leadmagic_capture "
            "WHERE company_status = 'found'")


# ── Source DSN + R2 plumbing ─────────────────────────────────────────────────────────────
def _hqx_dsn() -> str:
    """hq-x Postgres DSN — SESSION pooler (:5432), SSL enforced. Same rationale as the Blitz
    materializer: the DuckDB postgres scanner leans on session-scoped state that transaction-mode
    pooling breaks, and this is a single-container, one-run-at-a-time materializer."""
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


def _write_lance(table, uri: str, so: dict) -> None:
    import lance

    lance.write_dataset(
        table, uri, mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
        max_bytes_per_file=MAX_BYTES_PER_FILE,
        storage_options=so,
    )


def _create_indexes(so: dict) -> list[dict]:
    """Build the mandated scalar indexes (create_scalar_index defaults to replace=True →
    idempotent). Best-effort per index, logged loudly."""
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
                INSERT INTO ops.firmographics_leadmagic_runs
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
def run(trigger_callback_url: str | None = None) -> dict:
    """Project ops.firmographics_leadmagic_capture → deduplicated Gen-3 Lance. ATTACH hq-x
    READ_ONLY, project/cast/dedup in DuckDB → Arrow → cast(SCHEMA) → Lance overwrite →
    BTREE+BITMAP indexes → ops.* state + Trigger callback. Re-raises on failure."""
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
            con.execute(f"ATTACH '{dsn.replace(chr(39), chr(39) * 2)}' AS hqx "
                        "(TYPE postgres, READ_ONLY);")

            rows_source = con.sql(_source_row_count_sql()).fetchone()[0]
            print(f"source found-capture rows: {rows_source:,}")

            table = con.sql(_sql()).to_arrow_table()
            table = table.cast(_schema())
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
        raise RuntimeError(f"firmographics_leadmagic materialization failed: {error}")
    return {"feed": FEED, "source_db": SOURCE_DB, "rows_total": rows_total,
            "rows_source": rows_source, "datasets": {DATASET: rows_total},
            "indexes": index_status, "status": status}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15,
              memory=8192, cpu=4.0)
def reindex() -> dict:
    """(Re)build the scalar indexes on the already-written dataset without re-materializing."""
    so = _r2_storage_options()
    print(f"=== reindex {DATASET} ===")
    _create_indexes(so)
    return {DATASET: _committed_index_names(so)}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10, memory=8192)
def verify() -> dict:
    """Read-back assertions: row count, count_rows()==distinct(company_key) uniqueness invariant,
    schema, committed indexes, and a company_key BTREE lookup probe."""
    import pyarrow.compute as pc

    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    n = ds.count_rows()

    anchors = ds.to_table(columns=["company_key", "company_id", "domain_norm"])
    distinct_key = pc.count_distinct(anchors.column("company_key")).as_py()
    domain_nonnull = n - anchors.column("domain_norm").null_count
    unique_ok = (n == distinct_key)

    sample_key = next((v for v in anchors.column("company_key").to_pylist() if v), None)
    probe_key = ds.scanner(
        columns=["company_key"],
        filter=f"company_key = '{sample_key.replace(chr(39), chr(39) * 2)}'",
    ).to_table().num_rows if sample_key else -1

    out = {
        "uri": DATASET_URI,
        "rows": n,
        "distinct_company_key": distinct_key,
        "unique_invariant_ok": unique_ok,
        "domain_norm_nonnull": domain_nonnull,
        "schema": [f.name for f in ds.schema],
        "indexes": _committed_index_names(so),
        f"probe_company_key={sample_key!r}": probe_key,
    }
    print(f"{DATASET}: {n:,} rows · distinct(company_key)={distinct_key:,} · "
          f"unique_ok={unique_ok} · domain_norm_nonnull={domain_nonnull:,}")
    print(f"  indexes={out['indexes']}")
    if not unique_ok:
        raise RuntimeError(
            f"uniqueness invariant FAILED: rows={n} != distinct(company_key)={distinct_key}")
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.firmographics_leadmagic_runs in the HQX control-plane DB (idempotent)."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    return {"tables": ["ops.firmographics_leadmagic_runs"]}


@app.local_entrypoint()
def init_ops() -> None:
    import json
    print(json.dumps(apply_ops_ddl.remote(), indent=2, default=str))


@app.local_entrypoint()
def run_() -> None:
    import json
    print(json.dumps(run.remote(), indent=2, default=str))


@app.local_entrypoint()
def reindex_only() -> None:
    import json
    print(json.dumps(reindex.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_only() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
