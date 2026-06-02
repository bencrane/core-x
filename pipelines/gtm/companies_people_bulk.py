"""Compute worker — minimal Gen-3 materialization of the GTM identity graph.

Part of the ``gtm-company-people`` Modal app. Endpoint-less; spawned by the Universal
Dispatcher (core/modal_dispatcher.py). Clean-room data plane: DuckDB does 100% of the
transform, Lance is written straight to R2. No Iceberg, no Polaris, no catalog round-trip.

WHY THIS WORKER EXISTS (Directive 8)
    The core go-to-market data lives in the legacy PostgreSQL ``gtm`` schema of the DEX
    Supabase project (the same DB that backs the data-factory ``gtm.*`` tables). This worker
    migrates the two load-bearing entity grains — companies and people — out of Postgres and
    materializes them as native Gen-3 Lance datasets in the active data sink. It builds a
    *minimal* unified identity graph: every business entity is a company, every contact is a
    person, and each row tracks its source lineage verbatim. NO external API payloads are
    merged and NO enrichment engine runs here — this is a faithful, minimal projection.

SOURCE (live Postgres, read-only via the DuckDB postgres scanner):
    DEX Supabase · schema ``gtm``
      gtm.companies  (~748 rows)   — canonical companies, PK ``id`` (uuid)
      gtm.people     (~7,739 rows) — canonical contacts, PK ``id`` (uuid),
                                     FK ``company_id`` → gtm.companies.id (verified: 0 orphans)
    The DSN is the DEX pooled (Supavisor session-mode) URL, supplied by the ``dex-postgres``
    Modal secret as ``DEX_DB_URL_POOLED``. DuckDB ATTACHes it READ_ONLY — no server-side
    compute, the transform boundary is 100% local DuckDB.

TARGET (Gen-3 system of record — native Lance v2.1, full-snapshot overwrite):
    s3://data-sink/active/companies/
    s3://data-sink/active/people/

MINIMAL SCHEMAS (every identifier is STRICT VARCHAR — uuid rendered as its canonical
hyphenated text, the fleet convention; string equality is the join semantics):

    companies
        company_id            uuid → VARCHAR   (primary key)
        company_name          VARCHAR          (nullable)
        normalized_domain     VARCHAR          (the core anchor; null ONLY when missing)
        company_linkedin_url  VARCHAR          (nullable)
        source_platform       VARCHAR          (lineage — e.g. prospeo-parallel.ai, sfnet, exa-all)

    people
        contact_id            uuid → VARCHAR   (primary key)
        company_id            uuid → VARCHAR   (foreign key → companies.company_id)
        normalized_domain     VARCHAR          (DENORMALIZED from the person's company, for
                                                instant domain lookups — derived from the joined
                                                company row so it matches the company anchor exactly)
        full_name             VARCHAR
        first_name            VARCHAR          (nullable — split name where present)
        last_name             VARCHAR          (nullable — split name where present)
        title                 VARCHAR          (nullable)
        person_linkedin_url   VARCHAR          (nullable — the person's linkedin.com URL)
        source_platform       VARCHAR          (lineage)

NORMALIZED_DOMAIN is the anchor. Built inline (NOT from core.name_norm, which is a *name*
blocking key, not a domain rule): lower/trim → strip scheme → strip leading ``www.`` →
strip path/query → strip trailing dots → NULL if emptied. Inlined deliberately — there is
exactly one consumer today; if a second appears, lift it to core/ the way name_norm.py was
(the fleet's inline-then-extract precedent). NOTE: ``normalized_domain`` is the *anchor* but
is NOT unique in the source (743 non-null domains → 639 distinct; e.g. jpmorgan.com appears
3× across exa-all / sfnet / prospeo). The directive mandates a *minimal* projection with NO
merging, so all 748 company rows are preserved 1:1 (PK = company_id) and the BTREE on
normalized_domain is a lookup index, not a uniqueness constraint. Domain-level dedup is a
downstream resolution concern, explicitly out of scope here.

person_linkedin_url SOURCING (verified, 2026-06-02). The directive flags
``raw_sfnet_people_enriched_with_linkedin`` as a possible source. Structural probe of all
6,823 rows × every key: that table carries ZERO person linkedin.com/in/ URLs — its only
linkedin field is ``company_linkedin_url`` (company pages) and its ``person_profile_url`` is
an sfnet.com directory URL (which equals gtm.people.source_external_id, not a LinkedIn).
The authoritative person LinkedIn is therefore ``gtm.people.linkedin_url`` (954 populated:
34 sfnet + 920 csv); there is nothing additional to pull from the raw table for the person
grain, so it is not read. Likewise first/last name come straight from gtm.people
(complete for all sfnet + csv people; the only people lacking split names are the 56 elfa
rows, which have no counterpart in any raw sfnet source).

INDEXES (directive's hard deliverable — scalar BTREE only, kept minimal):
    companies : BTREE(normalized_domain)
    people    : BTREE(company_id), BTREE(normalized_domain)

Control plane (Trigger v4 durable callback): the worker accepts ``trigger_callback_url`` and,
on terminal state (success OR failure), (1) writes the run row to
``ops.companies_migration_runs`` (HQX_DB_URL_POOLED — the fleet control-plane DB, where every
other ops.*_runs table lives) via psycopg and (2) POSTs a FLAT JSON body to that url to wake
the suspended Trigger run. No ``{"data": ...}`` envelope.

    modal run    pipelines/gtm/companies_people_bulk.py::init_ops    # create ops table (HQX)
    modal run    pipelines/gtm/companies_people_bulk.py::run         # execute the migration (manual)
    modal run    pipelines/gtm/companies_people_bulk.py::run --only companies   # one dataset
    modal run    pipelines/gtm/companies_people_bulk.py::reindex_only           # rebuild indexes
    modal run    pipelines/gtm/companies_people_bulk.py::verify                 # read-back assertions
    modal deploy pipelines/gtm/companies_people_bulk.py              # make dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

# Gen-3 target sink (active tier). Net-new datasets land directly here.
_ACTIVE = "s3://data-sink/active"
DATASETS = ("companies", "people")
DATASET_URI = {
    "companies": os.environ.get("GTM_COMPANIES_URI", f"{_ACTIVE}/companies/"),
    "people": os.environ.get("GTM_PEOPLE_URI", f"{_ACTIVE}/people/"),
}

FEED = "gtm_companies_people"
SOURCE_DB = "dex:gtm"  # provenance label recorded in ops.* (DEX Supabase, schema gtm)

# Lance fragment sizing — fleet defaults (02_lancedb_storage.md §2.3). Both datasets are
# tiny (one fragment each); these caps simply mirror the rest of the fleet.
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3  # 90 GiB — Lance's documented default
# Net-new datasets → pin the current Lance default (directive: data_storage_version=2.1).
DATA_STORAGE_VERSION = "2.1"

# Scalar index plan — the directive's hard deliverable. BTREE only; no speculative BITMAPs.
INDEXES: dict[str, dict[str, list[str]]] = {
    "companies": {"BTREE": ["normalized_domain"]},
    "people": {"BTREE": ["company_id", "normalized_domain"]},
}

# ── ops.companies_migration_runs DDL — verbatim mirror of the canonical .sql sibling.
# Applied by `init_ops` and (defensively) before each terminal write. Lives in the HQX
# control-plane DB (HQX_DB_URL_POOLED), alongside every other ops.*_runs table. ──────────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.companies_migration_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,
    source_db      text        NOT NULL,
    datasets       jsonb       NOT NULL,
    rows_total     bigint      NOT NULL DEFAULT 0,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS companies_migration_runs_feed_idx        ON ops.companies_migration_runs (feed);
CREATE INDEX IF NOT EXISTS companies_migration_runs_status_idx      ON ops.companies_migration_runs (status);
CREATE INDEX IF NOT EXISTS companies_migration_runs_recorded_at_idx ON ops.companies_migration_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "psycopg[binary]>=3.2",  # source read DSN handling + terminal state → ops.*
    "requests>=2.32",        # Trigger callback
).env(
    # Tiny datasets never spill, but keep the fleet-standard in-memory sort path pinned so
    # index builds behave identically to the rest of the fleet.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("gtm-company-people", image=image)


# ── DuckDB projections. The DEX Postgres is ATTACHed READ_ONLY as `dex`; tables are read as
# dex.gtm.<table>. Every identifier is STRICT VARCHAR (uuid → canonical text); string fields
# are trimmed and emptied-to-NULL. ──────────────────────────────────────────────────────
def _normalized_domain(col: str) -> str:
    """Canonical domain-anchor normalization (a DuckDB SQL expression).

    lower/trim → strip scheme (http/https) → strip leading ``www.`` → strip path/query
    (everything from the first ``/``) → strip trailing dots → NULL if emptied. ``col`` is
    interpolated verbatim, so it may be a bare column (``"domain"``) or a qualified one
    (``"c.domain"``). ``\\.`` here emits ``\\.`` in the generated SQL.
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


def _sql_companies() -> str:
    return f"""
SELECT
    CAST(id AS VARCHAR)                  AS company_id,
    nullif(trim(company_name), '')       AS company_name,
    {_normalized_domain('domain')}       AS normalized_domain,
    nullif(trim(linkedin_url), '')       AS company_linkedin_url,
    nullif(trim(source), '')             AS source_platform
FROM dex.gtm.companies
"""


def _sql_people() -> str:
    # normalized_domain is denormalized from the person's company (LEFT JOIN on the FK, which
    # is verified complete — 0 orphans), so a person's anchor matches its company's exactly.
    return f"""
SELECT
    CAST(p.id AS VARCHAR)                 AS contact_id,
    CAST(p.company_id AS VARCHAR)         AS company_id,
    {_normalized_domain('c.domain')}      AS normalized_domain,
    nullif(trim(p.full_name), '')         AS full_name,
    nullif(trim(p.first_name), '')        AS first_name,
    nullif(trim(p.last_name), '')         AS last_name,
    nullif(trim(p.title), '')             AS title,
    nullif(trim(p.linkedin_url), '')      AS person_linkedin_url,
    nullif(trim(p.source), '')            AS source_platform
FROM dex.gtm.people p
LEFT JOIN dex.gtm.companies c ON c.id = p.company_id
"""


SQL_BUILDER = {
    "companies": _sql_companies,
    "people": _sql_people,
}


# ── Source DSN + R2 plumbing ─────────────────────────────────────────────────────────────
def _dex_dsn() -> str:
    """The DEX Postgres DSN (pooled/Supavisor session mode) with SSL enforced. The DuckDB
    postgres scanner hands this to libpq; Supabase requires TLS."""
    dsn = os.environ.get("DEX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("DEX_DB_URL_POOLED not set in the dex-postgres Modal secret.")
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


def _create_indexes(ds_name: str, so: dict) -> list[dict]:
    """Build the mandated BTREE scalar indexes for one dataset across all fragments.
    create_scalar_index defaults to replace=True → idempotent. Best-effort per index, but
    logged loudly: an index miss must not silently pass."""
    import lance

    ds = lance.dataset(DATASET_URI[ds_name], storage_options=so)
    out: list[dict] = []
    for col in INDEXES[ds_name].get("BTREE", []):
        try:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"  BTREE  ✓ {ds_name}.{col}")
            out.append({"col": col, "type": "BTREE", "ok": True})
        except Exception as exc:  # noqa: BLE001 — an index miss must not fail a good load
            print(f"  BTREE  ✗ {ds_name}.{col}: {exc}")
            out.append({"col": col, "type": "BTREE", "ok": False, "error": str(exc)})
    return out


def _committed_index_names(ds_name: str, so: dict) -> list[str]:
    import lance

    ds = lance.dataset(DATASET_URI[ds_name], storage_options=so)
    names = []
    for ix in ds.list_indices():
        names.append(ix.get("name", str(ix)) if isinstance(ix, dict)
                     else getattr(ix, "name", str(ix)))
    return sorted(names)


# ── Terminal state + callback ────────────────────────────────────────────────────────────
def _record_run(source_db, datasets, rows_total, status, error,
                started_at, completed_at) -> None:
    """Terminal run row → ops.companies_migration_runs (HQX_DB_URL_POOLED, psycopg).
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
                INSERT INTO ops.companies_migration_runs
                    (feed, source_db, datasets, rows_total, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, source_db, Jsonb(datasets), rows_total, status, error,
                 started_at, completed_at),
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
        modal.Secret.from_name("dex-postgres"),
        modal.Secret.from_name("hqx-postgres"),
    ],
    timeout=60 * 30,
    memory=8192,
    cpu=4.0,
)
def ingest_gtm_company_people(
    only: str = "",
    trigger_callback_url: str | None = None,
) -> dict:
    """Migrate gtm.companies + gtm.people → Gen-3 Lance. ATTACH the DEX Postgres READ_ONLY,
    project/cast each grain in DuckDB → Arrow → Lance overwrite → BTREE indexes. Then record
    ops.* state and wake the Trigger run. ``only`` restricts to one dataset. Re-raises on
    failure so the Modal call is marked failed."""
    import datetime as dt
    import gc

    import duckdb
    import lance  # noqa: F401 — imported so a missing dep fails early, not mid-write

    started_at = dt.datetime.now(dt.timezone.utc)
    targets = [only] if only else list(DATASETS)
    for t in targets:
        if t not in DATASET_URI:
            raise ValueError(f"unknown dataset {t!r}; expected one of {list(DATASETS)}")

    counts: dict[str, int] = {}
    index_status: dict[str, list] = {}
    status = "error"
    error: str | None = None

    try:
        so = _r2_storage_options()
        dsn = _dex_dsn()

        con = duckdb.connect(":memory:")
        try:
            con.execute("INSTALL postgres; LOAD postgres;")
            con.execute("PRAGMA threads=4;")
            # READ_ONLY ATTACH — no server-side writes, ever. DSN is single-quote escaped for
            # the string literal (never logged; it carries the password).
            con.execute(f"ATTACH '{dsn.replace(chr(39), chr(39) * 2)}' AS dex "
                        "(TYPE postgres, READ_ONLY);")
            for ds_name in targets:
                print(f"\n=== {ds_name} ===")
                table = con.sql(SQL_BUILDER[ds_name]()).to_arrow_table()
                counts[ds_name] = table.num_rows
                print(f"  read {table.num_rows:,} rows ({table.num_columns} cols) "
                      f"→ {DATASET_URI[ds_name]}")

                _write_lance(table, DATASET_URI[ds_name], so)
                print(f"  wrote Lance (overwrite, v{DATA_STORAGE_VERSION})")
                index_status[ds_name] = _create_indexes(ds_name, so)

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
        rows_total = int(sum(counts.values()))
        _record_run(SOURCE_DB, counts, rows_total, status, error, started_at, completed_at)
        _post_callback(
            trigger_callback_url,
            {"status": status, "feed": FEED, "source_db": SOURCE_DB,
             "rows_total": rows_total, "datasets": counts},
        )

    if status != "success":
        raise RuntimeError(f"gtm companies/people migration failed: {error}")
    return {"feed": FEED, "source_db": SOURCE_DB, "rows_total": int(sum(counts.values())),
            "datasets": counts, "indexes": index_status, "status": status}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 15,
    memory=8192,
    cpu=4.0,
)
def reindex(only: str = "") -> dict:
    """(Re)build the BTREE indexes on already-written datasets without re-migrating.
    Idempotent (create_scalar_index defaults to replace=True)."""
    so = _r2_storage_options()
    targets = [only] if only else list(DATASETS)
    out = {}
    for ds_name in targets:
        if ds_name not in INDEXES:
            continue
        print(f"=== reindex {ds_name} ===")
        _create_indexes(ds_name, so)
        out[ds_name] = _committed_index_names(ds_name, so)
    return out


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 10,
    memory=8192,
)
def verify() -> dict:
    """Read-back assertions against the materialized datasets: row count, schema, committed
    indexes, and a probe that the normalized_domain BTREE actually answers a lookup."""
    import lance

    so = _r2_storage_options()
    out: dict[str, dict] = {}
    for ds_name in DATASETS:
        ds = lance.dataset(DATASET_URI[ds_name], storage_options=so)
        n = ds.count_rows()
        idx = _committed_index_names(ds_name, so)
        # Probe the normalized_domain index with a known anchor.
        probe = ds.scanner(
            columns=["normalized_domain"],
            filter="normalized_domain = 'jpmorgan.com'",
        ).to_table().num_rows
        out[ds_name] = {
            "uri": DATASET_URI[ds_name],
            "rows": n,
            "schema": [f.name for f in ds.schema],
            "indexes": idx,
            "probe_normalized_domain=jpmorgan.com": probe,
        }
        print(f"{ds_name}: {n:,} rows · indexes={idx} · probe={probe}")
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.companies_migration_runs in the HQX control-plane DB (idempotent)."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name='companies_migration_runs'
            ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
    print(f"ops.companies_migration_runs ready — columns: {cols}")
    return {"table": "ops.companies_migration_runs", "columns": cols}


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.companies_migration_runs DDL (HQX)."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def run(only: str = "") -> None:
    """Manual migration (no Trigger callback). ops.* write still fires."""
    import json

    print(json.dumps(
        ingest_gtm_company_people.remote(only=only, trigger_callback_url=None),
        indent=2, default=str,
    ))


@app.local_entrypoint()
def reindex_only(only: str = "") -> None:
    """Rebuild indexes on the existing datasets (no re-migration)."""
    import json

    print(json.dumps(reindex.remote(only=only), indent=2, default=str))


@app.local_entrypoint()
def verify_only() -> None:
    """Read-back assertions on the materialized datasets."""
    import json

    print(json.dumps(verify.remote(), indent=2, default=str))
