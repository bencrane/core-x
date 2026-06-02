"""Compute worker — HMDA × GLEIF corporate-identity crosswalk (Pattern-B bridge).

The ``resolution-hmda-gleif-pipelines`` Modal app. A standalone bridge dataset that
resolves every HMDA mortgage originator to its globally-verified GLEIF legal entity on
the Legal Entity Identifier (LEI). This is the corporate-identity spine the NMLS recon
proved was NOT derivable from the public NMLS surface (state×period aggregates, no entity
key) — the realizable bridge runs through the LEI that HMDA already carries. Spawned only
by the Universal Dispatcher (core/modal_dispatcher.py); no web endpoint.

Source-of-truth inputs (read-only; this worker never mutates them):
  - HMDA   s3://data-sink/active/hmda_panels/        (Lance v2.1, institution roster,
             2016–2025, one row per institution per year). lei is the modern (2018+) key;
             pre-2018 panel rows carry NO lei (respondent_id only) and cannot bridge.
             Collapsed here to 1 row/lei — latest-year identity + year-coverage aggregates.
  - GLEIF  s3://data-sink/active/gleif_l1_entities/   (Lance v2.1, ~3.3M Level-1 LEI
             records, BTREE lei, daily golden-copy snapshot → already 1 row/lei). Supplies
             the canonical legal_name, entity_status, registered (legal) address, and the
             home registration-authority IDs.

Grain & join (100% DuckDB transform, bounded 12 GB / 4 threads / disk spill):
  hmda_panel = hmda_panels  → 1 row/lei (lei NOT NULL); arg_max() latest-year identity
                              (respondent_name / rssd / tax_id / state / agency_code) +
                              min/max year, distinct-year count, summed lar_count.
  gleif      = gleif_l1_entities → 1 row/lei (QUALIFY latest publish_date, defensive).
  crosswalk  = hmda_panel INNER JOIN gleif ON lei   (output = 1 row per matched LEI)

The GLEIF legal_name is run through the CANONICAL name-normalization macro (lifted
verbatim from pipelines/sos_normalized/normalize.py::_name_norm) into
``normalized_legal_name`` — so this column is byte-for-byte rule-compatible with the
sos_normalized_master blocking key, making HMDA/GLEIF entities joinable to the cross-state
SoS master with no second normalization pass.

Data plane (clean-room — no Iceberg, no Polaris):
  Lance(2 sources) → DuckDB inner join → Arrow → lance.write_dataset(R2 active, v2.1,
  overwrite, storage_options) → create_scalar_index BTREE on lei + normalized_legal_name
  directly on the R2 dataset (the crosswalk is ≤~15k rows — well under the multipart
  threshold that forces hmda_lar's local-round-trip index build; direct-to-R2 is correct
  here, the nmls/sos_normalized pattern). LANCE_BYPASS_SPILLING=true forces the in-memory
  index sort (lance#2650).

Control plane (Trigger v4 durable callback): accepts ``trigger_callback_url`` and, on
terminal state (success OR failure), (1) writes the run row to
``ops.crosswalk_hmda_gleif_runs`` via psycopg (match rate + counts) and (2) POSTs a FLAT
JSON body to that url to wake the suspended Trigger run.

    modal run    pipelines/resolution/crosswalk_hmda_gleif.py::init_ops   # create ops table
    modal deploy pipelines/resolution/crosswalk_hmda_gleif.py             # dispatcher-resolvable
    modal run    pipelines/resolution/crosswalk_hmda_gleif.py             # build (local entrypoint)
    modal run    pipelines/resolution/crosswalk_hmda_gleif.py --dry-run   # match-rate plan, no write
"""

from __future__ import annotations

import os

import modal

from core.name_norm import name_norm as _name_norm

BUCKET = "data-sink"
HMDA_PANELS_URI = os.environ.get("HMDA_PANEL_LANCE_URI", "s3://data-sink/active/hmda_panels/")
GLEIF_L1_URI = os.environ.get("GLEIF_L1_LANCE_URI", "s3://data-sink/active/gleif_l1_entities/")
DATASET_URI = os.environ.get(
    "CROSSWALK_HMDA_GLEIF_LANCE_URI", "s3://data-sink/active/crosswalk_hmda_gleif/"
)
FEED = "crosswalk_hmda_gleif"

# Net-new dataset → pin the current Lance default (every sibling worker does the same).
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

# Resolution keys → BTREE (equality point-lookup). lei = the spine key (hmda_lar joins
# here on lei); normalized_legal_name = the cross-source blocking key (joins to
# sos_normalized_master). Both high-cardinality string columns.
BTREE_INDEXES = ["lei", "normalized_legal_name"]

# Bounded compute envelope. Inputs are modest (GLEIF L1 ≈ 3.3M rows materialized once;
# hmda_panels ≈ 50k); 12 GB / 4 threads / disk spill is ample headroom.
DUCKDB_MEMORY_LIMIT = "12GB"
DUCKDB_THREADS = 4
SPILL_DIR = "/tmp/duckdb_spill"

# ── ops.crosswalk_hmda_gleif_runs DDL. Verbatim mirror of
# pipelines/resolution/ops_crosswalk_hmda_gleif_runs.sql (canonical copy). Applied by the
# `init_ops` entrypoint. Keep the two in sync. ──────────────────────────────────────────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.crosswalk_hmda_gleif_runs (
    id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                    text        NOT NULL,
    dataset_uri             text        NOT NULL,
    gleif_publish_date      text,
    hmda_panel_leis         bigint,
    gleif_total_leis        bigint,
    matched_leis            bigint,
    unmatched_leis          bigint,
    match_rate              double precision,
    normalized_name_nonnull bigint,
    rows_written            bigint,
    status                  text        NOT NULL,
    error                   text,
    started_at              timestamptz,
    completed_at            timestamptz,
    recorded_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS crosswalk_hmda_gleif_runs_feed_idx
    ON ops.crosswalk_hmda_gleif_runs (feed);
CREATE INDEX IF NOT EXISTS crosswalk_hmda_gleif_runs_status_idx
    ON ops.crosswalk_hmda_gleif_runs (status);
CREATE INDEX IF NOT EXISTS crosswalk_hmda_gleif_runs_recorded_at_idx
    ON ops.crosswalk_hmda_gleif_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "requests>=2.32",        # Trigger callback
    "psycopg[binary]>=3.2",  # terminal state → ops.crosswalk_hmda_gleif_runs
).env(
    # BTREE index builds sort the column; Lance's spill sorter under-sizes its DataFusion
    # pool and OOMs on high-cardinality string columns (lance#2650). Force the in-memory
    # sort path so lei / normalized_legal_name index builds every run.
    {"LANCE_BYPASS_SPILLING": "true"}
).add_local_python_source("core.name_norm")  # ship the canonical blocking-key macro to the container

app = modal.App("resolution-hmda-gleif-pipelines", image=image)


# --------------------------------------------------------------------------- #
# R2 / S3
# --------------------------------------------------------------------------- #
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
        "endpoint": endpoint,
        "region": "auto",
    }


# --------------------------------------------------------------------------- #
# DuckDB transform — pure SQL builders (importable without modal/auth)
# _name_norm is the canonical blocking-key macro, imported from core/name_norm.py so this
# crosswalk's normalized_legal_name stays byte-identical to the sos_normalized_master key.
# --------------------------------------------------------------------------- #
def build_crosswalk_sql() -> str:
    """Final join statement. Assumes two grain-temps are built: hmda_panel(1 row/lei,
    latest-year HMDA identity + year coverage) and gleif(1 row/lei, GLEIF L1 attributes).
    Output grain = 1 row per LEI present in BOTH (inner join). GLEIF legal_name →
    normalized_legal_name via the canonical macro."""
    return f"""
SELECT
    h.lei,
    {_name_norm('g.legal_name')}                  AS normalized_legal_name,
    g.legal_name                                  AS gleif_legal_name,
    g.entity_status                               AS gleif_entity_status,
    g.legal_address_city                          AS gleif_legal_address_city,
    g.legal_address_region                        AS gleif_legal_address_region,
    g.legal_address_country                       AS gleif_legal_address_country,
    g.registration_authority_id                   AS gleif_registration_authority_id,
    g.registration_authority_entity_id            AS gleif_registration_authority_entity_id,
    g.publish_date                                AS gleif_publish_date,
    h.hmda_respondent_name,
    h.hmda_respondent_rssd,
    h.hmda_tax_id,
    h.hmda_respondent_state,
    h.hmda_agency_code,
    h.hmda_first_year,
    h.hmda_last_year,
    h.hmda_panel_years,
    h.hmda_lar_count_total,
    CAST(now() AS TIMESTAMP) AS built_at
FROM hmda_panel h
INNER JOIN gleif g ON g.lei = h.lei
"""


def _hmda_panel_sql() -> str:
    """hmda_panels → 1 row/lei. arg_max(value, source_year) carries the latest-year
    non-null identity attribute; the aggregates describe the LEI's HMDA reporting tenure."""
    return """
CREATE TEMP TABLE hmda_panel AS
SELECT
    nullif(trim(lei), '')                                    AS lei,
    arg_max(nullif(trim(respondent_name), ''),  source_year) AS hmda_respondent_name,
    arg_max(nullif(trim(respondent_rssd), ''),  source_year) AS hmda_respondent_rssd,
    arg_max(nullif(trim(tax_id), ''),           source_year) AS hmda_tax_id,
    arg_max(nullif(trim(respondent_state), ''), source_year) AS hmda_respondent_state,
    arg_max(nullif(trim(agency_code), ''),      source_year) AS hmda_agency_code,
    min(source_year)                                         AS hmda_first_year,
    max(source_year)                                         AS hmda_last_year,
    count(DISTINCT source_year)                              AS hmda_panel_years,
    sum(TRY_CAST(lar_count AS BIGINT))                       AS hmda_lar_count_total
FROM hmda_rdr
WHERE nullif(trim(lei), '') IS NOT NULL
GROUP BY 1
"""


def _gleif_sql() -> str:
    """gleif_l1_entities → 1 row/lei. The daily snapshot is already 1/lei; QUALIFY the
    latest publish_date defensively in case a manifest ever carries two snapshots."""
    return """
CREATE TEMP TABLE gleif AS
SELECT lei, legal_name, entity_status, legal_address_city, legal_address_region,
       legal_address_country, registration_authority_id, registration_authority_entity_id,
       publish_date
FROM gleif_rdr
WHERE nullif(trim(lei), '') IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY lei ORDER BY publish_date DESC NULLS LAST) = 1
"""


def _materialize(con):
    """Register the two Lance sources, build the grain temps, run the inner join, and
    return (arrow_table, metrics, gleif_publish_date). One scan per source. `con` must be
    a DuckDB connection with the bounded pragmas already applied."""
    import lance

    so = _r2_storage_options()

    def rdr(uri, columns):
        return lance.dataset(uri, storage_options=so).scanner(columns=columns).to_reader()

    # HMDA panels → 1 row/lei (latest-year identity + tenure aggregates).
    con.register("hmda_rdr", rdr(HMDA_PANELS_URI,
        ["lei", "respondent_id", "agency_code", "tax_id", "respondent_rssd",
         "respondent_name", "respondent_state", "lar_count", "source_year"]))
    con.execute(_hmda_panel_sql())
    con.unregister("hmda_rdr")

    # GLEIF L1 → 1 row/lei.
    con.register("gleif_rdr", rdr(GLEIF_L1_URI,
        ["lei", "legal_name", "entity_status", "legal_address_city", "legal_address_region",
         "legal_address_country", "registration_authority_id",
         "registration_authority_entity_id", "publish_date"]))
    con.execute(_gleif_sql())
    con.unregister("gleif_rdr")

    # Inner join → crosswalk temp, then metrics + Arrow.
    con.execute(f"CREATE TEMP TABLE xw AS {build_crosswalk_sql()}")

    hmda_leis = con.execute("SELECT count(*) FROM hmda_panel").fetchone()[0]
    gleif_leis = con.execute("SELECT count(*) FROM gleif").fetchone()[0]
    matched, norm_nonnull = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE normalized_legal_name IS NOT NULL) FROM xw"
    ).fetchone()
    gleif_pub = con.execute("SELECT max(publish_date) FROM gleif").fetchone()[0]

    table = con.sql("SELECT * FROM xw").to_arrow_table()
    match_rate = round(matched / hmda_leis, 4) if hmda_leis else 0.0
    metrics = {
        "hmda_panel_leis": int(hmda_leis),
        "gleif_total_leis": int(gleif_leis),
        "matched_leis": int(matched),
        "unmatched_leis": int(hmda_leis - matched),
        "match_rate": match_rate,
        "normalized_name_nonnull": int(norm_nonnull),
        "rows_written": int(matched),
    }
    return table, metrics, gleif_pub


def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


# --------------------------------------------------------------------------- #
# ops ledger + Trigger callback
# --------------------------------------------------------------------------- #
def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record_run(*, gleif_publish_date, metrics, status, error, started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.crosswalk_hmda_gleif_runs
                    (feed, dataset_uri, gleif_publish_date, hmda_panel_leis, gleif_total_leis,
                     matched_leis, unmatched_leis, match_rate, normalized_name_nonnull,
                     rows_written, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, gleif_publish_date, metrics.get("hmda_panel_leis"),
                 metrics.get("gleif_total_leis"), metrics.get("matched_leis"),
                 metrics.get("unmatched_leis"), metrics.get("match_rate"),
                 metrics.get("normalized_name_nonnull"), metrics.get("rows_written"),
                 status, error, started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* write failed: {exc}")
    finally:
        conn.close()


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
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


# --------------------------------------------------------------------------- #
# Core build
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 30,
    memory=16384,
    cpu=4.0,
)
def build_crosswalk(trigger_callback_url: str | None = None) -> dict:
    """Rebuild the HMDA × GLEIF crosswalk and publish it to R2 active. Idempotent full
    overwrite; BTREE indexes rebuilt on the R2 dataset each run."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error, gleif_pub = "error", None, None
    metrics = {"hmda_panel_leis": 0, "gleif_total_leis": 0, "matched_leis": 0,
               "unmatched_leis": 0, "match_rate": 0.0, "normalized_name_nonnull": 0,
               "rows_written": 0}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics, gleif_pub = _materialize(con)
        finally:
            con.close()
        print(f"Built crosswalk: {metrics} gleif_publish_date={gleif_pub}")

        lance.write_dataset(
            table, DATASET_URI, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        print(f"Wrote Lance dataset (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")

        ds = lance.dataset(DATASET_URI, storage_options=so)
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            print(f"  BTREE ✓ {col}")
        committed = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        print(f"Committed rows: {committed:,}  (match_rate={metrics['match_rate']:.4f})")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(gleif_publish_date=gleif_pub, metrics=metrics, status=status,
                    error=error, started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "feed": FEED, "dataset_uri": DATASET_URI,
                        "rows": metrics["rows_written"], "gleif_publish_date": gleif_pub,
                        "hmda_panel_leis": metrics["hmda_panel_leis"],
                        "matched_leis": metrics["matched_leis"],
                        "match_rate": metrics["match_rate"]})

    if status != "success":
        raise RuntimeError(f"crosswalk build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, "gleif_publish_date": gleif_pub, **metrics}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_crosswalk() -> dict:
    """Read-back proof: open the published crosswalk from R2 and report row count, schema,
    indices, grain, and entity-status breakdown — independent of the write path."""
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
           for i in ds.list_indices()]
    con = duckdb.connect()
    con.register("d", ds.scanner(
        columns=["lei", "normalized_legal_name", "gleif_entity_status"]).to_reader())
    con.execute("CREATE TEMP TABLE d2 AS SELECT * FROM d")
    n, dist, normed = con.execute(
        "SELECT count(*), count(DISTINCT lei), "
        "count(*) FILTER (WHERE normalized_legal_name IS NOT NULL) FROM d2").fetchone()
    status_mix = dict(con.execute(
        "SELECT coalesce(gleif_entity_status,'<null>'), count(*) FROM d2 GROUP BY 1 "
        "ORDER BY 2 DESC").fetchall())
    con.close()
    return {"rows": n, "distinct_lei": dist, "grain_ok": n == dist,
            "normalized_name_nonnull": normed, "entity_status_breakdown": status_mix,
            "indices": idx, "uri": DATASET_URI,
            "schema": [f"{f.name}:{f.type}" for f in ds.schema]}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    """Apply the ops.crosswalk_hmda_gleif_runs DDL (idempotent)."""
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.crosswalk_hmda_gleif_runs"}


@app.local_entrypoint()
def build(dry_run: bool = False) -> None:
    if dry_run:
        # Compute the match rate without writing — the review gate. Runs locally, so the
        # local env must carry R2_* (e.g. `doppler run -- modal run … --dry-run`).
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            _table, metrics, gleif_pub = _materialize(con)
        finally:
            con.close()
        print(f"[dry-run] gleif_publish_date={gleif_pub} {metrics}")
        return
    print(build_crosswalk.remote(trigger_callback_url=None))
    print(verify_crosswalk.remote())


@app.local_entrypoint()
def init() -> None:
    import json
    print(json.dumps(init_ops.remote(), indent=2, default=str))
