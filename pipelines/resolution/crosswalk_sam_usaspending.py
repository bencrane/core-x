"""Compute worker — SAM.gov × USAspending canonical crosswalk (Pattern-B bridge).

The ``resolution-crosswalk-pipelines`` Modal app. A standalone bridge dataset that
resolves the historical USAspending recipient spine to the CURRENT SAM registry,
keyed on UEI with a CAGE fallback that reclaims the legacy defense tail. Spawned
only by the Universal Dispatcher (core/modal_dispatcher.py); no web endpoint.

Source-of-truth inputs (read-only; this worker never mutates them):
  - SAM    s3://data-sink/active/entity_registrations/        (Lance v2, 19.3M rows)
             Two stacked layouts. ``format_family='v2'`` carries the UEI (888,916
             distinct, the current registry, monthly extract). ``legacy_v1`` carries
             CAGE but NO uei (2014→present) — it IS the legacy defense tail.
  - USA    s3://data-sink/active/usaspending/recipient_lookup/   (Lance, 17.75M rows)
             The recipient dimension; the crosswalk spine (1.03M distinct UEI).
             Carries no cage_code.
  - USA    s3://data-sink/active/usaspending/transaction_search_fpds/ (Lance, 107M rows)
             The ONLY USAspending source carrying (recipient_uei, cage_code). Supplies
             the uei→cage bridge that drives CAGE recovery; recipient_lookup cannot.

Grain & join (100% DuckDB transform, bounded 16 GB / 4 threads / disk spill):
  sam            = entity_registrations[ff='v2'], QUALIFY 1 row/uei
                   (ORDER BY last_update_date DESC, registration_date DESC)
  sam_cage_map   = entity_registrations[ALL families], QUALIFY 1 row/cage_code
                   (legacy entities have sam_uei=NULL — the defense tail)
  usa            = recipient_lookup → 1 row/uei (any_value descriptors)
  fpds_uei_cage  = transaction_search_fpds → 1 row/uei (any_value cage)
  crosswalk      = usa LEFT JOIN sam ON uei
                       LEFT JOIN fpds_uei_cage ON (sam miss) AND uei
                       LEFT JOIN sam_cage_map ON bridging cage

Data plane (clean-room — no Iceberg, no Polaris):
  Lance(3 sources) → DuckDB join → Arrow → lance.write_dataset(R2 active, v2.1,
  overwrite, storage_options) → create_scalar_index BTREE on uei + cage_code,
  directly on the R2 dataset (co_ucc/transactions_bulk.py proven pattern — no local
  staging). LANCE_BYPASS_SPILLING=true forces the in-memory index sort so the
  high-cardinality string BTREE builds without OOM (lance#2650).

Control plane (Trigger v4 durable callback): accepts ``trigger_callback_url`` and,
on terminal state (success OR failure), (1) writes the run row to
``ops.crosswalk_sam_usaspending_runs`` via psycopg and (2) POSTs a FLAT JSON body
to that url to wake the suspended Trigger run.

    modal run    pipelines/resolution/crosswalk_sam_usaspending.py::init_ops   # create ops table
    modal deploy pipelines/resolution/crosswalk_sam_usaspending.py             # dispatcher-resolvable
    modal run    pipelines/resolution/crosswalk_sam_usaspending.py             # build (local entrypoint)
    modal run    pipelines/resolution/crosswalk_sam_usaspending.py --dry-run   # plan + counts, no write
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
SAM_SRC_URI = "s3://data-sink/active/entity_registrations/"
RL_SRC_URI = "s3://data-sink/active/usaspending/recipient_lookup/"
FPDS_SRC_URI = "s3://data-sink/active/usaspending/transaction_search_fpds/"
DATASET_URI = os.environ.get(
    "CROSSWALK_LANCE_URI", "s3://data-sink/active/crosswalk_sam_usaspending/"
)
FEED = "crosswalk_sam_usaspending"

# Net-new dataset → pin the current Lance default. Lance fragment sizing as per the
# sibling resolution/co_ucc workers (90 GiB == Lance default; output is one fragment).
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

# Resolution keys → BTREE (equality point-lookup). uei = the spine key; cage_code =
# the secondary defense-tail bridge; normalized_legal_name = the name-blocking key that
# lets PPP/SBA (and any nameonly feed) resolve into the federal UEI spine. All three are
# high-cardinality string columns. normalized_legal_name is derived from sam_legal_name
# via the canonical SoS macro (see _norm_sql + build_crosswalk_sql).
BTREE_INDEXES = ["uei", "cage_code", "normalized_legal_name"]

# Validated compute boundary (recon + materialization runs): 16 GB cap, 4 threads,
# disk spill. Honoured exactly so the worker matches the proven-stable envelope.
DUCKDB_MEMORY_LIMIT = "16GB"
DUCKDB_THREADS = 4
SPILL_DIR = "/tmp/duckdb_spill"

# ── ops.crosswalk_sam_usaspending_runs DDL. Verbatim mirror of
# pipelines/resolution/ops_crosswalk_sam_usaspending_runs.sql (canonical copy).
# Applied by the `init_ops` entrypoint. Keep the two in sync. ──────────────────────
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.crosswalk_sam_usaspending_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed             text        NOT NULL,
    dataset_uri      text        NOT NULL,
    sam_label        text,
    rows_written     bigint,
    matched_by_uei   bigint,
    matched_by_cage  bigint,
    matched_any      bigint,
    status           text        NOT NULL,
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS crosswalk_sam_usaspending_runs_feed_idx
    ON ops.crosswalk_sam_usaspending_runs (feed);
CREATE INDEX IF NOT EXISTS crosswalk_sam_usaspending_runs_status_idx
    ON ops.crosswalk_sam_usaspending_runs (status);
CREATE INDEX IF NOT EXISTS crosswalk_sam_usaspending_runs_recorded_at_idx
    ON ops.crosswalk_sam_usaspending_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",        # >=1.5 guarantees to_arrow_table; <2 stays below the v2.0 break
    "lancedb>=0.15",
    "pylance>=7",            # provides `import lance`; lancedb does not re-export it
    "pyarrow>=17",
    "requests>=2.32",        # Trigger callback
    "psycopg[binary]>=3.2",  # terminal state → ops.crosswalk_sam_usaspending_runs
    "pandas>=2.2",           # lance.add_columns BatchUDF path imports pandas (normalize_transform shim)
).env(
    # BTREE index builds sort the column; Lance's spill sorter under-sizes its
    # DataFusion pool and OOMs on high-cardinality string columns (lance#2650).
    # Force the in-memory sort path so uei / cage_code index builds every run.
    {"LANCE_BYPASS_SPILLING": "true"}
)

app = modal.App("resolution-crosswalk-pipelines", image=image)


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
# DuckDB transform — pure SQL builder (importable without modal/auth)
# --------------------------------------------------------------------------- #
def _norm_sql(expr: str) -> str:
    """The canonical cross-spine name-normalization macro — BYTE-IDENTICAL to
    pipelines/sos_normalized/normalize.py: UPPER → strip every non-[A-Z0-9 space]
    char → collapse whitespace runs → trim → NULL if emptied. ``\\s`` in this Python
    source emits ``\\s`` in the SQL. Applying the SAME macro on both sides
    (crosswalk.normalized_legal_name and a feed's normalized borrower name) is what
    makes the BTREE block-join valid."""
    return ("nullif(trim(regexp_replace(regexp_replace(upper(CAST(" + expr + " AS VARCHAR)),"
            " '[^A-Z0-9 ]+', '', 'g'), '\\s+', ' ', 'g')), '')")


def build_crosswalk_sql() -> str:
    """Final join statement. Assumes four relations are registered/built:
    sam(uei,cage_code,legal_business_name,dba_name) 1/uei (v2 registry),
    sam_cage_map(cage_code,sam_uei,sam_legal_name,sam_dba_name) 1/cage (all families),
    usa(uei,parent_uei,parent_legal_name,usa_legal_name,state,zip5) 1/uei,
    fpds_uei_cage(uei,fpds_cage) 1/uei. Output grain = 1 row per usa.uei.
    A trailing wrap derives normalized_legal_name from sam_legal_name via the canonical
    macro so every rebuild ships the name-blocking key natively (no drift vs. the in-place
    patch)."""
    inner = """
SELECT
    u.uei,
    (s.uei IS NOT NULL)                                   AS matched_by_uei,
    (s.uei IS NULL AND scage.cage_code IS NOT NULL)       AS matched_by_cage,
    (s.uei IS NOT NULL OR scage.cage_code IS NOT NULL)    AS matched_any,
    CASE WHEN s.uei IS NOT NULL THEN 'uei'
         WHEN scage.cage_code IS NOT NULL THEN 'cage'
         ELSE 'unmatched' END                             AS match_method,
    coalesce(s.uei, scage.sam_uei)                        AS sam_uei,
    coalesce(s.cage_code, scage.cage_code)                AS cage_code,
    coalesce(s.legal_business_name, scage.sam_legal_name) AS sam_legal_name,
    coalesce(s.dba_name, scage.sam_dba_name)              AS sam_dba_name,
    u.usa_legal_name,
    u.parent_uei,
    u.parent_legal_name,
    u.state,
    u.zip5
FROM usa u
LEFT JOIN sam s              ON s.uei = u.uei
LEFT JOIN fpds_uei_cage fc   ON s.uei IS NULL AND fc.uei = u.uei
LEFT JOIN sam_cage_map scage ON scage.cage_code = fc.fpds_cage
"""
    # Wrap: derive the name-blocking key from the final sam_legal_name (the coalesced
    # SAM legal name). Same macro the in-place patch uses → zero drift on rebuild.
    return (
        "WITH xw0 AS (" + inner + ")\n"
        "SELECT *, " + _norm_sql("sam_legal_name") + " AS normalized_legal_name\n"
        "FROM xw0\n"
    )


def _materialize(con):
    """Register the three Lance sources, build the grain temps, run the join, and
    return (arrow_table, metrics, sam_label). One scan per source. `con` must be a
    DuckDB connection with the bounded pragmas already applied."""
    import lance

    so = _r2_storage_options()

    def rdr(uri, columns, flt=None):
        return lance.dataset(uri, storage_options=so).scanner(
            columns=columns, filter=flt).to_reader()

    # SAM — one full scan (all families); v2 filter applied in SQL for the UEI grain.
    con.register("sam_rdr", rdr(SAM_SRC_URI,
        ["uei", "cage_code", "legal_business_name", "dba_name",
         "last_update_date", "registration_date", "format_family", "extract_label"]))
    con.execute("""
      CREATE TEMP TABLE sam_src AS
      SELECT nullif(trim(uei),'')       AS uei,
             nullif(trim(cage_code),'') AS cage_code,
             legal_business_name, dba_name, last_update_date, registration_date,
             format_family AS ff, extract_label AS el
      FROM sam_rdr
    """)
    con.unregister("sam_rdr")
    sam_label = con.execute(
        "SELECT max(el) FROM sam_src WHERE ff='v2'").fetchone()[0]
    # UEI-grain registry: v2 ONLY (current registry), strict 1 row/uei, latest state.
    con.execute("""
      CREATE TEMP TABLE sam AS
      SELECT uei, cage_code, legal_business_name, dba_name FROM sam_src
      WHERE uei IS NOT NULL AND ff='v2'
      QUALIFY row_number() OVER (PARTITION BY uei
              ORDER BY last_update_date DESC NULLS LAST, registration_date DESC NULLS LAST) = 1
    """)
    # CAGE bridge target: FULL SAM cage universe (legacy_v1 + v2). legacy_v1 carries
    # CAGE but no UEI → sam_uei NULL for the defense tail. 1 row/cage (latest owner).
    con.execute("""
      CREATE TEMP TABLE sam_cage_map AS
      SELECT cage_code, uei AS sam_uei, legal_business_name AS sam_legal_name,
             dba_name AS sam_dba_name FROM sam_src
      WHERE cage_code IS NOT NULL
      QUALIFY row_number() OVER (PARTITION BY cage_code
              ORDER BY last_update_date DESC NULLS LAST, registration_date DESC NULLS LAST) = 1
    """)
    con.execute("DROP TABLE sam_src")

    # USAspending recipient spine: 1 row/uei (any_value descriptors).
    con.register("rl_rdr", rdr(RL_SRC_URI,
        ["uei", "parent_uei", "parent_legal_business_name", "legal_business_name", "state", "zip5"]))
    con.execute("""
      CREATE TEMP TABLE usa AS
      SELECT nullif(trim(uei),'') AS uei,
             any_value(parent_uei)                 AS parent_uei,
             any_value(parent_legal_business_name) AS parent_legal_name,
             any_value(legal_business_name)        AS usa_legal_name,
             any_value(state)                      AS state,
             any_value(zip5)                       AS zip5
      FROM rl_rdr
      WHERE nullif(trim(uei),'') IS NOT NULL
      GROUP BY 1
    """)
    con.unregister("rl_rdr")

    # FPDS uei→cage bridge (only USAspending source carrying cage).
    con.register("fpds_rdr", rdr(FPDS_SRC_URI, ["recipient_uei", "cage_code"]))
    con.execute("""
      CREATE TEMP TABLE fpds_uei_cage AS
      SELECT nullif(trim(recipient_uei),'') AS uei,
             any_value(nullif(trim(cage_code),'')) AS fpds_cage
      FROM fpds_rdr
      WHERE nullif(trim(recipient_uei),'') IS NOT NULL
        AND nullif(trim(cage_code),'') IS NOT NULL
      GROUP BY 1
    """)
    con.unregister("fpds_rdr")

    # Join → crosswalk temp, then metrics + Arrow.
    con.execute(f"CREATE TEMP TABLE xw AS {build_crosswalk_sql()}")
    rows, m_uei, m_cage, m_any = con.execute("""
      SELECT count(*),
             count(*) FILTER (WHERE matched_by_uei),
             count(*) FILTER (WHERE matched_by_cage),
             count(*) FILTER (WHERE matched_any)
      FROM xw""").fetchone()
    table = con.sql("SELECT * FROM xw").to_arrow_table()
    metrics = {"rows": int(rows), "matched_by_uei": int(m_uei),
               "matched_by_cage": int(m_cage), "matched_any": int(m_any)}
    return table, metrics, sam_label


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


def _record_run(*, sam_label, metrics, status, error, started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.crosswalk_sam_usaspending_runs
                    (feed, dataset_uri, sam_label, rows_written, matched_by_uei,
                     matched_by_cage, matched_any, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, sam_label, metrics.get("rows"),
                 metrics.get("matched_by_uei"), metrics.get("matched_by_cage"),
                 metrics.get("matched_any"), status, error, started_at, completed_at),
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
    timeout=60 * 60,
    memory=32768,
    cpu=4.0,
)
def build_crosswalk(trigger_callback_url: str | None = None) -> dict:
    """Rebuild the SAM × USAspending crosswalk and publish it to R2 active. Idempotent
    full overwrite; BTREE indexes rebuilt on the R2 dataset each run."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error, sam_label = "error", None, None
    metrics = {"rows": 0, "matched_by_uei": 0, "matched_by_cage": 0, "matched_any": 0}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics, sam_label = _materialize(con)
        finally:
            con.close()
        print(f"Built crosswalk: {metrics} sam_label={sam_label}")

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
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"  BTREE ✓ {col}")
        committed = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        print(f"Committed rows: {committed:,}")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(sam_label=sam_label, metrics=metrics, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": metrics["rows"], "feed": FEED,
                        "dataset_uri": DATASET_URI, "sam_label": sam_label,
                        "matched_by_uei": metrics["matched_by_uei"],
                        "matched_by_cage": metrics["matched_by_cage"],
                        "matched_any": metrics["matched_any"]})

    if status != "success":
        raise RuntimeError(f"crosswalk build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, "sam_label": sam_label, **metrics}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_crosswalk() -> dict:
    """Read-back proof: open the published crosswalk from R2 and report row count,
    schema, indices, and match breakdown — independent of the write path."""
    import lance
    import duckdb

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
           for i in ds.list_indices()]
    con = duckdb.connect()
    con.register("d", ds.scanner(columns=["uei", "match_method", "matched_any"]).to_reader())
    con.execute("CREATE TEMP TABLE d2 AS SELECT * FROM d")
    n, dist = con.execute("SELECT count(*), count(DISTINCT uei) FROM d2").fetchone()
    mb = dict(con.execute("SELECT match_method, count(*) FROM d2 GROUP BY 1").fetchall())
    con.close()
    return {"rows": n, "distinct_uei": dist, "grain_ok": n == dist,
            "match_breakdown": mb, "indices": idx, "uri": DATASET_URI,
            "schema": [f"{f.name}:{f.type}" for f in ds.schema]}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    """Apply the ops.crosswalk_sam_usaspending_runs DDL (idempotent)."""
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.crosswalk_sam_usaspending_runs"}


# --------------------------------------------------------------------------- #
# In-place additive schema patch — normalized_legal_name + BTREE (no recreate)
# --------------------------------------------------------------------------- #
# Cross-worker ledger for additive in-place schema patches (column + index adds that
# do NOT recreate the dataset). Idempotent DDL; mirrored verbatim in sba_foia/ingest.py.
OPS_PATCH_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.schema_patch_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_uri     text        NOT NULL,
    operation       text        NOT NULL,
    column_added    text,
    index_built     text,
    rows            bigint,
    exact_dup_rows  bigint,
    version_before  bigint,
    version_after   bigint,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS schema_patch_runs_dataset_idx  ON ops.schema_patch_runs (dataset_uri);
CREATE INDEX IF NOT EXISTS schema_patch_runs_recorded_idx ON ops.schema_patch_runs (recorded_at DESC);
"""


def _record_patch(dataset_uri, operation, column_added, index_built, rows, exact_dup_rows,
                  version_before, version_after, status, error, started_at, completed_at) -> None:
    """Terminal row → ops.schema_patch_runs (psycopg). Best-effort; never masks the patch."""
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.schema_patch_runs write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(OPS_PATCH_DDL)
            cur.execute(
                """
                INSERT INTO ops.schema_patch_runs
                    (dataset_uri, operation, column_added, index_built, rows, exact_dup_rows,
                     version_before, version_after, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (dataset_uri, operation, column_added, index_built, rows, exact_dup_rows,
                 version_before, version_after, status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the patch
        print(f"WARN: ops.schema_patch_runs write failed: {exc}")


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 45,
    memory=16384,
    cpu=4.0,
)
def patch_normalized_name(trigger_callback_url: str | None = None) -> dict:
    """ADDITIVE IN-PLACE: derive normalized_legal_name from sam_legal_name (canonical
    macro) and BTREE-index it WITHOUT recreating the dataset — lance.add_columns +
    create_scalar_index write a new version while prior columns/indices stay intact.
    Idempotent (skips the add if the column already exists — e.g. a rebuild already
    shipped it — and (re)builds the index either way). Integrity-gated: every row's
    stored value must equal the macro recomputed from sam_legal_name, else roll the
    dataset back to the pre-patch version and fail."""
    import datetime as dt

    import duckdb
    import lance

    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    status, error, added, result = "error", None, None, {}
    ds = lance.dataset(DATASET_URI, storage_options=so)
    v_before = ds.version
    n0 = ds.count_rows()
    v_after = v_before
    macro = _norm_sql("sam_legal_name")
    try:
        if "normalized_legal_name" not in {f.name for f in ds.schema}:
            # One DuckDB pass keyed to _rowid → the exact macro → positional add_columns.
            con = duckdb.connect(":memory:")
            con.execute("PRAGMA threads=4;")
            con.register("rdr", ds.scanner(columns=["sam_legal_name"], with_row_id=True).to_reader())
            con.execute("CREATE TABLE t AS SELECT * FROM rdr")
            con.unregister("rdr")
            arrow = con.execute(
                f"SELECT {macro} AS normalized_legal_name FROM t ORDER BY _rowid"
            ).to_arrow_table().combine_chunks()
            con.close()
            ds.add_columns(arrow, batch_size=65536)   # positional zip in _rowid order
            ds = lance.dataset(DATASET_URI, storage_options=so)
            added = "normalized_legal_name"

        ds.create_scalar_index("normalized_legal_name", index_type="BTREE", replace=True)
        ds = lance.dataset(DATASET_URI, storage_options=so)
        v_after = ds.version

        # ── integrity gate: stored value == macro recomputed, row count stable, idx present
        con = duckdb.connect(":memory:")
        con.execute("PRAGMA threads=4;")
        con.register("rdr", ds.scanner(columns=["sam_legal_name", "normalized_legal_name"]).to_reader())
        con.execute("CREATE TABLE v AS SELECT * FROM rdr")
        con.unregister("rdr")
        mism = con.execute(
            f"SELECT count(*) FROM v WHERE normalized_legal_name IS DISTINCT FROM {macro}").fetchone()[0]
        n1, nn = con.execute(
            "SELECT count(*), count(normalized_legal_name) FROM v").fetchone()
        con.close()
        idx = {i.get("name") if isinstance(i, dict) else getattr(i, "name", None)
               for i in ds.list_indices()}
        ok = (mism == 0) and (n1 == n0) and ("normalized_legal_name_idx" in idx)
        if not ok:
            lance.dataset(DATASET_URI, storage_options=so, version=v_before).restore()
            raise RuntimeError(
                f"integrity gate failed (rolled back to v{v_before}): "
                f"macro_mismatches={mism} rows={n1}/{n0} indices={sorted(idx)}")
        result = {"rows": n1, "non_null_normalized": nn, "macro_mismatches": mism,
                  "indices": sorted(idx)}
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
        status = "error"
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_patch(DATASET_URI, "add_normalized_legal_name", added,
                      "normalized_legal_name_idx" if status == "success" else None,
                      result.get("rows"), None, v_before, v_after, status, error, started, completed)
        _post_callback(trigger_callback_url,
                       {"status": status, "dataset_uri": DATASET_URI,
                        "operation": "add_normalized_legal_name", **result})

    if status != "success":
        raise RuntimeError(f"patch_normalized_name failed: {error}")
    return {"dataset_uri": DATASET_URI, "operation": "add_normalized_legal_name",
            "version_before": v_before, "version_after": v_after, **result}


@app.local_entrypoint()
def build(dry_run: bool = False) -> None:
    if dry_run:
        # Compute counts without writing — the review gate.
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            _table, metrics, sam_label = _materialize(con)
        finally:
            con.close()
        print(f"[dry-run] sam_label={sam_label} {metrics}")
        return
    print(build_crosswalk.remote(trigger_callback_url=None))
    print(verify_crosswalk.remote())


@app.local_entrypoint()
def patch_norm() -> None:
    """In-place additive patch: add + BTREE-index normalized_legal_name on the live
    crosswalk WITHOUT recreating it. Run after deploying the patched worker so the daily
    rebuild also ships the column natively."""
    import json

    print(json.dumps(patch_normalized_name.remote(), indent=2, default=str))
