"""Compute worker — USAspending FFATA Executive Compensation (human layer).

The ``usaspending-ffata-pipelines`` Modal app. Reshapes the flat top-five officer
columns that USAspending denormalizes onto award/transaction rows into a long,
indexed executive-compensation dataset keyed on recipient UEI — the prime-recipient
human layer that attaches to the SAM × USAspending crosswalk spine. Spawned only by
the Universal Dispatcher; no web endpoint.

Source-of-truth inputs (read-only; never mutated). Each carries the SAME flat
layout — officer_{1..5}_name (string) + officer_{1..5}_amount (double):
  s3://data-sink/active/usaspending/award_search/            (78.4M rows, award grain)
  s3://data-sink/active/usaspending/transaction_search_fpds/ (107.3M, contract txns)
  s3://data-sink/active/usaspending/transaction_search_fabs/ (128.8M, assistance txns)

FFATA executive compensation is reported only by entities meeting the statutory
threshold (≥$25M federal awards AND ≥80% federal revenue), so the officer columns
are genuinely sparse — ~5.9k distinct UEI across all three carriers, 100% contained
in the crosswalk. The three are UNIONed (not just award_search) for maximal reach:
the transaction tables surface ~500 UEI whose award_search latest-state row dropped
the disclosure. Build-time cost only; the carrier set is a one-line change.

Grain & keys:
  union(award_search, fpds, fabs) WHERE any officer name present
    → 1 disclosure/uei (QUALIFY latest action_date, award_search-preferred tiebreak)
    → unpivot officer_{1..5} → 1 row per (recipient_uei, officer_rank) with a name.
  recipient_uei → joins crosswalk.uei.
Sub-award (sub-recipient) officer layout is a DISTINCT population and is deferred.

ZERO-ALTERATION NAME POLICY (operator mandate): officer_name is a single opaque
source string — USAspending provides no name parts. It is NEVER split, parsed, or
trimmed of components; it is carried through verbatim (whitespace hygiene only).
`name_key` ( upper(trim(officer_name)) ) is an ADDED, non-authoritative lookup
accelerator; the verbatim `officer_name` remains system-of-record.

Data plane (clean-room — DuckDB does 100% of the transform):
  Lance(3 carriers) → DuckDB union + latest-disclosure dedup + officer unpivot →
  Arrow → lance.write_dataset(R2 active, v2.1, overwrite) → BTREE(recipient_uei,
  name_key) + BITMAP(officer_rank, source_channel). LANCE_BYPASS_SPILLING=true for
  the high-cardinality string sort (lance#2650).

Control plane (Trigger v4 durable callback): on terminal state writes the run row
to ops.ffata_exec_comp_runs and POSTs the flat callback to wake the Trigger run.

    modal run    pipelines/usaspending/ffata_exec_comp.py::init_ops   # ops table
    modal deploy pipelines/usaspending/ffata_exec_comp.py             # dispatcher
    modal run    pipelines/usaspending/ffata_exec_comp.py             # build
    modal run    pipelines/usaspending/ffata_exec_comp.py --dry-run   # counts, no write
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
USA_BASE = "s3://data-sink/active/usaspending/"
# Carrier datasets (award grain first → preferred disclosure tiebreak).
CARRIERS = [
    ("award_search", 1),
    ("transaction_search_fpds", 2),
    ("transaction_search_fabs", 3),
]
DATASET_URI = os.environ.get(
    "FFATA_EXEC_COMP_LANCE_URI", "s3://data-sink/active/ffata_exec_comp/"
)
FEED = "ffata_exec_comp"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

# recipient_uei = forward spine lookup; name_key = reverse human-name lookup.
BTREE_INDEXES = ["recipient_uei", "name_key"]
BITMAP_INDEXES = ["officer_rank", "source_channel"]

DUCKDB_MEMORY_LIMIT = "24GB"
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/duckdb_spill"

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.ffata_exec_comp_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,
    dataset_uri     text        NOT NULL,
    rows_written    bigint,
    distinct_uei    bigint,
    officer_rows    bigint,
    status          text        NOT NULL,
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ffata_exec_comp_runs_feed_idx        ON ops.ffata_exec_comp_runs (feed);
CREATE INDEX IF NOT EXISTS ffata_exec_comp_runs_status_idx      ON ops.ffata_exec_comp_runs (status);
CREATE INDEX IF NOT EXISTS ffata_exec_comp_runs_recorded_at_idx ON ops.ffata_exec_comp_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("usaspending-ffata-pipelines", image=image)

_OFFICER_NAME_COLS = [f"officer_{i}_name" for i in range(1, 6)]
_OFFICER_AMOUNT_COLS = [f"officer_{i}_amount" for i in range(1, 6)]


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
# DuckDB transform
# --------------------------------------------------------------------------- #
def build_ffata_sql() -> str:
    """Latest-disclosure dedup + officer unpivot. Assumes a `disclosures` relation
    (the unioned officer-present carrier rows) with columns recipient_uei,
    action_date, source_channel, chan_pri, officer_1..5_name/amount."""
    rank_name_case = " ".join(
        f"WHEN {i} THEN officer_{i}_name" for i in range(1, 6)
    )
    rank_amount_case = " ".join(
        f"WHEN {i} THEN officer_{i}_amount" for i in range(1, 6)
    )
    return f"""
WITH latest AS (
    SELECT *
    FROM disclosures
    QUALIFY row_number() OVER (
        PARTITION BY recipient_uei
        ORDER BY action_date DESC NULLS LAST, chan_pri ASC
    ) = 1
),
ranked AS (
    SELECT
        recipient_uei,
        source_channel,
        action_date AS disclosure_action_date,
        s.rank      AS officer_rank,
        CASE s.rank {rank_name_case} END   AS officer_name,
        CASE s.rank {rank_amount_case} END AS officer_amount
    FROM latest
    CROSS JOIN (SELECT unnest([1, 2, 3, 4, 5]) AS rank) s
)
SELECT
    recipient_uei,
    officer_rank,
    officer_name,
    officer_amount,
    upper(trim(officer_name)) AS name_key,
    source_channel,
    disclosure_action_date
FROM ranked
WHERE nullif(trim(officer_name), '') IS NOT NULL
"""


def _materialize(con):
    """Register the three carriers (officer-present rows only), union, dedup, and
    unpivot. Returns (arrow_table, metrics). Three scans, one per carrier."""
    import lance

    so = _r2_storage_options()
    cols = ["recipient_uei", "action_date"] + _OFFICER_NAME_COLS + _OFFICER_AMOUNT_COLS
    select_cols = ", ".join(
        ["nullif(trim(recipient_uei), '') AS recipient_uei", "action_date"]
        + _OFFICER_NAME_COLS + _OFFICER_AMOUNT_COLS
    )
    name_present = " OR ".join(f"nullif(trim({c}), '') IS NOT NULL" for c in _OFFICER_NAME_COLS)

    union_parts = []
    for table, pri in CARRIERS:
        rel = f"ffsrc_{table}"
        con.register(rel, lance.dataset(f"{USA_BASE}{table}/", storage_options=so)
                     .scanner(columns=cols).to_reader())
        union_parts.append(f"""
            SELECT {select_cols}, '{table}' AS source_channel, {pri} AS chan_pri
            FROM {rel}
            WHERE nullif(trim(recipient_uei), '') IS NOT NULL AND ({name_present})
        """)
    con.execute(
        "CREATE TEMP TABLE disclosures AS\n" + "\nUNION ALL\n".join(union_parts)
    )
    for table, _ in CARRIERS:
        con.unregister(f"ffsrc_{table}")

    con.execute(f"CREATE TEMP TABLE ffata AS {build_ffata_sql()}")
    rows, d_uei = con.execute(
        "SELECT count(*), count(DISTINCT recipient_uei) FROM ffata"
    ).fetchone()
    table = con.sql("SELECT * FROM ffata").to_arrow_table()
    metrics = {"rows": int(rows), "distinct_uei": int(d_uei), "officer_rows": int(rows)}
    return table, metrics


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


def _record_run(*, metrics, status, error, started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.ffata_exec_comp_runs
                    (feed, dataset_uri, rows_written, distinct_uei, officer_rows,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, metrics.get("rows"), metrics.get("distinct_uei"),
                 metrics.get("officer_rows"), status, error, started_at, completed_at),
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
    cpu=8.0,
)
def build_ffata_exec_comp(trigger_callback_url: str | None = None) -> dict:
    """Rebuild the FFATA executive-compensation human layer and publish to R2
    active. Idempotent full overwrite; indexes rebuilt on the R2 dataset each run."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error = "error", None
    metrics = {"rows": 0, "distinct_uei": 0, "officer_rows": 0}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics = _materialize(con)
        finally:
            con.close()
        print(f"Built ffata_exec_comp: {metrics}")

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
        for col in BITMAP_INDEXES:
            ds.create_scalar_index(col, index_type="BITMAP")
            print(f"  BITMAP ✓ {col}")
        committed = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        print(f"Committed rows: {committed:,}")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(metrics=metrics, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": metrics["rows"], "feed": FEED,
                        "dataset_uri": DATASET_URI,
                        "distinct_uei": metrics["distinct_uei"]})

    if status != "success":
        raise RuntimeError(f"ffata_exec_comp build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, **metrics}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_ffata_exec_comp() -> dict:
    """Read-back proof: open the published dataset from R2 and report counts,
    schema, indices, and rank/channel distribution — independent of the write path."""
    import lance
    import duckdb

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
           for i in ds.list_indices()]
    con = duckdb.connect()
    con.register("d", ds.scanner(
        columns=["recipient_uei", "officer_rank", "source_channel", "name_key"]).to_reader())
    con.execute("CREATE TEMP TABLE d2 AS SELECT * FROM d")
    n, d_uei, d_name = con.execute(
        "SELECT count(*), count(DISTINCT recipient_uei), count(DISTINCT name_key) FROM d2"
    ).fetchone()
    by_rank = dict(con.execute("SELECT officer_rank, count(*) FROM d2 GROUP BY 1 ORDER BY 1").fetchall())
    by_chan = dict(con.execute("SELECT source_channel, count(*) FROM d2 GROUP BY 1").fetchall())
    con.close()
    sample = ds.scanner(
        columns=["recipient_uei", "officer_rank", "officer_name", "officer_amount",
                 "source_channel"], limit=6).to_table().to_pylist()
    return {"rows": n, "distinct_uei": d_uei, "distinct_name_key": d_name,
            "by_officer_rank": by_rank, "by_source_channel": by_chan,
            "indices": idx, "uri": DATASET_URI,
            "schema": [f"{f.name}:{f.type}" for f in ds.schema], "sample": sample}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    """Apply the ops.ffata_exec_comp_runs DDL (idempotent)."""
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.ffata_exec_comp_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 60, memory=32768, cpu=8.0,
)
def plan_ffata_exec_comp() -> dict:
    """Remote review gate — materialize + count, write NOTHING."""
    os.makedirs(SPILL_DIR, exist_ok=True)
    con = _new_con()
    try:
        _table, metrics = _materialize(con)
    finally:
        con.close()
    return metrics


@app.local_entrypoint()
def build(dry_run: bool = False) -> None:
    if dry_run:
        print(plan_ffata_exec_comp.remote())
        return
    print(build_ffata_exec_comp.remote(trigger_callback_url=None))
    print(verify_ffata_exec_comp.remote())
