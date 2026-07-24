"""Compute worker — Gold Mirror: entity_award_lines_gold (per-UEI prime award line items).

The ``entity-award-lines-gold-pipelines`` Modal app. A write-time precompute that
collapses the 78.6M-row USAspending ``award_search`` prime ledger into ONE row per UEI,
each carrying the entity's top-N active and closed prime contract line items as nested
``list<struct>`` columns — so the catalyst_api ``active-contracts`` / ``past-performance``
surfaces resolve via a single BTREE point-lookup instead of a cold ~80s scan of the
shared 78.6M-row dataset on every request.

Why this exists (the Gold-Mirror ruling, extended to line items): ``entity_profile_gold``
already pre-materializes the headline count/total aggregates so ``/overview`` is a pure
point-lookup. The active/past CONTRACT LISTS were the one surface left scanning
``award_search`` live (``lance_store._award_line_items``). award_search opens a fresh
dataset handle per request (never cached), so each call cold-loads the 379 MB
``recipient_uei_idx`` BTREE over R2 — ~80s every time, surfacing as a Railway 502 / UI
timeout. Mirroring the line items into a small 1-row-per-UEI gold dataset moves that heavy
scan offline and makes the hot path a sub-second indexed lookup.

Source-of-truth input (read-only; never mutated):
  s3://data-sink/active/usaspending/award_search/   (78.6M, 1/award — PRIME ledger)

GRAIN — strict 1 row per UEI. Per UEI the awards are split by PoP currency
(``period_of_performance_current_end_date >= CURRENT_DATE`` ⇒ active, else closed) and the
top ``CAP`` of each side by obligation (desc) are ARRAY_AGG'd into one ``list<struct>``
column. The cap bounds the row size for mega-primes (the dashboard only ever shows the top
N); ``active_line_count`` / ``closed_line_count`` retain the true pre-cap totals for ops.

Active/closed is classified at build CURRENT_DATE — identical convention to
``entity_profile_gold.active_award_count`` — so the line lists stay consistent with the
headline counts the same Overview reads. Staleness is bounded by the rebuild cadence.

Struct fields are the EXACT ``award_search`` column names the catalyst_api
``ActiveContract.from_row`` consumes (apps/catalyst_api/src/models.py), so the gateway maps
a gold struct to the wire model with no projection drift.

Data plane (clean-room — DuckDB does 100% of the transform, out-of-core with NVMe spill):
  Lance(award_search) → DuckDB project/classify/rank/aggregate → Arrow →
  lance.write_dataset(R2 active, v2.1, overwrite) → BTREE[uei].

Materialization durability — the overwrite blast-radius guard mirrors
``reconcile_entity_profiles``: ``mode="overwrite"`` drops the scalar index, so the build
captures the prior fully-indexed version, rebuilds + gates on ``list_indices()``, and on a
build/gate FAILURE rolls back ONLY to a prior version verified to still carry the index
(else it fails loud, recoverable in place via ``reindex_entity_award_lines_gold``).

Control plane (Trigger v4 durable callback): on terminal state writes the run row to
ops.entity_award_lines_gold_runs and POSTs the flat callback to wake the suspended run.

    modal run    pipelines/resolution/award_lines_gold.py::init_ops   # ops table
    modal deploy pipelines/resolution/award_lines_gold.py             # publish worker
    modal run    pipelines/resolution/award_lines_gold.py             # SPAWN build (fc-id)
    modal run    pipelines/resolution/award_lines_gold.py --dry-run   # counts, no write (sync)

The default entrypoint no longer tethers the build or prints its result: it deploy-if-stale
gates (pipelines/_shared/launch.py), spawns ``build_entity_award_lines_gold`` on the
DEPLOYED app, prints the ``fc-…`` id, and returns. The worker's terminal
ops.entity_award_lines_gold_runs row and the app logs are the record. Follow protocol
(build → verify is two phases):

  1. Follow:  ``modal app logs entity-award-lines-gold-pipelines``
     Result:  ``modal.FunctionCall.from_id("fc-...").get(timeout=0)``  # TimeoutError while running
  2. Build is terminal when the fc returns or ops.entity_award_lines_gold_runs gains the
     run's row (feed='entity_award_lines_gold', status='success').
  3. Only then run phase 2 (read-only, timeout 600 — sync is safe)::

         modal.Function.from_name("entity-award-lines-gold-pipelines",
                                  "verify_entity_award_lines_gold").remote()

  4. Stranded-unindexed generation (index-gate failure or suspected kill): recover via
     ``reindex_entity_award_lines_gold`` on the deployed app.
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"

# Source-of-truth input (read-only).
AWARD_URI = os.environ.get("USA_AWARD_SEARCH_URI", "s3://data-sink/active/usaspending/award_search/")

# Output — the per-UEI line-items Gold mirror (Silver/Gold tier ⇒ v2.1, per ARCHITECTURE.md §4).
DATASET_URI = os.environ.get("ENTITY_AWARD_LINES_GOLD_URI", "s3://data-sink/active/entity_award_lines_gold/")
FEED = "entity_award_lines_gold"

DATA_STORAGE_VERSION = "2.1"
# R2 multipart guard. The rows are WIDE — two nested list<struct> columns (up to 2·CAP
# structs/UEI). A 1.05M-row data file of these crosses R2's part-escalation boundary, where
# object_store bumps to a larger, NON-UNIFORM part size mid-upload and R2 rejects it
# ("400 InvalidPart: all non-trailing parts must have the same length" — AWS tolerates it,
# R2 does not; see pipelines/ingest_epa/materialize_epa.py). Capping each data file to 256 MiB
# keeps it in the uniform-5 MiB-part zone → R2-compliant direct write. The uei BTREE on
# ~1.5M rows stays far under the index multipart threshold (direct-R2-safe, per entity_profile_gold).
MAX_ROWS_PER_FILE = 250_000
MAX_BYTES_PER_FILE = 256 * 1024**2

# Per-UEI, per-side fan-out ceiling. Mirrors lance_store._AWARDS_HARD_CAP (the gateway's
# hard limit) — the dashboard never shows more than this many active/closed line items, so
# storing the top CAP-by-obligation of each side is lossless for the surface while bounding
# the row size of mega-primes (a defense prime can carry 100k+ awards).
CAP = 100

# The award_search line-item projection — the EXACT column names catalyst_api
# ActiveContract.from_row / RecentAward.from_row read off each struct (no renames).
LINE_ITEM_COLS = [
    "generated_unique_award_id", "display_award_id", "category", "type_description",
    "total_obligation", "award_amount", "naics_code", "naics_description",
    "product_or_service_description", "funding_toptier_agency_name",
    "awarding_toptier_agency_name", "period_of_performance_start_date",
    "period_of_performance_current_end_date", "description", "type_set_aside", "action_date",
]

# Index plan — a single BTREE on the resolution key. 1 row/uei → a point-lookup gateway read.
BTREE_INDEXES = ["uei"]

# Out-of-core config. The single heavy scan is award_search (78.6M × 17 cols) feeding a
# window(rank) + GROUP BY uei that collapses to ~1.5M rows; spill to local NVMe.
DUCKDB_MEMORY_LIMIT = "48GB"
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/duckdb_spill"

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.entity_award_lines_gold_runs (
    id                       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                     text        NOT NULL,
    dataset_uri              text        NOT NULL,
    rows_written             bigint,
    distinct_uei             bigint,
    entities_with_active     bigint,
    entities_with_closed     bigint,
    sum_active_line_count    bigint,
    sum_closed_line_count    bigint,
    indices_built            text[],
    version_before           bigint,
    version_after            bigint,
    status                   text        NOT NULL,
    error                    text,
    started_at               timestamptz,
    completed_at             timestamptz,
    recorded_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS entity_award_lines_gold_runs_feed_idx        ON ops.entity_award_lines_gold_runs (feed);
CREATE INDEX IF NOT EXISTS entity_award_lines_gold_runs_status_idx      ON ops.entity_award_lines_gold_runs (status);
CREATE INDEX IF NOT EXISTS entity_award_lines_gold_runs_recorded_at_idx ON ops.entity_award_lines_gold_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("entity-award-lines-gold-pipelines", image=image)


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


def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    con.execute("SET TimeZone='UTC'")
    return con


def _idx_name(i) -> str:
    return i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))


def _rollback_to_indexed(so, version_before, expected, ix_exc) -> None:
    """Roll back ONLY to a prior version carrying the COMPLETE expected index set —
    restoring a partially-indexed generation would reintroduce an unindexed-live URI.
    First builds / unverifiable priors fail loud (recover via reindex_*)."""
    import lance

    if version_before is None:
        print("No prior version to roll back to (first build). Dataset may be LIVE-BUT-"
              "UNINDEXED — run reindex_entity_award_lines_gold to recover.")
        return
    try:
        prior = lance.dataset(DATASET_URI, storage_options=so, version=version_before)
        prior_idx = {_idx_name(i) for i in prior.list_indices()}
        if expected.issubset(prior_idx):
            prior.restore()
            print(f"ROLLED BACK to last-good fully-indexed v{version_before}")
        else:
            print(f"WARN: prior v{version_before} is NOT fully indexed (has {sorted(prior_idx)}); "
                  "NOT restoring. Run reindex_entity_award_lines_gold to recover.")
    except Exception as rb_exc:  # noqa: BLE001 — must never mask the original ix_exc
        print(f"WARN: rollback to v{version_before} failed ({rb_exc}); original index "
              f"failure was: {ix_exc}. Run reindex_entity_award_lines_gold to recover.")


# --------------------------------------------------------------------------- #
# DuckDB transform
# --------------------------------------------------------------------------- #
def _struct_pack() -> str:
    """The line-item struct literal — field names mirror award_search columns 1:1."""
    return "struct_pack(" + ", ".join(f"{c} := {c}" for c in LINE_ITEM_COLS) + ")"


def _materialize(con):
    """Single award_search scan → per-(uei, side) top-CAP line items → 1 row/uei.

    Stage 1 (`ranked`): project the raw line-item columns, classify (active iff pop_end >=
             CURRENT_DATE), and compute the per-(uei, side) rank by obligation desc + the
             pre-cap per-side count (windows). NULL-uei and NULL pop_end rows are excluded
             (NULL pop_end ⇒ non-current, never active — matching lance_store._award_line_items).
    Stage 2 (`capped`): keep only the top CAP per (uei, side) BEFORE building any struct, so
             the aggregate input is ≤2·CAP rows/uei (a few hundred MB total) rather than the
             full 78.6M-row struct stream — the memory-safe shape for the heavy build.
    Stage 3: struct_pack + ARRAY_AGG (obligation desc) the two nested lists, carry the pre-cap
             counts (constant within a side ⇒ max()), collapse to 1 row/uei.
    """
    import lance

    so = _r2_storage_options()
    cols = ["recipient_uei", *LINE_ITEM_COLS]
    raw = ", ".join(LINE_ITEM_COLS)
    con.register("aw_reader", lance.dataset(AWARD_URI, storage_options=so)
                 .scanner(columns=cols).to_reader())

    con.execute(f"""
        CREATE TEMP TABLE capped AS
        SELECT uei, is_active, _obl, {raw}, side_count
        FROM (
            SELECT
                uei, is_active, _obl, {raw},
                row_number() OVER (PARTITION BY uei, is_active
                                   ORDER BY _obl DESC, generated_unique_award_id) AS rn,
                count(*)     OVER (PARTITION BY uei, is_active)                    AS side_count
            FROM (
                SELECT
                    nullif(trim(recipient_uei), '')                          AS uei,
                    (period_of_performance_current_end_date >= CURRENT_DATE)  AS is_active,
                    coalesce(total_obligation, award_amount, 0.0)             AS _obl,
                    {raw}
                FROM aw_reader
                WHERE nullif(trim(recipient_uei), '') IS NOT NULL
                  AND period_of_performance_current_end_date IS NOT NULL
            )
        )
        WHERE rn <= {CAP}
    """)
    con.unregister("aw_reader")

    con.execute(f"""
        CREATE TEMP TABLE entity_award_lines_gold AS
        SELECT
            uei,
            array_agg({_struct_pack()} ORDER BY _obl DESC) FILTER (WHERE is_active)      AS active_contracts,
            array_agg({_struct_pack()} ORDER BY _obl DESC) FILTER (WHERE NOT is_active)  AS past_performance,
            coalesce(max(side_count) FILTER (WHERE is_active), 0)                        AS active_line_count,
            coalesce(max(side_count) FILTER (WHERE NOT is_active), 0)                    AS closed_line_count,
            CURRENT_DATE                                                                 AS lines_as_of_date
        FROM capped
        GROUP BY uei
    """)

    rows, d_uei, with_active, with_closed, sum_active, sum_closed = con.execute("""
        SELECT count(*), count(DISTINCT uei),
               count(*) FILTER (WHERE active_line_count > 0),
               count(*) FILTER (WHERE closed_line_count > 0),
               sum(active_line_count), sum(closed_line_count)
        FROM entity_award_lines_gold
    """).fetchone()

    table = con.sql("SELECT * FROM entity_award_lines_gold").to_arrow_table()
    metrics = {
        "rows": int(rows),
        "distinct_uei": int(d_uei),
        "entities_with_active": int(with_active or 0),
        "entities_with_closed": int(with_closed or 0),
        "sum_active_line_count": int(sum_active or 0),
        "sum_closed_line_count": int(sum_closed or 0),
    }
    return table, metrics


# --------------------------------------------------------------------------- #
# ops ledger + Trigger callback
# --------------------------------------------------------------------------- #
def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED") or os.environ.get("HQX_DB_URL_DIRECT")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED/DIRECT not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record_run(*, metrics, indices, version_before, version_after,
                status, error, started_at, completed_at) -> None:
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
            cur.execute(
                """
                INSERT INTO ops.entity_award_lines_gold_runs
                    (feed, dataset_uri, rows_written, distinct_uei, entities_with_active,
                     entities_with_closed, sum_active_line_count, sum_closed_line_count,
                     indices_built, version_before, version_after,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, metrics.get("rows"), metrics.get("distinct_uei"),
                 metrics.get("entities_with_active"), metrics.get("entities_with_closed"),
                 metrics.get("sum_active_line_count"), metrics.get("sum_closed_line_count"),
                 indices, version_before, version_after,
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
    timeout=60 * 120,
    memory=65536,
    cpu=8.0,
    max_containers=1,
)
def build_entity_award_lines_gold(trigger_callback_url: str | None = None) -> dict:
    """Precompute per-UEI active/closed prime award line items and publish to R2 active.
    Idempotent full overwrite, guarded: the uei BTREE is rebuilt + gated, and on index
    failure the dataset is rolled back ONLY to a prior fully-indexed version (else fails
    loud, recoverable via reindex_entity_award_lines_gold)."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error = "error", None
    version_before, version_after = None, None
    indices: list[str] = []
    metrics = {"rows": 0, "distinct_uei": 0, "entities_with_active": 0,
               "entities_with_closed": 0, "sum_active_line_count": 0, "sum_closed_line_count": 0}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics = _materialize(con)
        finally:
            con.close()
        print(f"Built entity_award_lines_gold: {metrics}")

        try:
            version_before = lance.dataset(DATASET_URI, storage_options=so).version
        except Exception:  # noqa: BLE001 — dataset does not exist yet
            version_before = None

        lance.write_dataset(
            table, DATASET_URI, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        print(f"Wrote Lance dataset (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")

        # overwrite drops all scalar indices; rebuild + gate, roll back only to a fully-indexed prior.
        expected = {f"{c}_idx" for c in BTREE_INDEXES}
        try:
            ds = lance.dataset(DATASET_URI, storage_options=so)
            for col in BTREE_INDEXES:
                ds.create_scalar_index(col, index_type="BTREE", replace=True)
                print(f"  BTREE  ✓ {col}")
            ds = lance.dataset(DATASET_URI, storage_options=so)
            indices = sorted({_idx_name(i) for i in ds.list_indices()})
            missing = expected - set(indices)
            if missing:
                raise RuntimeError(f"index gate failed: missing {sorted(missing)} (built {indices})")
            version_after = ds.version
        except Exception as ix_exc:  # noqa: BLE001
            _rollback_to_indexed(so, version_before, expected, ix_exc)
            raise RuntimeError(f"index hydration failed: {ix_exc}") from ix_exc

        committed = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        metrics["rows"] = int(committed)
        print(f"Committed rows: {committed:,}  indices={indices}  v{version_before}→v{version_after}")
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(metrics=metrics, indices=indices, version_before=version_before,
                    version_after=version_after, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": metrics["rows"], "feed": FEED,
                        "dataset_uri": DATASET_URI,
                        "distinct_uei": metrics["distinct_uei"],
                        "entities_with_active": metrics["entities_with_active"]})

    if status != "success":
        raise RuntimeError(f"entity_award_lines_gold build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, **metrics,
            "indices": indices, "version_after": version_after}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_entity_award_lines_gold() -> dict:
    """Read-back proof — open the published dataset from R2, report counts, the index,
    list-length sanity, and an indexed point-lookup, independent of the write path."""
    import lance
    import duckdb

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = sorted({_idx_name(i) for i in ds.list_indices()})
    con = duckdb.connect()
    con.register("d", ds.scanner().to_reader())
    con.execute("CREATE TEMP TABLE d2 AS SELECT * FROM d")
    n, d_uei, dup = con.execute(
        "SELECT count(*), count(DISTINCT uei), count(*) - count(DISTINCT uei) FROM d2").fetchone()
    with_active, with_closed, max_active, max_closed = con.execute("""
        SELECT count(*) FILTER (WHERE len(active_contracts) > 0),
               count(*) FILTER (WHERE len(past_performance) > 0),
               max(len(active_contracts)), max(len(past_performance)) FROM d2""").fetchone()
    con.close()

    sample = ds.scanner(
        filter="uei = 'CW52DR9J9DY4'",
        columns=["uei", "active_contracts", "past_performance",
                 "active_line_count", "closed_line_count", "lines_as_of_date"],
        limit=1,
    ).to_table().to_pylist()
    sample_shape = None
    if sample:
        s = sample[0]
        sample_shape = {
            "uei": s["uei"],
            "active_contracts_len": len(s["active_contracts"] or []),
            "past_performance_len": len(s["past_performance"] or []),
            "active_line_count": s["active_line_count"],
            "closed_line_count": s["closed_line_count"],
            "lines_as_of_date": str(s["lines_as_of_date"]),
            "first_past_award": (s["past_performance"][0]["display_award_id"]
                                 if s["past_performance"] else None),
        }

    return {"rows": n, "distinct_uei": d_uei, "duplicate_uei": dup, "indices": idx,
            "uri": DATASET_URI, "entities_with_active": with_active,
            "entities_with_closed": with_closed, "max_active_lines": max_active,
            "max_closed_lines": max_closed,
            "cap": CAP,
            "schema": [f"{f.name}:{f.type}" for f in ds.schema],
            "sample_CW52DR9J9DY4": sample_shape}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 60)
def reindex_entity_award_lines_gold() -> dict:
    """Recovery — (re)build + gate the uei BTREE on the LIVE published dataset WITHOUT
    re-materializing. Use when a generation is stranded unindexed (e.g. a first-build
    index failure). Idempotent (replace=True)."""
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        print(f"  BTREE  ✓ {col}")
    ds = lance.dataset(DATASET_URI, storage_options=so)
    indices = sorted({_idx_name(i) for i in ds.list_indices()})
    missing = {f"{c}_idx" for c in BTREE_INDEXES} - set(indices)
    if missing:
        raise RuntimeError(f"reindex gate failed: missing {sorted(missing)} (built {indices})")
    return {"status": "ok", "uri": DATASET_URI, "indices": indices,
            "version": ds.version, "rows": ds.count_rows()}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    """Apply the ops.entity_award_lines_gold_runs DDL (idempotent)."""
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.entity_award_lines_gold_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 120, memory=65536, cpu=8.0,
)
def plan_entity_award_lines_gold() -> dict:
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
        print(plan_entity_award_lines_gold.remote())
        return
    from pipelines._shared.launch import spawn_deployed

    spawn_deployed("entity-award-lines-gold-pipelines", "build_entity_award_lines_gold",
                   deploy_file=__file__, trigger_callback_url=None)
    print("Phase 2 (after build is terminal — module docstring follow protocol): run "
          "verify_entity_award_lines_gold on the deployed app.")
