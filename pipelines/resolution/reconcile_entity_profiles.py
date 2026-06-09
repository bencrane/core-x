"""Compute worker — Gold Mirror reconciliation: entity_profile_gold (the unified profile).

The ``entity-profile-gold-pipelines`` Modal app. Write-time reconciliation that merges
the SAM.gov identity spine with the USAspending financial ledger into ONE row per UEI,
so the frontend profile and the GTM AI agents resolve an entity in a single BTREE
point-lookup. Spawned only by the Universal Dispatcher (cadence owned by Trigger v4,
``src/trigger/entity_profile_gold.ts``); no web endpoint, no embedded cron.

Source-of-truth inputs (read-only; never mutated):
  s3://data-sink/active/sam_master_entities/                     (1.54M, 1/uei — identity)
  s3://data-sink/active/sam_normalized_entities/                 (1.54M, 1/uei — name_norm sidecar)
  s3://data-sink/active/sam_pocs/                                (8.07M, uei×poc_type — contacts)
  s3://data-sink/active/usaspending/award_search/               (78.6M, 1/award — PRIME ledger)
  s3://data-sink/active/usaspending_api_fresh/contract_prime_txn (1.41M txn — 90d daily override)

GRAIN — strict 1 row per UEI. The SAM spine is already 1/uei (master ⋈ normalized are
both 1/uei). The fan-out hazard is sam_pocs (uei×poc_type, ~5.23 rows/uei): it is
ARRAY_AGG'd into one ``list<struct>`` column BEFORE the spine join. sam_pocs carries
NO phone and NO email — those fields are absent from the entire federal public stack.

FINANCIAL RECONCILIATION — the Rule of Recency, resolved at AWARD grain (NOT at the
UEI aggregate — a blind COALESCE of two UEI-level sums would replace an entity's
lifetime obligated with only its last-90-day window, a silent ~order-of-magnitude
undercount). The canonical 3-stage merge:
  A. Collapse the fresh transaction feed (1.174 txn/award) to one row per award with
     latest-modification-wins: QUALIFY row_number() … ORDER BY last_modified_date DESC.
     Project ``total_dollars_obligated`` (the cumulative-obligated snapshot) — empirically
     reconciled to award_search.total_obligation over 24,860 live overlapping awards
     (MAD $0.03, 99.99% exact). NOT federal_action_obligation (per-transaction delta),
     NOT current_total_value_of_award (contract ceiling, diverges for partial awards).
  B. FULL OUTER JOIN bulk award_search ⋈ fresh-award on the award key
     (generated_unique_award_id == contract_award_unique_key). Resolve per-award on a
     NULL-safe max(last_modified_date) precedence gate, not a blind COALESCE — a fresh
     row wins only when it is at least as recent as the bulk row. pop_end resolves from
     the winning side so brand-new fresh-only awards classify active correctly.
  C. SUM to UEI ONLY after the per-award resolve. Every lifetime award survives the
     join exactly once → no truncation, no fan-out, no double-count.

  Dollars are NOT sourced from financial_accounts_by_awards — that 454M table is a
  TAS account×award junction and carries no obligation column. award_search is the
  prime dollar ledger.

total_active_obligations DEFINITION (operator-resolved): PoP-active awards only
(``pop_end >= CURRENT_DATE``) — the current unbilled-backlog headline, not lifetime
historical spend. ``total_lifetime_obligations`` is retained unfiltered alongside it
(same GROUP BY, zero extra scan).

Data plane (clean-room — DuckDB does 100% of the transform, out-of-core with NVMe spill):
  Lance(5 datasets) → DuckDB project/resolve/aggregate → Arrow → lance.write_dataset(R2
  active, v2.1, overwrite) → BTREE[uei, cage_code, legal_name_base, total_active_obligations,
  primary_naics] + BITMAP[is_active].

Materialization durability — the overwrite blast-radius guard. ``mode="overwrite"``
drops every scalar index, so the live URI serves UNINDEXED during the in-flight index
build — an accepted, off-peak (19:00 UTC) degradation window on a ~1.54M-row table, not
a silent-wrong state. The build captures the prior version, rebuilds all indices, and
gates on list_indices() carrying every expected key. On a build/gate FAILURE it rolls
back, but ONLY to a prior version verified to still carry the full index set (restoring
to a partially-indexed generation would reintroduce the unindexed-live state); first
builds and unverifiable priors fail loud instead. A stranded-unindexed generation (e.g.
a first-build index failure) is recoverable in place via reindex_entity_profile_gold
without re-materializing. ~1.54M rows is far under the giant threshold → direct-R2
in-place index build. (A build-to-staging + atomic-swap variant would additionally close
the in-flight window; overwrite-in-place is the operator-directed shape here.)

Control plane (Trigger v4 durable callback): on terminal state writes the run row to
ops.entity_profile_gold_runs and POSTs the flat callback to wake the suspended Trigger run.

    modal run    pipelines/resolution/reconcile_entity_profiles.py::init_ops   # ops table
    modal deploy pipelines/resolution/reconcile_entity_profiles.py             # publish worker
    modal run    pipelines/resolution/reconcile_entity_profiles.py             # build + verify
    modal run    pipelines/resolution/reconcile_entity_profiles.py --dry-run   # counts, no write
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"

# Source-of-truth inputs (read-only).
MASTER_URI = os.environ.get("SAM_MASTER_URI", "s3://data-sink/active/sam_master_entities/")
NORMALIZED_URI = os.environ.get("SAM_NORMALIZED_URI", "s3://data-sink/active/sam_normalized_entities/")
POCS_URI = os.environ.get("SAM_POCS_URI", "s3://data-sink/active/sam_pocs/")
AWARD_URI = os.environ.get("USA_AWARD_SEARCH_URI", "s3://data-sink/active/usaspending/award_search/")
FRESH_URI = os.environ.get(
    "USASPENDING_API_FRESH_URI",
    "s3://data-sink/active/usaspending_api_fresh/contract_prime_txn",
)

# Output — the reconciled Gold mirror (Silver/Gold tier ⇒ v2.1, per ARCHITECTURE.md §4).
DATASET_URI = os.environ.get("ENTITY_PROFILE_GOLD_URI", "s3://data-sink/active/entity_profile_gold/")
FEED = "entity_profile_gold"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

# Index plan (approved corrections to the directive). BTREE for every load-bearing
# resolution / range key — primary_naics is BTREE (≈1k distinct 6-digit codes is
# mid-cardinality and the dominant GTM access is sector-PREFIX range, which BITMAP
# cannot serve). total_active_obligations is BTREE for "backlog ≥ $X" range filters.
# BITMAP only for the genuinely low-cardinality boolean is_active.
BTREE_INDEXES = ["uei", "cage_code", "legal_name_base", "total_active_obligations", "primary_naics"]
BITMAP_INDEXES = ["is_active"]

# Out-of-core config. The single heavy scan is award_search (78.6M × 5 cols) feeding a
# FULL OUTER JOIN + GROUP BY uei that collapses to ~1.54M rows; spill to local NVMe.
DUCKDB_MEMORY_LIMIT = "48GB"
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/duckdb_spill"

# Physically-impossible obligation guard (USAspending free-text). Largest real single
# federal award is ~$161B; anything over $1T is garbage. Nulled from dollar SUMS only —
# the award still counts toward award_count / active_award_count. Negatives (de-obligations) kept.
MAX_OBLIGATION = 1_000_000_000_000.0  # $1T

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.entity_profile_gold_runs (
    id                            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                          text        NOT NULL,
    dataset_uri                   text        NOT NULL,
    rows_written                  bigint,
    distinct_uei                  bigint,
    active_entities               bigint,
    entities_with_pocs            bigint,
    entities_with_awards          bigint,
    fresh_awards_in_window        bigint,
    sum_total_active_obligations  numeric,
    sum_total_lifetime_obligations numeric,
    indices_built                 text[],
    version_before                bigint,
    version_after                 bigint,
    status                        text        NOT NULL,
    error                         text,
    started_at                    timestamptz,
    completed_at                  timestamptz,
    recorded_at                   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS entity_profile_gold_runs_feed_idx        ON ops.entity_profile_gold_runs (feed);
CREATE INDEX IF NOT EXISTS entity_profile_gold_runs_status_idx      ON ops.entity_profile_gold_runs (status);
CREATE INDEX IF NOT EXISTS entity_profile_gold_runs_recorded_at_idx ON ops.entity_profile_gold_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("entity-profile-gold-pipelines", image=image)


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
    # Pin UTC so the bulk side's tz-naive last_modified_date (timestamp[us]) and the
    # fresh side's tz-aware string both normalize to the same UTC instant under the
    # TIMESTAMPTZ casts in the recency gate — comparison is offset-correct regardless
    # of upstream offset, not merely correct-by-coincidence of an always-+00 feed.
    con.execute("SET TimeZone='UTC'")
    return con


def _idx_name(i) -> str:
    return i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))


def _rollback_to_indexed(so, version_before, expected, ix_exc) -> None:
    """Roll the dataset back ONLY to a prior version that carries the COMPLETE expected
    index set. Restoring to a partially-indexed generation would reintroduce the
    unindexed-live state the guard prevents; in that case (or first build, where
    version_before is None) do NOT restore — leave the failure loud and point at the
    reindex recovery path. Never let a restore error mask the original index failure."""
    import lance

    if version_before is None:
        print("No prior version to roll back to (first build). Dataset may be LIVE-BUT-"
              "UNINDEXED — run reindex_entity_profile_gold to recover.")
        return
    try:
        prior = lance.dataset(DATASET_URI, storage_options=so, version=version_before)
        prior_idx = {_idx_name(i) for i in prior.list_indices()}
        if expected.issubset(prior_idx):
            prior.restore()
            print(f"ROLLED BACK to last-good fully-indexed v{version_before}")
        else:
            print(f"WARN: prior v{version_before} is NOT fully indexed (has {sorted(prior_idx)}); "
                  "NOT restoring. Dataset may be LIVE-BUT-UNINDEXED — run "
                  "reindex_entity_profile_gold to recover.")
    except Exception as rb_exc:  # noqa: BLE001 — must never mask the original ix_exc
        print(f"WARN: rollback to v{version_before} failed ({rb_exc}); original index "
              f"failure was: {ix_exc}. Run reindex_entity_profile_gold to recover.")


# --------------------------------------------------------------------------- #
# DuckDB transform — Phase 2 (financial), Phase 1 (SAM spine), Phase 3 (assembly)
# --------------------------------------------------------------------------- #
def _build_financial_resume(con, so) -> None:
    """Phase 2 — per-award recency resolve → SUM to UEI. Builds TEMP `financial_resume`
    (1 row/uei: total_active_obligations, total_lifetime_obligations, award counts) and
    TEMP `fresh_award` (kept for metrics)."""
    import lance

    # Stage A — collapse the transaction-grain fresh feed to one row per award,
    # latest-modification-wins. All fresh columns land VARCHAR (verbatim API) → TRY_CAST.
    fresh_cols = ["recipient_uei", "contract_award_unique_key", "contract_transaction_unique_key",
                  "total_dollars_obligated", "period_of_performance_current_end_date",
                  "last_modified_date"]
    con.register("fresh_reader", lance.dataset(FRESH_URI, storage_options=so)
                 .scanner(columns=fresh_cols).to_reader())
    # The fresh side WINS the recency resolve for the entire 90-day window, so it must
    # carry the SAME physical-impossibility obligation guard as the bulk side — TRY_CAST
    # nulls only unparseable strings, NOT a parseable sentinel like 1e18, which would
    # otherwise flow straight into the active/lifetime sums.
    con.execute(f"""
        CREATE TEMP TABLE fresh_award AS
        SELECT award_key, recipient_uei, obl, pop_end, lmd FROM (
            SELECT
                nullif(trim(contract_award_unique_key), '')              AS award_key,
                nullif(trim(contract_transaction_unique_key), '')        AS txn_key,
                nullif(trim(recipient_uei), '')                          AS recipient_uei,
                CASE WHEN abs(TRY_CAST(total_dollars_obligated AS DOUBLE)) <= {MAX_OBLIGATION}
                     THEN TRY_CAST(total_dollars_obligated AS DOUBLE) END AS obl,
                TRY_CAST(period_of_performance_current_end_date AS DATE) AS pop_end,
                TRY_CAST(last_modified_date AS TIMESTAMPTZ)              AS lmd
            FROM fresh_reader
        )
        WHERE award_key IS NOT NULL
        -- Deterministic latest-modification-wins: lmd DESC, then the transaction PK as a
        -- stable tiebreak so same-timestamp transactions resolve identically on every
        -- rebuild (bit-stable, matching the spine's source_file-DESC convention). Without
        -- it, threads pick an arbitrary tied row and pop_end → active classification drifts.
        QUALIFY row_number() OVER (PARTITION BY award_key
                ORDER BY lmd DESC NULLS LAST, txn_key DESC) = 1
    """)
    con.unregister("fresh_reader")

    # Stages B + C — FULL OUTER JOIN bulk award_search ⋈ fresh-award, precedence-resolve
    # per award, THEN aggregate to uei. award_search is streamed (not materialized); the
    # hash is built on the small fresh side.
    aw_cols = ["recipient_uei", "generated_unique_award_id", "total_obligation",
               "period_of_performance_current_end_date", "last_modified_date"]
    con.register("aw_reader", lance.dataset(AWARD_URI, storage_options=so)
                 .scanner(columns=aw_cols).to_reader())
    con.execute(f"""
        CREATE TEMP TABLE financial_resume AS
        WITH bulk_award AS (
            SELECT
                nullif(trim(generated_unique_award_id), '')   AS award_key,
                nullif(trim(recipient_uei), '')               AS recipient_uei,
                CASE WHEN abs(total_obligation) <= {MAX_OBLIGATION}
                     THEN total_obligation END                AS obl,
                period_of_performance_current_end_date        AS pop_end,
                TRY_CAST(last_modified_date AS TIMESTAMPTZ)   AS lmd
            FROM aw_reader
        ),
        joined AS (
            SELECT
                coalesce(f.recipient_uei, b.recipient_uei)    AS uei,
                f.obl AS f_obl, b.obl AS b_obl,
                f.pop_end AS f_pop, b.pop_end AS b_pop,
                -- NULL-safe recency gate: fresh wins only when at least as recent as bulk.
                (f.award_key IS NOT NULL
                 AND (b.lmd IS NULL OR f.lmd >= b.lmd))       AS api_wins
            FROM bulk_award b
            FULL OUTER JOIN fresh_award f ON f.award_key = b.award_key
        ),
        resolved AS (
            SELECT
                uei,
                CASE WHEN api_wins THEN f_obl ELSE b_obl END  AS obligation,
                CASE WHEN api_wins THEN f_pop ELSE b_pop END  AS pop_end
            FROM joined
        )
        SELECT
            uei,
            round(sum(obligation) FILTER (WHERE pop_end >= CURRENT_DATE), 2) AS total_active_obligations,
            round(sum(obligation), 2)                                        AS total_lifetime_obligations,
            count(*)                                                         AS award_count,
            count(*) FILTER (WHERE pop_end >= CURRENT_DATE)                  AS active_award_count
        FROM resolved
        WHERE uei IS NOT NULL
        GROUP BY 1
    """)
    con.unregister("aw_reader")


def _build_sam_spine(con, so) -> None:
    """Phase 1 — SAM identity spine (1 row/uei) with POCs compressed into a nested
    list<struct>. Builds TEMP `sam_spine`."""
    import lance

    # Fan-out fix: uei×poc_type (~5.23 rows/uei) → one list<struct> per uei. v2 rows
    # only (uei-keyed); legacy cage-keyed rows (uei NULL) are out of scope for the
    # UEI-grain gold. No phone/email exist in sam_pocs.
    poc_cols = ["uei", "source_family", "poc_slot_no", "poc_type", "first_name",
                "last_name", "title", "address_line_1", "address_line_2", "city",
                "state", "zip5", "zip4", "country", "full_name"]
    con.register("pocs_reader", lance.dataset(POCS_URI, storage_options=so)
                 .scanner(columns=poc_cols).to_reader())
    con.execute("""
        CREATE TEMP TABLE pocs_nested AS
        SELECT
            uei,
            array_agg(struct_pack(
                poc_type       := poc_type,
                poc_slot_no    := poc_slot_no,
                full_name      := full_name,
                first_name     := first_name,
                last_name      := last_name,
                title          := title,
                address_line_1 := address_line_1,
                address_line_2 := address_line_2,
                city           := city,
                state          := state,
                zip5           := zip5,
                zip4           := zip4,
                country        := country
            ) ORDER BY poc_slot_no) AS pocs
        FROM pocs_reader
        WHERE source_family = 'v2' AND nullif(trim(uei), '') IS NOT NULL
        GROUP BY uei
    """)
    con.unregister("pocs_reader")

    master_cols = ["uei", "cage_code", "legal_business_name", "dba_name", "primary_naics",
                   "is_active", "physical_address_line_1", "physical_address_city",
                   "physical_address_province_or_state", "physical_address_zip_postal_code",
                   "physical_address_country_code", "initial_registration_date",
                   "registration_expiration_date"]
    norm_cols = ["uei", "normalized_legal_name", "legal_name_base"]
    con.register("master_reader", lance.dataset(MASTER_URI, storage_options=so)
                 .scanner(columns=master_cols).to_reader())
    con.register("normalized_reader", lance.dataset(NORMALIZED_URI, storage_options=so)
                 .scanner(columns=norm_cols).to_reader())
    con.execute("""
        CREATE TEMP TABLE sam_spine AS
        SELECT
            m.uei,
            m.cage_code,
            m.legal_business_name,
            m.dba_name,
            n.normalized_legal_name,
            n.legal_name_base,
            m.primary_naics,
            m.is_active,
            m.physical_address_line_1,
            m.physical_address_city,
            m.physical_address_province_or_state AS physical_address_state,
            m.physical_address_zip_postal_code,
            m.physical_address_country_code,
            m.initial_registration_date,
            m.registration_expiration_date,
            p.pocs
        FROM master_reader m
        JOIN normalized_reader n USING (uei)          -- both 1/uei, non-fanning
        LEFT JOIN pocs_nested p USING (uei)           -- NULL pocs when no populated slot
    """)
    con.unregister("master_reader")
    con.unregister("normalized_reader")


def _materialize(con):
    """Run all three phases and return (arrow_table, metrics). One scan per source."""
    so = _r2_storage_options()
    _build_financial_resume(con, so)
    _build_sam_spine(con, so)

    # Phase 3 — assemble the gold row (1/uei). LEFT JOIN keeps registered entities with
    # zero federal awards (obligations default to 0.0, has_federal_awards = false).
    con.execute("""
        CREATE TEMP TABLE entity_profile_gold AS
        SELECT
            s.uei,
            s.cage_code,
            s.legal_business_name,
            s.dba_name,
            s.normalized_legal_name,
            s.legal_name_base,
            s.primary_naics,
            s.is_active,
            s.physical_address_line_1,
            s.physical_address_city,
            s.physical_address_state,
            s.physical_address_zip_postal_code,
            s.physical_address_country_code,
            s.initial_registration_date,
            s.registration_expiration_date,
            coalesce(f.total_active_obligations, 0.0)   AS total_active_obligations,
            coalesce(f.total_lifetime_obligations, 0.0) AS total_lifetime_obligations,
            coalesce(f.award_count, 0)                  AS award_count,
            coalesce(f.active_award_count, 0)           AS active_award_count,
            (f.uei IS NOT NULL)                         AS has_federal_awards,
            s.pocs,
            CURRENT_DATE                                AS profile_as_of_date
        FROM sam_spine s
        LEFT JOIN financial_resume f ON f.uei = s.uei
    """)

    rows, d_uei, active_ent, with_pocs, with_awards, sum_act, sum_life = con.execute("""
        SELECT count(*), count(DISTINCT uei),
               count(*) FILTER (WHERE is_active),
               count(*) FILTER (WHERE pocs IS NOT NULL),
               count(*) FILTER (WHERE has_federal_awards),
               round(sum(total_active_obligations), 2),
               round(sum(total_lifetime_obligations), 2)
        FROM entity_profile_gold
    """).fetchone()
    fresh_awards = con.execute("SELECT count(*) FROM fresh_award").fetchone()[0]

    table = con.sql("SELECT * FROM entity_profile_gold").to_arrow_table()
    metrics = {
        "rows": int(rows),
        "distinct_uei": int(d_uei),
        "active_entities": int(active_ent or 0),
        "entities_with_pocs": int(with_pocs or 0),
        "entities_with_awards": int(with_awards or 0),
        "fresh_awards_in_window": int(fresh_awards or 0),
        "sum_total_active_obligations": float(sum_act or 0.0),
        "sum_total_lifetime_obligations": float(sum_life or 0.0),
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
                INSERT INTO ops.entity_profile_gold_runs
                    (feed, dataset_uri, rows_written, distinct_uei, active_entities,
                     entities_with_pocs, entities_with_awards, fresh_awards_in_window,
                     sum_total_active_obligations, sum_total_lifetime_obligations,
                     indices_built, version_before, version_after,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, metrics.get("rows"), metrics.get("distinct_uei"),
                 metrics.get("active_entities"), metrics.get("entities_with_pocs"),
                 metrics.get("entities_with_awards"), metrics.get("fresh_awards_in_window"),
                 metrics.get("sum_total_active_obligations"),
                 metrics.get("sum_total_lifetime_obligations"),
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
)
def build_entity_profile_gold(trigger_callback_url: str | None = None) -> dict:
    """Reconcile SAM + USAspending into entity_profile_gold and publish to R2 active.
    Idempotent full overwrite, guarded: indices are rebuilt and gated, and on index
    failure the dataset is rolled back ONLY to a prior version verified fully-indexed
    (else it fails loud, recoverable via reindex_entity_profile_gold)."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error = "error", None
    version_before, version_after = None, None
    indices: list[str] = []
    metrics = {"rows": 0, "distinct_uei": 0, "active_entities": 0, "entities_with_pocs": 0,
               "entities_with_awards": 0, "fresh_awards_in_window": 0,
               "sum_total_active_obligations": 0.0, "sum_total_lifetime_obligations": 0.0}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics = _materialize(con)
        finally:
            con.close()
        print(f"Built entity_profile_gold: {metrics}")

        # Capture the last-good (fully-indexed) generation for rollback. None on first build.
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

        # ── overwrite blast-radius guard ───────────────────────────────────────────
        # overwrite drops all scalar indices; the live URI serves unindexed during the
        # in-flight index build (an accepted, off-peak 19:00-UTC window on a ~1.54M-row
        # table — recoverable via reindex_entity_profile_gold, never silently wrong).
        # On a build/gate FAILURE we roll back, but ONLY to a prior version verified to
        # carry the full index set — restoring to a partially-indexed prior generation
        # would reintroduce the unindexed-live state this guard exists to prevent.
        expected = {f"{c}_idx" for c in BTREE_INDEXES + BITMAP_INDEXES}
        try:
            ds = lance.dataset(DATASET_URI, storage_options=so)
            for col in BTREE_INDEXES:
                ds.create_scalar_index(col, index_type="BTREE", replace=True)
                print(f"  BTREE  ✓ {col}")
            for col in BITMAP_INDEXES:
                ds.create_scalar_index(col, index_type="BITMAP", replace=True)
                print(f"  BITMAP ✓ {col}")
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
                        "entities_with_awards": metrics["entities_with_awards"],
                        "sum_total_active_obligations": metrics["sum_total_active_obligations"]})

    if status != "success":
        raise RuntimeError(f"entity_profile_gold build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, **metrics,
            "indices": indices, "version_after": version_after}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_entity_profile_gold() -> dict:
    """Read-back proof — open the published dataset from R2 and report counts, indices,
    POC fan-out sanity, dollar sums, and an indexed point-lookup, independent of the
    write path."""
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
    active, with_pocs, with_awards = con.execute("""
        SELECT count(*) FILTER (WHERE is_active),
               count(*) FILTER (WHERE pocs IS NOT NULL),
               count(*) FILTER (WHERE has_federal_awards) FROM d2""").fetchone()
    sum_act, sum_life, avg_pocs = con.execute("""
        SELECT round(sum(total_active_obligations), 2),
               round(sum(total_lifetime_obligations), 2),
               round(avg(len(pocs)) FILTER (WHERE pocs IS NOT NULL), 2) FROM d2""").fetchone()
    con.close()

    sample = ds.scanner(
        filter="has_federal_awards = true AND total_active_obligations > 0",
        columns=["uei", "legal_business_name", "cage_code", "primary_naics", "is_active",
                 "total_active_obligations", "total_lifetime_obligations", "award_count",
                 "active_award_count", "pocs"],
        limit=1,
    ).to_table().to_pylist()

    return {"rows": n, "distinct_uei": d_uei, "duplicate_uei": dup, "indices": idx,
            "uri": DATASET_URI, "active_entities": active, "entities_with_pocs": with_pocs,
            "entities_with_awards": with_awards, "avg_pocs_per_entity": avg_pocs,
            "sum_total_active_obligations": float(sum_act or 0),
            "sum_total_lifetime_obligations": float(sum_life or 0),
            "schema": [f"{f.name}:{f.type}" for f in ds.schema],
            "sample": sample}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 60)
def reindex_entity_profile_gold() -> dict:
    """Recovery — (re)build + gate every scalar index on the LIVE published dataset
    WITHOUT re-materializing. Use when a generation is stranded unindexed (e.g. a
    first-build index failure, where there is no prior version to roll back to). The
    table is ~1.54M rows → direct-R2 in-place build, idempotent (replace=True)."""
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        print(f"  BTREE  ✓ {col}")
    for col in BITMAP_INDEXES:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        print(f"  BITMAP ✓ {col}")
    ds = lance.dataset(DATASET_URI, storage_options=so)
    indices = sorted({_idx_name(i) for i in ds.list_indices()})
    missing = {f"{c}_idx" for c in BTREE_INDEXES + BITMAP_INDEXES} - set(indices)
    if missing:
        raise RuntimeError(f"reindex gate failed: missing {sorted(missing)} (built {indices})")
    return {"status": "ok", "uri": DATASET_URI, "indices": indices,
            "version": ds.version, "rows": ds.count_rows()}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    """Apply the ops.entity_profile_gold_runs DDL (idempotent)."""
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.entity_profile_gold_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 120, memory=65536, cpu=8.0,
)
def plan_entity_profile_gold() -> dict:
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
        print(plan_entity_profile_gold.remote())
        return
    print(build_entity_profile_gold.remote(trigger_callback_url=None))
    print(verify_entity_profile_gold.remote())
