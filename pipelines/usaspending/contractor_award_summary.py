"""Compute worker — USAspending Contractor Award Summary (the resume, precomputed).

The ``usaspending-award-summary-pipelines`` Modal app. Collapses the two unbounded
per-UEI federal-spend tables into ONE summary row per recipient UEI so the GovCon
contractor profile resume renders as a single BTREE point-lookup (< 100 ms) instead
of a 100k–1.7M-row at-request aggregation. Implements §6.1 of
``docs/reference/GOVCON_PROFILE_MATERIALIZATION_SCHEMA.md`` and extends it with the
prime + subaward split mandated by Directive 7. Spawned only by the Universal
Dispatcher; no web endpoint.

Source-of-truth inputs (read-only; never mutated):
  s3://data-sink/active/usaspending/award_search/     (78.4M rows, 1/award — PRIME)
  s3://data-sink/active/usaspending/subaward_search/  (9.8M rows, 1/subaward — SUB)

GRAIN & DOLLAR SEMANTICS (the resume = federal dollars this entity RECEIVED):
  • PRIME  — award_search grouped by ``recipient_uei`` (the entity that WON the prime
    award). Dollars = ``total_obligation``.
  • SUB    — subaward_search grouped by ``sub_awardee_or_recipient_uei`` (the entity
    that RECEIVED the subaward as a subcontractor — NOT ``awardee_or_recipient_uei``,
    which is the prime that issued it). Dollars = ``subaward_amount``.
  • The two UEI populations are UNIONed (FULL OUTER JOIN) → 1 row per recipient_uei
    across (prime recipients ∪ subawardees). ``total_combined_obligated`` =
    ``lifetime_prime_obligated`` + ``lifetime_subaward_obligated`` is therefore a
    clean, non-double-counted total of all federal dollars the entity received.

ACTIVE vs CLOSED:
  • PRIME  — ``period_of_performance_current_end_date >= CURRENT_DATE`` is active;
    closed = total − active (so the partition is exhaustive even for the ~30% of
    award rows with a NULL PoP end date — those are not provably active → closed).
  • SUB    — subaward_search carries NO period-of-performance column, so a subaward
    is "active" only when its PARENT PRIME AWARD is positively confirmed within PoP:
    join ``subaward.unique_award_key → award.generated_unique_award_id`` and test the
    prime's PoP end date. Everything else (key unmatched, or matched-but-NULL-PoP) →
    closed. Measured live: ~69% of subaward keys match a current award_search row,
    and the ~31% that miss are overwhelmingly pre-2020 (9.7% post-2020 vs 51.8% for
    matched) — i.e. genuinely old → correctly defaulted to closed. The per-run join
    match rate is recorded in ops.contractor_award_summary_runs for transparency.

Source data quality (FSRS/USAspending free-text): sentinel-garbage action dates
(years 0001, 2202, 6010) → display min/max date fields clamped to [1776-01-01,
CURRENT_DATE]; and physically-impossible dollar amounts (subaward_amount tops out at
a 1.0e18 sentinel) → amounts beyond the §MAX_* physical guards are nulled from dollar
SUMS only. Counts (total/active/closed) are never clamped — a garbage-amount award
still happened.

Data plane (clean-room — DuckDB does 100% of the transform):
  Lance(award_search, subaward_search) → DuckDB project → prime GROUP BY recipient_uei
  + top-3 funding agencies + sub GROUP BY sub_awardee_uei (PoP-joined) → FULL OUTER
  JOIN → Arrow → lance.write_dataset(R2 active, v2.1, overwrite) → BTREE(recipient_uei).

Control plane (Trigger v4 durable callback): on terminal state writes the run row to
ops.contractor_award_summary_runs and POSTs the flat callback to wake the Trigger run.

    modal run    pipelines/usaspending/contractor_award_summary.py::init_ops   # ops table
    modal deploy pipelines/usaspending/contractor_award_summary.py             # publish worker
    modal run    pipelines/usaspending/contractor_award_summary.py             # build + verify
    modal run    pipelines/usaspending/contractor_award_summary.py --dry-run   # counts, no write
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
USA_BASE = "s3://data-sink/active/usaspending/"
DATASET_URI = os.environ.get(
    "CONTRACTOR_AWARD_SUMMARY_LANCE_URI",
    "s3://data-sink/active/contractor_award_summary/",
)
FEED = "contractor_award_summary"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3

# recipient_uei = the single forward spine lookup the frontend point-queries (§6.1).
BTREE_INDEXES = ["recipient_uei"]

DUCKDB_MEMORY_LIMIT = "48GB"
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/duckdb_spill"

# Award-category → resume dollar bucket. Verified live category distribution:
# contract, direct payment, loans, grant, other, insurance, idv, (null).
CONTRACT_CATEGORIES = ("contract", "idv")
GRANT_CATEGORIES = ("grant",)

# Physically-impossible dollar guards (FSRS / source free-text entry errors). The
# largest real single federal award is ~$161B; nothing exceeds $1T, and no single
# subaward can exceed its prime, so anything over $100B is garbage. Verified live:
# subaward_amount tops out at a 1.0e18 sentinel (+ values of $4.8e15, $7.5e13, …,
# $265B — 31 rows > $100B); total_obligation is clean (max $160.75B, 0 rows > $1T).
# Amounts beyond the guard are nulled from dollar SUMS only — the row still counts
# toward award/subaward totals and active/closed. Negatives (de-obligations) are kept.
MAX_PRIME_OBLIGATION = 1_000_000_000_000.0   # $1T  — defensive; 0 rows in current snapshot
MAX_SUBAWARD_AMOUNT = 100_000_000_000.0      # $100B — removes 31 confirmed-garbage rows

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.contractor_award_summary_runs (
    id                          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                        text        NOT NULL,
    dataset_uri                 text        NOT NULL,
    rows_written                bigint,
    distinct_prime_uei          bigint,
    distinct_subaward_uei       bigint,
    distinct_combined_uei       bigint,
    lifetime_prime_obligated    numeric,
    lifetime_subaward_obligated numeric,
    total_combined_obligated    numeric,
    subaward_join_match_pct     numeric,
    status                      text        NOT NULL,
    error                       text,
    started_at                  timestamptz,
    completed_at                timestamptz,
    recorded_at                 timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS contractor_award_summary_runs_feed_idx        ON ops.contractor_award_summary_runs (feed);
CREATE INDEX IF NOT EXISTS contractor_award_summary_runs_status_idx      ON ops.contractor_award_summary_runs (status);
CREATE INDEX IF NOT EXISTS contractor_award_summary_runs_recorded_at_idx ON ops.contractor_award_summary_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("usaspending-award-summary-pipelines", image=image)


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
def _in_list(values) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def _build_summary(con) -> None:
    """Build the TEMP TABLE `summary` (1 row per recipient_uei). Assumes the source
    relations `aw` (projected award rows) and `sw` (projected subaward rows) exist."""
    contract_in = _in_list(CONTRACT_CATEGORIES)
    grant_in = _in_list(GRANT_CATEGORIES)
    nongroup_in = _in_list(CONTRACT_CATEGORIES + GRANT_CATEGORIES)

    # Prime resume — one row per prime recipient UEI.
    con.execute(f"""
        CREATE TEMP TABLE prime_core AS
        SELECT
            recipient_uei,
            round(sum(total_obligation), 2)                                         AS lifetime_prime_obligated,
            count(*)                                                                AS prime_total_awards,
            count(*) FILTER (WHERE period_of_performance_current_end_date >= CURRENT_DATE)
                                                                                    AS prime_active_awards,
            count(*) - count(*) FILTER (WHERE period_of_performance_current_end_date >= CURRENT_DATE)
                                                                                    AS prime_closed_awards,
            round(sum(total_obligation) FILTER (WHERE category IN ({contract_in})), 2)  AS contract_dollars,
            round(sum(total_obligation) FILTER (WHERE category IN ({grant_in})), 2)      AS grant_dollars,
            round(sum(total_obligation) FILTER (WHERE category IS NULL
                                                 OR category NOT IN ({nongroup_in})), 2) AS other_dollars,
            min(clean_action_date)                                                  AS prime_first_award_date,
            max(clean_action_date)                                                  AS prime_most_recent_action_date,
            arg_max(total_obligation,
                    {{'nz': (total_obligation <> 0),
                      'd': coalesce(clean_action_date, DATE '1776-01-01')}})        AS prime_most_recent_obligation,
            mode(naics_code)                                                        AS primary_naics,
            mode(product_or_service_code)                                           AS primary_psc
        FROM aw
        WHERE recipient_uei IS NOT NULL
        GROUP BY 1
    """)

    # Top-3 funding agencies by obligated dollars (named agencies only).
    con.execute("""
        CREATE TEMP TABLE agency_top3 AS
        WITH ag AS (
            SELECT recipient_uei, funding_toptier_agency_name AS agency,
                   sum(total_obligation) AS dollars
            FROM aw
            WHERE recipient_uei IS NOT NULL
              AND nullif(trim(funding_toptier_agency_name), '') IS NOT NULL
            GROUP BY 1, 2
        ),
        ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY recipient_uei ORDER BY dollars DESC NULLS LAST) AS rn
            FROM ag
        )
        SELECT
            recipient_uei,
            max(agency)                  FILTER (WHERE rn = 1) AS top_agency_1_name,
            round(max(dollars)           FILTER (WHERE rn = 1), 2) AS top_agency_1_dollars,
            max(agency)                  FILTER (WHERE rn = 2) AS top_agency_2_name,
            round(max(dollars)           FILTER (WHERE rn = 2), 2) AS top_agency_2_dollars,
            max(agency)                  FILTER (WHERE rn = 3) AS top_agency_3_name,
            round(max(dollars)           FILTER (WHERE rn = 3), 2) AS top_agency_3_dollars
        FROM ranked
        WHERE rn <= 3
        GROUP BY 1
    """)

    # Prime period-of-performance dimension, restricted to the awards subawards
    # reference (semi-join keeps the join target small). max() collapses any dup key.
    con.execute("""
        CREATE TEMP TABLE award_pop AS
        SELECT generated_unique_award_id AS award_key,
               max(period_of_performance_current_end_date) AS pop_end
        FROM aw
        WHERE generated_unique_award_id IN (SELECT unique_award_key FROM sw)
        GROUP BY 1
    """)

    # Subaward resume — one row per subawardee UEI; active derived from prime PoP.
    con.execute("""
        CREATE TEMP TABLE sub_core AS
        SELECT
            s.recipient_uei,
            round(sum(s.subaward_amount), 2)                          AS lifetime_subaward_obligated,
            count(*)                                                  AS subaward_total,
            count(*) FILTER (WHERE p.pop_end >= CURRENT_DATE)         AS subaward_active,
            count(*) - count(*) FILTER (WHERE p.pop_end >= CURRENT_DATE) AS subaward_closed,
            min(s.clean_sub_date)                                     AS subaward_first_action_date,
            max(s.clean_sub_date)                                     AS subaward_most_recent_action_date
        FROM sw s
        LEFT JOIN award_pop p ON s.unique_award_key = p.award_key
        GROUP BY 1
    """)

    # Combine — FULL OUTER JOIN over (prime recipients ∪ subawardees).
    con.execute("""
        CREATE TEMP TABLE summary AS
        SELECT
            coalesce(p.recipient_uei, s.recipient_uei)                      AS recipient_uei,
            coalesce(p.lifetime_prime_obligated, 0.0)                       AS lifetime_prime_obligated,
            coalesce(s.lifetime_subaward_obligated, 0.0)                    AS lifetime_subaward_obligated,
            round(coalesce(p.lifetime_prime_obligated, 0.0)
                  + coalesce(s.lifetime_subaward_obligated, 0.0), 2)        AS total_combined_obligated,
            coalesce(p.prime_total_awards, 0)                              AS prime_total_awards,
            coalesce(p.prime_active_awards, 0)                             AS prime_active_awards,
            coalesce(p.prime_closed_awards, 0)                             AS prime_closed_awards,
            coalesce(s.subaward_total, 0)                                  AS subaward_total,
            coalesce(s.subaward_active, 0)                                 AS subaward_active,
            coalesce(s.subaward_closed, 0)                                 AS subaward_closed,
            coalesce(p.prime_total_awards, 0)
                  + coalesce(s.subaward_total, 0)                          AS total_combined_awards,
            p.prime_first_award_date,
            p.prime_most_recent_action_date,
            coalesce(p.prime_most_recent_obligation, 0.0)                  AS prime_most_recent_obligation,
            s.subaward_first_action_date,
            s.subaward_most_recent_action_date,
            coalesce(p.contract_dollars, 0.0)                             AS contract_dollars,
            coalesce(p.grant_dollars, 0.0)                                AS grant_dollars,
            coalesce(p.other_dollars, 0.0)                                AS other_dollars,
            a.top_agency_1_name, coalesce(a.top_agency_1_dollars, 0.0)    AS top_agency_1_dollars,
            a.top_agency_2_name, coalesce(a.top_agency_2_dollars, 0.0)    AS top_agency_2_dollars,
            a.top_agency_3_name, coalesce(a.top_agency_3_dollars, 0.0)    AS top_agency_3_dollars,
            p.primary_naics,
            p.primary_psc,
            CURRENT_DATE                                                  AS summary_as_of_date
        FROM prime_core p
        FULL OUTER JOIN sub_core s ON p.recipient_uei = s.recipient_uei
        LEFT JOIN agency_top3 a
               ON a.recipient_uei = coalesce(p.recipient_uei, s.recipient_uei)
    """)


def _materialize(con):
    """Register both source datasets (projected), build the summary, return
    (arrow_table, metrics). One scan per source dataset from R2."""
    import lance

    so = _r2_storage_options()

    # PRIME projection. clean_action_date drops sentinel-garbage years.
    aw_cols = ["recipient_uei", "total_obligation", "category",
               "period_of_performance_current_end_date", "action_date",
               "funding_toptier_agency_name", "generated_unique_award_id",
               "naics_code", "product_or_service_code"]
    con.register("aw_reader", lance.dataset(f"{USA_BASE}award_search/", storage_options=so)
                 .scanner(columns=aw_cols).to_reader())
    con.execute(f"""
        CREATE TEMP TABLE aw AS
        SELECT
            nullif(trim(recipient_uei), '')        AS recipient_uei,
            CASE WHEN abs(total_obligation) <= {MAX_PRIME_OBLIGATION}
                 THEN total_obligation END         AS total_obligation,
            lower(nullif(trim(category), ''))      AS category,
            period_of_performance_current_end_date,
            CASE WHEN action_date BETWEEN DATE '1776-01-01' AND CURRENT_DATE
                 THEN action_date END              AS clean_action_date,
            funding_toptier_agency_name,
            nullif(trim(generated_unique_award_id), '') AS generated_unique_award_id,
            nullif(trim(naics_code), '')           AS naics_code,
            nullif(trim(product_or_service_code), '') AS product_or_service_code
        FROM aw_reader
    """)
    con.unregister("aw_reader")

    # SUB projection. recipient_uei = the subawardee (entity receiving the sub).
    sw_cols = ["sub_awardee_or_recipient_uei", "subaward_amount",
               "sub_action_date", "unique_award_key"]
    con.register("sw_reader", lance.dataset(f"{USA_BASE}subaward_search/", storage_options=so)
                 .scanner(columns=sw_cols).to_reader())
    con.execute(f"""
        CREATE TEMP TABLE sw AS
        SELECT
            nullif(trim(sub_awardee_or_recipient_uei), '') AS recipient_uei,
            CASE WHEN abs(subaward_amount) <= {MAX_SUBAWARD_AMOUNT}
                 THEN subaward_amount END                  AS subaward_amount,
            CASE WHEN sub_action_date BETWEEN DATE '1776-01-01' AND CURRENT_DATE
                 THEN sub_action_date END                  AS clean_sub_date,
            nullif(trim(unique_award_key), '')             AS unique_award_key
        FROM sw_reader
        WHERE nullif(trim(sub_awardee_or_recipient_uei), '') IS NOT NULL
    """)
    con.unregister("sw_reader")

    # Build-quality metric: fraction of subaward rows whose prime key resolves.
    match_pct = con.execute("""
        SELECT round(100.0 * avg(CASE WHEN unique_award_key IN
                   (SELECT generated_unique_award_id FROM aw) THEN 1 ELSE 0 END), 2)
        FROM sw
    """).fetchone()[0]

    _build_summary(con)

    rows, d_comb = con.execute(
        "SELECT count(*), count(DISTINCT recipient_uei) FROM summary").fetchone()
    d_prime = con.execute("SELECT count(*) FROM prime_core").fetchone()[0]
    d_sub = con.execute("SELECT count(*) FROM sub_core").fetchone()[0]
    tot_prime, tot_sub, tot_comb = con.execute("""
        SELECT round(sum(lifetime_prime_obligated), 2),
               round(sum(lifetime_subaward_obligated), 2),
               round(sum(total_combined_obligated), 2)
        FROM summary
    """).fetchone()

    table = con.sql("SELECT * FROM summary").to_arrow_table()
    metrics = {
        "rows": int(rows),
        "distinct_prime_uei": int(d_prime),
        "distinct_subaward_uei": int(d_sub),
        "distinct_combined_uei": int(d_comb),
        "lifetime_prime_obligated": float(tot_prime or 0.0),
        "lifetime_subaward_obligated": float(tot_sub or 0.0),
        "total_combined_obligated": float(tot_comb or 0.0),
        "subaward_join_match_pct": float(match_pct or 0.0),
    }
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
                INSERT INTO ops.contractor_award_summary_runs
                    (feed, dataset_uri, rows_written, distinct_prime_uei,
                     distinct_subaward_uei, distinct_combined_uei,
                     lifetime_prime_obligated, lifetime_subaward_obligated,
                     total_combined_obligated, subaward_join_match_pct,
                     status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, metrics.get("rows"),
                 metrics.get("distinct_prime_uei"), metrics.get("distinct_subaward_uei"),
                 metrics.get("distinct_combined_uei"),
                 metrics.get("lifetime_prime_obligated"),
                 metrics.get("lifetime_subaward_obligated"),
                 metrics.get("total_combined_obligated"),
                 metrics.get("subaward_join_match_pct"),
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
    timeout=60 * 90,
    memory=65536,
    cpu=8.0,
)
def build_contractor_award_summary(trigger_callback_url: str | None = None) -> dict:
    """Rebuild the contractor award-summary resume and publish to R2 active.
    Idempotent full overwrite; the recipient_uei BTREE is rebuilt each run."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error = "error", None
    metrics = {"rows": 0, "distinct_prime_uei": 0, "distinct_subaward_uei": 0,
               "distinct_combined_uei": 0, "lifetime_prime_obligated": 0.0,
               "lifetime_subaward_obligated": 0.0, "total_combined_obligated": 0.0,
               "subaward_join_match_pct": 0.0}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics = _materialize(con)
        finally:
            con.close()
        print(f"Built contractor_award_summary: {metrics}")

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
        _record_run(metrics=metrics, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": metrics["rows"], "feed": FEED,
                        "dataset_uri": DATASET_URI,
                        "distinct_combined_uei": metrics["distinct_combined_uei"],
                        "total_combined_obligated": metrics["total_combined_obligated"]})

    if status != "success":
        raise RuntimeError(f"contractor_award_summary build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, **metrics}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify_contractor_award_summary() -> dict:
    """Read-back proof: open the published dataset from R2 and report counts,
    schema, index, dollar splits, and a KIPPER spot-check — independent of the
    write path."""
    import lance
    import duckdb

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
           for i in ds.list_indices()]
    con = duckdb.connect()
    con.register("d", ds.scanner().to_reader())
    con.execute("CREATE TEMP TABLE d2 AS SELECT * FROM d")
    n, d_uei = con.execute(
        "SELECT count(*), count(DISTINCT recipient_uei) FROM d2").fetchone()
    tot_prime, tot_sub, tot_comb = con.execute("""
        SELECT round(sum(lifetime_prime_obligated), 2),
               round(sum(lifetime_subaward_obligated), 2),
               round(sum(total_combined_obligated), 2) FROM d2""").fetchone()
    splits = con.execute("""
        SELECT
          count(*) FILTER (WHERE lifetime_prime_obligated > 0 AND lifetime_subaward_obligated > 0) AS both,
          count(*) FILTER (WHERE lifetime_prime_obligated > 0 AND lifetime_subaward_obligated = 0) AS prime_only,
          count(*) FILTER (WHERE lifetime_prime_obligated = 0 AND lifetime_subaward_obligated > 0) AS sub_only,
          sum(prime_active_awards) AS prime_active, sum(subaward_active) AS sub_active
        FROM d2""").fetchone()
    con.close()

    # Indexed point-lookup proof (KIPPER TOOL — §3.2 reference, lifetime ≈ $162.0M prime).
    kipper = ds.scanner(
        filter="recipient_uei = 'DD1BCRF2QQG8'",
        columns=["recipient_uei", "lifetime_prime_obligated", "lifetime_subaward_obligated",
                 "total_combined_obligated", "prime_total_awards", "prime_active_awards",
                 "prime_closed_awards", "subaward_total", "top_agency_1_name",
                 "top_agency_1_dollars", "prime_most_recent_action_date"],
    ).to_table().to_pylist()

    return {"rows": n, "distinct_uei": d_uei, "indices": idx, "uri": DATASET_URI,
            "lifetime_prime_obligated": float(tot_prime or 0),
            "lifetime_subaward_obligated": float(tot_sub or 0),
            "total_combined_obligated": float(tot_comb or 0),
            "uei_both_prime_and_sub": int(splits[0]), "uei_prime_only": int(splits[1]),
            "uei_sub_only": int(splits[2]), "sum_prime_active": int(splits[3] or 0),
            "sum_subaward_active": int(splits[4] or 0),
            "schema": [f"{f.name}:{f.type}" for f in ds.schema],
            "kipper_DD1BCRF2QQG8": kipper}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    """Apply the ops.contractor_award_summary_runs DDL (idempotent)."""
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.contractor_award_summary_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 90, memory=65536, cpu=8.0,
)
def plan_contractor_award_summary() -> dict:
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
        print(plan_contractor_award_summary.remote())
        return
    print(build_contractor_award_summary.remote(trigger_callback_url=None))
    print(verify_contractor_award_summary.remote())
