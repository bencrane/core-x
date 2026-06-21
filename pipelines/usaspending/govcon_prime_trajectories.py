"""Compute worker — GovCon Prime Trajectories (multi-window net-new award curve).

The ``usaspending-govcon-trajectories-pipelines`` Modal app. Materializes ONE row
per ``recipient_uei`` carrying the full trailing-window growth curve of net-new
prime contract awards — the substrate for the surety / bonding GTM ("High-Growth
Prime Trajectories"). 1 row/UEI, BTREE(recipient_uei) + BITMAP(is_bonded_vertical),
written v2.1 to R2. Spawned by the Universal Dispatcher (or run/spawn'd manually);
no web endpoint.

WHY a windowed sibling of ``contractor_award_summary``: that table is a LIFETIME
rollup. This one exposes t12m / t24m / t36m / lifetime brackets so the UI filters
trajectory dynamically (no 24-month window hardcoded into the schema).

────────────────────────────── sources (read-only) ──────────────────────────────
  DEEP_FPDS  s3://data-sink/active/usaspending/transaction_search_fpds/   (107.25M txns; pg-dump names; Lance-typed)
  LIVE       s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/ (verbatim-API names, ALL VARCHAR; freshness tail to ~yesterday)
  DEEP_AWARD s3://data-sink/active/usaspending/award_search/              (78.6M award-grain rows; supplemental primary NAICS/PSC only)

DEEP_FPDS + LIVE are the transaction-grain spine (carry modification_number +
the Davis-Bacon construction flag). DEEP_AWARD is award-grain (NO modification_number,
NO construction flag) and is used ONLY as a supplemental, authoritative source of the
per-UEI primary NAICS/PSC.

──────────────────────── cross-source semantics (VERIFIED LIVE) ────────────────────────
  • Union AWARD key:  DEEP_FPDS.generated_unique_award_id  ==  LIVE.contract_award_unique_key
                      (both the CONT_AWD_<piid>_<agency>_-NONE-_-NONE- composite). Verified byte-shape.
  • Union TXN  key:   DEEP_FPDS.detached_award_proc_unique ==  LIVE.contract_transaction_unique_key
                      (== DEEP_FPDS.transaction_unique_id). Verified byte-for-byte; 24 overlapping txns
                      on the Olgoonik probe → the ~1993-onward deep↔live overlap MUST be deduped.
  • Net-new award:    modification_number = '0' (base action). Non-zero mods (P#####, PS####) are
                      admin/funding mods, excluded from the new_awards COUNT.
  • Construction flag: DEEP_FPDS.construction_wage_rate_req='Y'  /  LIVE.construction_wage_rate_requirements_code='Y'
                      (Davis-Bacon applies). Do NOT conflate with labor_standards (Service Contract Act).

──────────────────────────── dedup (Strategy A) ────────────────────────────
UNION ALL(DEEP, LIVE) → collapse to one row per txn_key via row_number() preferring
(non-null-obligation, then LIVE over DEEP). Rows with a NULL txn_key cannot be
cross-source dups (no join key) and are kept un-deduped — never silently dropped
(their count is recorded in the ops ledger).

──────────────────────────── metrics (per recipient_uei) ────────────────────────────
For each bracket t12m / t24m / t36m / lifetime, anchored on as_of_date (= build date):
  • new_awards_<w> = count of DISTINCT net-new (mod-0) award_keys whose base action_date
    falls in the window. One row per award_key (base action), dated → monotone t12≤t24≤t36≤lifetime.
  • obligated_<w>  = SUM of federal_action_obligation over ALL deduped mods in the window
    (total signed dollars flowing; de-obligations kept → NOT guaranteed monotone).
Plus first_award_date / latest_award_date (mod-0), primary_naics / primary_psc, and
is_bonded_vertical = (any Davis-Bacon 'Y') OR primary_naics LIKE '23%' OR ='561210'
OR primary_psc LIKE 'Y%'/'Z%'.

Synthetic aggregate recipients (UEI 'LN9PU5M2YZN5' = MISCELLANEOUS FOREIGN AWARDEES,
1.16M rows; plus MISCELLANEOUS%/MULTIPLE%/REDACTED%/INDIVIDUAL%/UNKNOWN% names and
NULL/empty UEI) are excluded BEFORE aggregation.

    modal run    pipelines/usaspending/govcon_prime_trajectories.py::init_ops      # ops table
    modal deploy pipelines/usaspending/govcon_prime_trajectories.py               # publish worker
    modal run    pipelines/usaspending/govcon_prime_trajectories.py::launch       # spawn build (fire-and-forget, durable)
    modal run    pipelines/usaspending/govcon_prime_trajectories.py               # build + verify (synchronous)
    modal run    pipelines/usaspending/govcon_prime_trajectories.py --dry-run     # counts, no write
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
USA_BASE = "s3://data-sink/active/usaspending/"
LIVE_URI = "s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/"
DATASET_URI = os.environ.get(
    "GOVCON_PRIME_TRAJECTORIES_URI",
    "s3://data-sink/active/govcon_prime_trajectories/",
)
FEED = "govcon_prime_trajectories"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 250_000          # R2-safe uniform multipart parts (small dataset → ~3 files)
MAX_BYTES_PER_FILE = 90 * 1024**3

BTREE_INDEXES = ["recipient_uei"]
BITMAP_INDEXES = ["is_bonded_vertical"]

DUCKDB_MEMORY_LIMIT = os.environ.get("GOVCON_TRAJ_DUCKDB_MEM", "96GB")  # leave headroom on the 128 GiB box
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/duckdb_spill"

# Physically-impossible dollar guard (FSRS / free-text entry errors). Largest real
# single federal award ≈ $161B; >$1T is garbage → nulled from dollar SUMS only.
# Negatives (de-obligations) are KEPT. (Same constant as contractor_award_summary.py.)
MAX_ABS_OBLIGATION = 1_000_000_000_000.0

# Synthetic / aggregate recipients to exclude before aggregation.
SYNTHETIC_UEIS = ["LN9PU5M2YZN5"]    # MISCELLANEOUS FOREIGN AWARDEES (~1.16M rows)
SYNTHETIC_NAME_PATTERNS = [
    "MISCELLANEOUS%", "MULTIPLE RECIPIENT%", "MULTIPLE%",
    "REDACTED%", "INDIVIDUAL%", "UNKNOWN%",
]

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.govcon_prime_trajectories_runs (
    id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                    text        NOT NULL,
    dataset_uri             text        NOT NULL,
    as_of_date              date,
    rows_written            bigint,         -- = distinct recipient_uei
    bonded_uei              bigint,         -- is_bonded_vertical = true
    deep_rows               bigint,         -- DEEP_FPDS rows scanned
    live_rows               bigint,         -- LIVE rows scanned
    union_rows              bigint,         -- deep + live before dedup
    deduped_rows            bigint,         -- after txn_key collapse
    dup_collapsed           bigint,         -- union_rows - deduped_rows (overlap removed)
    null_txn_key_rows       bigint,         -- rows kept un-deduped (no txn_key)
    new_awards_t24m_total   bigint,         -- corpus sum, sanity
    obligated_t24m_total    numeric,        -- corpus sum, sanity
    status                  text        NOT NULL,
    error                   text,
    started_at              timestamptz,
    completed_at            timestamptz,
    recorded_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS govcon_prime_trajectories_runs_feed_idx        ON ops.govcon_prime_trajectories_runs (feed);
CREATE INDEX IF NOT EXISTS govcon_prime_trajectories_runs_status_idx      ON ops.govcon_prime_trajectories_runs (status);
CREATE INDEX IF NOT EXISTS govcon_prime_trajectories_runs_recorded_at_idx ON ops.govcon_prime_trajectories_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("usaspending-govcon-trajectories-pipelines", image=image)


# --------------------------------------------------------------------------- #
# R2
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
    return con


# --------------------------------------------------------------------------- #
# DuckDB transform (100% of the work)
# --------------------------------------------------------------------------- #
def _name_not_like(col: str) -> str:
    return " AND ".join(
        f"UPPER(COALESCE({col},'')) NOT LIKE '{p}'" for p in SYNTHETIC_NAME_PATTERNS
    )


def _uei_exclusion(uei_col: str, name_col: str) -> str:
    uei_list = ", ".join(f"'{u}'" for u in SYNTHETIC_UEIS)
    return (
        f"nullif(trim({uei_col}),'') IS NOT NULL "
        f"AND {uei_col} NOT IN ({uei_list}) "
        f"AND {_name_not_like(name_col)}"
    )


def _scan(ds, cols, uei_filter):
    """Lance scanner with optional recipient_uei pushdown (BTREE) — the local-test seam.
    uei_filter is a SQL predicate string, e.g. "recipient_uei IN ('AAA','BBB')"."""
    return (ds.scanner(columns=cols, filter=uei_filter) if uei_filter
            else ds.scanner(columns=cols)).to_reader()


def _materialize(con, uei_filter: str | None = None):
    """Build the TEMP TABLE `trajectories` (1 row/recipient_uei) and return
    (arrow_table, metrics). Registers the three Lance sources, dedups the deep↔live
    transaction overlap, applies mod-0 net-new semantics, and rolls up the windows.

    uei_filter (optional) pushes a recipient_uei predicate into every source scanner
    via the BTREE index — used by the local validation harness to run this EXACT
    pipeline against a cheap known-UEI subset before the full Modal build."""
    import lance

    so = _r2_storage_options()

    # ── params: as_of anchor + trailing bracket lower bounds (scalars) ──
    con.execute("""
        CREATE TEMP TABLE params AS
        SELECT CURRENT_DATE                        AS as_of_date,
               CURRENT_DATE - INTERVAL 12 MONTH    AS lo12,
               CURRENT_DATE - INTERVAL 24 MONTH    AS lo24,
               CURRENT_DATE - INTERVAL 36 MONTH    AS lo36
    """)

    # ── DEEP_FPDS projection (Lance-typed; defensive TRY_CAST anyway) ──
    deep_cols = ["recipient_uei", "recipient_name", "modification_number",
                 "generated_unique_award_id", "detached_award_proc_unique",
                 "transaction_unique_id", "action_date", "federal_action_obligation",
                 "naics_code", "product_or_service_code", "construction_wage_rate_req"]
    con.register("fpds_r", _scan(lance.dataset(f"{USA_BASE}transaction_search_fpds/", storage_options=so),
                                 deep_cols, uei_filter))
    con.execute("""
        CREATE TEMP TABLE uni AS
        SELECT
            nullif(trim(recipient_uei),'')                                   AS recipient_uei,
            nullif(trim(recipient_name),'')                                  AS recipient_name,
            nullif(trim(modification_number),'')                             AS modification_number,
            nullif(trim(generated_unique_award_id),'')                       AS award_key,
            COALESCE(nullif(trim(detached_award_proc_unique),''),
                     nullif(trim(transaction_unique_id),''))                 AS txn_key,
            TRY_CAST(action_date AS DATE)                                    AS txn_date,
            TRY_CAST(federal_action_obligation AS DOUBLE)                    AS obligated_amt,
            nullif(trim(naics_code),'')                                      AS naics_code,
            nullif(trim(product_or_service_code),'')                         AS psc,
            CASE WHEN nullif(trim(construction_wage_rate_req),'')='Y' THEN 1 ELSE 0 END AS constr_y,
            2 AS src_priority
        FROM fpds_r
    """)
    con.unregister("fpds_r")
    deep_rows = con.execute("SELECT count(*) FROM uni").fetchone()[0]

    # ── LIVE projection (verbatim API, ALL VARCHAR) → append to uni ──
    live_cols = ["recipient_uei", "recipient_name", "modification_number",
                 "contract_award_unique_key", "contract_transaction_unique_key",
                 "action_date", "federal_action_obligation", "naics_code",
                 "product_or_service_code", "construction_wage_rate_requirements_code"]
    con.register("live_r", _scan(lance.dataset(LIVE_URI, storage_options=so),
                                 live_cols, uei_filter))
    con.execute("""
        INSERT INTO uni
        SELECT
            nullif(trim(recipient_uei),'')                                   AS recipient_uei,
            nullif(trim(recipient_name),'')                                  AS recipient_name,
            nullif(trim(modification_number),'')                             AS modification_number,
            nullif(trim(contract_award_unique_key),'')                       AS award_key,
            nullif(trim(contract_transaction_unique_key),'')                 AS txn_key,
            TRY_CAST(action_date AS DATE)                                    AS txn_date,
            TRY_CAST(federal_action_obligation AS DOUBLE)                    AS obligated_amt,
            nullif(trim(naics_code),'')                                      AS naics_code,
            nullif(trim(product_or_service_code),'')                         AS psc,
            CASE WHEN nullif(trim(construction_wage_rate_requirements_code),'')='Y' THEN 1 ELSE 0 END AS constr_y,
            1 AS src_priority
        FROM live_r
    """)
    con.unregister("live_r")
    union_rows = con.execute("SELECT count(*) FROM uni").fetchone()[0]
    live_rows = union_rows - deep_rows
    null_txn_key_rows = con.execute("SELECT count(*) FROM uni WHERE txn_key IS NULL").fetchone()[0]

    # ── dedup the deep↔live overlap on txn_key; keep NULL-txn_key rows un-deduped ──
    # Preference: non-null obligation first, then LIVE (src_priority=1) over DEEP (2).
    con.execute("""
        CREATE TEMP TABLE deduped AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (
                       PARTITION BY txn_key
                       ORDER BY (obligated_amt IS NULL) ASC, src_priority ASC) AS rn
            FROM uni WHERE txn_key IS NOT NULL
        ) WHERE rn = 1
        UNION ALL
        SELECT * FROM uni WHERE txn_key IS NULL
    """)
    con.execute("DROP TABLE uni")
    deduped_rows = con.execute("SELECT count(*) FROM deduped").fetchone()[0]
    dup_collapsed = union_rows - deduped_rows

    # ── clamp dates/dollars, apply synthetic exclusion, attach bracket bounds ──
    con.execute(f"""
        CREATE TEMP TABLE txns AS
        SELECT
            d.recipient_uei,
            d.recipient_name,
            (d.modification_number = '0')                                    AS is_mod0,
            d.award_key,
            CASE WHEN d.txn_date BETWEEN DATE '1776-01-01' AND p.as_of_date
                 THEN d.txn_date END                                        AS txn_date,
            CASE WHEN abs(d.obligated_amt) <= {MAX_ABS_OBLIGATION}
                 THEN d.obligated_amt END                                    AS obligated_amt,
            d.naics_code, d.psc, d.constr_y,
            p.as_of_date, p.lo12, p.lo24, p.lo36
        FROM deduped d CROSS JOIN params p
        WHERE {_uei_exclusion('d.recipient_uei', 'd.recipient_name')}
    """)
    con.execute("DROP TABLE deduped")

    # ── net-new (mod-0) awards reduced to 1 row per (uei, award_key), dated ──
    con.execute("""
        CREATE TEMP TABLE mod0_base AS
        SELECT recipient_uei, award_key,
               min(txn_date) AS base_date
        FROM txns
        WHERE is_mod0 AND award_key IS NOT NULL AND txn_date IS NOT NULL
        GROUP BY recipient_uei, award_key
    """)

    # ── new_awards windows + first/latest (count(*): already 1 row per award) ──
    con.execute("""
        CREATE TEMP TABLE awards_agg AS
        SELECT m.recipient_uei,
               count(*) FILTER (WHERE m.base_date >  p.lo12 AND m.base_date <= p.as_of_date) AS new_awards_t12m,
               count(*) FILTER (WHERE m.base_date >  p.lo24 AND m.base_date <= p.as_of_date) AS new_awards_t24m,
               count(*) FILTER (WHERE m.base_date >  p.lo36 AND m.base_date <= p.as_of_date) AS new_awards_t36m,
               count(*)                                                                       AS new_awards_lifetime,
               min(m.base_date)                                                               AS first_award_date,
               max(m.base_date)                                                               AS latest_award_date
        FROM mod0_base m CROSS JOIN params p
        GROUP BY m.recipient_uei
    """)

    # ── obligated windows (sum over ALL deduped mods in window; de-obligations kept) ──
    con.execute("""
        CREATE TEMP TABLE dollars_agg AS
        SELECT recipient_uei,
               round(sum(obligated_amt) FILTER (WHERE txn_date > lo12 AND txn_date <= as_of_date), 2) AS obligated_t12m,
               round(sum(obligated_amt) FILTER (WHERE txn_date > lo24 AND txn_date <= as_of_date), 2) AS obligated_t24m,
               round(sum(obligated_amt) FILTER (WHERE txn_date > lo36 AND txn_date <= as_of_date), 2) AS obligated_t36m,
               round(sum(obligated_amt) FILTER (WHERE txn_date IS NOT NULL), 2)                       AS obligated_lifetime
        FROM txns
        GROUP BY recipient_uei
    """)

    # ── per-UEI flags + canonical name (placeholder names excluded from arg_max) ──
    con.execute(f"""
        CREATE TEMP TABLE flags_agg AS
        SELECT recipient_uei,
               max(constr_y) AS is_constr_wage,
               arg_max(CASE WHEN {_name_not_like('recipient_name')} THEN recipient_name END,
                       txn_date) AS recipient_name,
               any_value(as_of_date) AS as_of_date
        FROM txns
        GROUP BY recipient_uei
    """)

    # ── deterministic primary NAICS/PSC: award_search (contracts) first, txn fallback ──
    award_cols = ["recipient_uei", "naics_code", "product_or_service_code", "is_fpds"]
    con.register("award_r", _scan(lance.dataset(f"{USA_BASE}award_search/", storage_options=so),
                                  award_cols, uei_filter))
    con.execute(f"""
        CREATE TEMP TABLE aw_codes AS
        WITH base AS (
            SELECT nullif(trim(recipient_uei),'')        AS recipient_uei,
                   nullif(trim(naics_code),'')           AS naics_code,
                   nullif(trim(product_or_service_code),'') AS psc
            FROM award_r
            WHERE is_fpds AND {_uei_exclusion('recipient_uei', 'CAST(NULL AS VARCHAR)')}
        ),
        nc AS (
            SELECT recipient_uei, naics_code,
                   row_number() OVER (PARTITION BY recipient_uei
                       ORDER BY count(*) DESC, naics_code ASC) AS rn
            FROM base WHERE naics_code IS NOT NULL GROUP BY 1, 2
        ),
        pc AS (
            SELECT recipient_uei, psc,
                   row_number() OVER (PARTITION BY recipient_uei
                       ORDER BY count(*) DESC, psc ASC) AS rn
            FROM base WHERE psc IS NOT NULL GROUP BY 1, 2
        )
        SELECT COALESCE(nc.recipient_uei, pc.recipient_uei)        AS recipient_uei,
               max(nc.naics_code) FILTER (WHERE nc.rn = 1)         AS aw_naics,
               max(pc.psc)        FILTER (WHERE pc.rn = 1)         AS aw_psc
        FROM nc FULL OUTER JOIN pc ON nc.recipient_uei = pc.recipient_uei
        GROUP BY 1
    """)
    con.unregister("award_r")

    # transaction-stream fallback primary codes (deterministic)
    con.execute("""
        CREATE TEMP TABLE txn_codes AS
        WITH nc AS (
            SELECT recipient_uei, naics_code,
                   row_number() OVER (PARTITION BY recipient_uei
                       ORDER BY count(*) DESC, naics_code ASC) AS rn
            FROM txns WHERE naics_code IS NOT NULL GROUP BY 1, 2
        ),
        pc AS (
            SELECT recipient_uei, psc,
                   row_number() OVER (PARTITION BY recipient_uei
                       ORDER BY count(*) DESC, psc ASC) AS rn
            FROM txns WHERE psc IS NOT NULL GROUP BY 1, 2
        )
        SELECT COALESCE(nc.recipient_uei, pc.recipient_uei)        AS recipient_uei,
               max(nc.naics_code) FILTER (WHERE nc.rn = 1)         AS tx_naics,
               max(pc.psc)        FILTER (WHERE pc.rn = 1)         AS tx_psc
        FROM nc FULL OUTER JOIN pc ON nc.recipient_uei = pc.recipient_uei
        GROUP BY 1
    """)

    # ── final assembly (1 row / recipient_uei) ──
    con.execute("""
        CREATE TEMP TABLE trajectories AS
        SELECT
            f.recipient_uei,
            f.recipient_name,
            f.as_of_date,
            COALESCE(a.aw_naics, t.tx_naics)                       AS primary_naics,
            COALESCE(a.aw_psc,   t.tx_psc)                         AS primary_psc,
            (
                f.is_constr_wage = 1
                OR COALESCE(a.aw_naics, t.tx_naics) LIKE '23%'
                OR COALESCE(a.aw_naics, t.tx_naics) = '561210'
                OR COALESCE(a.aw_psc,   t.tx_psc)   LIKE 'Y%'
                OR COALESCE(a.aw_psc,   t.tx_psc)   LIKE 'Z%'
            )                                                      AS is_bonded_vertical,
            w.first_award_date,
            w.latest_award_date,
            COALESCE(w.new_awards_t12m, 0)                         AS new_awards_t12m,
            COALESCE(d.obligated_t12m, 0.0)                        AS obligated_t12m,
            COALESCE(w.new_awards_t24m, 0)                         AS new_awards_t24m,
            COALESCE(d.obligated_t24m, 0.0)                        AS obligated_t24m,
            COALESCE(w.new_awards_t36m, 0)                         AS new_awards_t36m,
            COALESCE(d.obligated_t36m, 0.0)                        AS obligated_t36m,
            COALESCE(w.new_awards_lifetime, 0)                     AS new_awards_lifetime,
            COALESCE(d.obligated_lifetime, 0.0)                    AS obligated_lifetime
        FROM flags_agg f
        LEFT JOIN awards_agg  w ON w.recipient_uei = f.recipient_uei
        LEFT JOIN dollars_agg d ON d.recipient_uei = f.recipient_uei
        LEFT JOIN aw_codes    a ON a.recipient_uei = f.recipient_uei
        LEFT JOIN txn_codes   t ON t.recipient_uei = f.recipient_uei
    """)

    rows, bonded = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE is_bonded_vertical) FROM trajectories").fetchone()
    na_t24, ob_t24, as_of = con.execute(
        "SELECT sum(new_awards_t24m), round(sum(obligated_t24m),2), max(as_of_date) FROM trajectories").fetchone()

    table = con.execute("SELECT * FROM trajectories").arrow()
    metrics = {
        "as_of_date": str(as_of),
        "rows": int(rows),
        "bonded_uei": int(bonded),
        "deep_rows": int(deep_rows),
        "live_rows": int(live_rows),
        "union_rows": int(union_rows),
        "deduped_rows": int(deduped_rows),
        "dup_collapsed": int(dup_collapsed),
        "null_txn_key_rows": int(null_txn_key_rows),
        "new_awards_t24m_total": int(na_t24 or 0),
        "obligated_t24m_total": float(ob_t24 or 0.0),
    }
    return table, metrics


# --------------------------------------------------------------------------- #
# ops ledger + Trigger callback
# --------------------------------------------------------------------------- #
def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.", flush=True)
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
                INSERT INTO ops.govcon_prime_trajectories_runs
                    (feed, dataset_uri, as_of_date, rows_written, bonded_uei, deep_rows,
                     live_rows, union_rows, deduped_rows, dup_collapsed, null_txn_key_rows,
                     new_awards_t24m_total, obligated_t24m_total, status, error,
                     started_at, completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (FEED, DATASET_URI, metrics.get("as_of_date"), metrics.get("rows"),
                 metrics.get("bonded_uei"), metrics.get("deep_rows"), metrics.get("live_rows"),
                 metrics.get("union_rows"), metrics.get("deduped_rows"), metrics.get("dup_collapsed"),
                 metrics.get("null_txn_key_rows"), metrics.get("new_awards_t24m_total"),
                 metrics.get("obligated_t24m_total"), status, error, started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* write failed: {exc}", flush=True)
    finally:
        conn.close()


def _post_callback(url, payload, attempts: int = 3) -> None:
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.", flush=True)
        return
    import time

    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}", flush=True)
                return
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}", flush=True)
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}", flush=True)


# --------------------------------------------------------------------------- #
# Core build
# --------------------------------------------------------------------------- #
@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 120,
    memory=131072,            # 128 GiB — dedup over ~109M txns + per-UEI rollup
    cpu=8.0,
)
def run_build(trigger_callback_url: str | None = None) -> dict:
    """Rebuild govcon_prime_trajectories and publish to R2 active. Idempotent full
    overwrite; recipient_uei BTREE + is_bonded_vertical BITMAP rebuilt each run."""
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error = "error", None
    metrics = {"as_of_date": None, "rows": 0, "bonded_uei": 0, "deep_rows": 0,
               "live_rows": 0, "union_rows": 0, "deduped_rows": 0, "dup_collapsed": 0,
               "null_txn_key_rows": 0, "new_awards_t24m_total": 0, "obligated_t24m_total": 0.0}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics = _materialize(con)
        finally:
            con.close()
        print(f"Built govcon_prime_trajectories: {metrics}", flush=True)

        lance.write_dataset(
            table, DATASET_URI, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        print(f"Wrote Lance dataset (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}", flush=True)

        ds = lance.dataset(DATASET_URI, storage_options=so)
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"  BTREE  ✓ {col}", flush=True)
        for col in BITMAP_INDEXES:
            ds.create_scalar_index(col, index_type="BITMAP")
            print(f"  BITMAP ✓ {col}", flush=True)
        committed = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        print(f"Committed rows: {committed:,}", flush=True)
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling below + re-raise
        error = f"{type(exc).__name__}: {exc}"
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(metrics=metrics, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": metrics["rows"], "feed": FEED,
                        "dataset_uri": DATASET_URI, "as_of_date": metrics["as_of_date"],
                        "bonded_uei": metrics["bonded_uei"]})

    if status != "success":
        raise RuntimeError(f"govcon_prime_trajectories build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, **metrics}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials")],
    timeout=60 * 90, memory=131072, cpu=8.0,
)
def plan_build() -> dict:
    """Remote dry-run — materialize + count, write NOTHING."""
    os.makedirs(SPILL_DIR, exist_ok=True)
    con = _new_con()
    try:
        _table, metrics = _materialize(con)
    finally:
        con.close()
    return metrics


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=600)
def verify() -> dict:
    """Independent read-back: counts, schema, indices, window monotonicity, bonded split,
    and a known-prime spot-check (Olgoonik General — JLUNXC1LM1N7)."""
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = [getattr(i, "name", None) if not isinstance(i, dict) else i.get("name")
           for i in ds.list_indices()]
    con = duckdb.connect()
    con.register("d", ds.scanner().to_reader())
    con.execute("CREATE TEMP TABLE t AS SELECT * FROM d")
    n, bonded = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE is_bonded_vertical) FROM t").fetchone()
    # monotonicity of new_awards (must hold by construction)
    mono = con.execute("""
        SELECT count(*) FROM t
        WHERE new_awards_t12m > new_awards_t24m
           OR new_awards_t24m > new_awards_t36m
           OR new_awards_t36m > new_awards_lifetime""").fetchone()[0]
    dupes = con.execute(
        "SELECT count(*) - count(DISTINCT recipient_uei) FROM t").fetchone()[0]
    olg = con.execute("""
        SELECT recipient_uei, recipient_name, is_bonded_vertical, primary_naics, primary_psc,
               new_awards_t12m, new_awards_t24m, new_awards_t36m, new_awards_lifetime,
               obligated_t24m, obligated_lifetime, first_award_date, latest_award_date
        FROM t WHERE recipient_uei = 'JLUNXC1LM1N7'""").fetchdf().to_dict("records")
    con.close()
    return {"uri": DATASET_URI, "rows": int(n), "bonded_uei": int(bonded),
            "duplicate_uei": int(dupes), "new_awards_monotonicity_violations": int(mono),
            "indices": idx, "schema": [f"{f.name}:{f.type}" for f in ds.schema],
            "olgoonik_JLUNXC1LM1N7": olg}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def apply_ops_ddl(sql: str) -> dict:
    import psycopg
    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    return {"applied": True}


# --------------------------------------------------------------------------- #
# local entrypoints
# --------------------------------------------------------------------------- #
@app.local_entrypoint()
def init_ops() -> None:
    """Create ops.govcon_prime_trajectories_runs (idempotent)."""
    print(apply_ops_ddl.remote(OPS_DDL))


@app.local_entrypoint()
def launch() -> None:
    """Fire-and-forget build on the DEPLOYED app (durable; survives this process exit).
    Requires `modal deploy` first. Resolves run_build BY NAME so the call runs on the
    deployed app's infra — NOT the ephemeral `modal run` app, which is torn down when
    this entrypoint returns (that would cancel an in-ephemeral spawn). Terminal state
    lands in ops.govcon_prime_trajectories_runs."""
    call = modal.Function.from_name(
        "usaspending-govcon-trajectories-pipelines", "run_build").spawn(trigger_callback_url=None)
    print(f"spawned deployed run_build: call_id={call.object_id}")


@app.local_entrypoint()
def build(dry_run: bool = False) -> None:
    """Synchronous build + verify (manual ops). Use ::launch for fire-and-forget."""
    import json
    if dry_run:
        print(json.dumps(plan_build.remote(), indent=2, default=str))
        return
    print(json.dumps(run_build.remote(trigger_callback_url=None), indent=2, default=str))
    print(json.dumps(verify.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_table() -> None:
    import json
    print(json.dumps(verify.remote(), indent=2, default=str))
