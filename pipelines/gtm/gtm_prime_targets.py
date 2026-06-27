"""GTM PRIME TARGETS — serving target list of accelerating mid-market govcon primes
for demand-side (staffing/labor) partner outreach. Self-contained (uv-run, no Modal).

WHAT THIS IS. A derived/materialized serving table over the Lance SoR: one row per prime
in the ICP cohort, carrying trailing-window velocity ($ + distinct new awards), growth,
NAICS labor-vertical, demonstrated subcontracting behavior, and a scripted outreach motion.
It is a pure function of two upstream Lance datasets — recomputed each run, full overwrite
(Lance commits a new version; readers on the prior version are unaffected).

    s3://data-sink/active/gtm_prime_targets/

SOURCES (read-only):
  • active/govcon_prime_trajectories  — per-prime trailing-window metrics (new_awards_t{12,24}m,
    obligated_t{12,24}m, primary_naics/psc, as_of_date). Refreshes ~weekly; we pin to the
    LATEST as_of_date snapshot per prime.
  • active/contractor_award_summary   — per-UEI lifetime rollup; we read subaward_total to
    flag demonstrated subcontracting (known vs cold).

THE ICP (tunable constants below). Accelerating, mid-market, project-shaped work:
  • t24m obligated in [BAND_LO, BAND_HI]      — drops mega-primes (entrenched sub networks)
                                                and trivial one-off winners.
  • new_awards_t12m >= MIN_T12M_AWARDS        — recurring procurement cadence.
  • new_awards_t12m > new_awards_prior12m     — winning MORE distinct awards lately (growth).
  • avg award (t24m $ / t24m awards) >= MIN_AVG_AWARD — project-shaped labor work, NOT
    commodity micro-purchase supply (which self-performs and never subcontracts labor).
    This award-substance gate is the discriminator, NOT service-vs-product PSC: it keeps
    labor-relevant manufacturing (assembly/vehicle/equipment) and drops bolt/food vendors.

OUTREACH MOTION (derived):
  • known_subcontractor (subaward_total > 0) → 'expand_roster' ("add us to your eval set").
  • cold                                      → 'form_roster'   ("scaling fast — bench early").
  Caveat: cold == no *reported* subaward (FSRS self-report floor); some cold primes subcontract
  heavily but under-report. Motion is a conversation frame, not a gate.

INDEXES (rebuilt each run): BTREE on resolution/range keys (recipient_uei, primary_naics,
dollar_growth, obligated_t12m, new_awards_t12m, award_growth); BITMAP on low-cardinality
facets (vertical, known_subcontractor, outreach_motion).

IDEMPOTENT. Full overwrite — safe to re-run; same snapshot → same cohort. A terminal-state row
is written to ops.gtm_prime_targets_runs (HQX control plane) each run; best-effort, degrades
to R2-only if HQX is unreachable.

    doppler run -p core-x -c prd -- uv run --no-project \\
      --with boto3 --with pylance --with duckdb --with 'psycopg[binary]>=3.2' \\
      python3 pipelines/gtm/gtm_prime_targets.py [build|verify]
"""
from __future__ import annotations

import datetime as dt
import os
import sys

FEED = "gtm_prime_targets"
DATASET_URI = os.environ.get("GTM_PRIME_TARGETS_URI", "s3://data-sink/active/gtm_prime_targets/").rstrip("/") + "/"
TRAJ_URI = "s3://data-sink/active/govcon_prime_trajectories/"
CAS_URI = "s3://data-sink/active/contractor_award_summary/"
DATA_STORAGE_VERSION = "2.1"

# ── ICP knobs (the only business parameters; change here to re-cut the cohort) ──
BAND_LO = 1_000_000          # t24m obligated lower bound
BAND_HI = 50_000_000         # t24m obligated upper bound (mega-primes excluded)
MIN_T12M_AWARDS = 2          # recurring cadence floor
MIN_AVG_AWARD = 100_000      # avg $/award over t24m — project-shaped, not commodity micro-purchase

BTREE_INDEXES = ["recipient_uei", "primary_naics", "dollar_growth", "obligated_t12m",
                 "new_awards_t12m", "award_growth"]
BITMAP_INDEXES = ["vertical", "known_subcontractor", "outreach_motion"]


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT (or R2_ACCOUNT_ID).")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _materialize(con):
    """Return (arrow_table, metrics). DuckDB does the transform over the two Lance sources
    bridged in as Arrow; output is the ICP cohort with derived velocity/growth/motion cols.
    Metrics are computed FROM the fetched arrow table (not a second con query, which would
    invalidate the freshly-fetched result)."""
    import lance
    import pyarrow as pa
    import pyarrow.compute as pc

    so = _r2_storage_options()
    con.register("traj", lance.dataset(TRAJ_URI, storage_options=so).to_table())
    con.register("cas", lance.dataset(CAS_URI, storage_options=so).scanner(
        columns=["recipient_uei", "subaward_total"]).to_table())

    # latest snapshot per prime (defensive against a time-series grain), then derive + filter
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cohort AS
        WITH latest AS (
            SELECT * FROM (
                SELECT *, row_number() OVER (PARTITION BY recipient_uei ORDER BY as_of_date DESC) rn
                FROM traj
            ) WHERE rn = 1
        ), derived AS (
            SELECT
                l.recipient_uei, l.recipient_name, l.primary_naics, l.primary_psc, l.as_of_date,
                l.new_awards_t12m,
                l.new_awards_t24m,
                GREATEST(l.new_awards_t24m - l.new_awards_t12m, 0)              AS new_awards_prior12m,
                l.new_awards_t12m - GREATEST(l.new_awards_t24m - l.new_awards_t12m, 0) AS award_growth,
                l.obligated_t12m,
                l.obligated_t24m,
                GREATEST(l.obligated_t24m - l.obligated_t12m, 0)               AS obligated_prior12m,
                l.obligated_t12m - GREATEST(l.obligated_t24m - l.obligated_t12m, 0) AS dollar_growth,
                CASE WHEN l.new_awards_t24m > 0 THEN l.obligated_t24m / l.new_awards_t24m END AS avg_award,
                coalesce(c.subaward_total, 0)                                  AS subaward_total,
                coalesce(c.subaward_total, 0) > 0                              AS known_subcontractor,
                CASE substr(l.primary_naics, 1, 2)
                    WHEN '23' THEN 'Construction'
                    WHEN '54' THEN 'Professional/Technical Svc'
                    WHEN '56' THEN 'Admin/Facilities/Staffing'
                    WHEN '62' THEN 'Healthcare'
                    WHEN '51' THEN 'Information/Software'
                    WHEN '48' THEN 'Transport' WHEN '49' THEN 'Transport'
                    WHEN '31' THEN 'Manufacturing' WHEN '32' THEN 'Manufacturing' WHEN '33' THEN 'Manufacturing'
                    WHEN '42' THEN 'Wholesale/Distribution'
                    WHEN '44' THEN 'Retail' WHEN '45' THEN 'Retail'
                    ELSE 'Other' END                                          AS vertical
            FROM latest l LEFT JOIN cas c USING (recipient_uei)
        )
        SELECT *,
            CASE WHEN known_subcontractor THEN 'expand_roster' ELSE 'form_roster' END AS outreach_motion
        FROM derived
        WHERE obligated_t24m BETWEEN {BAND_LO} AND {BAND_HI}
          AND new_awards_t12m >= {MIN_T12M_AWARDS}
          AND new_awards_t12m > new_awards_prior12m
          AND avg_award >= {MIN_AVG_AWARD}
    """)

    table = con.execute("""
        SELECT
            recipient_uei, recipient_name, primary_naics, primary_psc, vertical,
            new_awards_t12m::INTEGER          AS new_awards_t12m,
            new_awards_t24m::INTEGER          AS new_awards_t24m,
            new_awards_prior12m::INTEGER      AS new_awards_prior12m,
            award_growth::INTEGER             AS award_growth,
            obligated_t12m::DOUBLE            AS obligated_t12m,
            obligated_t24m::DOUBLE            AS obligated_t24m,
            obligated_prior12m::DOUBLE        AS obligated_prior12m,
            dollar_growth::DOUBLE             AS dollar_growth,
            avg_award::DOUBLE                 AS avg_award,
            subaward_total::BIGINT            AS subaward_total,
            known_subcontractor,
            outreach_motion,
            (rank() OVER (ORDER BY dollar_growth DESC))::INTEGER AS rank_dollar_growth,
            (rank() OVER (ORDER BY award_growth DESC, new_awards_t12m DESC))::INTEGER AS rank_award_growth,
            as_of_date,
            current_timestamp                 AS built_at
        FROM cohort
        ORDER BY dollar_growth DESC
    """).to_arrow_table()

    rows = table.num_rows
    known = int(pc.sum(pc.cast(table.column("known_subcontractor"), pa.int64())).as_py() or 0)
    total = float(pc.sum(table.column("obligated_t12m")).as_py() or 0.0)
    as_of = pc.max(table.column("as_of_date")).as_py()
    metrics = {"rows": rows, "known_rows": known, "cold_rows": rows - known,
               "total_obligated_t12m": total, "as_of_date": as_of}
    return table, metrics


def _record_run(metrics: dict, status: str, error: str | None,
                started_at: dt.datetime, completed_at: dt.datetime) -> None:
    """Best-effort terminal-state row in ops.gtm_prime_targets_runs (HQX). Applies the DDL
    defensively (IF NOT EXISTS) so the ledger self-heals; degrades to a warning if HQX is
    unreachable so a control-plane hiccup never sinks the data write."""
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("[warn] HQX_DB_URL_POOLED unset — skipping ops ledger row.", file=sys.stderr)
        return
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    ddl = """
        CREATE SCHEMA IF NOT EXISTS ops;
        CREATE TABLE IF NOT EXISTS ops.gtm_prime_targets_runs (
            id                   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            feed                 text NOT NULL,
            as_of_date           date,
            band_lo              bigint,
            band_hi              bigint,
            min_t12m_awards      integer,
            min_avg_award        bigint,
            cohort_rows          bigint NOT NULL DEFAULT 0,
            known_rows           bigint NOT NULL DEFAULT 0,
            cold_rows            bigint NOT NULL DEFAULT 0,
            total_obligated_t12m double precision,
            dataset_uri          text,
            status               text NOT NULL,
            error                text,
            started_at           timestamptz,
            completed_at         timestamptz,
            recorded_at          timestamptz NOT NULL DEFAULT now()
        );
    """
    try:
        import psycopg
        with psycopg.connect(dsn, connect_timeout=15) as conn, conn.cursor() as cur:
            cur.execute(ddl)
            cur.execute(
                """INSERT INTO ops.gtm_prime_targets_runs
                   (feed, as_of_date, band_lo, band_hi, min_t12m_awards, min_avg_award,
                    cohort_rows, known_rows, cold_rows, total_obligated_t12m,
                    dataset_uri, status, error, started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (FEED, metrics.get("as_of_date"), BAND_LO, BAND_HI, MIN_T12M_AWARDS, MIN_AVG_AWARD,
                 metrics.get("rows", 0), metrics.get("known_rows", 0), metrics.get("cold_rows", 0),
                 metrics.get("total_obligated_t12m"), DATASET_URI, status, error,
                 started_at, completed_at),
            )
            conn.commit()
        print(f"[ledger] ops.gtm_prime_targets_runs ← {status} ({metrics.get('rows',0)} rows)")
    except Exception as exc:  # noqa: BLE001 — ledger is observability, never load-bearing
        print(f"[warn] ops ledger write failed ({type(exc).__name__}: {exc}); R2 write stands.",
              file=sys.stderr)


def build() -> int:
    import duckdb
    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error, metrics = "error", None, {}
    # Keep the connection OPEN through write_dataset: a DuckDB .arrow() result is backed by
    # connection-owned buffers, so closing con before the write consumes it writes 0 rows.
    con = duckdb.connect()
    con.execute("SET threads=4")
    con.execute("SET memory_limit='6GB'")
    try:
        table, metrics = _materialize(con)
        print(f"Materialized {FEED}: {metrics}")
        if metrics["rows"] == 0:
            raise RuntimeError("cohort is empty — refusing to overwrite with 0 rows")

        lance.write_dataset(table, DATASET_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
        print(f"Wrote Lance dataset (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")

        ds = lance.dataset(DATASET_URI, storage_options=so)
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE", replace=True); print(f"  BTREE  ✓ {col}")
        for col in BITMAP_INDEXES:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True); print(f"  BITMAP ✓ {col}")

        committed = lance.dataset(DATASET_URI, storage_options=so).count_rows()
        print(f"Committed rows: {committed:,}")
        if committed != metrics["rows"]:
            raise RuntimeError(f"row mismatch: materialized {metrics['rows']} but committed {committed}")
        status = "success"
        return 0
    except Exception as exc:  # noqa: BLE001 — record terminal state then re-raise
        error = str(exc)
        raise
    finally:
        con.close()
        _record_run(metrics, status, error, started_at, dt.datetime.now(dt.timezone.utc))


def verify() -> int:
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    print(f"{DATASET_URI}  ·  {ds.count_rows():,} rows")
    print("indices:", [(i["name"], i.get("type")) for i in ds.list_indices()])
    import duckdb
    con = duckdb.connect(); con.register("t", ds.to_table())
    for r in con.execute("""SELECT outreach_motion, vertical, count(*) n
                            FROM t GROUP BY 1,2 ORDER BY 1, n DESC""").fetchall():
        print(f"  {r[0]:14} | {r[1]:28} | {r[2]:,}")
    print("\ntop 5 by dollar_growth:")
    for r in con.execute("""SELECT recipient_name, vertical, new_awards_t12m, obligated_t12m,
                                   dollar_growth, outreach_motion
                            FROM t ORDER BY dollar_growth DESC LIMIT 5""").fetchall():
        print(f"  {r[0][:38]:38} | {r[1][:22]:22} | aw {r[2]:>3} | ${r[3]:,.0f} | Δ${r[4]:,.0f} | {r[5]}")
    return 0


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    sys.exit(build() if cmd == "build" else verify() if cmd == "verify" else 2)
