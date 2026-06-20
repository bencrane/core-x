"""Serving worker — govcon_active_awards: the objective active prime-award substrate.

One row per prime award still in performance — a projection of usaspending_api_fresh/contract_prime_txn
collapsed to award grain, bounded ONLY by the active/done line. Every analytic dimension (runway,
prime size, set-aside posture, NAICS, value, geography) is stored as a RAW field; NO thresholds or
subjective filters are baked in. Consumers apply their own predicates (runway >= N months, large-prime,
NAICS match, etc.) against the indexed columns.

GRAIN   1 row per contract_award_unique_key = the LATEST transaction per award (latest mod reflects
        current PoP / value / set-aside), deterministic tiebreak: action_date DESC, contract_transaction_unique_key DESC.
MEMBERSHIP (the only boundary — definitional, not a tunable filter):
        GREATEST(current_end, potential_end) >= as_of_date   OR   both PoP ends NULL.
        i.e. keep an award unless it is DEFINITIVELY done (both current AND potential end elapsed).
        Option-tail awards (potential_end beyond current_end — govt holds an unexercised option) and
        PoP-unknown awards are INCLUDED and flagged, never dropped.
OBJECTIVE FLAGS (derived only from raw dates; no judgment):
        active_current   = current_end   >= as_of_date
        active_potential = potential_end >= as_of_date
        has_option_tail  = potential_end > current_end (both non-null)  ← govt has an option
        pop_unknown      = current_end IS NULL AND potential_end IS NULL
SoR     s3://data-sink/active/govcon_active_awards/  (Lance v2.1; derived, snapshot-overwrite).
LEDGER  ops.active_awards_serving_runs (HQX_DB_URL_POOLED) on every terminal state.

Local (doppler-scoped) run:
    doppler run --project core-x --config prd -- python pipelines/serving/materialize_active_awards.py --cmd build
    doppler run --project core-x --config prd -- python pipelines/serving/materialize_active_awards.py --cmd verify
Modal (deploy / dispatcher-resolvable):
    modal run    pipelines/serving/materialize_active_awards.py::main --cmd build
    modal deploy pipelines/serving/materialize_active_awards.py
"""

from __future__ import annotations

import os

try:                       # Modal is optional: the build runs locally via doppler+venv without it.
    import modal
    _HAS_MODAL = True
except Exception:          # noqa: BLE001
    modal = None
    _HAS_MODAL = False

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
ACTIVE = "s3://data-sink/active"
SERVING_URI = os.environ.get("ACTIVE_AWARDS_URI", f"{ACTIVE}/govcon_active_awards/")
PRIMETXN_URI = f"{ACTIVE}/usaspending_api_fresh/contract_prime_txn/"
FEED = "active_awards"
DATA_STORAGE_VERSION = "2.1"

BTREE_INDEXES = ["contract_award_unique_key", "recipient_uei", "naics_code",
                 "pop_current_end", "pop_potential_end"]
BITMAP_INDEXES = ["business_size", "type_of_set_aside", "award_or_idv_flag",
                  "active_current", "active_potential", "has_option_tail", "pop_unknown"]

DUCK_MEM = os.environ.get("ACTIVE_AWARDS_DUCKDB_MEMORY_LIMIT", "8GB")
DUCK_TMP = os.environ.get("ACTIVE_AWARDS_DUCKDB_TEMP_DIR", "/tmp/active_awards_duckdb")

# Raw transaction columns pulled from contract_prime_txn (scalar-only projection).
_SRC_COLS = [
    "contract_award_unique_key", "contract_transaction_unique_key", "award_id_piid", "parent_award_id_piid",
    "award_or_idv_flag", "award_type", "award_type_code", "idv_type",
    "period_of_performance_start_date", "period_of_performance_current_end_date",
    "period_of_performance_potential_end_date", "ordering_period_end_date", "action_date",
    "recipient_uei", "recipient_name", "recipient_parent_uei", "recipient_parent_name", "cage_code",
    "contracting_officers_determination_of_business_size",
    "contracting_officers_determination_of_business_size_code",
    "type_of_set_aside", "type_of_set_aside_code",
    "naics_code", "naics_description", "product_or_service_code", "product_or_service_code_description",
    "federal_action_obligation", "current_total_value_of_award", "base_and_all_options_value",
    "potential_total_value_of_award",
    "awarding_agency_name", "awarding_sub_agency_name", "funding_agency_name",
    "primary_place_of_performance_state_code", "primary_place_of_performance_state_name",
    "primary_place_of_performance_city_name", "primary_place_of_performance_zip_4",
    "primary_place_of_performance_country_code",
]

# --------------------------------------------------------------------------- #
# ops.active_awards_serving_runs — terminal-state ledger (idempotent DDL, self-bootstrapping).
# --------------------------------------------------------------------------- #
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.active_awards_serving_runs (
    id                    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                text,
    feed                  text        NOT NULL,
    as_of_date            date,
    distinct_awards_feed  bigint,
    rows_written          bigint,
    active_current_rows   bigint,
    active_potential_rows bigint,
    option_tail_rows      bigint,
    pop_unknown_rows      bigint,
    write_mode            text,
    indices_built         text,
    status                text        NOT NULL,
    error_message         text,
    metrics               jsonb,
    started_at            timestamptz,
    completed_at          timestamptz,
    recorded_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS active_awards_serving_runs_status_idx
    ON ops.active_awards_serving_runs (status);
CREATE INDEX IF NOT EXISTS active_awards_serving_runs_recorded_at_idx
    ON ops.active_awards_serving_runs (recorded_at DESC);
"""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def log(m) -> None:
    import datetime as dt
    print(f"[{dt.datetime.now(dt.timezone.utc).strftime('%H:%M:%S')}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


def _duck():
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    os.makedirs(DUCK_TMP, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{DUCK_TMP}'")
    return con


# --------------------------------------------------------------------------- #
# Assembly — collapse to award grain, apply membership boundary, derive objective flags.
# No subjective filters; all analytic dimensions are raw columns.
# --------------------------------------------------------------------------- #
def _assemble(so: dict) -> tuple:
    """Returns (final_arrow_table, stats_dict). No SoR write here."""
    import datetime as dt

    import lance
    import pyarrow as pa

    as_of = dt.datetime.now(dt.timezone.utc).date().isoformat()
    con = _duck()
    con.register("txn", lance.dataset(PRIMETXN_URI, storage_options=so))

    log("collapsing contract_prime_txn → award grain (latest txn per award) …")
    proj = ",\n        ".join(_SRC_COLS)
    con.execute(f"""
        CREATE TEMP TABLE latest AS
        WITH ranked AS (
            SELECT {proj},
                   row_number() OVER (
                       PARTITION BY contract_award_unique_key
                       ORDER BY TRY_CAST(action_date AS DATE) DESC NULLS LAST,
                                contract_transaction_unique_key DESC) AS rn
            FROM txn
            WHERE contract_award_unique_key IS NOT NULL
              AND length(trim(contract_award_unique_key)) > 0
        )
        SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
    """)
    distinct_awards = con.execute("SELECT count(*) FROM latest").fetchone()[0]
    log(f"distinct awards in feed: {distinct_awards:,}")

    log(f"applying membership boundary (as_of={as_of}) + deriving objective flags …")
    final = con.execute(f"""
        WITH t AS (
            SELECT *,
                TRY_CAST(period_of_performance_start_date         AS DATE) AS pop_start,
                TRY_CAST(period_of_performance_current_end_date   AS DATE) AS pop_current_end,
                TRY_CAST(period_of_performance_potential_end_date AS DATE) AS pop_potential_end,
                TRY_CAST(ordering_period_end_date                 AS DATE) AS ordering_period_end,
                TRY_CAST(action_date                             AS DATE) AS latest_action_date
            FROM latest
        )
        SELECT
            contract_award_unique_key, award_id_piid, parent_award_id_piid,
            award_or_idv_flag, award_type, award_type_code, idv_type,
            pop_start, pop_current_end, pop_potential_end, ordering_period_end, latest_action_date,
            (pop_current_end   >= DATE '{as_of}')                                          AS active_current,
            (pop_potential_end >= DATE '{as_of}')                                          AS active_potential,
            (pop_potential_end IS NOT NULL AND pop_current_end IS NOT NULL
                 AND pop_potential_end > pop_current_end)                                  AS has_option_tail,
            (pop_current_end IS NULL AND pop_potential_end IS NULL)                        AS pop_unknown,
            recipient_uei, recipient_name, recipient_parent_uei, recipient_parent_name, cage_code,
            contracting_officers_determination_of_business_size      AS business_size,
            contracting_officers_determination_of_business_size_code AS business_size_code,
            type_of_set_aside, type_of_set_aside_code,
            naics_code, naics_description,
            product_or_service_code             AS psc_code,
            product_or_service_code_description AS psc_description,
            TRY_CAST(federal_action_obligation       AS DOUBLE) AS federal_action_obligation,
            TRY_CAST(current_total_value_of_award    AS DOUBLE) AS current_total_value_of_award,
            TRY_CAST(base_and_all_options_value      AS DOUBLE) AS base_and_all_options_value,
            TRY_CAST(potential_total_value_of_award  AS DOUBLE) AS potential_total_value_of_award,
            awarding_agency_name, awarding_sub_agency_name, funding_agency_name,
            primary_place_of_performance_state_code  AS pop_state_code,
            primary_place_of_performance_state_name  AS pop_state_name,
            primary_place_of_performance_city_name   AS pop_city,
            primary_place_of_performance_zip_4       AS pop_zip,
            primary_place_of_performance_country_code AS pop_country_code,
            CAST(DATE '{as_of}' AS DATE) AS as_of_date
        FROM t
        WHERE (pop_current_end   >= DATE '{as_of}')
           OR (pop_potential_end >= DATE '{as_of}')
           OR (pop_current_end IS NULL AND pop_potential_end IS NULL)
    """).to_arrow_table()
    if final.num_rows == 0:
        raise RuntimeError("zero active awards after membership boundary — check the prime feed")

    stamp = pa.array([dt.datetime.now(dt.timezone.utc)] * final.num_rows, pa.timestamp("us", tz="UTC"))
    final = final.append_column("built_at", stamp)

    con.register("final", final)
    s = con.execute("""
        SELECT count(*) rows_written,
               count(*) FILTER (WHERE active_current)   active_current_rows,
               count(*) FILTER (WHERE active_potential) active_potential_rows,
               count(*) FILTER (WHERE has_option_tail)  option_tail_rows,
               count(*) FILTER (WHERE pop_unknown)      pop_unknown_rows
        FROM final""").fetchone()
    stats = dict(zip(["rows_written", "active_current_rows", "active_potential_rows",
                      "option_tail_rows", "pop_unknown_rows"], s))
    stats["distinct_awards_feed"] = distinct_awards
    stats["as_of_date"] = as_of
    con.close()
    return final, stats


def _write_index(tbl, so: dict) -> list[str]:
    import lance

    lance.write_dataset(tbl, SERVING_URI, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    ds = lance.dataset(SERVING_URI, storage_options=so)
    built: list[str] = []
    for col in BTREE_INDEXES:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        built.append(f"{col}:BTREE")
        log(f"  BTREE ✓ {col}")
    for col in BITMAP_INDEXES:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        built.append(f"{col}:BITMAP")
        log(f"  BITMAP ✓ {col}")
    return built


# --------------------------------------------------------------------------- #
# ops ledger
# --------------------------------------------------------------------------- #
def _pg_connect():
    try:
        import psycopg
    except Exception:  # noqa: BLE001 — psycopg optional in local runs
        print("WARN: psycopg unavailable; skipping ops.* write.")
        return None
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _record_run(*, run_id, stats, write_mode, indices, status, error, started, completed) -> None:
    from psycopg.types.json import Jsonb

    conn = _pg_connect()
    if conn is None:
        return
    st = stats or {}
    try:
        with conn.cursor() as cur:
            with conn.transaction():
                cur.execute("SELECT to_regclass('ops.active_awards_serving_runs')")
                if cur.fetchone()[0] is None:
                    cur.execute(OPS_DDL)
            with conn.transaction():
                cur.execute(
                    """
                    INSERT INTO ops.active_awards_serving_runs
                        (run_id, feed, as_of_date, distinct_awards_feed, rows_written,
                         active_current_rows, active_potential_rows, option_tail_rows, pop_unknown_rows,
                         write_mode, indices_built, status, error_message, metrics, started_at, completed_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (run_id, FEED, st.get("as_of_date"), st.get("distinct_awards_feed"), st.get("rows_written"),
                     st.get("active_current_rows"), st.get("active_potential_rows"), st.get("option_tail_rows"),
                     st.get("pop_unknown_rows"), write_mode, ",".join(indices), status, error,
                     Jsonb(st) if st else None, started, completed),
                )
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.active_awards_serving_runs write failed: {exc}")
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# Core build / verify (plain — callable from Modal and from the local CLI)
# --------------------------------------------------------------------------- #
def build() -> dict:
    import datetime as dt

    run_id = f"active_awards_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    started = dt.datetime.now(dt.timezone.utc)
    status, error, stats, indices = "error", None, {}, []
    try:
        so = _r2_so()
        tbl, stats = _assemble(so)
        log(f"assembled {stats['rows_written']:,} rows · active_current={stats['active_current_rows']:,} "
            f"active_potential={stats['active_potential_rows']:,} option_tail={stats['option_tail_rows']:,} "
            f"pop_unknown={stats['pop_unknown_rows']:,} (of {stats['distinct_awards_feed']:,} awards)")
        indices = _write_index(tbl, so)
        status = "success"
        log(f"DONE → {SERVING_URI} rows={stats['rows_written']:,}")
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        log(f"FAILED: {error}")
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, stats=stats, write_mode="overwrite", indices=indices,
                    status=status, error=error, started=started, completed=completed)
    if status != "success":
        raise RuntimeError(f"build failed: {error}")
    return {"status": status, "uri": SERVING_URI, **stats}


def verify() -> dict:
    import json

    import lance

    so = _r2_so()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    out = {
        "uri": SERVING_URI, "rows": ds.count_rows(), "columns": len(ds.schema.names),
        "active_current": ds.count_rows(filter="active_current = true"),
        "active_potential": ds.count_rows(filter="active_potential = true"),
        "has_option_tail": ds.count_rows(filter="has_option_tail = true"),
        "pop_unknown": ds.count_rows(filter="pop_unknown = true"),
        "indices": idx,
    }
    print(json.dumps(out, indent=2, default=str))
    return out


# --------------------------------------------------------------------------- #
# Modal wrappers (only when modal is importable)
# --------------------------------------------------------------------------- #
if _HAS_MODAL:
    image = (
        modal.Image.debian_slim(python_version="3.12")
        .pip_install("duckdb>=1.5,<2", "pylance>=7", "pyarrow>=17",
                     "boto3>=1.34", "psycopg[binary]>=3.2")
        .env({"RUST_LOG": "error"})
    )
    app = modal.App("active-awards", image=image)

    @app.function(
        secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
        timeout=60 * 30, memory=16384, cpu=8.0, retries=1,
    )
    def run_build() -> dict:
        return build()

    @app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10, memory=8192, cpu=2.0)
    def run_verify() -> dict:
        return verify()

    @app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 2)
    def init_ops() -> dict:
        conn = _pg_connect()
        if conn is None:
            return {"status": "skipped"}
        try:
            with conn, conn.cursor() as cur:
                cur.execute(OPS_DDL)
            return {"status": "applied"}
        finally:
            conn.close()

    @app.local_entrypoint()
    def main(cmd: str = "build") -> None:
        import json
        fn = {"build": run_build, "verify": run_verify, "init_ops": init_ops}.get(cmd)
        if fn is None:
            raise SystemExit(f"unknown cmd {cmd!r}")
        print(json.dumps(fn.remote(), indent=2, default=str))


# --------------------------------------------------------------------------- #
# Local CLI (no Modal required — runs in-process under `doppler run`)
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import argparse
    import json

    ap = argparse.ArgumentParser()
    ap.add_argument("--cmd", default="build", choices=["build", "verify"])
    a = ap.parse_args()
    print(json.dumps(build() if a.cmd == "build" else verify(), indent=2, default=str))
