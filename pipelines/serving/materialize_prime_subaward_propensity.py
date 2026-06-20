"""Serving worker — govcon_prime_subaward_propensity: prime-keyed realized-subcontracting annotation.

An ADDITIVE, OPTIONAL layer over the work substrate (govcon_active_awards). It does NOT gate the work:
it is a behavioral prior you LEFT JOIN onto active awards on the prime UEI. Absence of a row for a
prime means "no reported subaward history" (FSRS is under-reported — 1,317 primes in 25 years), which
is UNKNOWN, never "won't subcontract." Membership in the work set is unaffected by this table.

Source  usaspending_api_fresh/contract_subaward (realized FSRS subaward reports, full history, status-agnostic).
GRAIN   1 row per (prime_awardee_uei, naics_code) — the prime's FULL NAICS footprint of subawarded work,
        NOT just its single top NAICS. A prime that subcontracts across many NAICS gets many rows; drill
        to "primary only" via is_primary_naics at query time (no rebuild).
SoR     s3://data-sink/active/govcon_prime_subaward_propensity/ (Lance v2.1; derived, snapshot-overwrite).
JOIN    govcon_active_awards.recipient_uei  =  prime_awardee_uei   (optionally AND naics_code = naics_code).
        LEFT JOIN only — additive. Never an inner/semi join that would narrow the work set.
LEDGER  ops.prime_subaward_propensity_serving_runs (HQX_DB_URL_POOLED) on every terminal state.

Local (doppler-scoped):
    doppler run --project core-x --config prd -- python pipelines/serving/materialize_prime_subaward_propensity.py --cmd build
    doppler run --project core-x --config prd -- python pipelines/serving/materialize_prime_subaward_propensity.py --cmd verify
Modal:
    modal run    pipelines/serving/materialize_prime_subaward_propensity.py::main --cmd build
    modal deploy pipelines/serving/materialize_prime_subaward_propensity.py
"""

from __future__ import annotations

import os

try:
    import modal
    _HAS_MODAL = True
except Exception:  # noqa: BLE001
    modal = None
    _HAS_MODAL = False

# --------------------------------------------------------------------------- #
ACTIVE = "s3://data-sink/active"
SERVING_URI = os.environ.get("PRIME_SUBK_PROPENSITY_URI", f"{ACTIVE}/govcon_prime_subaward_propensity/")
SUBAWARD_URI = f"{ACTIVE}/usaspending_api_fresh/contract_subaward/"
FEED = "prime_subaward_propensity"
DATA_STORAGE_VERSION = "2.1"

BTREE_INDEXES = ["prime_awardee_uei", "naics_code", "last_subaward_date"]
BITMAP_INDEXES = ["is_primary_naics"]

DUCK_MEM = os.environ.get("PRIME_SUBK_DUCKDB_MEMORY_LIMIT", "8GB")
DUCK_TMP = os.environ.get("PRIME_SUBK_DUCKDB_TEMP_DIR", "/tmp/prime_subk_duckdb")

_SRC_COLS = [
    "prime_awardee_uei", "prime_awardee_name", "prime_awardee_business_types",
    "prime_award_naics_code", "prime_award_naics_description",
    "subawardee_uei", "subawardee_business_types", "subaward_amount", "subaward_action_date",
]

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.prime_subaward_propensity_serving_runs (
    id                     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                 text,
    feed                   text        NOT NULL,
    subaward_rows          bigint,
    distinct_primes        bigint,
    distinct_naics         bigint,
    rows_written           bigint,
    total_subaward_dollars numeric,
    write_mode             text,
    indices_built          text,
    status                 text        NOT NULL,
    error_message          text,
    metrics                jsonb,
    started_at             timestamptz,
    completed_at           timestamptz,
    recorded_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS prime_subaward_propensity_serving_runs_status_idx
    ON ops.prime_subaward_propensity_serving_runs (status);
CREATE INDEX IF NOT EXISTS prime_subaward_propensity_serving_runs_recorded_at_idx
    ON ops.prime_subaward_propensity_serving_runs (recorded_at DESC);
"""


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
# Assembly — aggregate realized subawards to (prime, naics). Full NAICS footprint, no narrowing.
# --------------------------------------------------------------------------- #
def _assemble(so: dict) -> tuple:
    import datetime as dt

    import lance
    import pyarrow as pa

    con = _duck()
    cs = lance.dataset(SUBAWARD_URI, storage_options=so)
    con.register("cs", cs.scanner(columns=_SRC_COLS).to_table())
    subaward_rows = con.execute("SELECT count(*) FROM cs").fetchone()[0]
    log(f"contract_subaward rows: {subaward_rows:,}")

    log("aggregating realized subawards → (prime_awardee_uei, naics_code) — full NAICS footprint …")
    con.execute("""
        CREATE TEMP TABLE agg AS
        SELECT
            prime_awardee_uei,
            any_value(prime_awardee_name)            AS prime_awardee_name,
            any_value(prime_awardee_business_types)  AS prime_awardee_business_types,
            prime_award_naics_code                   AS naics_code,
            any_value(prime_award_naics_description) AS naics_description,
            sum(TRY_CAST(subaward_amount AS DOUBLE)) AS subaward_dollars,
            count(*)                                 AS n_subawards,
            count(DISTINCT subawardee_uei)           AS n_distinct_subs,
            COALESCE(list(DISTINCT subawardee_business_types)
                     FILTER (WHERE subawardee_business_types IS NOT NULL), []::VARCHAR[]) AS distinct_sub_business_types,
            min(TRY_CAST(subaward_action_date AS DATE)) AS first_subaward_date,
            max(TRY_CAST(subaward_action_date AS DATE)) AS last_subaward_date
        FROM cs
        WHERE prime_awardee_uei IS NOT NULL AND length(trim(prime_awardee_uei)) > 0
        GROUP BY prime_awardee_uei, prime_award_naics_code
    """)

    log("attaching prime-level rollups + is_primary_naics flag …")
    final = con.execute("""
        SELECT
            prime_awardee_uei, prime_awardee_name, prime_awardee_business_types,
            naics_code, naics_description,
            subaward_dollars, n_subawards, n_distinct_subs, distinct_sub_business_types,
            first_subaward_date, last_subaward_date,
            (COALESCE(subaward_dollars, 0) = max(COALESCE(subaward_dollars, 0))
                 OVER (PARTITION BY prime_awardee_uei))            AS is_primary_naics,
            sum(subaward_dollars) OVER (PARTITION BY prime_awardee_uei) AS prime_total_subaward_dollars,
            sum(n_subawards)      OVER (PARTITION BY prime_awardee_uei) AS prime_total_subawards,
            count(*)              OVER (PARTITION BY prime_awardee_uei) AS prime_n_naics
        FROM agg
    """).to_arrow_table()
    if final.num_rows == 0:
        raise RuntimeError("zero propensity rows — check contract_subaward feed")

    stamp = pa.array([dt.datetime.now(dt.timezone.utc)] * final.num_rows, pa.timestamp("us", tz="UTC"))
    final = final.append_column("built_at", stamp)

    con.register("final", final)
    s = con.execute("""SELECT count(*) rows_written, count(DISTINCT prime_awardee_uei) distinct_primes,
        count(DISTINCT naics_code) distinct_naics, sum(subaward_dollars) total_subaward_dollars
        FROM final""").fetchone()
    stats = dict(zip(["rows_written", "distinct_primes", "distinct_naics", "total_subaward_dollars"], s))
    stats["subaward_rows"] = subaward_rows
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
def _pg_connect():
    try:
        import psycopg
    except Exception:  # noqa: BLE001
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
                cur.execute("SELECT to_regclass('ops.prime_subaward_propensity_serving_runs')")
                if cur.fetchone()[0] is None:
                    cur.execute(OPS_DDL)
            with conn.transaction():
                cur.execute(
                    """
                    INSERT INTO ops.prime_subaward_propensity_serving_runs
                        (run_id, feed, subaward_rows, distinct_primes, distinct_naics, rows_written,
                         total_subaward_dollars, write_mode, indices_built, status, error_message,
                         metrics, started_at, completed_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (run_id, FEED, st.get("subaward_rows"), st.get("distinct_primes"), st.get("distinct_naics"),
                     st.get("rows_written"), st.get("total_subaward_dollars"), write_mode, ",".join(indices),
                     status, error, Jsonb(st) if st else None, started, completed),
                )
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.prime_subaward_propensity_serving_runs write failed: {exc}")
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
def build() -> dict:
    import datetime as dt

    run_id = f"prime_subk_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    started = dt.datetime.now(dt.timezone.utc)
    status, error, stats, indices = "error", None, {}, []
    try:
        so = _r2_so()
        tbl, stats = _assemble(so)
        log(f"assembled {stats['rows_written']:,} rows · {stats['distinct_primes']:,} primes · "
            f"{stats['distinct_naics']:,} NAICS · ${(stats['total_subaward_dollars'] or 0)/1e9:.1f}B subawarded")
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
        "is_primary_naics": ds.count_rows(filter="is_primary_naics = true"),
        "indices": idx,
    }
    print(json.dumps(out, indent=2, default=str))
    return out


# --------------------------------------------------------------------------- #
if _HAS_MODAL:
    image = (
        modal.Image.debian_slim(python_version="3.12")
        .pip_install("duckdb>=1.5,<2", "pylance>=7", "pyarrow>=17", "boto3>=1.34", "psycopg[binary]>=3.2")
        .env({"RUST_LOG": "error"})
    )
    app = modal.App("prime-subaward-propensity", image=image)

    @app.function(
        secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
        timeout=60 * 20, memory=16384, cpu=8.0, retries=1,
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


if __name__ == "__main__":
    import argparse
    import json

    ap = argparse.ArgumentParser()
    ap.add_argument("--cmd", default="build", choices=["build", "verify"])
    a = ap.parse_args()
    print(json.dumps(build() if a.cmd == "build" else verify(), indent=2, default=str))
