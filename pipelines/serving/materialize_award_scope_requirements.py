"""Serving worker — govcon_award_scope_requirements: the COMPLETE award-keyed scope + requirements
annotation. Surfaces ALL 11 requirement types (the 7 buried in govcon_award_requirements that the
existing profile never exposed) alongside the scope summary + work-domain tags, additive and
LEFT-joinable to the work substrate (govcon_active_awards).

WHY: govcon_award_solicitation_profiles surfaces only clearance/cmmc/certs/labor; govcon_award_requirements
holds 11 validated types (deliverable, standard_compliance, insurance_bonding, license, equipment_capability,
past_performance, staffing_constraint, vehicle_constraint, certification, clearance, labor_category). This
assembles them all per award — no new harvesting, pure assembly over data we already hold.

GRAIN   1 row per contract_award_unique_key (the profile's exploded award grain).
SPINE   Driven from govcon_award_solicitation_profiles.source_resource_ids (the manifest award_keys[]
        fan-out, frozen to the profile snapshot). VERIFIED: exploding source_resource_ids -> resource_id
        and joining govcon_award_requirements reproduces the profile's n_requirements/n_validated 100%
        (35,028/35,028). The inline contract_award_unique_key on requirements is NOT used (it loses 75%).
        Snapshot-frozen to the profile's reqs version; rebuild after the profile rebuilds for freshness.
SCOPE   scope_summary / solicitation_scope_tags / has_extracted_scope carried verbatim from the profile.
REQS    each of the 11 types -> has_<type> (BITMAP) + <type>_values (deduped, alpha, p99-sized cap).
        Derived: req_clearance_level_max (ordinal), requires_cmmc (cert subset), labor_headcount_total,
        wage_floor_max. Caps that clip set req_lists_truncated.
SoR     s3://data-sink/active/govcon_award_scope_requirements/ (Lance v2.1; derived, snapshot-overwrite).
JOIN    govcon_active_awards LEFT JOIN this ON contract_award_unique_key. Additive; absence = UNKNOWN,
        never a gate. Key format is identical (both CONT_* SAM keys; no normalization). Overlap: 28,774
        active / 20,221 active-future annotated.
LEDGER  ops.award_scope_requirements_serving_runs (HQX_DB_URL_POOLED).

Local:  doppler run --project core-x --config prd -- python pipelines/serving/materialize_award_scope_requirements.py --cmd build
        doppler run --project core-x --config prd -- python pipelines/serving/materialize_award_scope_requirements.py --cmd verify
Modal:  modal run pipelines/serving/materialize_award_scope_requirements.py::main --cmd build
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
SERVING_URI = os.environ.get("AWARD_SCOPE_REQ_URI", f"{ACTIVE}/govcon_award_scope_requirements/")
PROFILE_URI = f"{ACTIVE}/govcon_award_solicitation_profiles/"
REQUIREMENTS_URI = f"{ACTIVE}/govcon_award_requirements/"
FEED = "award_scope_requirements"
DATA_STORAGE_VERSION = "2.1"

# (requirement_type, value-list cap) — caps sized off measured p99 (standard_compliance heavy tail).
TYPES = [
    ("standard_compliance", 50), ("vehicle_constraint", 25), ("labor_category", 25),
    ("deliverable", 30), ("past_performance", 25), ("equipment_capability", 25),
    ("certification", 25), ("clearance", 25), ("staffing_constraint", 25),
    ("insurance_bonding", 25), ("license", 25),
]
# clearance ordinal (mirrors build_award_capability_profiles.py CLEARANCE_ORD)
CLEARANCE_ORD = {"TS_SCI": 5, "TOP_SECRET": 4, "SECRET": 3, "CONFIDENTIAL": 2, "PUBLIC_TRUST": 1}

BTREE_INDEXES = ["contract_award_unique_key", "labor_headcount_total", "wage_floor_max",
                 "n_requirement_types", "n_requirements"]
BITMAP_INDEXES = [f"has_{t}" for t, _ in TYPES] + [
    "requires_cmmc", "req_clearance_level_max", "has_extracted_scope",
    "is_primary_target", "req_lists_truncated", "coverage_truncated"]

DUCK_MEM = os.environ.get("ASR_DUCKDB_MEMORY_LIMIT", "10GB")
DUCK_TMP = os.environ.get("ASR_DUCKDB_TEMP_DIR", "/tmp/asr_duckdb")

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.award_scope_requirements_serving_runs (
    id                       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                   text,
    feed                     text        NOT NULL,
    profile_awards           bigint,
    requirement_rows_joined  bigint,
    rows_written             bigint,
    has_extracted_scope_rows bigint,
    requires_clearance_rows  bigint,
    requires_cmmc_rows       bigint,
    req_lists_truncated_rows bigint,
    type_flag_counts         jsonb,
    write_mode               text,
    indices_built            text,
    status                   text        NOT NULL,
    error_message            text,
    metrics                  jsonb,
    started_at               timestamptz,
    completed_at             timestamptz,
    recorded_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS award_scope_requirements_serving_runs_status_idx
    ON ops.award_scope_requirements_serving_runs (status);
CREATE INDEX IF NOT EXISTS award_scope_requirements_serving_runs_recorded_at_idx
    ON ops.award_scope_requirements_serving_runs (recorded_at DESC);
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
def _assemble(so: dict) -> tuple:
    import datetime as dt

    import lance
    import pyarrow as pa

    con = _duck()
    prof = lance.dataset(PROFILE_URI, storage_options=so)
    req = lance.dataset(REQUIREMENTS_URI, storage_options=so)
    con.register("prof", prof.scanner(columns=[
        "contract_award_unique_key", "has_extracted_scope", "scope_summary", "solicitation_scope_tags",
        "source_resource_ids", "is_primary_target", "coverage_truncated",
        "n_requirements", "n_validated", "txn_snapshot_run_id"]).to_table())
    con.register("req", req.scanner(columns=[
        "resource_id", "requirement_type", "requirement_value", "validated",
        "clearance_level", "headcount", "wage_floor"]).to_table())
    profile_awards = con.execute("SELECT count(*) FROM prof").fetchone()[0]
    log(f"profile awards: {profile_awards:,}")

    # req_join: profile.source_resource_ids -> resource_id -> requirements (the verified spine)
    log("building req_join (profile source_resource_ids fan-out → requirements) …")
    con.execute("""
        CREATE TEMP TABLE rj AS
        WITH prof_src AS (
            SELECT contract_award_unique_key AS ak, UNNEST(source_resource_ids) AS resource_id
            FROM prof WHERE source_resource_ids IS NOT NULL AND len(source_resource_ids) > 0)
        SELECT p.ak, r.requirement_type, r.requirement_value, r.validated,
               r.clearance_level, r.headcount, r.wage_floor
        FROM prof_src p JOIN req r ON p.resource_id = r.resource_id
    """)
    rj_rows = con.execute("SELECT count(*) FROM rj").fetchone()[0]
    log(f"req_join rows (fan-out attributed): {rj_rows:,}")

    has_cols = ",\n        ".join(
        f"bool_or(requirement_type='{t}' AND validated) AS has_{t}" for t, _ in TYPES)
    nd_cols = ",\n        ".join(
        f"count(DISTINCT CASE WHEN requirement_type='{t}' AND validated AND requirement_value IS NOT NULL "
        f"THEN requirement_value END) AS nd_{t}" for t, _ in TYPES)
    clr_whens = " ".join(f"WHEN '{lvl}' THEN {o}" for lvl, o in CLEARANCE_ORD.items())

    con.execute(f"""
        CREATE TEMP TABLE agg AS
        SELECT ak,
            count(*) AS n_requirements,
            count(*) FILTER (WHERE validated) AS n_validated,
            {has_cols},
            {nd_cols},
            max(CASE WHEN requirement_type='clearance' AND validated
                     THEN (CASE clearance_level {clr_whens} ELSE 0 END) ELSE 0 END) AS clr_ord,
            bool_or(requirement_type='certification' AND validated
                    AND lower(requirement_value) IN ('cmmc','cmmc_l1','cmmc_l2','cmmc_l3')) AS requires_cmmc,
            CAST(sum(CASE WHEN validated AND requirement_type IN ('labor_category','staffing_constraint')
                         THEN coalesce(headcount,0) ELSE 0 END) AS INTEGER) AS labor_headcount_total,
            max(CASE WHEN validated THEN wage_floor ELSE NULL END) AS wage_floor_max
        FROM rj GROUP BY 1
    """)

    # per-type capped value lists (DISTINCT validated non-null value, alpha, capped)
    list_ctes, list_joins, list_sel = [], [], []
    for t, cap in TYPES:
        list_ctes.append(f"""{t}_roll AS (
            SELECT ak, list(v ORDER BY v) AS {t}_values FROM (
                SELECT ak, v, row_number() OVER (PARTITION BY ak ORDER BY v) AS rk FROM (
                    SELECT DISTINCT ak, requirement_value AS v FROM rj
                    WHERE requirement_type='{t}' AND validated AND requirement_value IS NOT NULL)
            ) WHERE rk <= {cap} GROUP BY 1)""")
        list_joins.append(f"LEFT JOIN {t}_roll ON prof.contract_award_unique_key = {t}_roll.ak")
        list_sel.append(f"{t}_roll.{t}_values AS {t}_values")

    has_sel = ",\n        ".join(f"coalesce(agg.has_{t}, false) AS has_{t}" for t, _ in TYPES)
    n_types_expr = " + ".join(f"CAST(coalesce(agg.has_{t}, false) AS INTEGER)" for t, _ in TYPES)
    trunc_expr = " OR ".join(f"coalesce(agg.nd_{t}, 0) > {cap}" for t, cap in TYPES)
    val_sel = ",\n        ".join(list_sel)
    clr_rev = " ".join(f"WHEN {o} THEN '{lvl}'" for o, lvl in {v: k for k, v in CLEARANCE_ORD.items()}.items())

    log("assembling final (scope carry + 11-type rollup) …")
    final = con.execute(f"""
        WITH {", ".join(list_ctes)}
        SELECT
            prof.contract_award_unique_key AS contract_award_unique_key,
            prof.has_extracted_scope, prof.scope_summary, prof.solicitation_scope_tags,
            {has_sel},
            {val_sel},
            CASE agg.clr_ord {clr_rev} ELSE NULL END AS req_clearance_level_max,
            coalesce(agg.requires_cmmc, false) AS requires_cmmc,
            coalesce(agg.labor_headcount_total, 0) AS labor_headcount_total,
            agg.wage_floor_max AS wage_floor_max,
            CAST(({n_types_expr}) AS INTEGER) AS n_requirement_types,
            CAST(coalesce(agg.n_requirements, 0) AS INTEGER) AS n_requirements,
            CAST(coalesce(agg.n_validated, 0) AS INTEGER) AS n_validated,
            prof.source_resource_ids, prof.is_primary_target,
            coalesce({trunc_expr}, false) AS req_lists_truncated,
            prof.coverage_truncated,
            prof.txn_snapshot_run_id AS src_snapshot_run_id
        FROM prof
        LEFT JOIN agg ON prof.contract_award_unique_key = agg.ak
        {" ".join(list_joins)}
    """).to_arrow_table()
    if final.num_rows == 0:
        raise RuntimeError("zero rows assembled — check profile/requirements feeds")

    stamp = pa.array([dt.datetime.now(dt.timezone.utc)] * final.num_rows, pa.timestamp("us", tz="UTC"))
    final = final.append_column("built_at", stamp)

    con.register("final", final)
    flagcounts = {t: con.execute(f"SELECT count(*) FROM final WHERE has_{t}").fetchone()[0] for t, _ in TYPES}
    s = con.execute("""SELECT count(*) rows_written,
        count(*) FILTER (WHERE has_extracted_scope) has_scope,
        count(*) FILTER (WHERE has_clearance) requires_clearance,
        count(*) FILTER (WHERE requires_cmmc) requires_cmmc,
        count(*) FILTER (WHERE req_lists_truncated) trunc FROM final""").fetchone()
    stats = dict(zip(["rows_written", "has_extracted_scope_rows", "requires_clearance_rows",
                      "requires_cmmc_rows", "req_lists_truncated_rows"], s))
    stats["profile_awards"] = profile_awards
    stats["requirement_rows_joined"] = rj_rows
    stats["type_flag_counts"] = flagcounts
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
                cur.execute("SELECT to_regclass('ops.award_scope_requirements_serving_runs')")
                if cur.fetchone()[0] is None:
                    cur.execute(OPS_DDL)
            with conn.transaction():
                cur.execute(
                    """
                    INSERT INTO ops.award_scope_requirements_serving_runs
                        (run_id, feed, profile_awards, requirement_rows_joined, rows_written,
                         has_extracted_scope_rows, requires_clearance_rows, requires_cmmc_rows,
                         req_lists_truncated_rows, type_flag_counts, write_mode, indices_built,
                         status, error_message, metrics, started_at, completed_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (run_id, FEED, st.get("profile_awards"), st.get("requirement_rows_joined"),
                     st.get("rows_written"), st.get("has_extracted_scope_rows"),
                     st.get("requires_clearance_rows"), st.get("requires_cmmc_rows"),
                     st.get("req_lists_truncated_rows"), Jsonb(st.get("type_flag_counts") or {}),
                     write_mode, ",".join(indices), status, error, Jsonb(st) if st else None,
                     started, completed),
                )
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.award_scope_requirements_serving_runs write failed: {exc}")
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
def build() -> dict:
    import datetime as dt

    run_id = f"asr_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    started = dt.datetime.now(dt.timezone.utc)
    status, error, stats, indices = "error", None, {}, []
    try:
        so = _r2_so()
        tbl, stats = _assemble(so)
        log(f"assembled {stats['rows_written']:,} rows · scope={stats['has_extracted_scope_rows']:,} · "
            f"flags={stats['type_flag_counts']}")
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
    return {"status": status, "uri": SERVING_URI, **{k: v for k, v in stats.items() if k != "type_flag_counts"},
            "type_flag_counts": stats.get("type_flag_counts")}


def verify() -> dict:
    import json

    import lance

    so = _r2_so()
    ds = lance.dataset(SERVING_URI, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)) for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    out = {"uri": SERVING_URI, "rows": ds.count_rows(), "columns": len(ds.schema.names), "indices_n": len(idx)}
    for t, _ in TYPES:
        out[f"has_{t}"] = ds.count_rows(filter=f"has_{t} = true")
    out["requires_cmmc"] = ds.count_rows(filter="requires_cmmc = true")
    out["has_extracted_scope"] = ds.count_rows(filter="has_extracted_scope = true")
    out["req_lists_truncated"] = ds.count_rows(filter="req_lists_truncated = true")
    print(json.dumps(out, indent=2, default=str))
    return out


# --------------------------------------------------------------------------- #
if _HAS_MODAL:
    image = (
        modal.Image.debian_slim(python_version="3.12")
        .pip_install("duckdb>=1.5,<2", "pylance>=7", "pyarrow>=17", "boto3>=1.34", "psycopg[binary]>=3.2")
        .env({"RUST_LOG": "error"})
    )
    app = modal.App("award-scope-requirements", image=image)

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
