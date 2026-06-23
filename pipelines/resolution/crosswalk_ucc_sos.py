"""Compute worker — UCC debtor → SoS entity crosswalk (the UCC half of SAM⟷UCC).

The ``resolution-ucc-sos-pipelines`` Modal app. Resolves every CA/CO UCC *debtor*
(the company that pledged collateral — the borrower) to its canonical Secretary-of-State
entity, on the canonical ``(legal_name_base, zip_code)`` blocking key. This is the UCC-side
mirror of ``crosswalk_sos_sam`` (which resolves SoS→SAM/UEI); composing the two on
``sos_entity_key`` yields SAM entities that are CA/CO UCC debtors.

Source-of-truth inputs (read-only; never mutated):
  s3://data-sink/active/ca_ucc/debtors/   (5.86M rows; ORG debtors carry the canonical key
    materialized by ucc_debtor_normalize_index: normalized_legal_name/legal_name_base/zip_code)
  s3://data-sink/active/ucc_co_debtors/    (1.99M rows; same canonical keys)
  s3://data-sink/active/sos_normalized_master/ (17.93M; current-macro normalized_legal_name +
    MATERIALIZED legal_name_base; geo source_state=JURISDICTION, zip_code=PHYSICAL.
    Entity key = (source_state, original_entity_id).)

Match design (mirror crosswalk_sos_sam §4): UCC debtors are deduped to a COMPANY grain
(one row per (ucc_state, normalized_legal_name, zip5) with filing-backing counts); SoS is
deduped to one row per entity. Join on the MATERIALIZED legal_name_base BOTH sides, CONSTRAINED
WITHIN STATE (ucc_state = sos.source_state) — a CA UCC debtor resolves only against CA SoS, CO
against CO; cross-jurisdiction (e.g. DE-registered) is out of scope (our SoS spine is CA/NY/FL/CO
only). Label match_key by exact-normalized equality, score a 4-tier name+zip ladder, mark exactly
one is_canonical per UCC debtor company over tiers 1-4. The base join reads the materialized
columns — core.name_norm is NOT called to recompute (the keys are already canonical on both sides).

Grain note: only ORG debtors resolve (normalized_legal_name IS NOT NULL); INDIVIDUAL debtors are
excluded here. UCC *activity* (active-vs-lapsed lien, filing recency) is attached downstream in
sam_ucc_debtor_overlap, not here — this worker is pure identity resolution.

Data plane (clean-room — DuckDB does 100% of the transform):
  Lance(ca_ucc_debtors, ucc_co_debtors, sos) → DuckDB unify+dedup+join+score+canonical window →
  Arrow → [pre-write structural gates] → lance.write_dataset(R2 active, v2.1, overwrite) →
  BTREE(sos_entity_key, ucc_debtor_key, ucc_normalized_legal_name) + BITMAP(match_tier,
  is_canonical, ucc_state) → [post-write gates; restore-to-v_before on failure].
  LANCE_BYPASS_SPILLING=true for the high-cardinality index sort.

Control plane: on terminal state writes ops.crosswalk_ucc_sos_runs and POSTs the flat callback.

    modal run    pipelines/resolution/crosswalk_ucc_sos.py::init_ops   # create ops table
    modal run    pipelines/resolution/crosswalk_ucc_sos.py --dry-run   # gates, no write
    modal run    pipelines/resolution/crosswalk_ucc_sos.py             # build + verify
    modal deploy pipelines/resolution/crosswalk_ucc_sos.py             # dispatcher-resolvable
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
CA_UCC_DEBTORS_URI = os.environ.get("CA_UCC_DEBTORS_URI", "s3://data-sink/active/ca_ucc/debtors/")
CO_UCC_DEBTORS_URI = os.environ.get("UCC_CO_DEBTORS_URI", "s3://data-sink/active/ucc_co_debtors/")
SOS_URI = os.environ.get("SOS_NORMALIZED_MASTER_URI", "s3://data-sink/active/sos_normalized_master/")
DATASET_URI = os.environ.get("CROSSWALK_UCC_SOS_URI", "s3://data-sink/active/crosswalk_ucc_sos/")
FEED = "crosswalk_ucc_sos"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576

# Reverse (sos_entity_key → compose w/ crosswalk_sos_sam) + forward (ucc_debtor_key) + audit.
BTREE_INDEXES = ["sos_entity_key", "ucc_debtor_key", "ucc_normalized_legal_name"]
BITMAP_INDEXES = ["match_tier", "is_canonical", "ucc_state"]

DUCKDB_MEMORY_LIMIT = "24GB"
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/duckdb_spill"

# Structural-invariant gates + a loose first-build row floor (no prior coverage baseline).
ROW_FLOOR = 10_000

# Source scan projections (only the columns the transform needs).
CA_COLS = ["normalized_legal_name", "legal_name_base", "zip_code", "ucc1_num"]
CO_COLS = ["normalized_legal_name", "legal_name_base", "zip_code", "file_id"]
SOS_COLS = ["source_state", "original_entity_id", "normalized_legal_name", "legal_name_base",
            "source_entity_name", "zip_code", "entity_status", "snapshot_date"]

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.crosswalk_ucc_sos_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,
    dataset_uri         text        NOT NULL,
    rows_written        bigint,
    canonical_rows      bigint,
    distinct_ucc_debtor bigint,
    distinct_sos_entity bigint,
    tier1_rows          bigint,
    ca_canonical        bigint,
    co_canonical        bigint,
    status              text        NOT NULL,
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS crosswalk_ucc_sos_runs_feed_idx        ON ops.crosswalk_ucc_sos_runs (feed);
CREATE INDEX IF NOT EXISTS crosswalk_ucc_sos_runs_status_idx      ON ops.crosswalk_ucc_sos_runs (status);
CREATE INDEX IF NOT EXISTS crosswalk_ucc_sos_runs_recorded_at_idx ON ops.crosswalk_ucc_sos_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("resolution-ucc-sos-pipelines", image=image)


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
# DuckDB transform — pure SQL builder
# --------------------------------------------------------------------------- #
def build_crosswalk_sql() -> str:
    """Unify CA+CO org-debtors to company grain, dedup SoS to entity grain, join on the
    materialized legal_name_base WITHIN STATE, score the 4-tier name+zip ladder, mark
    is_canonical per UCC debtor company over tiers 1-4. Reads `ca_src`, `co_src`, `sos_src`."""
    return """
    WITH ucc_union AS (
        SELECT 'CA' AS ucc_state, normalized_legal_name, legal_name_base,
               left(nullif(trim(zip_code), ''), 5) AS zip5, ucc1_num AS filing_id
        FROM ca_src WHERE normalized_legal_name IS NOT NULL AND legal_name_base IS NOT NULL
        UNION ALL
        SELECT 'CO' AS ucc_state, normalized_legal_name, legal_name_base,
               left(nullif(trim(zip_code), ''), 5) AS zip5, file_id AS filing_id
        FROM co_src WHERE normalized_legal_name IS NOT NULL AND legal_name_base IS NOT NULL
    ),
    ucc AS (
        SELECT
            ucc_state || ':' || normalized_legal_name || '|' || coalesce(zip5, '') AS ucc_debtor_key,
            ucc_state,
            normalized_legal_name AS ucc_normalized_legal_name,
            legal_name_base       AS ucc_legal_name_base,
            zip5                  AS ucc_zip_code,
            count(*)                       AS n_debtor_rows,
            count(DISTINCT filing_id)      AS n_filings
        FROM ucc_union
        GROUP BY 1, 2, 3, 4, 5
    ),
    sos AS (
        SELECT
            source_state || ':' || original_entity_id      AS sos_entity_key,
            source_state                                   AS sos_source_state,
            original_entity_id                             AS sos_original_entity_id,
            normalized_legal_name                          AS sos_normalized_legal_name,
            source_entity_name                             AS sos_source_entity_name,
            left(nullif(trim(zip_code), ''), 5)            AS sos_zip_code,
            entity_status                                  AS sos_entity_status,
            (upper(coalesce(entity_status, '')) IN ('ACTIVE', 'GOOD STANDING')) AS status_is_active,
            legal_name_base                                AS sos_legal_name_base
        FROM sos_src
        WHERE normalized_legal_name IS NOT NULL
          AND legal_name_base IS NOT NULL
          AND source_state IN ('CA', 'CO')
          AND nullif(trim(original_entity_id), '') IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY source_state, original_entity_id
            ORDER BY snapshot_date DESC NULLS LAST) = 1
    ),
    pairs AS (
        SELECT
            u.ucc_debtor_key, u.ucc_state, u.ucc_normalized_legal_name, u.ucc_legal_name_base,
            u.ucc_zip_code, u.n_debtor_rows, u.n_filings,
            s.sos_entity_key, s.sos_source_state, s.sos_original_entity_id,
            s.sos_normalized_legal_name, s.sos_source_entity_name, s.sos_zip_code,
            s.sos_entity_status, s.status_is_active,
            (u.ucc_normalized_legal_name = s.sos_normalized_legal_name) AS exact_name,
            (u.ucc_zip_code IS NOT NULL AND u.ucc_zip_code = s.sos_zip_code) AS zip_confirms
        FROM ucc u
        JOIN sos s
          ON s.sos_legal_name_base = u.ucc_legal_name_base
         AND s.sos_source_state = u.ucc_state
    ),
    scored AS (
        SELECT *,
            CASE WHEN exact_name THEN 'normalized_legal_name' ELSE 'legal_name_base' END AS match_key,
            CASE WHEN exact_name AND zip_confirms THEN 1
                 WHEN exact_name                 THEN 2
                 WHEN zip_confirms               THEN 3
                 ELSE 4 END                                  AS match_tier
        FROM pairs
    ),
    ranked AS (
        SELECT *,
            row_number() OVER (
                PARTITION BY ucc_debtor_key
                ORDER BY match_tier ASC, zip_confirms DESC, status_is_active DESC,
                         sos_entity_key ASC)                 AS rn
        FROM scored
    )
    SELECT
        * EXCLUDE (exact_name, rn),
        CASE match_tier WHEN 1 THEN 'high' WHEN 2 THEN 'medium_high'
                        WHEN 3 THEN 'medium' ELSE 'low' END  AS match_confidence,
        (rn = 1)                                             AS is_canonical,
        'crosswalk_ucc_sos'                                  AS source_dataset
    FROM ranked
    """


def _new_con():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads TO {DUCKDB_THREADS}")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _materialize(con):
    """Register the three layers, run the join/score/rank, return (arrow_table, metrics)."""
    import lance

    so = _r2_storage_options()
    con.register("ca_src", lance.dataset(CA_UCC_DEBTORS_URI, storage_options=so).scanner(columns=CA_COLS).to_reader())
    con.register("co_src", lance.dataset(CO_UCC_DEBTORS_URI, storage_options=so).scanner(columns=CO_COLS).to_reader())
    con.register("sos_src", lance.dataset(SOS_URI, storage_options=so).scanner(columns=SOS_COLS).to_reader())
    con.execute(f"CREATE TEMP TABLE xwalk AS {build_crosswalk_sql()}")
    con.unregister("ca_src")
    con.unregister("co_src")
    con.unregister("sos_src")

    row = con.execute("""
        SELECT
            count(*)                                                       AS rows,
            count(*) FILTER (WHERE is_canonical)                          AS canonical_rows,
            count(DISTINCT ucc_debtor_key)                                AS distinct_ucc_debtor,
            count(DISTINCT sos_entity_key)                                AS distinct_sos_entity,
            count(*) FILTER (WHERE match_tier = 1)                        AS tier1_rows,
            count(*) FILTER (WHERE is_canonical AND ucc_state = 'CA')     AS ca_canonical,
            count(*) FILTER (WHERE is_canonical AND ucc_state = 'CO')     AS co_canonical,
            count(DISTINCT ucc_debtor_key) FILTER (WHERE is_canonical)    AS canonical_keys,
            count(*) FILTER (WHERE sos_entity_key IS NULL OR ucc_debtor_key IS NULL) AS orphans,
            count(*) FILTER (WHERE match_tier NOT BETWEEN 1 AND 4)        AS tier_violations,
            count(*) FILTER (WHERE (match_tier <= 2) <> (match_key = 'normalized_legal_name')) AS key_violations
        FROM xwalk
    """).fetchone()
    keys = ["rows", "canonical_rows", "distinct_ucc_debtor", "distinct_sos_entity", "tier1_rows",
            "ca_canonical", "co_canonical", "canonical_keys", "orphans", "tier_violations", "key_violations"]
    metrics = {k: int(v) for k, v in zip(keys, row)}
    table = con.sql("SELECT * FROM xwalk").to_arrow_table()
    return table, metrics


# --------------------------------------------------------------------------- #
# Pre-write structural gates
# --------------------------------------------------------------------------- #
def assert_pre_write_gates(m: dict) -> list[str]:
    checks: list[str] = []

    def gate(ok: bool, label: str) -> None:
        checks.append(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            raise RuntimeError(f"PRE-WRITE GATE FAILED → {label}\n" + "\n".join(checks))

    gate(m["rows"] >= ROW_FLOOR, f"1 row floor: {m['rows']:,} >= {ROW_FLOOR:,}")
    gate(m["canonical_rows"] == m["canonical_keys"],
         f"2 canonical uniqueness: is_canonical {m['canonical_rows']:,} == distinct ucc_debtor w/ canonical {m['canonical_keys']:,}")
    gate(m["canonical_rows"] == m["distinct_ucc_debtor"],
         f"2b every matched debtor has exactly one canonical: {m['canonical_rows']:,} == distinct ucc_debtor {m['distinct_ucc_debtor']:,}")
    gate(m["orphans"] == 0, f"3 no orphan keys: {m['orphans']}")
    gate(m["tier_violations"] == 0 and m["key_violations"] == 0,
         f"4 tier/key monotonicity: {m['tier_violations']} tier + {m['key_violations']} key violations")
    gate(m["ca_canonical"] > 0 and m["co_canonical"] > 0,
         f"5 both states resolve: CA={m['ca_canonical']:,} CO={m['co_canonical']:,}")
    return checks


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
                INSERT INTO ops.crosswalk_ucc_sos_runs
                    (feed, dataset_uri, rows_written, canonical_rows, distinct_ucc_debtor,
                     distinct_sos_entity, tier1_rows, ca_canonical, co_canonical, status, error,
                     started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, metrics.get("rows"), metrics.get("canonical_rows"),
                 metrics.get("distinct_ucc_debtor"), metrics.get("distinct_sos_entity"),
                 metrics.get("tier1_rows"), metrics.get("ca_canonical"), metrics.get("co_canonical"),
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
    timeout=60 * 60,
    memory=49152,
    cpu=8.0,
)
def build_crosswalk(trigger_callback_url: str | None = None) -> dict:
    """Materialize → pre-write gates → write + index → post-write gates (restore on failure)."""
    import datetime as dt
    import time

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error = "error", None
    metrics = {"rows": 0, "canonical_rows": 0, "distinct_ucc_debtor": 0, "distinct_sos_entity": 0,
               "tier1_rows": 0, "ca_canonical": 0, "co_canonical": 0}
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics = _materialize(con)
        finally:
            con.close()
        print(f"materialized: {metrics}")
        for line in assert_pre_write_gates(metrics):
            print("  ", line)

        try:
            v_before = lance.dataset(DATASET_URI, storage_options=so).version
        except Exception:
            v_before = None
        print(f"v_before = {v_before}")

        lance.write_dataset(table, DATASET_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        print(f"wrote dataset (overwrite, v{DATA_STORAGE_VERSION}) → {DATASET_URI}")
        ds = lance.dataset(DATASET_URI, storage_options=so)
        for col in BTREE_INDEXES:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"  BTREE ✓ {col}")
        for col in BITMAP_INDEXES:
            ds.create_scalar_index(col, index_type="BITMAP")
            print(f"  BITMAP ✓ {col}")

        # ── post-write gates; restore-to-v_before on failure ──
        try:
            ds = lance.dataset(DATASET_URI, storage_options=so)
            committed = ds.count_rows()
            idx_names = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                         for i in ds.list_indices()}
            expect_idx = {f"{c}_idx" for c in BTREE_INDEXES + BITMAP_INDEXES}
            if not expect_idx.issubset(idx_names):
                raise RuntimeError(f"gate indices: missing {sorted(expect_idx - idx_names)} (have {sorted(idx_names)})")
            if committed != metrics["rows"]:
                raise RuntimeError(f"gate rowcount: committed {committed} != materialized {metrics['rows']}")
            # round-trip: a tier-1 canonical row's sos_entity_key must resolve in SoS
            t1 = ds.scanner(columns=["sos_entity_key", "sos_source_state", "sos_original_entity_id"],
                            filter="match_tier = 1 AND is_canonical", limit=1).to_table().to_pylist()
            if not t1:
                raise RuntimeError("gate round-trip: no tier-1 canonical row found")
            ss, oid = t1[0]["sos_source_state"], t1[0]["sos_original_entity_id"]
            t0 = time.monotonic()
            sos_hit = lance.dataset(SOS_URI, storage_options=so).scanner(
                columns=["original_entity_id"],
                filter=f"source_state = '{ss}' AND original_entity_id = '{oid}'").to_table().num_rows
            seek_ms = (time.monotonic() - t0) * 1000
            if sos_hit < 1:
                raise RuntimeError(f"gate round-trip: sos_entity_key {t1[0]['sos_entity_key']} → sos {sos_hit}")
            print(f"post-write gates PASS — committed={committed:,} indices={sorted(idx_names)} "
                  f"round-trip {t1[0]['sos_entity_key']}→sos {sos_hit} ({seek_ms:.0f}ms)")
        except Exception as gate_exc:  # noqa: BLE001
            if v_before is not None:
                lance.dataset(DATASET_URI, storage_options=so, version=v_before).restore()
                raise RuntimeError(f"post-write gate failed → rolled back to v{v_before}: {gate_exc}")
            raise RuntimeError(f"post-write gate failed on net-new dataset (inspect/drop {DATASET_URI}): {gate_exc}")

        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(metrics=metrics, status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        _post_callback(trigger_callback_url,
                       {"status": status, "rows": metrics["rows"], "feed": FEED,
                        "dataset_uri": DATASET_URI, "canonical_rows": metrics["canonical_rows"],
                        "distinct_sos_entity": metrics["distinct_sos_entity"]})

    if status != "success":
        raise RuntimeError(f"crosswalk_ucc_sos build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, **metrics}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=900)
def verify_crosswalk() -> dict:
    """Read-back proof from R2 — counts, tier distribution, canonical reach, per-state, sample."""
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = sorted((i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                 for i in ds.list_indices())
    con = duckdb.connect()
    con.register("d", ds.scanner(columns=[
        "ucc_debtor_key", "sos_entity_key", "ucc_state", "match_tier", "match_confidence",
        "is_canonical", "zip_confirms", "n_filings"]).to_reader())
    con.execute("CREATE TEMP TABLE d2 AS SELECT * FROM d")
    con.unregister("d")
    rows, d_ucc, d_sos, canon = con.execute(
        "SELECT count(*), count(DISTINCT ucc_debtor_key), count(DISTINCT sos_entity_key), "
        "count(*) FILTER (WHERE is_canonical) FROM d2").fetchone()
    by_tier = dict(con.execute("SELECT match_tier, count(*) FROM d2 GROUP BY 1 ORDER BY 1").fetchall())
    canon_by_state = dict(con.execute(
        "SELECT ucc_state, count(*) FROM d2 WHERE is_canonical GROUP BY 1").fetchall())
    con.close()
    sample = ds.scanner(columns=[
        "ucc_state", "ucc_normalized_legal_name", "ucc_zip_code", "sos_entity_key",
        "sos_source_entity_name", "match_tier", "match_confidence", "n_filings", "is_canonical"],
        filter="is_canonical AND match_tier = 1", limit=6).to_table().to_pylist()
    return {"uri": DATASET_URI, "rows": rows, "distinct_ucc_debtor": d_ucc, "distinct_sos_entity": d_sos,
            "canonical_rows": canon, "indices": idx, "by_match_tier": by_tier,
            "canonical_by_state": canon_by_state,
            "schema": [f"{f.name}:{f.type}" for f in ds.schema], "sample": sample}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    """Apply the ops.crosswalk_ucc_sos_runs DDL (idempotent)."""
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.crosswalk_ucc_sos_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 45, memory=49152, cpu=8.0,
)
def plan_crosswalk() -> dict:
    """Dry-run gate — materialize + run gates, write NOTHING."""
    os.makedirs(SPILL_DIR, exist_ok=True)
    con = _new_con()
    try:
        _table, metrics = _materialize(con)
    finally:
        con.close()
    checks = assert_pre_write_gates(metrics)
    return {"feed": FEED, "gates": checks, **metrics}


@app.local_entrypoint()
def build(dry_run: bool = False) -> None:
    import json

    if dry_run:
        print(json.dumps(plan_crosswalk.remote(), indent=2, default=str))
        return
    print(json.dumps(build_crosswalk.remote(trigger_callback_url=None), indent=2, default=str))
    print(json.dumps(verify_crosswalk.remote(), indent=2, default=str))
