"""Compute worker — sam_ucc_debtor_overlap: SAM.gov entities that are CA/CO UCC debtors.

The ``resolution-sam-ucc-overlap-pipelines`` Modal app — the CAPSTONE of the SAM⟷UCC
resolution. Composes the two canonical bridges through the SoS hub:

    crosswalk_ucc_sos (UCC debtor → SoS, is_canonical)  ⋈  crosswalk_sos_sam (SoS → UEI,
    is_canonical)   ON sos_entity_key

→ one row per SAM entity (uei) that resolves to a CA/CO UCC debtor, enriched with:
  · UCC ACTIVITY (debtor → ca_ucc_filings / co_ucc_transactions): n_ucc_financing,
    n_active_ucc_liens, has_active_lien, has_tax_lien. "Taking $" = VOLUNTARY secured
    financing only — CA filing_type='UCC'; CO filing_type IN ('ucc','efs'). Involuntary
    liens (CA 'Notice of … Tax Lien'/'Judgment Lien'; CO 'lien_*') are EXCLUDED from
    "taking $" but surfaced as has_tax_lien (a distinct, useful segment). Active lien =
    secured financing with max(lapse_date) >= today and not terminated (CA action_type
    'Termination'; CO termination_flag).
  · OFFICER CORROBORATION (inline): SAM POC person-names (sam_pocs) vs SoS officer names
    (ca_sos_principals; co_sos registered agent), both normalized by the SHARED
    core.name_norm macro → officer_match_count / officer_confirms. Breaks the 1-vs-N name
    ambiguity; CA-strong (full principal roster), CO thin (agent only).
  · COMPOSITE confidence from the two name-match tiers × officer corroboration.

Grain: one row per (uei, sos_entity_key) — since crosswalk_sos_sam is 1 canonical uei per
SoS entity and crosswalk_ucc_sos is 1 canonical SoS per UCC debtor, this is the SAM-entity
grain, aggregating the UCC debtor backing (one SoS entity can absorb several debtor
name-variants).

COVERAGE EXPECTATION (not a defect): SAM registration is for federal contractors/grant
recipients; most small private UCC debtors never registered. This dataset is the
high-value INTERSECTION — federally-registered companies carrying secured debt.

Source-of-truth inputs (read-only; never mutated):
  active/crosswalk_ucc_sos/   active/crosswalk_sos_sam/
  active/ca_ucc/debtors/ + active/ca_ucc/filings/      (CA activity, join on ucc1_num)
  active/ucc_co_debtors/ + active/co_ucc_transactions/ (CO activity, join on file_id)
  active/sam_pocs/  active/ca_sos_principals/  active/co_sos/   (officer corroboration)

Data plane (clean-room — DuckDB does 100% of the transform):
  Lance(8) → DuckDB compose+activity+officer+score → Arrow → [pre-write gates] →
  lance.write_dataset(R2 active, v2.1, overwrite) → BTREE(uei, sos_entity_key,
  sam_legal_business_name) + BITMAP(overlap_confidence, has_active_lien, officer_confirms,
  ucc_states) → [post-write gates; restore-to-v_before on failure].

    modal run    pipelines/resolution/sam_ucc_debtor_overlap.py::init_ops
    modal run    pipelines/resolution/sam_ucc_debtor_overlap.py --dry-run   # gates, no write
    modal run    pipelines/resolution/sam_ucc_debtor_overlap.py             # build + verify
    modal deploy pipelines/resolution/sam_ucc_debtor_overlap.py
"""

from __future__ import annotations

import os

import modal

from core.name_norm import name_norm as _name_norm

BUCKET = "data-sink"
CROSSWALK_UCC_SOS_URI = os.environ.get("CROSSWALK_UCC_SOS_URI", "s3://data-sink/active/crosswalk_ucc_sos/")
CROSSWALK_SOS_SAM_URI = os.environ.get("CROSSWALK_SOS_SAM_URI", "s3://data-sink/active/crosswalk_sos_sam/")
CA_DEBTORS_URI = os.environ.get("CA_UCC_DEBTORS_URI", "s3://data-sink/active/ca_ucc/debtors/")
CA_FILINGS_URI = os.environ.get("CA_UCC_FILINGS_URI", "s3://data-sink/active/ca_ucc/filings/")
CO_DEBTORS_URI = os.environ.get("UCC_CO_DEBTORS_URI", "s3://data-sink/active/ucc_co_debtors/")
CO_TXN_URI = os.environ.get("CO_UCC_TRANSACTIONS_LANCE_URI", "s3://data-sink/active/co_ucc_transactions/")
SAM_POCS_URI = os.environ.get("SAM_POCS_URI", "s3://data-sink/active/sam_pocs/")
CA_PRINCIPALS_URI = os.environ.get("CA_SOS_PRINCIPALS_LANCE_URI", "s3://data-sink/active/ca_sos_principals/")
CO_SOS_URI = os.environ.get("CO_SOS_LANCE_URI", "s3://data-sink/active/co_sos/")
DATASET_URI = os.environ.get("SAM_UCC_DEBTOR_OVERLAP_URI", "s3://data-sink/active/sam_ucc_debtor_overlap/")
FEED = "sam_ucc_debtor_overlap"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576

BTREE_INDEXES = ["uei", "sos_entity_key", "sam_legal_business_name"]
BITMAP_INDEXES = ["overlap_confidence", "has_active_lien", "officer_confirms", "ucc_states"]

DUCKDB_MEMORY_LIMIT = "40GB"
DUCKDB_THREADS = 8
SPILL_DIR = "/tmp/duckdb_spill"

ROW_FLOOR = 1_000  # the intersection is a high-value sliver; conservative floor

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sam_ucc_debtor_overlap_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,
    dataset_uri         text        NOT NULL,
    rows_written        bigint,
    distinct_uei        bigint,
    with_active_lien    bigint,
    officer_confirmed   bigint,
    ca_rows             bigint,
    co_rows             bigint,
    status              text        NOT NULL,
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sam_ucc_overlap_runs_feed_idx        ON ops.sam_ucc_debtor_overlap_runs (feed);
CREATE INDEX IF NOT EXISTS sam_ucc_overlap_runs_status_idx      ON ops.sam_ucc_debtor_overlap_runs (status);
CREATE INDEX IF NOT EXISTS sam_ucc_overlap_runs_recorded_at_idx ON ops.sam_ucc_debtor_overlap_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"}).add_local_python_source("core.name_norm")

app = modal.App("resolution-sam-ucc-overlap-pipelines", image=image)


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
def build_overlap_sql() -> str:
    """Compose the two canonical bridges through sos_entity_key, attach UCC activity +
    officer corroboration, score composite confidence. Reads the scanned relations
    cu_src, cs_src, ca_deb_src, ca_fil_src, co_deb_src, co_txn_src, sam_poc_src,
    ca_prin_src, co_sos_src. Person-name key uses the shared core.name_norm macro."""
    sam_pk = _name_norm("sam_poc_src.first_name || ' ' || sam_poc_src.last_name")
    ca_pk = _name_norm("ca_prin_src.first_name || ' ' || ca_prin_src.last_name")
    co_pk = _name_norm("co_sos_src.agent_first_name || ' ' || co_sos_src.agent_last_name")
    return f"""
    WITH cu AS (   -- canonical UCC debtor → SoS (one per debtor company)
        SELECT ucc_debtor_key, ucc_state, ucc_normalized_legal_name, ucc_zip_code,
               sos_entity_key, sos_source_entity_name, match_tier AS ucc_sos_tier
        FROM cu_src WHERE is_canonical
    ),
    cs AS (        -- canonical SoS → UEI (one per SoS entity)
        SELECT sos_entity_key, uei, sam_legal_business_name, sam_is_active, sam_primary_naics,
               match_tier AS sos_sam_tier
        FROM cs_src WHERE is_canonical AND uei IS NOT NULL
    ),
    base AS (      -- the overlap, per UCC debtor name-variant
        SELECT cu.ucc_debtor_key, cu.ucc_state, cu.ucc_normalized_legal_name, cu.ucc_zip_code,
               cu.sos_entity_key, cu.sos_source_entity_name, cu.ucc_sos_tier,
               cs.uei, cs.sam_legal_business_name, cs.sam_is_active, cs.sam_primary_naics, cs.sos_sam_tier
        FROM cu JOIN cs ON cs.sos_entity_key = cu.sos_entity_key
    ),
    -- ── UCC financing-statement status (CA: per ucc1_num; CO: per file_id) ──
    ca_fil AS (
        SELECT ucc1_num,
               max(CASE WHEN filing_type = 'UCC' THEN 1 ELSE 0 END)                          AS is_ucc,
               max(lapse_date)                                                               AS max_lapse,
               max(CASE WHEN action_type = 'Termination' THEN 1 ELSE 0 END)                  AS terminated,
               max(CASE WHEN filing_type LIKE '%Tax Lien' OR filing_type = 'Judgment Lien'
                        THEN 1 ELSE 0 END)                                                   AS is_tax
        FROM ca_fil_src WHERE ucc1_num IS NOT NULL GROUP BY ucc1_num
    ),
    co_fil AS (
        SELECT file_id,
               max(CASE WHEN filing_type IN ('ucc', 'efs') THEN 1 ELSE 0 END)               AS is_ucc,
               max(lapse_date)                                                               AS max_lapse,
               max(CASE WHEN termination_flag THEN 1 ELSE 0 END)                             AS terminated,
               max(CASE WHEN filing_type LIKE 'lien_%' THEN 1 ELSE 0 END)                    AS is_tax
        FROM co_txn_src WHERE file_id IS NOT NULL GROUP BY file_id
    ),
    -- ── UCC activity per ucc_debtor_key (recomputed from the keyed debtor tables) ──
    ca_act AS (
        SELECT 'CA:' || d.normalized_legal_name || '|' || coalesce(d.zip_code, '')          AS ucc_debtor_key,
               count(DISTINCT CASE WHEN f.is_ucc = 1 THEN d.ucc1_num END)                    AS n_ucc_financing,
               count(DISTINCT CASE WHEN f.is_ucc = 1 AND f.terminated = 0
                                    AND f.max_lapse >= CURRENT_DATE THEN d.ucc1_num END)     AS n_active_ucc,
               max(f.is_tax)                                                                 AS has_tax_lien
        FROM ca_deb_src d JOIN ca_fil f ON f.ucc1_num = d.ucc1_num
        WHERE d.normalized_legal_name IS NOT NULL GROUP BY 1
    ),
    co_act AS (
        SELECT 'CO:' || d.normalized_legal_name || '|' || coalesce(d.zip_code, '')          AS ucc_debtor_key,
               count(DISTINCT CASE WHEN f.is_ucc = 1 THEN d.file_id END)                     AS n_ucc_financing,
               count(DISTINCT CASE WHEN f.is_ucc = 1 AND f.terminated = 0
                                    AND f.max_lapse >= CURRENT_DATE THEN d.file_id END)      AS n_active_ucc,
               max(f.is_tax)                                                                 AS has_tax_lien
        FROM co_deb_src d JOIN co_fil f ON f.file_id = d.file_id
        WHERE d.normalized_legal_name IS NOT NULL GROUP BY 1
    ),
    ucc_act AS (SELECT * FROM ca_act UNION ALL SELECT * FROM co_act),
    base_act AS (
        SELECT b.*, coalesce(a.n_ucc_financing, 0) AS n_ucc_financing,
               coalesce(a.n_active_ucc, 0) AS n_active_ucc, coalesce(a.has_tax_lien, 0) AS has_tax_lien
        FROM base b LEFT JOIN ucc_act a USING (ucc_debtor_key)
    ),
    -- ── officer person-name sets (shared core.name_norm macro both sides) ──
    sam_nm AS (
        SELECT DISTINCT uei, {sam_pk} AS pk FROM sam_poc_src
        WHERE uei IS NOT NULL AND {sam_pk} IS NOT NULL AND {sam_pk} LIKE '% %' AND length({sam_pk}) >= 6
    ),
    sos_nm AS (
        SELECT 'CA:' || entity_num AS sos_entity_key, {ca_pk} AS pk FROM ca_prin_src
        WHERE entity_num IS NOT NULL AND {ca_pk} IS NOT NULL AND {ca_pk} LIKE '% %' AND length({ca_pk}) >= 6
        UNION
        SELECT 'CO:' || entity_id AS sos_entity_key, {co_pk} AS pk FROM co_sos_src
        WHERE entity_id IS NOT NULL AND {co_pk} IS NOT NULL AND {co_pk} LIKE '% %' AND length({co_pk}) >= 6
    ),
    cand AS (SELECT DISTINCT uei, sos_entity_key FROM base_act),
    officer AS (
        SELECT c.uei, c.sos_entity_key, count(DISTINCT sn.pk) AS officer_match_count
        FROM cand c
        JOIN sam_nm sn ON sn.uei = c.uei
        JOIN sos_nm so ON so.sos_entity_key = c.sos_entity_key AND so.pk = sn.pk
        GROUP BY 1, 2
    ),
    -- ── aggregate to SAM-entity grain (uei, sos_entity_key) ──
    agg AS (
        SELECT
            uei, sos_entity_key,
            any_value(sam_legal_business_name)      AS sam_legal_business_name,
            any_value(sam_is_active)                AS sam_is_active,
            any_value(sam_primary_naics)            AS sam_primary_naics,
            any_value(sos_source_entity_name)       AS sos_source_entity_name,
            min(ucc_sos_tier)                       AS ucc_sos_tier,
            min(sos_sam_tier)                        AS sos_sam_tier,
            count(DISTINCT ucc_debtor_key)          AS n_ucc_debtor_names,
            any_value(ucc_normalized_legal_name)    AS ucc_example_name,
            sum(n_ucc_financing)                    AS n_ucc_financing,
            sum(n_active_ucc)                       AS n_active_ucc_liens,
            max(has_tax_lien)                       AS has_tax_lien_i,
            CASE WHEN count(DISTINCT ucc_state) > 1 THEN 'CA+CO' ELSE any_value(ucc_state) END AS ucc_states
        FROM base_act GROUP BY uei, sos_entity_key
    )
    SELECT
        g.uei, g.sos_entity_key, g.sos_source_entity_name, g.sam_legal_business_name,
        g.sam_is_active, g.sam_primary_naics, g.ucc_states, g.ucc_example_name,
        g.n_ucc_debtor_names, g.ucc_sos_tier, g.sos_sam_tier,
        g.n_ucc_financing, g.n_active_ucc_liens,
        (g.n_active_ucc_liens > 0)                  AS has_active_lien,
        (g.has_tax_lien_i = 1)                      AS has_tax_lien,
        coalesce(o.officer_match_count, 0)          AS officer_match_count,
        (coalesce(o.officer_match_count, 0) >= 1)   AS officer_confirms,
        CASE
            WHEN g.ucc_sos_tier = 1 AND g.sos_sam_tier = 1 AND coalesce(o.officer_match_count, 0) >= 1 THEN 'very_high'
            WHEN g.ucc_sos_tier = 1 AND g.sos_sam_tier = 1 THEN 'high'
            WHEN g.ucc_sos_tier <= 2 AND g.sos_sam_tier <= 2 THEN 'medium_high'
            WHEN g.ucc_sos_tier <= 2 AND g.sos_sam_tier <= 3 THEN 'medium'
            ELSE 'low'
        END                                         AS overlap_confidence,
        'sam_ucc_debtor_overlap'                    AS source_dataset
    FROM agg g LEFT JOIN officer o USING (uei, sos_entity_key)
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
    import lance

    so = _r2_storage_options()

    def scan(uri, cols):
        return lance.dataset(uri, storage_options=so).scanner(columns=cols).to_reader()

    con.register("cu_src", scan(CROSSWALK_UCC_SOS_URI, [
        "ucc_debtor_key", "ucc_state", "ucc_normalized_legal_name", "ucc_zip_code",
        "sos_entity_key", "sos_source_entity_name", "match_tier", "is_canonical"]))
    con.register("cs_src", scan(CROSSWALK_SOS_SAM_URI, [
        "sos_entity_key", "uei", "sam_legal_business_name", "sam_is_active",
        "sam_primary_naics", "match_tier", "is_canonical"]))
    con.register("ca_deb_src", scan(CA_DEBTORS_URI, ["normalized_legal_name", "zip_code", "ucc1_num"]))
    con.register("ca_fil_src", scan(CA_FILINGS_URI, ["ucc1_num", "filing_type", "lapse_date", "action_type"]))
    con.register("co_deb_src", scan(CO_DEBTORS_URI, ["normalized_legal_name", "zip_code", "file_id"]))
    con.register("co_txn_src", scan(CO_TXN_URI, ["file_id", "filing_type", "lapse_date", "termination_flag"]))
    con.register("sam_poc_src", scan(SAM_POCS_URI, ["uei", "first_name", "last_name"]))
    con.register("ca_prin_src", scan(CA_PRINCIPALS_URI, ["entity_num", "first_name", "last_name"]))
    con.register("co_sos_src", scan(CO_SOS_URI, ["entity_id", "agent_first_name", "agent_last_name"]))
    con.execute(f"CREATE TEMP TABLE overlap AS {build_overlap_sql()}")
    for r in ("cu_src", "cs_src", "ca_deb_src", "ca_fil_src", "co_deb_src", "co_txn_src",
              "sam_poc_src", "ca_prin_src", "co_sos_src"):
        con.unregister(r)

    row = con.execute("""
        SELECT
            count(*)                                                    AS rows,
            count(DISTINCT uei)                                         AS distinct_uei,
            count(*) FILTER (WHERE has_active_lien)                     AS with_active_lien,
            count(*) FILTER (WHERE officer_confirms)                    AS officer_confirmed,
            count(*) FILTER (WHERE ucc_states LIKE 'CA%')               AS ca_rows,
            count(*) FILTER (WHERE ucc_states = 'CO')                   AS co_rows,
            count(*) FILTER (WHERE has_tax_lien)                        AS with_tax_lien,
            count(*) FILTER (WHERE uei IS NULL OR sos_entity_key IS NULL) AS orphans,
            count(*) FILTER (WHERE overlap_confidence = 'very_high')    AS very_high,
            count(*) FILTER (WHERE n_ucc_financing = 0)                 AS zero_financing
        FROM overlap
    """).fetchone()
    keys = ["rows", "distinct_uei", "with_active_lien", "officer_confirmed", "ca_rows", "co_rows",
            "with_tax_lien", "orphans", "very_high", "zero_financing"]
    metrics = {k: int(v) for k, v in zip(keys, row)}
    table = con.sql("SELECT * FROM overlap").to_arrow_table()
    return table, metrics


def assert_pre_write_gates(m: dict) -> list[str]:
    checks: list[str] = []

    def gate(ok: bool, label: str) -> None:
        checks.append(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            raise RuntimeError(f"PRE-WRITE GATE FAILED → {label}\n" + "\n".join(checks))

    gate(m["rows"] >= ROW_FLOOR, f"1 row floor: {m['rows']:,} >= {ROW_FLOOR:,}")
    gate(m["orphans"] == 0, f"2 no orphan keys: {m['orphans']}")
    gate(m["distinct_uei"] > 0, f"3 distinct uei: {m['distinct_uei']:,}")
    gate(m["ca_rows"] > 0 and m["co_rows"] > 0, f"4 both states present: CA={m['ca_rows']:,} CO={m['co_rows']:,}")
    return checks


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
                INSERT INTO ops.sam_ucc_debtor_overlap_runs
                    (feed, dataset_uri, rows_written, distinct_uei, with_active_lien,
                     officer_confirmed, ca_rows, co_rows, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (FEED, DATASET_URI, metrics.get("rows"), metrics.get("distinct_uei"),
                 metrics.get("with_active_lien"), metrics.get("officer_confirmed"),
                 metrics.get("ca_rows"), metrics.get("co_rows"), status, error,
                 started_at, completed_at),
            )
    except Exception as exc:  # noqa: BLE001
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


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 90,
    memory=65536,
    cpu=8.0,
)
def build_overlap(trigger_callback_url: str | None = None) -> dict:
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error = "error", None
    metrics = {"rows": 0, "distinct_uei": 0, "with_active_lien": 0, "officer_confirmed": 0,
               "ca_rows": 0, "co_rows": 0}
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

        try:
            ds = lance.dataset(DATASET_URI, storage_options=so)
            committed = ds.count_rows()
            idx_names = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                         for i in ds.list_indices()}
            expect_idx = {f"{c}_idx" for c in BTREE_INDEXES + BITMAP_INDEXES}
            if not expect_idx.issubset(idx_names):
                raise RuntimeError(f"gate indices: missing {sorted(expect_idx - idx_names)}")
            if committed != metrics["rows"]:
                raise RuntimeError(f"gate rowcount: committed {committed} != materialized {metrics['rows']}")
            print(f"post-write gates PASS — committed={committed:,} indices={sorted(idx_names)}")
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
                        "dataset_uri": DATASET_URI, "distinct_uei": metrics["distinct_uei"],
                        "with_active_lien": metrics["with_active_lien"]})

    if status != "success":
        raise RuntimeError(f"sam_ucc_debtor_overlap build failed: {error}")
    return {"feed": FEED, "dataset": DATASET_URI, **metrics}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=900)
def verify_overlap() -> dict:
    import duckdb
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(DATASET_URI, storage_options=so)
    idx = sorted((i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
                 for i in ds.list_indices())
    con = duckdb.connect()
    con.register("d", ds.scanner(columns=[
        "uei", "ucc_states", "overlap_confidence", "has_active_lien", "has_tax_lien",
        "officer_confirms", "n_active_ucc_liens"]).to_reader())
    con.execute("CREATE TEMP TABLE d2 AS SELECT * FROM d")
    con.unregister("d")
    rows, d_uei = con.execute("SELECT count(*), count(DISTINCT uei) FROM d2").fetchone()
    by_conf = dict(con.execute("SELECT overlap_confidence, count(*) FROM d2 GROUP BY 1 ORDER BY 2 DESC").fetchall())
    by_state = dict(con.execute("SELECT ucc_states, count(*) FROM d2 GROUP BY 1").fetchall())
    active = con.execute("SELECT count(*) FILTER (WHERE has_active_lien), count(*) FILTER (WHERE officer_confirms), "
                         "count(*) FILTER (WHERE has_tax_lien) FROM d2").fetchone()
    con.close()
    sample = ds.scanner(columns=[
        "uei", "sam_legal_business_name", "ucc_example_name", "ucc_states", "n_ucc_financing",
        "n_active_ucc_liens", "has_active_lien", "officer_confirms", "overlap_confidence"],
        filter="overlap_confidence IN ('very_high','high') AND has_active_lien", limit=8).to_table().to_pylist()
    return {"uri": DATASET_URI, "rows": rows, "distinct_uei": d_uei, "indices": idx,
            "by_confidence": by_conf, "by_state": by_state,
            "with_active_lien": active[0], "officer_confirmed": active[1], "with_tax_lien": active[2],
            "schema": [f"{f.name}:{f.type}" for f in ds.schema], "sample": sample}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def init_ops() -> dict:
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.sam_ucc_debtor_overlap_runs"}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60, memory=65536, cpu=8.0,
)
def plan_overlap() -> dict:
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
        print(json.dumps(plan_overlap.remote(), indent=2, default=str))
        return
    print(json.dumps(build_overlap.remote(trigger_callback_url=None), indent=2, default=str))
    print(json.dumps(verify_overlap.remote(), indent=2, default=str))


# =========================================================================== #
# sam_ucc_filings — the FILING-GRAIN companion (UCC debt-layer cycle,
# operator-directed 2026-07-10). One row per (uei, ucc_state, filing_id) for
# every SAM entity the capstone resolves: filing dates (recency), lapse/
# termination state, financing vs tax/judgment class, CA lease designation,
# SECURED-PARTY names (who holds the paper), CO collateral text (what is
# encumbered). The overlap table answers "carries debt?"; this answers
# "when, from whom, against what" — and interleaves with the award event
# stream (gtm_txn_events_slim) for win-then-borrow sequencing.
# =========================================================================== #

FILINGS_DATASET_URI = os.environ.get("SAM_UCC_FILINGS_URI", "s3://data-sink/active/sam_ucc_filings/")
FILINGS_FEED = "sam_ucc_filings"
FILINGS_ROW_FLOOR = 30_000
FILINGS_BTREE = ["uei", "filing_id"]
FILINGS_BITMAP = ["ucc_state", "filing_class", "is_active_financing", "is_lease"]

CA_SECURED_URI = os.environ.get("CA_UCC_SECURED_URI", "s3://data-sink/active/ca_ucc/secured_parties/")
CO_SECURED_URI = os.environ.get("UCC_CO_SECURED_URI", "s3://data-sink/active/ucc_co_secured_parties/")
CO_COLLATERAL_URI = os.environ.get("UCC_CO_COLLATERAL_URI", "s3://data-sink/active/ucc_co_collateral/")


def build_filings_sql() -> str:
    """Filing-grain compose. Reads cu_src, cs_src, ca_deb_src, ca_fil_src,
    ca_sp_src, co_deb_src, co_txn_src, co_sp_src, co_col_src. All join keys
    are pure equalities (builder doctrine)."""
    return """
    WITH cu AS (
        SELECT ucc_debtor_key, sos_entity_key FROM cu_src WHERE is_canonical
    ),
    cs AS (
        SELECT sos_entity_key, uei FROM cs_src WHERE is_canonical AND uei IS NOT NULL
    ),
    firm AS (
        SELECT DISTINCT cu.ucc_debtor_key, cu.sos_entity_key, cs.uei
        FROM cu JOIN cs ON cs.sos_entity_key = cu.sos_entity_key
    ),
    ca_fil AS (
        SELECT ucc1_num,
               min(filing_date) AS first_filing_date,
               max(filing_date) AS last_filing_date,
               max(lapse_date)  AS lapse_date,
               max(CASE WHEN filing_type = 'UCC' THEN 1 ELSE 0 END) AS is_financing,
               max(CASE WHEN filing_type LIKE '%Tax Lien' OR filing_type = 'Judgment Lien'
                        THEN 1 ELSE 0 END) AS is_tax,
               max(CASE WHEN action_type = 'Termination' THEN 1 ELSE 0 END) AS terminated,
               max(CASE WHEN alt_designation_type IN ('Lessee', 'Lessor') THEN 1 ELSE 0 END) AS is_lease
        FROM ca_fil_src WHERE ucc1_num IS NOT NULL GROUP BY 1
    ),
    ca_sp AS (
        SELECT ucc1_num,
               count(DISTINCT org_name)                            AS n_secured_parties,
               left(string_agg(DISTINCT org_name, '; '), 500)      AS secured_parties
        FROM ca_sp_src WHERE ucc1_num IS NOT NULL AND org_name IS NOT NULL GROUP BY 1
    ),
    ca_rows AS (
        SELECT f2.uei, f2.sos_entity_key, 'CA' AS ucc_state, d.ucc1_num AS filing_id,
               fil.first_filing_date, fil.last_filing_date, fil.lapse_date,
               fil.is_financing, fil.is_tax, fil.terminated, fil.is_lease,
               sp.n_secured_parties, sp.secured_parties,
               CAST(NULL AS VARCHAR) AS collateral_text
        FROM ca_deb_src d
        JOIN firm f2
          ON f2.ucc_debtor_key = 'CA:' || d.normalized_legal_name || '|' || coalesce(d.zip_code, '')
        JOIN ca_fil fil ON fil.ucc1_num = d.ucc1_num
        LEFT JOIN ca_sp sp ON sp.ucc1_num = d.ucc1_num
        WHERE d.normalized_legal_name IS NOT NULL
    ),
    co_fil AS (
        SELECT file_id,
               min(filing_date) AS first_filing_date,
               max(filing_date) AS last_filing_date,
               max(lapse_date)  AS lapse_date,
               max(CASE WHEN filing_type IN ('ucc', 'efs') THEN 1 ELSE 0 END) AS is_financing,
               max(CASE WHEN filing_type LIKE 'lien_%' THEN 1 ELSE 0 END)     AS is_tax,
               max(CASE WHEN termination_flag THEN 1 ELSE 0 END)              AS terminated,
               0                                                              AS is_lease
        FROM co_txn_src WHERE file_id IS NOT NULL GROUP BY 1
    ),
    co_sp AS (
        SELECT file_id,
               count(DISTINCT coalesce(organization_name, party_name_normalized)) AS n_secured_parties,
               left(string_agg(DISTINCT coalesce(organization_name, party_name_normalized), '; '), 500)
                                                                                  AS secured_parties
        FROM co_sp_src
        WHERE file_id IS NOT NULL
          AND coalesce(organization_name, party_name_normalized) IS NOT NULL
        GROUP BY 1
    ),
    co_col AS (
        SELECT file_id,
               left(string_agg(DISTINCT coalesce(collateral_description_normalized,
                                                 collateral_description), ' | '), 500) AS collateral_text
        FROM co_col_src
        WHERE file_id IS NOT NULL
          AND coalesce(collateral_description_normalized, collateral_description) IS NOT NULL
        GROUP BY 1
    ),
    co_rows AS (
        SELECT f2.uei, f2.sos_entity_key, 'CO' AS ucc_state, d.file_id AS filing_id,
               fil.first_filing_date, fil.last_filing_date, fil.lapse_date,
               fil.is_financing, fil.is_tax, fil.terminated, fil.is_lease,
               sp.n_secured_parties, sp.secured_parties, col.collateral_text
        FROM co_deb_src d
        JOIN firm f2
          ON f2.ucc_debtor_key = 'CO:' || d.normalized_legal_name || '|' || coalesce(d.zip_code, '')
        JOIN co_fil fil ON fil.file_id = d.file_id
        LEFT JOIN co_sp sp ON sp.file_id = d.file_id
        LEFT JOIN co_col col ON col.file_id = d.file_id
        WHERE d.normalized_legal_name IS NOT NULL
    ),
    u AS (SELECT * FROM ca_rows UNION ALL SELECT * FROM co_rows)
    SELECT uei, ucc_state, filing_id,
           any_value(sos_entity_key)          AS sos_entity_key,
           min(first_filing_date)             AS first_filing_date,
           max(last_filing_date)              AS last_filing_date,
           max(lapse_date)                    AS lapse_date,
           CASE WHEN max(is_financing) = 1 THEN 'financing'
                WHEN max(is_tax) = 1 THEN 'tax_or_judgment'
                ELSE 'other' END              AS filing_class,
           (max(terminated) = 1)              AS terminated,
           (max(is_financing) = 1 AND max(terminated) = 0
            AND max(lapse_date) >= CURRENT_DATE) AS is_active_financing,
           (max(is_lease) = 1)                AS is_lease,
           max(n_secured_parties)             AS n_secured_parties,
           any_value(secured_parties)         AS secured_parties,
           any_value(collateral_text)         AS collateral_text
    FROM u
    GROUP BY uei, ucc_state, filing_id
    """


def _materialize_filings(con):
    import lance

    so = _r2_storage_options()

    def scan(uri, cols):
        return lance.dataset(uri, storage_options=so).scanner(columns=cols).to_reader()

    con.register("cu_src", scan(CROSSWALK_UCC_SOS_URI,
                                ["ucc_debtor_key", "sos_entity_key", "is_canonical"]))
    con.register("cs_src", scan(CROSSWALK_SOS_SAM_URI,
                                ["sos_entity_key", "uei", "is_canonical"]))
    con.register("ca_deb_src", scan(CA_DEBTORS_URI, ["normalized_legal_name", "zip_code", "ucc1_num"]))
    con.register("ca_fil_src", scan(CA_FILINGS_URI, [
        "ucc1_num", "filing_type", "filing_date", "lapse_date", "action_type",
        "alt_designation_type"]))
    con.register("ca_sp_src", scan(CA_SECURED_URI, ["ucc1_num", "org_name"]))
    con.register("co_deb_src", scan(CO_DEBTORS_URI, ["normalized_legal_name", "zip_code", "file_id"]))
    con.register("co_txn_src", scan(CO_TXN_URI, [
        "file_id", "filing_type", "filing_date", "lapse_date", "termination_flag"]))
    con.register("co_sp_src", scan(CO_SECURED_URI,
                                   ["file_id", "organization_name", "party_name_normalized"]))
    con.register("co_col_src", scan(CO_COLLATERAL_URI, [
        "file_id", "collateral_description", "collateral_description_normalized"]))
    con.execute(f"CREATE TEMP TABLE filings AS {build_filings_sql()}")
    for r in ("cu_src", "cs_src", "ca_deb_src", "ca_fil_src", "ca_sp_src",
              "co_deb_src", "co_txn_src", "co_sp_src", "co_col_src"):
        con.unregister(r)

    row = con.execute("""
        SELECT count(*)                                                     AS rows,
               count(DISTINCT uei)                                          AS distinct_uei,
               count(*) FILTER (WHERE is_active_financing)                  AS with_active_lien,
               count(*) FILTER (WHERE secured_parties IS NOT NULL)          AS officer_confirmed,
               count(*) FILTER (WHERE ucc_state = 'CA')                     AS ca_rows,
               count(*) FILTER (WHERE ucc_state = 'CO')                     AS co_rows,
               count(*) FILTER (WHERE uei IS NULL OR filing_id IS NULL)     AS orphans
        FROM filings
    """).fetchone()
    keys = ["rows", "distinct_uei", "with_active_lien", "officer_confirmed",
            "ca_rows", "co_rows", "orphans"]
    metrics = {k: int(v) for k, v in zip(keys, row)}
    table = con.sql("SELECT * FROM filings").to_arrow_table()
    return table, metrics


def assert_filings_gates(m: dict) -> list[str]:
    checks: list[str] = []

    def gate(ok: bool, label: str) -> None:
        checks.append(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            raise RuntimeError(f"PRE-WRITE GATE FAILED → {label}\n" + "\n".join(checks))

    gate(m["rows"] >= FILINGS_ROW_FLOOR, f"1 row floor: {m['rows']:,} >= {FILINGS_ROW_FLOOR:,}")
    gate(m["orphans"] == 0, f"2 no orphan keys: {m['orphans']}")
    gate(m["distinct_uei"] > 0, f"3 distinct uei: {m['distinct_uei']:,}")
    gate(m["ca_rows"] > 0 and m["co_rows"] > 0,
         f"4 both states present: CA={m['ca_rows']:,} CO={m['co_rows']:,}")
    gate(m["officer_confirmed"] > 0, f"5 secured-party coverage nonzero: {m['officer_confirmed']:,}")
    return checks


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60, memory=65536, cpu=8.0,
)
def build_filings() -> dict:
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error = "error", None
    metrics = {"rows": 0, "distinct_uei": 0, "with_active_lien": 0,
               "officer_confirmed": 0, "ca_rows": 0, "co_rows": 0}
    global FEED, DATASET_URI
    feed_prev, uri_prev = FEED, DATASET_URI
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics = _materialize_filings(con)
        finally:
            con.close()
        print(f"materialized: {metrics}")
        for line in assert_filings_gates(metrics):
            print("  ", line)

        try:
            v_before = lance.dataset(FILINGS_DATASET_URI, storage_options=so).version
        except Exception:  # noqa: BLE001
            v_before = None
        print(f"v_before = {v_before}")

        lance.write_dataset(table, FILINGS_DATASET_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        ds = lance.dataset(FILINGS_DATASET_URI, storage_options=so)
        for col in FILINGS_BTREE:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"  BTREE ✓ {col}")
        for col in FILINGS_BITMAP:
            ds.create_scalar_index(col, index_type="BITMAP")
            print(f"  BITMAP ✓ {col}")

        ds = lance.dataset(FILINGS_DATASET_URI, storage_options=so)
        committed = ds.count_rows()
        if committed != metrics["rows"]:
            if v_before is not None:
                lance.dataset(FILINGS_DATASET_URI, storage_options=so, version=v_before).restore()
            raise RuntimeError(f"post-write rowcount {committed} != {metrics['rows']}")
        print(f"post-write gates PASS — committed={committed:,}")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        # ledger reuse: same ops table, distinct feed name; the
        # officer_confirmed slot carries secured-party coverage here.
        FEED, DATASET_URI = FILINGS_FEED, FILINGS_DATASET_URI
        try:
            _record_run(metrics=metrics, status=status, error=error,
                        started_at=started_at, completed_at=completed_at)
        finally:
            FEED, DATASET_URI = feed_prev, uri_prev

    if status != "success":
        raise RuntimeError(f"sam_ucc_filings build failed: {error}")
    return {"feed": FILINGS_FEED, "dataset": FILINGS_DATASET_URI, **metrics}


@app.local_entrypoint()
def filings() -> None:
    import json

    print(json.dumps(build_filings.remote(), indent=2, default=str))


# =========================================================================== #
# sam_ucc_lenders — the LENDER-side surface (operator-directed 2026-07-10).
# Lender grain over sam_ucc_filings' secured parties, classified against the
# FDIC/NCUA name authorities + curated masks:
#   bank_or_cu | filing_agent | government_sba | non_bank
# plus in_efc (name-matched membership in equipment_finance_candidates — the
# incumbent-vs-whitespace reconciliation; that dataset is another lane's and
# is READ-ONLY here). Name-normalization is intentionally simple/deterministic
# (upper, strip punctuation + suffix tokens); the recon doc's finding stands:
# lender NATURE beyond these brackets is green-field.
# =========================================================================== #

LENDERS_DATASET_URI = os.environ.get("SAM_UCC_LENDERS_URI", "s3://data-sink/active/sam_ucc_lenders/")
LENDERS_FEED = "sam_ucc_lenders"
LENDERS_ROW_FLOOR = 5_000
FDIC_URI = os.environ.get("FDIC_INSTITUTIONS_URI", "s3://data-sink/active/fdic_institutions/")
NCUA_URI = os.environ.get("NCUA_CREDIT_UNIONS_URI", "s3://data-sink/active/ncua_credit_unions/")
EFC_URI = os.environ.get("EQUIPMENT_FINANCE_CANDIDATES_URI",
                         "s3://data-sink/active/equipment_finance_candidates/")

_LK = ("trim(regexp_replace(regexp_replace(upper({x}), '[^A-Z0-9 ]', '', 'g'), "
       "' (INC|LLC|LP|LLP|CORP|CORPORATION|CO|COMPANY|NA|NATIONAL ASSOCIATION|"
       "ASSOCIATION|LTD|THE)$', '', 'g'))")

_AGENT_RE = ("(CORPORATION SERVICE|CT CORPORATION|C T CORPORATION|LIEN SOLUTIONS|"
             "WOLTERS KLUWER|UCC DIRECT|CAPITOL SERVICES|PARACORP|INCORP|"
             "FIRST CORPORATE SOL|NATIONAL REGISTERED AGENT|AS REPRESENTATIVE)")
_BANK_RE = "(BANK|BANCORP|CREDIT UNION|FCU|SAVINGS)"
_SBA_RE = "(SMALL BUSINESS ADMIN|U S SBA|^SBA |US SBA)"


def build_lenders_sql() -> str:
    lk = _LK.format(x="lender")
    lk_fd = _LK.format(x="name")
    lk_cu = _LK.format(x="credit_union_name")
    lk_ef = _LK.format(x="company_name")
    return f"""
    WITH lenders AS (
        SELECT trim(unnest(string_split(secured_parties, '; '))) AS lender,
               uei, ucc_state, is_active_financing, first_filing_date
        FROM fil_src WHERE filing_class = 'financing' AND secured_parties IS NOT NULL
    ),
    norm AS (
        SELECT {lk} AS lender_key, lender, uei, ucc_state, is_active_financing,
               first_filing_date
        FROM lenders WHERE length(lender) > 3
    ),
    banks AS (SELECT DISTINCT {lk_fd} AS lender_key FROM fd_src),
    cus AS (SELECT DISTINCT {lk_cu} AS lender_key FROM cu_src),
    efcs AS (SELECT DISTINCT {lk_ef} AS lender_key FROM efc_src)
    SELECT n.lender_key,
           any_value(n.lender)                          AS lender_name,
           CASE
             WHEN regexp_matches(n.lender_key, '{_AGENT_RE}') THEN 'filing_agent'
             WHEN regexp_matches(n.lender_key, '{_SBA_RE}')   THEN 'government_sba'
             WHEN max(CASE WHEN b.lender_key IS NOT NULL OR c.lender_key IS NOT NULL
                           THEN 1 ELSE 0 END) = 1
                  OR regexp_matches(n.lender_key, '{_BANK_RE}') THEN 'bank_or_cu'
             ELSE 'non_bank'
           END                                          AS lender_class,
           (max(CASE WHEN e.lender_key IS NOT NULL THEN 1 ELSE 0 END) = 1)
                                                        AS in_efc,
           count(DISTINCT n.uei)                        AS sam_firms,
           count(*)                                     AS filings,
           sum(n.is_active_financing::INT)              AS active_filings,
           count(DISTINCT CASE WHEN n.ucc_state = 'CA' THEN n.uei END) AS ca_firms,
           count(DISTINCT CASE WHEN n.ucc_state = 'CO' THEN n.uei END) AS co_firms,
           min(n.first_filing_date)                     AS first_filing_date,
           max(n.first_filing_date)                     AS last_filing_date
    FROM norm n
    LEFT JOIN banks b USING (lender_key)
    LEFT JOIN cus c USING (lender_key)
    LEFT JOIN efcs e USING (lender_key)
    GROUP BY n.lender_key
    """


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=1800, memory=32768, cpu=8.0,
)
def build_lenders() -> dict:
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error = "error", None
    metrics = {"rows": 0, "distinct_uei": 0, "with_active_lien": 0,
               "officer_confirmed": 0, "ca_rows": 0, "co_rows": 0}
    global FEED, DATASET_URI
    feed_prev, uri_prev = FEED, DATASET_URI
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            def scan(uri, cols):
                return lance.dataset(uri, storage_options=so).scanner(columns=cols).to_reader()
            con.register("fil_src", scan(FILINGS_DATASET_URI, [
                "secured_parties", "uei", "ucc_state", "filing_class",
                "is_active_financing", "first_filing_date"]))
            con.register("fd_src", scan(FDIC_URI, ["name"]))
            con.register("cu_src", scan(NCUA_URI, ["credit_union_name"]))
            con.register("efc_src", scan(EFC_URI, ["company_name"]))
            con.execute(f"CREATE TEMP TABLE lend AS {build_lenders_sql()}")
            for r in ("fil_src", "fd_src", "cu_src", "efc_src"):
                con.unregister(r)
            row = con.execute("""
                SELECT count(*), count(*) FILTER (WHERE lender_class = 'non_bank'),
                       count(*) FILTER (WHERE active_filings > 0),
                       count(*) FILTER (WHERE in_efc),
                       count(*) FILTER (WHERE ca_firms > 0),
                       count(*) FILTER (WHERE co_firms > 0)
                FROM lend""").fetchone()
            metrics = dict(zip(["rows", "distinct_uei", "with_active_lien",
                                "officer_confirmed", "ca_rows", "co_rows"],
                               [int(v) for v in row]))
            # semantics of the reused slots: distinct_uei=non_bank lenders,
            # with_active_lien=lenders w/ active book, officer_confirmed=in_efc
            table = con.sql("SELECT * FROM lend").to_arrow_table()
        finally:
            con.close()
        print(f"materialized: {metrics}")
        if metrics["rows"] < LENDERS_ROW_FLOOR:
            raise RuntimeError(f"row floor: {metrics['rows']} < {LENDERS_ROW_FLOOR}")
        if metrics["officer_confirmed"] == 0:
            raise RuntimeError("efc reconciliation matched zero candidates — check name norm")
        if not (metrics["ca_rows"] > 0 and metrics["co_rows"] > 0):
            raise RuntimeError("both states must be present")

        try:
            v_before = lance.dataset(LENDERS_DATASET_URI, storage_options=so).version
        except Exception:  # noqa: BLE001
            v_before = None
        lance.write_dataset(table, LENDERS_DATASET_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        ds = lance.dataset(LENDERS_DATASET_URI, storage_options=so)
        for col in ("lender_key", "lender_name"):
            ds.create_scalar_index(col, index_type="BTREE")
        for col in ("lender_class", "in_efc"):
            ds.create_scalar_index(col, index_type="BITMAP")
        ds = lance.dataset(LENDERS_DATASET_URI, storage_options=so)
        if ds.count_rows() != metrics["rows"]:
            if v_before is not None:
                lance.dataset(LENDERS_DATASET_URI, storage_options=so, version=v_before).restore()
            raise RuntimeError("post-write rowcount mismatch")
        print(f"post-write gates PASS — {metrics['rows']:,} lenders")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        FEED, DATASET_URI = LENDERS_FEED, LENDERS_DATASET_URI
        try:
            _record_run(metrics=metrics, status=status, error=error,
                        started_at=started_at, completed_at=completed_at)
        finally:
            FEED, DATASET_URI = feed_prev, uri_prev

    if status != "success":
        raise RuntimeError(f"sam_ucc_lenders build failed: {error}")
    return {"feed": LENDERS_FEED, "dataset": LENDERS_DATASET_URI, **metrics}


@app.local_entrypoint()
def lenders() -> None:
    import json

    print(json.dumps(build_lenders.remote(), indent=2, default=str))


# =========================================================================== #
# ucc_filings_all — the FULL-CORPUS filing surface (operator-directed
# 2026-07-16). The sam_ucc_* pair is the SAM INTERSECTION by construction
# (crosswalks inner-joined at the source); this lifts that constraint:
# one row per (ucc_state, filing_id, debtor_key) over EVERY CA/CO UCC
# debtor — org AND individual — with SAM identity attached as a NULLABLE
# enrichment (uei/sos_entity_key via LEFT JOIN through the same canonical
# crosswalks). Same filing attributes as sam_ucc_filings (dates, lapse,
# class, termination, lease, secured parties, CO collateral) plus debtor
# identity/geo (name, normalized name, city, state, zip, is_org).
# =========================================================================== #

FILINGS_ALL_DATASET_URI = os.environ.get(
    "UCC_FILINGS_ALL_URI", "s3://data-sink/active/ucc_filings_all/")
FILINGS_ALL_FEED = "ucc_filings_all"
FILINGS_ALL_ROW_FLOOR = 400_000     # strict superset of sam_ucc_filings (376,451)
FILINGS_ALL_BTREE = ["debtor_key", "filing_id", "uei", "debtor_name_norm"]
FILINGS_ALL_BITMAP = ["ucc_state", "filing_class", "is_active_financing",
                      "is_lease", "is_org", "in_sam"]


def build_filings_all_sql() -> str:
    """Full-corpus filing-grain compose. Reads cu_src, cs_src, ca_deb_src,
    ca_fil_src, ca_sp_src, co_deb_src, co_txn_src, co_sp_src, co_col_src.
    debtor_key is CASE-derived in a CTE so every join stays a pure equality
    (builder doctrine); firm attaches via LEFT JOIN — uei is nullable."""
    return """
    WITH cu AS (
        SELECT ucc_debtor_key, sos_entity_key FROM cu_src WHERE is_canonical
    ),
    cs AS (
        SELECT sos_entity_key, uei FROM cs_src WHERE is_canonical AND uei IS NOT NULL
    ),
    firm AS (
        SELECT DISTINCT cu.ucc_debtor_key, cu.sos_entity_key, cs.uei
        FROM cu JOIN cs ON cs.sos_entity_key = cu.sos_entity_key
    ),
    ca_fil AS (
        SELECT ucc1_num,
               min(filing_date) AS first_filing_date,
               max(filing_date) AS last_filing_date,
               max(lapse_date)  AS lapse_date,
               max(CASE WHEN filing_type = 'UCC' THEN 1 ELSE 0 END) AS is_financing,
               max(CASE WHEN filing_type LIKE '%Tax Lien' OR filing_type = 'Judgment Lien'
                        THEN 1 ELSE 0 END) AS is_tax,
               max(CASE WHEN action_type = 'Termination' THEN 1 ELSE 0 END) AS terminated,
               max(CASE WHEN alt_designation_type IN ('Lessee', 'Lessor') THEN 1 ELSE 0 END) AS is_lease
        FROM ca_fil_src WHERE ucc1_num IS NOT NULL GROUP BY 1
    ),
    ca_sp AS (
        SELECT ucc1_num,
               count(DISTINCT org_name)                            AS n_secured_parties,
               left(string_agg(DISTINCT org_name, '; '), 500)      AS secured_parties
        FROM ca_sp_src WHERE ucc1_num IS NOT NULL AND org_name IS NOT NULL GROUP BY 1
    ),
    ca_deb AS (
        SELECT ucc1_num,
               CASE WHEN normalized_legal_name IS NOT NULL
                    THEN 'CA:' || normalized_legal_name || '|' || coalesce(zip_code, '')
                    ELSE 'CA:IND:' || upper(trim(coalesce(last_name, '') || ' ' || coalesce(first_name, '')))
                         || '|' || coalesce(zip_code, '')
               END AS debtor_key,
               (org_name IS NOT NULL) AS is_org,
               coalesce(org_name,
                        trim(coalesce(last_name, '') || ', ' || coalesce(first_name, ''))) AS debtor_name,
               normalized_legal_name AS debtor_name_norm,
               city AS debtor_city, state AS debtor_state,
               coalesce(zip_code, left(postal_code, 5)) AS debtor_zip
        FROM ca_deb_src
        WHERE ucc1_num IS NOT NULL
          AND (normalized_legal_name IS NOT NULL OR last_name IS NOT NULL)
    ),
    ca_rows AS (
        SELECT f2.uei, f2.sos_entity_key, 'CA' AS ucc_state, d.ucc1_num AS filing_id,
               d.debtor_key, d.is_org, d.debtor_name, d.debtor_name_norm,
               d.debtor_city, d.debtor_state, d.debtor_zip,
               fil.first_filing_date, fil.last_filing_date, fil.lapse_date,
               fil.is_financing, fil.is_tax, fil.terminated, fil.is_lease,
               sp.n_secured_parties, sp.secured_parties,
               CAST(NULL AS VARCHAR) AS collateral_text
        FROM ca_deb d
        JOIN ca_fil fil ON fil.ucc1_num = d.ucc1_num
        LEFT JOIN ca_sp sp ON sp.ucc1_num = d.ucc1_num
        LEFT JOIN firm f2 ON f2.ucc_debtor_key = d.debtor_key
    ),
    co_fil AS (
        SELECT file_id,
               min(filing_date) AS first_filing_date,
               max(filing_date) AS last_filing_date,
               max(lapse_date)  AS lapse_date,
               max(CASE WHEN filing_type IN ('ucc', 'efs') THEN 1 ELSE 0 END) AS is_financing,
               max(CASE WHEN filing_type LIKE 'lien_%' THEN 1 ELSE 0 END)     AS is_tax,
               max(CASE WHEN termination_flag THEN 1 ELSE 0 END)              AS terminated,
               0                                                              AS is_lease
        FROM co_txn_src WHERE file_id IS NOT NULL GROUP BY 1
    ),
    co_sp AS (
        SELECT file_id,
               count(DISTINCT coalesce(organization_name, party_name_normalized)) AS n_secured_parties,
               left(string_agg(DISTINCT coalesce(organization_name, party_name_normalized), '; '), 500)
                                                                                  AS secured_parties
        FROM co_sp_src
        WHERE file_id IS NOT NULL
          AND coalesce(organization_name, party_name_normalized) IS NOT NULL
        GROUP BY 1
    ),
    co_col AS (
        SELECT file_id,
               left(string_agg(DISTINCT coalesce(collateral_description_normalized,
                                                 collateral_description), ' | '), 500) AS collateral_text
        FROM co_col_src
        WHERE file_id IS NOT NULL
          AND coalesce(collateral_description_normalized, collateral_description) IS NOT NULL
        GROUP BY 1
    ),
    co_deb AS (
        SELECT file_id,
               CASE WHEN normalized_legal_name IS NOT NULL
                    THEN 'CO:' || normalized_legal_name || '|' || coalesce(zip_code, '')
                    ELSE 'CO:IND:' || upper(trim(coalesce(last_name, '') || ' ' || coalesce(first_name, '')))
                         || '|' || coalesce(zip_code, '')
               END AS debtor_key,
               (organization_name IS NOT NULL) AS is_org,
               coalesce(organization_name,
                        trim(coalesce(last_name, '') || ', ' || coalesce(first_name, ''))) AS debtor_name,
               normalized_legal_name AS debtor_name_norm,
               city AS debtor_city, state AS debtor_state,
               coalesce(zip_code, left(zipcode, 5)) AS debtor_zip
        FROM co_deb_src
        WHERE file_id IS NOT NULL
          AND (normalized_legal_name IS NOT NULL OR last_name IS NOT NULL)
    ),
    co_rows AS (
        SELECT f2.uei, f2.sos_entity_key, 'CO' AS ucc_state, d.file_id AS filing_id,
               d.debtor_key, d.is_org, d.debtor_name, d.debtor_name_norm,
               d.debtor_city, d.debtor_state, d.debtor_zip,
               fil.first_filing_date, fil.last_filing_date, fil.lapse_date,
               fil.is_financing, fil.is_tax, fil.terminated, fil.is_lease,
               sp.n_secured_parties, sp.secured_parties, col.collateral_text
        FROM co_deb d
        JOIN co_fil fil ON fil.file_id = d.file_id
        LEFT JOIN co_sp sp ON sp.file_id = d.file_id
        LEFT JOIN co_col col ON col.file_id = d.file_id
        LEFT JOIN firm f2 ON f2.ucc_debtor_key = d.debtor_key
    ),
    u AS (SELECT * FROM ca_rows UNION ALL SELECT * FROM co_rows)
    SELECT ucc_state, filing_id, debtor_key,
           any_value(uei)                     AS uei,
           any_value(sos_entity_key)          AS sos_entity_key,
           (max(uei) IS NOT NULL)             AS in_sam,
           bool_or(is_org)                    AS is_org,
           any_value(debtor_name)             AS debtor_name,
           any_value(debtor_name_norm)        AS debtor_name_norm,
           any_value(debtor_city)             AS debtor_city,
           any_value(debtor_state)            AS debtor_state,
           any_value(debtor_zip)              AS debtor_zip,
           min(first_filing_date)             AS first_filing_date,
           max(last_filing_date)              AS last_filing_date,
           max(lapse_date)                    AS lapse_date,
           CASE WHEN max(is_financing) = 1 THEN 'financing'
                WHEN max(is_tax) = 1 THEN 'tax_or_judgment'
                ELSE 'other' END              AS filing_class,
           (max(terminated) = 1)              AS terminated,
           (max(is_financing) = 1 AND max(terminated) = 0
            AND max(lapse_date) >= CURRENT_DATE) AS is_active_financing,
           (max(is_lease) = 1)                AS is_lease,
           max(n_secured_parties)             AS n_secured_parties,
           any_value(secured_parties)         AS secured_parties,
           any_value(collateral_text)         AS collateral_text
    FROM u
    GROUP BY ucc_state, filing_id, debtor_key
    """


def _materialize_filings_all(con):
    import lance

    so = _r2_storage_options()

    def scan(uri, cols):
        return lance.dataset(uri, storage_options=so).scanner(columns=cols).to_reader()

    con.register("cu_src", scan(CROSSWALK_UCC_SOS_URI,
                                ["ucc_debtor_key", "sos_entity_key", "is_canonical"]))
    con.register("cs_src", scan(CROSSWALK_SOS_SAM_URI,
                                ["sos_entity_key", "uei", "is_canonical"]))
    con.register("ca_deb_src", scan(CA_DEBTORS_URI, [
        "ucc1_num", "org_name", "last_name", "first_name", "city", "state",
        "postal_code", "normalized_legal_name", "zip_code"]))
    con.register("ca_fil_src", scan(CA_FILINGS_URI, [
        "ucc1_num", "filing_type", "filing_date", "lapse_date", "action_type",
        "alt_designation_type"]))
    con.register("ca_sp_src", scan(CA_SECURED_URI, ["ucc1_num", "org_name"]))
    con.register("co_deb_src", scan(CO_DEBTORS_URI, [
        "file_id", "organization_name", "last_name", "first_name", "city",
        "state", "zipcode", "normalized_legal_name", "zip_code"]))
    con.register("co_txn_src", scan(CO_TXN_URI, [
        "file_id", "filing_type", "filing_date", "lapse_date", "termination_flag"]))
    con.register("co_sp_src", scan(CO_SECURED_URI,
                                   ["file_id", "organization_name", "party_name_normalized"]))
    con.register("co_col_src", scan(CO_COLLATERAL_URI, [
        "file_id", "collateral_description", "collateral_description_normalized"]))
    con.execute(f"CREATE TEMP TABLE filings_all AS {build_filings_all_sql()}")
    for r in ("cu_src", "cs_src", "ca_deb_src", "ca_fil_src", "ca_sp_src",
              "co_deb_src", "co_txn_src", "co_sp_src", "co_col_src"):
        con.unregister(r)

    row = con.execute("""
        SELECT count(*)                                                     AS rows,
               count(DISTINCT uei)                                          AS distinct_uei,
               count(*) FILTER (WHERE is_active_financing)                  AS with_active_lien,
               count(*) FILTER (WHERE secured_parties IS NOT NULL)          AS officer_confirmed,
               count(*) FILTER (WHERE ucc_state = 'CA')                     AS ca_rows,
               count(*) FILTER (WHERE ucc_state = 'CO')                     AS co_rows,
               count(*) FILTER (WHERE filing_id IS NULL OR debtor_key IS NULL) AS orphans
        FROM filings_all
    """).fetchone()
    keys = ["rows", "distinct_uei", "with_active_lien", "officer_confirmed",
            "ca_rows", "co_rows", "orphans"]
    metrics = {k: int(v) for k, v in zip(keys, row)}
    table = con.sql("SELECT * FROM filings_all").to_arrow_table()
    return table, metrics


def assert_filings_all_gates(m: dict) -> list[str]:
    checks: list[str] = []

    def gate(ok: bool, label: str) -> None:
        checks.append(f"{'PASS' if ok else 'FAIL'}  {label}")
        if not ok:
            raise RuntimeError(f"PRE-WRITE GATE FAILED → {label}\n" + "\n".join(checks))

    gate(m["rows"] >= FILINGS_ALL_ROW_FLOOR,
         f"1 row floor: {m['rows']:,} >= {FILINGS_ALL_ROW_FLOOR:,}")
    gate(m["orphans"] == 0, f"2 no orphan keys: {m['orphans']}")
    gate(m["distinct_uei"] > 0, f"3 SAM enrichment nonzero: {m['distinct_uei']:,}")
    gate(m["ca_rows"] > 0 and m["co_rows"] > 0,
         f"4 both states present: CA={m['ca_rows']:,} CO={m['co_rows']:,}")
    gate(m["officer_confirmed"] > 0,
         f"5 secured-party coverage nonzero: {m['officer_confirmed']:,}")
    return checks


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60, memory=65536, cpu=8.0,
)
def build_filings_all() -> dict:
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error = "error", None
    metrics = {"rows": 0, "distinct_uei": 0, "with_active_lien": 0,
               "officer_confirmed": 0, "ca_rows": 0, "co_rows": 0}
    global FEED, DATASET_URI
    feed_prev, uri_prev = FEED, DATASET_URI
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            table, metrics = _materialize_filings_all(con)
        finally:
            con.close()
        print(f"materialized: {metrics}")
        for line in assert_filings_all_gates(metrics):
            print("  ", line)

        try:
            v_before = lance.dataset(FILINGS_ALL_DATASET_URI, storage_options=so).version
        except Exception:  # noqa: BLE001
            v_before = None
        print(f"v_before = {v_before}")

        lance.write_dataset(table, FILINGS_ALL_DATASET_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        ds = lance.dataset(FILINGS_ALL_DATASET_URI, storage_options=so)
        for col in FILINGS_ALL_BTREE:
            ds.create_scalar_index(col, index_type="BTREE")
            print(f"  BTREE ✓ {col}")
        for col in FILINGS_ALL_BITMAP:
            ds.create_scalar_index(col, index_type="BITMAP")
            print(f"  BITMAP ✓ {col}")

        ds = lance.dataset(FILINGS_ALL_DATASET_URI, storage_options=so)
        committed = ds.count_rows()
        if committed != metrics["rows"]:
            if v_before is not None:
                lance.dataset(FILINGS_ALL_DATASET_URI, storage_options=so,
                              version=v_before).restore()
            raise RuntimeError(f"post-write rowcount {committed} != {metrics['rows']}")
        print(f"post-write gates PASS — committed={committed:,}")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        FEED, DATASET_URI = FILINGS_ALL_FEED, FILINGS_ALL_DATASET_URI
        try:
            _record_run(metrics=metrics, status=status, error=error,
                        started_at=started_at, completed_at=completed_at)
        finally:
            FEED, DATASET_URI = feed_prev, uri_prev

    if status != "success":
        raise RuntimeError(f"ucc_filings_all build failed: {error}")
    return {"feed": FILINGS_ALL_FEED, "dataset": FILINGS_ALL_DATASET_URI, **metrics}


@app.local_entrypoint()
def filings_all() -> None:
    import json

    print(json.dumps(build_filings_all.remote(), indent=2, default=str))


# =========================================================================== #
# ucc_lenders_all — the FULL-CORPUS lender surface (operator-directed
# 2026-07-16). Same classification brackets as sam_ucc_lenders (FDIC/NCUA
# authorities + curated masks + in_efc reconciliation) but computed over
# ucc_filings_all: total_firms counts EVERY distinct debtor (org grain,
# SAM or not); sam_firms is the SAM-registered subset — the ratio is the
# lender's federal-exposure share, previously invisible.
# =========================================================================== #

LENDERS_ALL_DATASET_URI = os.environ.get(
    "UCC_LENDERS_ALL_URI", "s3://data-sink/active/ucc_lenders_all/")
LENDERS_ALL_FEED = "ucc_lenders_all"
LENDERS_ALL_ROW_FLOOR = 25_000      # superset of sam_ucc_lenders (21,686)


def build_lenders_all_sql() -> str:
    lk = _LK.format(x="lender")
    lk_fd = _LK.format(x="name")
    lk_cu = _LK.format(x="credit_union_name")
    lk_ef = _LK.format(x="company_name")
    return f"""
    WITH lenders AS (
        SELECT trim(unnest(string_split(secured_parties, '; '))) AS lender,
               debtor_key, uei, ucc_state, is_active_financing, first_filing_date
        FROM fil_src WHERE filing_class = 'financing' AND secured_parties IS NOT NULL
    ),
    norm AS (
        SELECT {lk} AS lender_key, lender, debtor_key, uei, ucc_state,
               is_active_financing, first_filing_date
        FROM lenders WHERE length(lender) > 3
    ),
    banks AS (SELECT DISTINCT {lk_fd} AS lender_key FROM fd_src),
    cus AS (SELECT DISTINCT {lk_cu} AS lender_key FROM cu_src),
    efcs AS (SELECT DISTINCT {lk_ef} AS lender_key FROM efc_src)
    SELECT n.lender_key,
           any_value(n.lender)                          AS lender_name,
           CASE
             WHEN regexp_matches(n.lender_key, '{_AGENT_RE}') THEN 'filing_agent'
             WHEN regexp_matches(n.lender_key, '{_SBA_RE}')   THEN 'government_sba'
             WHEN max(CASE WHEN b.lender_key IS NOT NULL OR c.lender_key IS NOT NULL
                           THEN 1 ELSE 0 END) = 1
                  OR regexp_matches(n.lender_key, '{_BANK_RE}') THEN 'bank_or_cu'
             ELSE 'non_bank'
           END                                          AS lender_class,
           (max(CASE WHEN e.lender_key IS NOT NULL THEN 1 ELSE 0 END) = 1)
                                                        AS in_efc,
           count(DISTINCT n.debtor_key)                 AS total_firms,
           count(DISTINCT n.uei)                        AS sam_firms,
           count(*)                                     AS filings,
           sum(n.is_active_financing::INT)              AS active_filings,
           count(DISTINCT CASE WHEN n.ucc_state = 'CA' THEN n.debtor_key END) AS ca_firms,
           count(DISTINCT CASE WHEN n.ucc_state = 'CO' THEN n.debtor_key END) AS co_firms,
           min(n.first_filing_date)                     AS first_filing_date,
           max(n.first_filing_date)                     AS last_filing_date
    FROM norm n
    LEFT JOIN banks b USING (lender_key)
    LEFT JOIN cus c USING (lender_key)
    LEFT JOIN efcs e USING (lender_key)
    GROUP BY n.lender_key
    """


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=1800, memory=32768, cpu=8.0,
)
def build_lenders_all() -> dict:
    import datetime as dt

    import lance

    started_at = dt.datetime.now(dt.timezone.utc)
    so = _r2_storage_options()
    status, error = "error", None
    metrics = {"rows": 0, "distinct_uei": 0, "with_active_lien": 0,
               "officer_confirmed": 0, "ca_rows": 0, "co_rows": 0}
    global FEED, DATASET_URI
    feed_prev, uri_prev = FEED, DATASET_URI
    try:
        os.makedirs(SPILL_DIR, exist_ok=True)
        con = _new_con()
        try:
            def scan(uri, cols):
                return lance.dataset(uri, storage_options=so).scanner(columns=cols).to_reader()
            con.register("fil_src", scan(FILINGS_ALL_DATASET_URI, [
                "secured_parties", "debtor_key", "uei", "ucc_state",
                "filing_class", "is_active_financing", "first_filing_date"]))
            con.register("fd_src", scan(FDIC_URI, ["name"]))
            con.register("cu_src", scan(NCUA_URI, ["credit_union_name"]))
            con.register("efc_src", scan(EFC_URI, ["company_name"]))
            con.execute(f"CREATE TEMP TABLE lend_all AS {build_lenders_all_sql()}")
            for r in ("fil_src", "fd_src", "cu_src", "efc_src"):
                con.unregister(r)
            row = con.execute("""
                SELECT count(*), count(*) FILTER (WHERE lender_class = 'non_bank'),
                       count(*) FILTER (WHERE active_filings > 0),
                       count(*) FILTER (WHERE in_efc),
                       count(*) FILTER (WHERE ca_firms > 0),
                       count(*) FILTER (WHERE co_firms > 0)
                FROM lend_all""").fetchone()
            metrics = dict(zip(["rows", "distinct_uei", "with_active_lien",
                                "officer_confirmed", "ca_rows", "co_rows"],
                               [int(v) for v in row]))
            # semantics of the reused slots: distinct_uei=non_bank lenders,
            # with_active_lien=lenders w/ active book, officer_confirmed=in_efc
            table = con.sql("SELECT * FROM lend_all").to_arrow_table()
        finally:
            con.close()
        print(f"materialized: {metrics}")
        if metrics["rows"] < LENDERS_ALL_ROW_FLOOR:
            raise RuntimeError(f"row floor: {metrics['rows']} < {LENDERS_ALL_ROW_FLOOR}")
        if metrics["officer_confirmed"] == 0:
            raise RuntimeError("efc reconciliation matched zero candidates — check name norm")
        if not (metrics["ca_rows"] > 0 and metrics["co_rows"] > 0):
            raise RuntimeError("both states must be present")

        try:
            v_before = lance.dataset(LENDERS_ALL_DATASET_URI, storage_options=so).version
        except Exception:  # noqa: BLE001
            v_before = None
        lance.write_dataset(table, LENDERS_ALL_DATASET_URI, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, storage_options=so)
        ds = lance.dataset(LENDERS_ALL_DATASET_URI, storage_options=so)
        for col in ("lender_key", "lender_name"):
            ds.create_scalar_index(col, index_type="BTREE")
        for col in ("lender_class", "in_efc"):
            ds.create_scalar_index(col, index_type="BITMAP")
        ds = lance.dataset(LENDERS_ALL_DATASET_URI, storage_options=so)
        if ds.count_rows() != metrics["rows"]:
            if v_before is not None:
                lance.dataset(LENDERS_ALL_DATASET_URI, storage_options=so,
                              version=v_before).restore()
            raise RuntimeError("post-write rowcount mismatch")
        print(f"post-write gates PASS — {metrics['rows']:,} lenders")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        FEED, DATASET_URI = LENDERS_ALL_FEED, LENDERS_ALL_DATASET_URI
        try:
            _record_run(metrics=metrics, status=status, error=error,
                        started_at=started_at, completed_at=completed_at)
        finally:
            FEED, DATASET_URI = feed_prev, uri_prev

    if status != "success":
        raise RuntimeError(f"ucc_lenders_all build failed: {error}")
    return {"feed": LENDERS_ALL_FEED, "dataset": LENDERS_ALL_DATASET_URI, **metrics}


@app.local_entrypoint()
def lenders_all() -> None:
    import json

    print(json.dumps(build_lenders_all.remote(), indent=2, default=str))
