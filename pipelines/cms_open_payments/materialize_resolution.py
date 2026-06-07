"""Compute worker — CMS Open Payments resolution layer (provider rollup + manufacturer dim).

Part of the ``cms-open-payments-resolution`` Modal app. Endpoint-less; driven by the local
entrypoints or a scheduler. Clean-room data plane: DuckDB does 100% of the transform out-of-core,
Lance is the system of record on R2. This is a DERIVE step (reads committed Lance, writes new
Lance) — it never re-ingests the source CSVs.

WHY. The NPI resolution spine (NPPES ⨝ CMS) is already fully BTREE-indexed on both sides, but the
cross-table *signal* is not materialized: answering "total industry money to this provider across
general + research + ownership, from how many distinct manufacturers, over what year span" requires
scanning 82M + 5.9M + 27k payment rows live. This worker materializes that signal ONCE into two
lean, append-only, composable artifacts that ride the existing ``npi`` / manufacturer-id BTREEs:

  active/cms_provider_payment_rollup/   1 row / payment-active NPI (~1.6M)   — the sellable provider asset
  active/cms_manufacturer_dim/          1 row / manufacturer-GPO id (~few k) — name resolution + reach + spend

Neither mutates the source datasets — blast radius is two NET-NEW prefixes under active/.

GRAIN & JOINS.
  cms_provider_payment_rollup.npi      ── BTREE ──>  nppes_provider.npi (npi-clustered dim)
  cms_manufacturer_dim.manufacturer_id ── BTREE ──>  cms_*.applicable_manufacturer..._id
The rollup is the FACT/AGG that hangs off the NPPES provider DIM on the existing npi index — it does
NOT duplicate the 9.5M-row provider master, only the ~1.6M NPIs with Open Payments activity (plus
research-only principal investigators).

OUT-OF-CORE. Each source is scanned from R2 exactly ONCE into a DuckDB temp table (cast narrow:
amount→DOUBLE, year→INT), then every aggregate reads the local temp (re-scannable) instead of
re-hitting R2. ``memory_limit`` + ``temp_directory`` on the Modal ephemeral disk bound RSS; the
82M-row general table is the only heavy scan and spills cleanly.

DETERMINISM (idempotency). Name/state/country resolution per id uses MAX(non-null) — deterministic
across reruns — NOT any_value()/mode() (non-deterministic / version-fragile). A rerun reproduces
byte-identical aggregates, so the wipe-and-republish is safe to retry.

WRITE PATH (the nppes / sam_gov lesson). Aggregates are written to a LOCAL Lance stage, indexed
locally, then boto3-published to R2 (uniform-part multipart) — never a direct Lance write to R2
(the multipart-InvalidPart rule on large index pages). Lance is still the format, R2 still the SoR.

    modal run pipelines/cms_open_payments/materialize_resolution.py::init_state
    modal run pipelines/cms_open_payments/materialize_resolution.py::materialize_cli
    modal run pipelines/cms_open_payments/materialize_resolution.py::verify
    modal run pipelines/cms_open_payments/materialize_resolution.py::show_ledger
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
FEED = "cms_open_payments_resolution"

# Source datasets (committed Lance, system of record).
GENERAL_URI = os.environ.get("CMS_GENERAL_LANCE_URI", "s3://data-sink/active/cms_general_payments/")
RESEARCH_URI = os.environ.get("CMS_RESEARCH_LANCE_URI", "s3://data-sink/active/cms_research_payments/")
OWNERSHIP_URI = os.environ.get("CMS_OWNERSHIP_LANCE_URI", "s3://data-sink/active/cms_ownership/")

# Derived artifacts (net-new prefixes under active/). Leaf datasets, overwrite-on-rerun
# (fully rebuildable single-writer derive — mirrors the cms_* leaf convention, not snapshot-partitioned).
ROLLUP_PREFIX = os.environ.get("CMS_ROLLUP_PREFIX", "active/cms_provider_payment_rollup").strip("/")
MFG_PREFIX = os.environ.get("CMS_MFG_DIM_PREFIX", "active/cms_manufacturer_dim").strip("/")

SCRATCH_DIR = "/tmp/cms_resolution"
SPILL_DIR = os.path.join(SCRATCH_DIR, "duckdb_spill")

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1048576
MAX_BYTES_PER_FILE = 90 * 1024**3
DUCKDB_MEMORY_LIMIT = os.environ.get("CMS_RES_MEM_LIMIT", "24GB")

# Index plan. BTREE = high-card resolution keys; BITMAP = low/medium-card categoricals.
ROLLUP_INDEX = {
    "btree": ["npi"],
    "bitmap": ["recipient_type", "has_ownership_interest", "last_payment_year"],
}
MFG_INDEX = {
    "btree": ["manufacturer_id", "manufacturer_name"],
    "bitmap": ["manufacturer_state"],
}

# Source column projections (minimal — only what the aggregates need).
_MFG_COLS = [
    "applicable_manufacturer_or_applicable_gpo_making_payment_id",
    "applicable_manufacturer_or_applicable_gpo_making_payment_name",
    "applicable_manufacturer_or_applicable_gpo_making_payment_state",
    "applicable_manufacturer_or_applicable_gpo_making_payment_country",
]
GEN_COLS = ["covered_recipient_npi", "covered_recipient_type",
            "total_amount_of_payment_usdollars", "payment_year"] + _MFG_COLS
RES_COLS = (["covered_recipient_npi", "covered_recipient_type",
             "total_amount_of_payment_usdollars", "payment_year"]
            + [f"principal_investigator_{i}_npi" for i in range(1, 6)] + _MFG_COLS)
OWN_COLS = ["physician_npi", "total_amount_invested_usdollars", "value_of_interest",
            "payment_year"] + _MFG_COLS

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.cms_resolution_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,
    artifact       text        NOT NULL,
    dataset_uri    text,
    rows_written   bigint,
    indices        text,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS cms_resolution_runs_feed_idx     ON ops.cms_resolution_runs (feed);
CREATE INDEX IF NOT EXISTS cms_resolution_runs_artifact_idx ON ops.cms_resolution_runs (artifact);
CREATE INDEX IF NOT EXISTS cms_resolution_runs_status_idx   ON ops.cms_resolution_runs (status);
CREATE INDEX IF NOT EXISTS cms_resolution_runs_recorded_idx ON ops.cms_resolution_runs (recorded_at DESC);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "boto3>=1.35",
    "psycopg[binary]>=3.2",
).env({"LANCE_BYPASS_SPILLING": "true"})

app = modal.App("cms-open-payments-resolution", image=image)


# --------------------------------------------------------------------------- #
# R2 / object-store helpers (verbatim fleet pattern)
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


def _s3_client():
    import boto3
    from botocore.config import Config

    so = _r2_storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client(
        "s3", endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto", config=cfg,
    )


def _replace_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Idempotent publish: wipe the artifact's R2 prefix, then upload the local Lance dataset
    (boto3/s3transfer = uniform-part multipart, R2-compliant). Returns files uploaded."""
    to_del: list[dict] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            to_del.append({"Key": o["Key"]})
            if len(to_del) == 1000:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})
                to_del = []
    if to_del:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})

    uploaded = 0
    for root, _, files in os.walk(local_dir):
        for fn in files:
            lp = os.path.join(root, fn)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            s3.upload_file(lp, BUCKET, prefix.rstrip("/") + "/" + rel)
            uploaded += 1
    return uploaded


# --------------------------------------------------------------------------- #
# Lance index + write
# --------------------------------------------------------------------------- #
def _create_indexes(local_path: str, btree: list[str], bitmap: list[str]) -> list[str]:
    import lance

    ds = lance.dataset(local_path)
    built: list[str] = []
    for col in btree:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in bitmap:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    return built


def _write_local(con, sql: str, local_path: str) -> int:
    """Execute the assembly SQL → Arrow reader → local Lance (overwrite). Returns rows."""
    import lance
    import shutil

    shutil.rmtree(local_path, ignore_errors=True)
    reader = con.execute(sql).to_arrow_reader(65536)
    lance.write_dataset(
        reader, local_path, schema=reader.schema, mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
    )
    return lance.dataset(local_path).count_rows()


# --------------------------------------------------------------------------- #
# Core build — scan sources once, assemble both artifacts
# --------------------------------------------------------------------------- #
def _load_sources(con) -> None:
    """Scan each source from R2 exactly once into a narrow, re-scannable DuckDB temp table."""
    import lance

    so = _r2_storage_options()
    gen = lance.dataset(GENERAL_URI, storage_options=so)
    res = lance.dataset(RESEARCH_URI, storage_options=so)
    own = lance.dataset(OWNERSHIP_URI, storage_options=so)

    m = "applicable_manufacturer_or_applicable_gpo_making_payment_"
    mfg_proj = (f"nullif(trim({m}id),'') AS mfg_id, "
                f"nullif(trim({m}name),'') AS mfg_name, "
                f"nullif(trim({m}state),'') AS mfg_state, "
                f"nullif(trim({m}country),'') AS mfg_country")

    con.register("gen_r", gen.scanner(columns=GEN_COLS).to_reader())
    con.execute(f"""CREATE TEMP TABLE g AS SELECT
        nullif(trim(covered_recipient_npi),'') AS npi,
        nullif(trim(covered_recipient_type),'') AS rtype,
        TRY_CAST(total_amount_of_payment_usdollars AS DOUBLE) AS amt,
        TRY_CAST(payment_year AS INTEGER) AS yr,
        {mfg_proj}
        FROM gen_r""")
    con.unregister("gen_r")
    print(f"  loaded general:   {con.execute('SELECT count(*) FROM g').fetchone()[0]:,} rows")

    pi_proj = ", ".join(f"nullif(trim(principal_investigator_{i}_npi),'') AS pi{i}" for i in range(1, 6))
    con.register("res_r", res.scanner(columns=RES_COLS).to_reader())
    con.execute(f"""CREATE TEMP TABLE r AS SELECT
        nullif(trim(covered_recipient_npi),'') AS npi,
        nullif(trim(covered_recipient_type),'') AS rtype,
        TRY_CAST(total_amount_of_payment_usdollars AS DOUBLE) AS amt,
        TRY_CAST(payment_year AS INTEGER) AS yr,
        {pi_proj},
        {mfg_proj}
        FROM res_r""")
    con.unregister("res_r")
    print(f"  loaded research:  {con.execute('SELECT count(*) FROM r').fetchone()[0]:,} rows")

    con.register("own_r", own.scanner(columns=OWN_COLS).to_reader())
    con.execute(f"""CREATE TEMP TABLE o AS SELECT
        nullif(trim(physician_npi),'') AS npi,
        TRY_CAST(total_amount_invested_usdollars AS DOUBLE) AS invested,
        TRY_CAST(value_of_interest AS DOUBLE) AS interest_val,
        TRY_CAST(payment_year AS INTEGER) AS yr,
        {mfg_proj}
        FROM own_r""")
    con.unregister("own_r")
    print(f"  loaded ownership: {con.execute('SELECT count(*) FROM o').fetchone()[0]:,} rows")


# Provider payment rollup — 1 row / payment-active NPI (incl. research-only PIs).
ROLLUP_SQL = """
WITH gen_agg AS (
    SELECT npi, count(*) gcnt, sum(amt) gusd, max(rtype) rtype,
           min(yr) gy0, max(yr) gy1
    FROM g WHERE npi IS NOT NULL GROUP BY npi),
res_agg AS (
    SELECT npi, count(*) rcnt, sum(amt) rusd, max(rtype) rtype,
           min(yr) ry0, max(yr) ry1
    FROM r WHERE npi IS NOT NULL GROUP BY npi),
own_agg AS (
    SELECT npi, count(*) ocnt, sum(invested) oinv, sum(interest_val) oval,
           min(yr) oy0, max(yr) oy1
    FROM o WHERE npi IS NOT NULL GROUP BY npi),
pi_agg AS (
    SELECT pi_npi AS npi, count(*) as_pi_cnt FROM (
        SELECT pi1 AS pi_npi FROM r WHERE pi1 IS NOT NULL
        UNION ALL SELECT pi2 FROM r WHERE pi2 IS NOT NULL
        UNION ALL SELECT pi3 FROM r WHERE pi3 IS NOT NULL
        UNION ALL SELECT pi4 FROM r WHERE pi4 IS NOT NULL
        UNION ALL SELECT pi5 FROM r WHERE pi5 IS NOT NULL
    ) GROUP BY pi_npi),
mfg_per_npi AS (
    SELECT npi, count(DISTINCT mfg_id) dmfg FROM (
        SELECT npi, mfg_id FROM g WHERE npi IS NOT NULL AND mfg_id IS NOT NULL
        UNION ALL SELECT npi, mfg_id FROM r WHERE npi IS NOT NULL AND mfg_id IS NOT NULL
        UNION ALL SELECT npi, mfg_id FROM o WHERE npi IS NOT NULL AND mfg_id IS NOT NULL
    ) GROUP BY npi),
keys AS (
    SELECT npi FROM gen_agg
    UNION SELECT npi FROM res_agg
    UNION SELECT npi FROM own_agg
    UNION SELECT npi FROM pi_agg)
SELECT
    k.npi                                            AS npi,
    coalesce(g.rtype, r.rtype)                       AS recipient_type,
    coalesce(g.gcnt, 0)                              AS general_payment_count,
    coalesce(g.gusd, 0.0)                            AS general_total_usd,
    coalesce(r.rcnt, 0)                              AS research_payment_count,
    coalesce(r.rusd, 0.0)                            AS research_total_usd,
    coalesce(p.as_pi_cnt, 0)                         AS research_as_investigator_count,
    coalesce(o.ocnt, 0)                              AS ownership_record_count,
    coalesce(o.oinv, 0.0)                            AS ownership_total_invested_usd,
    coalesce(o.oval, 0.0)                            AS ownership_total_value_usd,
    (o.ocnt IS NOT NULL AND o.ocnt > 0)             AS has_ownership_interest,
    coalesce(m.dmfg, 0)                              AS distinct_manufacturers,
    coalesce(g.gusd, 0.0) + coalesce(r.rusd, 0.0)   AS total_payments_usd,
    least(g.gy0, r.ry0, o.oy0)                       AS first_payment_year,
    greatest(g.gy1, r.ry1, o.oy1)                    AS last_payment_year
FROM keys k
LEFT JOIN gen_agg     g USING (npi)
LEFT JOIN res_agg     r USING (npi)
LEFT JOIN own_agg     o USING (npi)
LEFT JOIN pi_agg      p USING (npi)
LEFT JOIN mfg_per_npi m USING (npi)
WHERE k.npi IS NOT NULL
"""

# Manufacturer dim — 1 row / manufacturer-GPO id. MAX(non-null) name = deterministic resolution.
MFG_SQL = """
WITH gen_m AS (
    SELECT mfg_id, count(*) gcnt, sum(amt) gusd, max(mfg_name) nm,
           max(mfg_state) st, max(mfg_country) cy, min(yr) y0, max(yr) y1
    FROM g WHERE mfg_id IS NOT NULL GROUP BY mfg_id),
res_m AS (
    SELECT mfg_id, count(*) rcnt, sum(amt) rusd, max(mfg_name) nm,
           max(mfg_state) st, max(mfg_country) cy, min(yr) y0, max(yr) y1
    FROM r WHERE mfg_id IS NOT NULL GROUP BY mfg_id),
own_m AS (
    SELECT mfg_id, count(*) ocnt, sum(invested) ousd, max(mfg_name) nm,
           max(mfg_state) st, max(mfg_country) cy, min(yr) y0, max(yr) y1
    FROM o WHERE mfg_id IS NOT NULL GROUP BY mfg_id),
recip_m AS (
    SELECT mfg_id, count(DISTINCT npi) drecip FROM (
        SELECT mfg_id, npi FROM g WHERE mfg_id IS NOT NULL AND npi IS NOT NULL
        UNION ALL SELECT mfg_id, npi FROM r WHERE mfg_id IS NOT NULL AND npi IS NOT NULL
        UNION ALL SELECT mfg_id, npi FROM o WHERE mfg_id IS NOT NULL AND npi IS NOT NULL
    ) GROUP BY mfg_id),
keys AS (
    SELECT mfg_id FROM gen_m
    UNION SELECT mfg_id FROM res_m
    UNION SELECT mfg_id FROM own_m)
SELECT
    k.mfg_id                                          AS manufacturer_id,
    coalesce(g.nm, r.nm, o.nm)                        AS manufacturer_name,
    coalesce(g.st, r.st, o.st)                        AS manufacturer_state,
    coalesce(g.cy, r.cy, o.cy)                        AS manufacturer_country,
    coalesce(g.gcnt, 0)                               AS general_payment_count,
    coalesce(g.gusd, 0.0)                             AS general_total_usd,
    coalesce(r.rcnt, 0)                               AS research_payment_count,
    coalesce(r.rusd, 0.0)                             AS research_total_usd,
    coalesce(o.ocnt, 0)                               AS ownership_record_count,
    coalesce(o.ousd, 0.0)                             AS ownership_total_invested_usd,
    coalesce(rc.drecip, 0)                            AS distinct_recipients,
    coalesce(g.gusd, 0.0) + coalesce(r.rusd, 0.0)     AS total_payments_usd,
    least(g.y0, r.y0, o.y0)                           AS first_year,
    greatest(g.y1, r.y1, o.y1)                        AS last_year
FROM keys k
LEFT JOIN gen_m   g USING (mfg_id)
LEFT JOIN res_m   r USING (mfg_id)
LEFT JOIN own_m   o USING (mfg_id)
LEFT JOIN recip_m rc USING (mfg_id)
WHERE k.mfg_id IS NOT NULL
"""


def build_all(*, scratch_dir: str = SCRATCH_DIR) -> dict:
    """Scan the three CMS sources once, assemble both artifacts, index + publish. Returns a
    summary per artifact (rows, indices, dataset_uri). Re-raises on failure."""
    import duckdb

    os.makedirs(SPILL_DIR, exist_ok=True)
    rollup_local = os.path.join(scratch_dir, "rollup")
    mfg_local = os.path.join(scratch_dir, "mfg")

    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}';")
    con.execute(f"SET temp_directory='{SPILL_DIR}';")
    con.execute("PRAGMA threads=8;")
    con.execute("SET preserve_insertion_order=false;")

    print("Loading sources from R2 (one scan each) …")
    _load_sources(con)

    print("Assembling provider payment rollup …")
    rollup_rows = _write_local(con, ROLLUP_SQL, rollup_local)
    print(f"  rollup rows: {rollup_rows:,}")
    rollup_idx = _create_indexes(rollup_local, ROLLUP_INDEX["btree"], ROLLUP_INDEX["bitmap"])

    print("Assembling manufacturer dim …")
    mfg_rows = _write_local(con, MFG_SQL, mfg_local)
    print(f"  manufacturer rows: {mfg_rows:,}")
    mfg_idx = _create_indexes(mfg_local, MFG_INDEX["btree"], MFG_INDEX["bitmap"])
    con.close()

    s3 = _s3_client()
    up_r = _replace_r2_prefix(s3, ROLLUP_PREFIX + "/", rollup_local)
    up_m = _replace_r2_prefix(s3, MFG_PREFIX + "/", mfg_local)
    print(f"Published rollup ({up_r} files) + manufacturer dim ({up_m} files) → R2")

    return {
        "rollup": {"artifact": "cms_provider_payment_rollup", "rows": rollup_rows,
                   "indices": rollup_idx, "dataset_uri": f"s3://{BUCKET}/{ROLLUP_PREFIX}/"},
        "manufacturer": {"artifact": "cms_manufacturer_dim", "rows": mfg_rows,
                         "indices": mfg_idx, "dataset_uri": f"s3://{BUCKET}/{MFG_PREFIX}/"},
    }


# --------------------------------------------------------------------------- #
# ops ledger
# --------------------------------------------------------------------------- #
def _record_run(*, artifact, dataset_uri, rows, indices, status, error,
                started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* state write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """INSERT INTO ops.cms_resolution_runs
                   (feed, artifact, dataset_uri, rows_written, indices, status, error,
                    started_at, completed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (FEED, artifact, dataset_uri, rows, ", ".join(indices or []),
                 status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.* state write failed: {exc}")


# --------------------------------------------------------------------------- #
# Workers
# --------------------------------------------------------------------------- #
@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_state_schema() -> dict:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres secret.")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
        cur.execute("SELECT to_regclass('ops.cms_resolution_runs')")
        present = cur.fetchone()[0]
    print(f"ops.cms_resolution_runs present = {present}")
    return {"status": "success", "table": "ops.cms_resolution_runs", "present": str(present)}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 60 * 3,
    memory=32768,
    cpu=8.0,
    ephemeral_disk=524288,
)
def materialize() -> dict:
    """Read CMS sources from R2 → DuckDB out-of-core aggregate → local Lance → index →
    boto3 publish both artifacts. Records ops.* per artifact. Re-raises on failure."""
    import datetime as dt
    import shutil

    started_at = dt.datetime.now(dt.timezone.utc)
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    status, error, result = "error", None, {}
    try:
        result = build_all()
        status = "success"
    except Exception as exc:  # noqa: BLE001 — terminal handling + re-raise
        error = str(exc)
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        for key in ("rollup", "manufacturer"):
            art = result.get(key, {})
            _record_run(
                artifact=art.get("artifact", key),
                dataset_uri=art.get("dataset_uri"),
                rows=int(art.get("rows", 0)),
                indices=art.get("indices", []),
                status=status, error=error,
                started_at=started_at, completed_at=completed_at,
            )
        shutil.rmtree(SCRATCH_DIR, ignore_errors=True)

    if status != "success":
        raise RuntimeError(f"cms resolution materialize failed: {error}")
    return {"status": status, **result}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 15)
def verify_artifacts() -> dict:
    """Read-back proof: open both artifacts from R2 and report rows, committed indices, and
    a couple of integrity gates (no negative totals; last_year >= first_year)."""
    import lance

    so = _r2_storage_options()
    out: dict = {}
    for key, prefix in (("rollup", ROLLUP_PREFIX), ("manufacturer", MFG_PREFIX)):
        uri = f"s3://{BUCKET}/{prefix}/"
        ds = lance.dataset(uri, storage_options=so)
        rec = {"dataset_uri": uri, "rows": ds.count_rows()}
        try:
            idx = []
            for ix in ds.list_indices():
                d = dict(ix) if isinstance(ix, dict) else {
                    "fields": getattr(ix, "fields", None), "type": str(getattr(ix, "type", None))}
                idx.append(d)
            rec["indices"] = idx
        except Exception as exc:  # noqa: BLE001
            rec["indices_error"] = str(exc)[:120]
        out[key] = rec
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 10) -> list:
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, artifact, dataset_uri, rows_written, indices, status, error, "
            "started_at, completed_at FROM ops.cms_resolution_runs ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


# --------------------------------------------------------------------------- #
# Local entrypoints
# --------------------------------------------------------------------------- #
@app.local_entrypoint()
def init_state() -> None:
    import json
    print(json.dumps(apply_state_schema.remote(), indent=2, default=str))


@app.local_entrypoint()
def materialize_cli() -> None:
    import json
    print(json.dumps(materialize.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify() -> None:
    import json
    print(json.dumps(verify_artifacts.remote(), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 10) -> None:
    import json
    print(json.dumps(ledger.remote(limit), indent=2, default=str))
