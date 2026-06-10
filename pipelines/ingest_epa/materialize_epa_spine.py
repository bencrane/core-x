"""Compute worker — EPA Unified Facility Spine (identifiers-only).

Modal app ``epa-spine-pipelines``. Builds the key-clustered facility dimension +
composable crosswalks/rollups that ride the ``REGISTRY_ID`` BTREE — the NPI-spine
pattern applied to EPA's 124-dataset detail graph. Spec of record:
``docs/reference/EPA_UNIFIED_SPINE_PLAN.md``.

APPEND-ONLY blast radius: creates ONLY new ``spine_*`` / ``crosswalk_*`` / ``rollup_*``
prefixes under ``s3://data-sink/active/``. Every EPA source dataset is read READ-ONLY;
none of the 124 source datasets is mutated. ``epa_npdes_dmrs`` (422M) is read with a
streaming scan only — its 350M historical floor is confirmed un-mutated post-build.

Phases (each gated against R2 before proceeding):
    0  ops ledger + source-inventory preflight (no data writes)
    1  6 crosswalks  (REGISTRY_ID <-> program keys; both-way BTREE)
    2  spine_epa_facility  (the dimension master, 1 row / REGISTRY_ID)
    3  5 rollups  (rollup_epa_{npdes,rcra,sdwa,air,enforcement}; ride the BTREE)
    4  spine_epa_facility_360  (capstone; spine LEFT JOIN every rollup)
    5  refresh wiring + published-layer re-gate (verify_epa_spine)

    modal deploy pipelines/ingest_epa/materialize_epa_spine.py
    modal run    pipelines/ingest_epa/materialize_epa_spine.py::init
    modal run    pipelines/ingest_epa/materialize_epa_spine.py::preflight
    modal run    pipelines/ingest_epa/materialize_epa_spine.py::crosswalks
    modal run    pipelines/ingest_epa/materialize_epa_spine.py::spine
    modal run    pipelines/ingest_epa/materialize_epa_spine.py::rollups
    modal run    pipelines/ingest_epa/materialize_epa_spine.py::capstone
    modal run    pipelines/ingest_epa/materialize_epa_spine.py::run
    modal run    pipelines/ingest_epa/materialize_epa_spine.py::verify
"""

from __future__ import annotations

import os

import modal

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
LANDING_BUCKET = "data-sink"
ACTIVE_BASE = os.environ.get("EPA_ACTIVE_BASE", "s3://data-sink/active/")
FEED = "epa_spine"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576
PARQUET_ROW_GROUP = 1_048_576
LANCE_READ_BATCH = 100_000

# epa_npdes_dmrs historical floor — the spine must read the full-history DMR table; a
# read must never mutate it, and post-build the count is re-asserted >= this floor.
DMR_HISTORICAL_FLOOR = 350_000_000

_GiB = 1024 ** 3

# --------------------------------------------------------------------------- #
# Images
# --------------------------------------------------------------------------- #
# Standard image: in-memory BTREE sort for high-card string keys (LANCE_BYPASS_SPILLING).
image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2",
        "lancedb>=0.15",
        "pylance>=7",
        "pyarrow>=17",
        "boto3>=1.34",
        "requests>=2.32",
        "psycopg[binary]>=3.2",
    )
    .env({"LANCE_BYPASS_SPILLING": "true"})
)

# Heavy image (NPDES/DMR rollup): the DMR-scale local-spill index build must NOT bypass
# spilling, and the DataFusion temp/mem caps are raised. Mirrors reindex_dmrs_local.
LANCE_MAX_TEMP_BYTES = str(250 * _GiB)
LANCE_MEM_POOL_BYTES = str(24 * _GiB)
INDEX_SPILL_DIR = "/tmp"

heavy_image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "duckdb>=1.5,<2", "lancedb>=0.15", "pylance>=7", "pyarrow>=17",
        "boto3>=1.34", "psycopg[binary]>=3.2",
    )
    .env({  # LANCE_BYPASS_SPILLING deliberately ABSENT → local sort spills to disk
        "TMPDIR": INDEX_SPILL_DIR,
        "LANCE_MAX_TEMP_DIRECTORY_SIZE": LANCE_MAX_TEMP_BYTES,
        "LANCE_MEM_POOL_SIZE": LANCE_MEM_POOL_BYTES,
    })
)

app = modal.App("epa-spine-pipelines", image=image)

SECRETS = [modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")]
R2_SECRET = [modal.Secret.from_name("r2-credentials")]

SPILL_DIR = "/tmp/duckdb_spill"
SCRATCH_DIR = "/tmp/epa_spine"
DUCKDB_THREADS = 8
DUCKDB_MEMORY_LIMIT = "40GB"

# --------------------------------------------------------------------------- #
# ops.epa_spine_runs — idempotency ledger (canonical mirror: ops_epa_spine_runs.sql)
# --------------------------------------------------------------------------- #
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.epa_spine_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        text  NOT NULL,
    phase         text  NOT NULL,
    artifact      text  NOT NULL,
    dataset_uri   text,
    grain         text,
    rows_written  bigint,
    reach_pct     double precision,
    null_key_pct  double precision,
    indices_built text,
    gates         jsonb,
    status        text  NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS epa_spine_runs_run_idx      ON ops.epa_spine_runs (run_id);
CREATE INDEX IF NOT EXISTS epa_spine_runs_artifact_idx ON ops.epa_spine_runs (artifact);
CREATE INDEX IF NOT EXISTS epa_spine_runs_phase_idx    ON ops.epa_spine_runs (phase);
CREATE INDEX IF NOT EXISTS epa_spine_runs_status_idx   ON ops.epa_spine_runs (status);
CREATE INDEX IF NOT EXISTS epa_spine_runs_recorded_idx ON ops.epa_spine_runs (recorded_at DESC);
"""


# --------------------------------------------------------------------------- #
# R2 / S3 helpers (mirrored from materialize_epa.py — proven this session)
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
    # request/response checksum scoped to "when_required" — REQUIRED for R2 range reads
    # (botocore >=1.36 otherwise validates a full-object checksum on a partial GET → spurious
    # FlexibleChecksumError).
    return boto3.client(
        "s3",
        endpoint_url=so["endpoint"],
        aws_access_key_id=so["aws_access_key_id"],
        aws_secret_access_key=so["aws_secret_access_key"],
        region_name="auto",
        config=Config(
            signature_version="s3v4",
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def _new_con(memory_limit: str = DUCKDB_MEMORY_LIMIT, threads: int = DUCKDB_THREADS):
    import duckdb

    os.makedirs(SPILL_DIR, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute(f"SET threads TO {threads}")
    con.execute(f"SET memory_limit='{memory_limit}'")
    con.execute(f"SET temp_directory='{SPILL_DIR}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _dataset_exists(uri: str, so: dict) -> bool:
    import lance

    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001
        return False


def _index_names(ds) -> list[str]:
    return [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
            for i in ds.list_indices()]


def _indexed_columns(ds) -> set[str]:
    """Columns carrying a committed scalar index (BTREE/BITMAP)."""
    cols: set[str] = set()
    for i in ds.list_indices():
        c = i.get("fields") if isinstance(i, dict) else getattr(i, "fields", None)
        if c:
            cols.update(c if isinstance(c, (list, tuple)) else [c])
        c2 = i.get("columns") if isinstance(i, dict) else getattr(i, "columns", None)
        if c2:
            cols.update(c2 if isinstance(c2, (list, tuple)) else [c2])
    return cols


def _build_indexes(uri_or_path: str, btree: list[str], bitmap: list[str], so: dict | None) -> list[str]:
    import lance

    ds = lance.dataset(uri_or_path, storage_options=so)
    built = []
    for col in btree:
        ds.create_scalar_index(col, index_type="BTREE", replace=True)
        built.append(f"{col}:BTREE")
        print(f"  BTREE ✓ {col}")
    for col in bitmap:
        ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        built.append(f"{col}:BITMAP")
        print(f"  BITMAP ✓ {col}")
    return built


# --------------------------------------------------------------------------- #
# R2-safe local-stage write path (the r2_safe_local idiom). Every spine/rollup
# artifact stages to local NVMe, indices build locally (no R2 multipart), then the
# committed dataset is published to R2 via boto3 uniform-part uploads (manifest last).
# A direct Lance->R2 write of a wide/large page trips R2's multipart rule (400 InvalidPart).
# --------------------------------------------------------------------------- #
def _delete_r2_prefix(s3, prefix: str) -> int:
    paginator = s3.get_paginator("list_objects_v2")
    batch: list[dict] = []
    deleted = 0
    for page in paginator.paginate(Bucket=LANDING_BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            batch.append({"Key": o["Key"]})
            if len(batch) == 1000:
                s3.delete_objects(Bucket=LANDING_BUCKET, Delete={"Objects": batch})
                deleted += len(batch)
                batch = []
    if batch:
        s3.delete_objects(Bucket=LANDING_BUCKET, Delete={"Objects": batch})
        deleted += len(batch)
    return deleted


def _upload_new_files(s3, prefix: str, local_dir: str, existing: set[str]) -> int:
    """Upload local files whose relative key is NOT already in R2. Manifest/version files
    upload LAST so the new version resolves only once every referenced file is present."""
    new: list[tuple[str, str]] = []
    for root, _, files in os.walk(local_dir):
        for f in files:
            lp = os.path.join(root, f)
            rel = os.path.relpath(lp, local_dir).replace(os.sep, "/")
            if rel not in existing:
                new.append((rel, lp))
    new.sort(key=lambda t: ("_versions/" in t[0] or t[0].endswith(".manifest"), t[0]))
    for rel, lp in new:
        s3.upload_file(lp, LANDING_BUCKET, prefix + rel)
    return len(new)


def _publish_local_dataset(name: str, local: str, btree: list[str], bitmap: list[str]) -> dict:
    """Build indices on a LOCAL committed Lance dataset, then publish it to active/<name>/
    via boto3 (R2-safe uniform parts). Clears any stale prefix first (full-snapshot derive).
    Returns the read-back proof (rows + indices from R2)."""
    import lance

    so = _r2_storage_options()
    uri = ACTIVE_BASE + name + "/"
    prefix = "active/" + name + "/"
    built = _build_indexes(local, btree, bitmap, None)
    s3 = _s3_client()
    removed = _delete_r2_prefix(s3, prefix)
    published = _upload_new_files(s3, prefix, local, set())
    ds = lance.dataset(uri, storage_options=so)
    out = {"rows": ds.count_rows(), "indices": _index_names(ds),
           "built": built, "cleared": removed, "published": published}
    print(f"[{name}] R2-safe publish: cleared {removed} stale, published {published}; "
          f"rows={out['rows']:,} indices={out['indices']}")
    return out


def _write_local_and_publish(name: str, reader, btree: list[str], bitmap: list[str]) -> dict:
    """Stream an Arrow reader → a LOCAL Lance dataset (overwrite), index locally, publish to R2."""
    import shutil

    import lance

    local = f"{SCRATCH_DIR}/{name}_local"
    shutil.rmtree(local, ignore_errors=True)
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    lance.write_dataset(
        reader, local, mode="overwrite",
        data_storage_version=DATA_STORAGE_VERSION,
        max_rows_per_file=MAX_ROWS_PER_FILE,
    )
    return _publish_local_dataset(name, local, btree, bitmap)


# --------------------------------------------------------------------------- #
# ops ledger
# --------------------------------------------------------------------------- #
def _pg_connect():
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return None
    return psycopg.connect(dsn)


def _ensure_ops_ledger() -> None:
    """Run OPS_DDL exactly once from the orchestrator BEFORE any fan-out (deadlock-safe)."""
    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.epa_spine_runs bootstrap failed: {exc}")
    finally:
        conn.close()


def _record_run(*, run_id, phase, artifact, dataset_uri=None, grain=None, rows_written=0,
                reach_pct=None, null_key_pct=None, indices_built=None, gates=None,
                status, error=None, started_at=None, completed_at=None) -> None:
    """Terminal row → ops.epa_spine_runs (psycopg). Best-effort; never masks the build."""
    import time

    from psycopg import errors as pg_errors
    from psycopg.types.json import Jsonb

    conn = _pg_connect()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            with conn.transaction():
                cur.execute("SELECT to_regclass('ops.epa_spine_runs')")
                if cur.fetchone()[0] is None:
                    cur.execute(OPS_DDL)
            params = (run_id, phase, artifact, dataset_uri, grain, rows_written,
                      reach_pct, null_key_pct, indices_built,
                      Jsonb(gates) if gates is not None else None, status, error,
                      started_at, completed_at)
            for attempt in range(3):
                try:
                    with conn.transaction():
                        cur.execute(
                            """
                            INSERT INTO ops.epa_spine_runs
                                (run_id, phase, artifact, dataset_uri, grain, rows_written,
                                 reach_pct, null_key_pct, indices_built, gates, status, error,
                                 started_at, completed_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            params,
                        )
                    break
                except (pg_errors.DeadlockDetected, pg_errors.SerializationFailure) as exc:
                    if attempt == 2:
                        raise
                    print(f"WARN: ops.epa_spine_runs INSERT retry {attempt + 1}/3 ({exc.__class__.__name__})")
                    time.sleep(0.25 * (attempt + 1))
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.epa_spine_runs write failed: {exc}")
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
            print(f"Callback attempt {i + 1} non-2xx: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


# =========================================================================== #
# PHASE 0 — ops ledger + source-inventory preflight
# =========================================================================== #
# Candidate inputs this plan reads. Each entry: (dataset, expected_key_cols, is_hub,
# row_floor). The preflight opens each from R2 and reports schema cols + indices + count;
# a hub input additionally must carry a committed BTREE on its primary join key. The probe
# is exhaustive (dumps every input's schema) so the harness can be reconciled to R2 truth.
PREFLIGHT_INPUTS = [
    # (name, primary_key_col, hub?, row_floor)
    ("epa_facilities",                "REGISTRY_ID",          True,  3_200_000),
    ("epa_echo_exporter",             "REGISTRY_ID",          True,  1_500_000),
    ("epa_program_links",             "REGISTRY_ID",          True,  4_300_000),
    ("epa_air_facilities",            "PGM_SYS_ID",           False, 0),
    ("epa_icis_air_facilities",       "PGM_SYS_ID",           False, 0),
    ("epa_rcra_handlers",             "RCRA_ID",              False, 0),
    ("epa_rcra_facilities",           "ID_NUMBER",            False, 0),
    ("epa_sdwa_pub_water_systems",    "PWSID",                False, 0),
    ("epa_sdwa_facilities",           "PWSID",                False, 0),
    ("epa_icis_permits",              "EXTERNAL_PERMIT_NMBR", False, 0),
    ("epa_case_facilities",           "ACTIVITY_ID",          False, 0),
    ("epa_npdes_inspections",         "NPDES_ID",             False, 0),
    ("epa_frs_naics_codes",           "REGISTRY_ID",          False, 0),
    ("epa_frs_sic_codes",             "REGISTRY_ID",          False, 0),
    # Phase-3 detail giants
    ("epa_npdes_dmrs",                "EXTERNAL_PERMIT_NMBR", True,  DMR_HISTORICAL_FLOOR),
    ("epa_sdwa_violations_enforcement", "PWSID",              False, 0),
    ("epa_npdes_limits",              "EXTERNAL_PERMIT_NMBR", False, 0),
    ("epa_npdes_qncr_history",        "NPDES_ID",             False, 0),
    ("epa_rcra_violations",           "ID_NUMBER",            False, 0),
    ("epa_icis_air_violation_history", "PGM_SYS_ID",          False, 0),
    ("epa_case_penalties",            "ACTIVITY_ID",          False, 0),
    ("epa_case_enforcements",         "ACTIVITY_ID",          True,  0),
]


@app.function(secrets=SECRETS, timeout=900)
def preflight(run_id: str, extra: list[str] | None = None) -> dict:
    """Open every candidate input from R2; report schema cols + indices + count. Assert hub
    row floors + the expected BTREE on hub join keys. Pure-read; writes one ledger row."""
    import datetime as dt

    import lance

    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    inventory: dict[str, dict] = {}
    failures: list[str] = []

    names = [r[0] for r in PREFLIGHT_INPUTS] + list(extra or [])
    meta = {r[0]: r for r in PREFLIGHT_INPUTS}
    for name in names:
        uri = ACTIVE_BASE + name + "/"
        rec = meta.get(name, (name, None, False, 0))
        _, key_col, is_hub, floor = rec
        try:
            ds = lance.dataset(uri, storage_options=so)
            cols = list(ds.schema.names)
            idx = _index_names(ds)
            idx_cols = _indexed_columns(ds)
            rows = ds.count_rows()
            entry = {
                "rows": rows, "n_cols": len(cols), "indices": idx,
                "indexed_columns": sorted(idx_cols), "columns": cols,
                "key_col": key_col, "key_present": (key_col in cols) if key_col else None,
                "key_indexed": (key_col in idx_cols) if key_col else None,
                "floor": floor, "floor_ok": rows >= floor,
            }
            inventory[name] = entry
            problems = []
            if rows < floor:
                problems.append(f"rows {rows:,} < floor {floor:,}")
            if key_col and key_col not in cols:
                problems.append(f"key {key_col} absent")
            if is_hub and key_col and key_col not in idx_cols:
                problems.append(f"hub key {key_col} NOT BTREE-indexed")
            if problems:
                failures.append(f"{name}: {'; '.join(problems)}")
                entry["problems"] = problems
            print(f"[preflight] {name}: rows={rows:,} cols={len(cols)} "
                  f"key={key_col}{'✓' if entry['key_present'] else '✗'} "
                  f"idx={'✓' if entry['key_indexed'] else '✗'} indices={idx}")
        except Exception as exc:  # noqa: BLE001
            inventory[name] = {"error": str(exc), "key_col": key_col, "is_hub": is_hub}
            print(f"[preflight] {name}: OPEN FAILED — {exc}")
            if is_hub:
                failures.append(f"{name}: open failed — {exc}")

    # G0.3 — epa_program_links carries BTREE on all three join columns.
    pl = inventory.get("epa_program_links", {})
    pl_idx = set(pl.get("indexed_columns", []))
    for need in ("REGISTRY_ID", "PGM_SYS_ID", "PGM_SYS_ACRNM"):
        if pl.get("rows") and need not in pl_idx:
            failures.append(f"epa_program_links: missing index on {need} (have {sorted(pl_idx)})")

    status = "success" if not failures else "error"
    completed = dt.datetime.now(dt.timezone.utc)
    gates = {
        "G0.1_ledger": True,
        "G0.2_hub_floors": all(
            inventory.get(n, {}).get("floor_ok", False)
            for n, _, hub, _ in PREFLIGHT_INPUTS if hub),
        "G0.3_program_links_btree": not any("epa_program_links:" in f for f in failures),
        "G0.4_dmr_floor": inventory.get("epa_npdes_dmrs", {}).get("floor_ok", False),
        "failures": failures,
    }
    _record_run(run_id=run_id, phase="preflight", artifact="__preflight__",
                dataset_uri=ACTIVE_BASE, grain="probe", rows_written=0,
                gates={"inventory": {k: {kk: vv for kk, vv in v.items() if kk != "columns"}
                                     for k, v in inventory.items()}, **gates},
                status=status, error=("; ".join(failures) if failures else None),
                started_at=started, completed_at=completed)

    print(f"[preflight] status={status} failures={failures}")
    return {"run_id": run_id, "status": status, "gates": gates, "inventory": inventory}


# =========================================================================== #
# PHASE 1 — Crosswalks (REGISTRY_ID <-> program keys)
# =========================================================================== #
# Each per-program crosswalk: SELECT DISTINCT REGISTRY_ID, PGM_SYS_ID AS <native> FROM
# epa_program_links WHERE PGM_SYS_ACRNM='<ACRNM>' AND both non-null. Enforcement is the
# epa_case_facilities (ACTIVITY_ID, REGISTRY_ID) edge (the 99.3% reacher). The universal
# crosswalk is the full DISTINCT (REGISTRY_ID, PGM_SYS_ACRNM, PGM_SYS_ID) superset.
#
# spec: name, source dataset, the program-native key name the detail family carries, the
# program filter, both BTREE key cols, and (reach gate) the detail table + key col to
# anti-join the resolution reach against.
CROSSWALK_SPECS: list[dict] = [
    dict(name="crosswalk_epa_registry_program", source="epa_program_links",
         kind="universal",
         btree=["REGISTRY_ID", "PGM_SYS_ID"], bitmap=["PGM_SYS_ACRNM"]),
    dict(name="crosswalk_epa_registry_npdes", source="epa_program_links",
         kind="program", acrnm="NPDES", key="NPDES_ID",
         reach_dataset="epa_icis_permits", reach_key="EXTERNAL_PERMIT_NMBR", reach_floor=0.99,
         btree=["REGISTRY_ID", "NPDES_ID"], bitmap=[]),
    # RCRA reach floor reconciled to R2 truth (0.985): measured 98.80% of distinct ID_NUMBER
    # in the RAW epa_rcra_facilities mirror (1,597,673) resolve a REGISTRY_ID via the RCRAINFO
    # program-link — 19,169 raw handler IDs have NO RCRAINFO edge to FRS and are deterministically
    # unresolvable (confirmed: the curated epa_rcra_handlers, which resolves RID via the same join,
    # is exactly the 1,578,504-row matched subset). The plan's "≥99%" was an estimate; 98.80% is
    # the true ceiling against the raw denominator. Floor kept as an anti-regression tripwire.
    dict(name="crosswalk_epa_registry_rcra", source="epa_program_links",
         kind="program", acrnm="RCRAINFO", key="ID_NUMBER",
         reach_dataset="epa_rcra_facilities", reach_key="ID_NUMBER", reach_floor=0.985,
         btree=["REGISTRY_ID", "ID_NUMBER"], bitmap=[]),
    dict(name="crosswalk_epa_registry_sdwa", source="epa_program_links",
         kind="program", acrnm="SFDW", key="PWSID",
         reach_dataset="epa_sdwa_pub_water_systems", reach_key="PWSID", reach_floor=0.995,
         btree=["REGISTRY_ID", "PWSID"], bitmap=[]),
    dict(name="crosswalk_epa_registry_air", source="epa_program_links",
         kind="program", acrnm="AIR", key="PGM_SYS_ID",
         reach_dataset="epa_air_facilities", reach_key="PGM_SYS_ID", reach_floor=1.0,
         btree=["REGISTRY_ID", "PGM_SYS_ID"], bitmap=[]),
    # Enforcement reach floor reconciled to R2 truth (0.985): measured 98.86% of distinct
    # ACTIVITY_ID in epa_case_enforcements (135,053) resolve a REGISTRY_ID via the
    # epa_case_facilities edge — 1,544 enforcement cases have no facility row carrying a non-null
    # REGISTRY_ID (federal cases not facility-linked). The plan's "99.3%" referred to the
    # facility→RID reach, not the case→facility-edge reach. 98.86% is the true ceiling.
    dict(name="crosswalk_epa_registry_enforcement", source="epa_case_facilities",
         kind="enforcement", key="ACTIVITY_ID",
         reach_dataset="epa_case_enforcements", reach_key="ACTIVITY_ID", reach_floor=0.985,
         btree=["REGISTRY_ID", "ACTIVITY_ID"], bitmap=[]),
]


def _scan_distinct_keys(con, uri: str, so: dict, col: str, alias: str) -> None:
    """Register a Lance dataset and pull DISTINCT non-null values of one column → a temp."""
    import lance

    rdr = lance.dataset(uri, storage_options=so).scanner(columns=[col]).to_reader()
    con.register(f"_src_{alias}", rdr)
    con.execute(f"CREATE TEMP TABLE {alias} AS "
                f"SELECT DISTINCT nullif(trim(CAST({col} AS VARCHAR)),'') AS k "
                f"FROM _src_{alias} WHERE nullif(trim(CAST({col} AS VARCHAR)),'') IS NOT NULL")
    con.unregister(f"_src_{alias}")


@app.function(secrets=SECRETS, timeout=60 * 60, memory=32768, cpu=8.0)
def build_crosswalk(spec: dict, run_id: str) -> dict:
    """Materialize one crosswalk → active/<name>/ (R2-safe local stage). Computes reach_pct
    against the program's detail-table key denominator, null_key_pct, records a ledger row."""
    import datetime as dt

    import lance
    import pyarrow as pa

    name = spec["name"]
    uri = ACTIVE_BASE + name + "/"
    so = _r2_storage_options()
    src_uri = ACTIVE_BASE + spec["source"] + "/"
    started = dt.datetime.now(dt.timezone.utc)
    status, error, rows, reach_pct, null_key_pct, built = "error", None, 0, None, None, []
    gates: dict = {}
    os.makedirs(SCRATCH_DIR, exist_ok=True)

    try:
        con = _new_con()
        try:
            if spec["kind"] == "universal":
                rdr = lance.dataset(src_uri, storage_options=so).scanner(
                    columns=["REGISTRY_ID", "PGM_SYS_ACRNM", "PGM_SYS_ID"]).to_reader()
                con.register("_links", rdr)
                con.execute("""
                    CREATE TEMP TABLE xw AS
                    SELECT DISTINCT
                        nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'')   AS REGISTRY_ID,
                        nullif(trim(CAST(PGM_SYS_ACRNM AS VARCHAR)),'') AS PGM_SYS_ACRNM,
                        nullif(trim(CAST(PGM_SYS_ID AS VARCHAR)),'')    AS PGM_SYS_ID
                    FROM _links
                    WHERE nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') IS NOT NULL
                      AND nullif(trim(CAST(PGM_SYS_ID AS VARCHAR)),'') IS NOT NULL
                """)
                con.unregister("_links")
                gates["distinct_registry_id"] = con.execute(
                    "SELECT count(DISTINCT REGISTRY_ID) FROM xw").fetchone()[0]
                key_cols = ["REGISTRY_ID", "PGM_SYS_ID"]
            elif spec["kind"] == "enforcement":
                rdr = lance.dataset(src_uri, storage_options=so).scanner(
                    columns=["REGISTRY_ID", "ACTIVITY_ID"]).to_reader()
                con.register("_cf", rdr)
                con.execute("""
                    CREATE TEMP TABLE xw AS
                    SELECT DISTINCT
                        nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') AS REGISTRY_ID,
                        nullif(trim(CAST(ACTIVITY_ID AS VARCHAR)),'') AS ACTIVITY_ID
                    FROM _cf
                    WHERE nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') IS NOT NULL
                      AND nullif(trim(CAST(ACTIVITY_ID AS VARCHAR)),'') IS NOT NULL
                """)
                con.unregister("_cf")
                key_cols = ["REGISTRY_ID", "ACTIVITY_ID"]
            else:  # program slice of epa_program_links
                key = spec["key"]
                rdr = lance.dataset(src_uri, storage_options=so).scanner(
                    columns=["REGISTRY_ID", "PGM_SYS_ID", "PGM_SYS_ACRNM"]).to_reader()
                con.register("_links", rdr)
                con.execute(f"""
                    CREATE TEMP TABLE xw AS
                    SELECT DISTINCT
                        nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') AS REGISTRY_ID,
                        nullif(trim(CAST(PGM_SYS_ID AS VARCHAR)),'')  AS {key}
                    FROM _links
                    WHERE PGM_SYS_ACRNM = '{spec['acrnm']}'
                      AND nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') IS NOT NULL
                      AND nullif(trim(CAST(PGM_SYS_ID AS VARCHAR)),'') IS NOT NULL
                """)
                con.unregister("_links")
                key_cols = ["REGISTRY_ID", key]

            rows = con.execute("SELECT count(*) FROM xw").fetchone()[0]
            # null_key_pct (G1.3) — must be 0 (WHERE clause guarantees, gate proves)
            nulls = con.execute(
                "SELECT " + " + ".join(
                    f"count(*) FILTER (WHERE {c} IS NULL)" for c in key_cols)
                + " FROM xw").fetchone()[0]
            null_key_pct = 0.0 if rows == 0 else (nulls / (rows * len(key_cols)))

            # Reach gate (G1.2) — fraction of the detail-table key denominator resolved.
            if spec.get("reach_dataset"):
                _scan_distinct_keys(con, ACTIVE_BASE + spec["reach_dataset"] + "/", so,
                                    spec["reach_key"], "denom")
                denom = con.execute("SELECT count(*) FROM denom").fetchone()[0]
                xkey = spec["key"]
                matched = con.execute(
                    f"SELECT count(*) FROM denom d WHERE EXISTS "
                    f"(SELECT 1 FROM xw x WHERE x.{xkey} = d.k)").fetchone()[0]
                reach_pct = None if denom == 0 else matched / denom
                gates["reach"] = {"denom": denom, "matched": matched, "reach_pct": reach_pct,
                                  "floor": spec["reach_floor"],
                                  "ok": (reach_pct is not None and reach_pct >= spec["reach_floor"])}
                con.execute("DROP TABLE denom")

            table = con.execute(
                "SELECT * FROM xw ORDER BY " + ", ".join(key_cols)).fetch_arrow_table()
            # Force string dtype on key columns (never int — leading-zero / precision loss).
            schema = pa.schema([(c, pa.string()) for c in table.column_names])
            table = table.cast(schema)
        finally:
            con.close()

        reader = pa.Table.to_reader(table, max_chunksize=LANCE_READ_BATCH)
        pub = _write_local_and_publish(name, reader, spec["btree"], spec["bitmap"])
        built = pub["built"]
        rows = pub["rows"]

        # Gate validation
        if spec["kind"] == "universal" and gates.get("distinct_registry_id", 0) < 3_385_000:
            raise RuntimeError(
                f"G1.1 reach floor: distinct REGISTRY_ID {gates['distinct_registry_id']:,} < 3,385,000")
        if gates.get("reach") and not gates["reach"]["ok"]:
            raise RuntimeError(
                f"G1.2 per-program reach below floor: {gates['reach']}")
        if null_key_pct != 0.0:
            raise RuntimeError(f"G1.3 null_key_pct = {null_key_pct} (expected 0)")
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[{name}] FAILED: {error}")
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, phase="crosswalk", artifact=name, dataset_uri=uri,
                    grain="(REGISTRY_ID, key)", rows_written=int(rows), reach_pct=reach_pct,
                    null_key_pct=null_key_pct, indices_built=",".join(built), gates=gates,
                    status=status, error=error, started_at=started, completed_at=completed)

    return {"artifact": name, "uri": uri, "rows": int(rows), "reach_pct": reach_pct,
            "status": status, "indices": built, "gates": gates, "error": error}


@app.function(secrets=SECRETS, timeout=60 * 120, memory=2048)
def build_crosswalks(run_id: str, only: list[str] | None = None) -> dict:
    _ensure_ops_ledger()
    specs = CROSSWALK_SPECS
    if only:
        specs = [s for s in specs if s["name"] in set(only)]
    calls = [(s["name"], build_crosswalk.spawn(s, run_id)) for s in specs]
    results = []
    for nm, call in calls:
        try:
            results.append(call.get())
        except Exception as exc:  # noqa: BLE001
            results.append({"artifact": nm, "status": "error", "error": str(exc)})
    bad = [r for r in results if r.get("status") != "success"]
    summary = {"run_id": run_id, "status": "success" if not bad else "error",
               "crosswalks": {r["artifact"]: {"rows": r.get("rows"), "reach_pct": r.get("reach_pct"),
                                              "status": r.get("status")} for r in results}}
    print(summary)
    if bad:
        raise RuntimeError(f"crosswalk build failed: {[r['artifact'] for r in bad]}")
    return summary


# =========================================================================== #
# PHASE 2 — spine_epa_facility (the dimension master, 1 row / REGISTRY_ID)
# =========================================================================== #
SPINE_NAME = "spine_epa_facility"
SPINE_BTREE = ["registry_id", "fac_name", "fac_zip5"]
SPINE_BITMAP = ["fac_state", "primary_naics", "has_npdes", "has_rcra", "has_sdwa",
                "has_air", "has_enforcement", "program_count", "fac_compliance_status",
                "fac_programs_with_snc", "fac_major_flag", "has_active_violation"]
SPINE_SIG_VIOLATION_RECON = 19_968  # ECHO 'Significant Violation' facility count (G2.5, ±1%)


@app.function(secrets=SECRETS, timeout=60 * 90, memory=49152, cpu=8.0)
def build_spine_facility(run_id: str) -> dict:
    """spine_epa_facility — epa_facilities base ⨝ epa_echo_exporter (headline signals) ⨝
    pre-aggregated NAICS/SIC lists ⨝ program-presence (from the Phase-1 crosswalks), filtered
    to the program-present subset (>=1 program_links edge OR an ECHO row). Wide → r2_safe.
    NAICS/SIC pre-aggregate to list BEFORE the join (never fan the base)."""
    import datetime as dt

    import lance
    import pyarrow as pa

    name = SPINE_NAME
    uri = ACTIVE_BASE + name + "/"
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    status, error, rows, built = "error", None, 0, []
    gates: dict = {}
    os.makedirs(SCRATCH_DIR, exist_ok=True)

    def _reg(con, ds_name: str, cols: list[str], alias: str) -> None:
        rdr = lance.dataset(ACTIVE_BASE + ds_name + "/", storage_options=so).scanner(
            columns=cols).to_reader()
        con.register(alias, rdr)

    try:
        con = _new_con()
        try:
            # Base identity/geo from epa_facilities (REGISTRY_ID, FAC_NAME, address, geo, state).
            _reg(con, "epa_facilities",
                 ["REGISTRY_ID", "FAC_NAME", "FAC_STREET", "FAC_CITY", "FAC_STATE",
                  "FAC_ZIP", "FAC_COUNTY", "LATITUDE_MEASURE", "LONGITUDE_MEASURE"], "fac_rdr")
            con.execute("""
                CREATE TEMP TABLE fac AS
                SELECT
                    nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') AS registry_id,
                    nullif(trim(FAC_NAME),'')   AS fac_name,
                    nullif(trim(FAC_STREET),'') AS fac_street,
                    nullif(trim(FAC_CITY),'')   AS fac_city,
                    nullif(trim(FAC_STATE),'')  AS fac_state,
                    nullif(left(regexp_replace(CAST(FAC_ZIP AS VARCHAR),'[^0-9]','','g'),5),'') AS fac_zip5,
                    nullif(trim(FAC_COUNTY),'') AS fac_county,
                    LATITUDE_MEASURE  AS latitude,
                    LONGITUDE_MEASURE AS longitude
                FROM fac_rdr
                WHERE nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') IS NOT NULL
            """)
            con.unregister("fac_rdr")

            # ECHO headline signals (already a per-facility rollup). FAC_FIPS_CODE lives here,
            # not in epa_facilities. FAC_SNC_FLG is dead → significant-violation derives from
            # FAC_COMPLIANCE_STATUS / FAC_PROGRAMS_WITH_SNC.
            _reg(con, "epa_echo_exporter",
                 ["REGISTRY_ID", "FAC_FIPS_CODE", "FAC_COMPLIANCE_STATUS", "FAC_PROGRAMS_WITH_SNC",
                  "FAC_INSPECTION_COUNT", "FAC_DATE_LAST_INSPECTION", "FAC_FORMAL_ACTION_COUNT",
                  "FAC_INFORMAL_COUNT", "FAC_TOTAL_PENALTIES", "FAC_PENALTY_COUNT",
                  "FAC_MAJOR_FLAG", "FAC_QTRS_WITH_NC"], "echo_rdr")
            con.execute("""
                CREATE TEMP TABLE echo AS
                SELECT
                    nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') AS registry_id,
                    nullif(trim(FAC_FIPS_CODE),'')         AS fac_fips,
                    nullif(trim(FAC_COMPLIANCE_STATUS),'') AS fac_compliance_status,
                    FAC_PROGRAMS_WITH_SNC                  AS fac_programs_with_snc,
                    FAC_INSPECTION_COUNT                   AS fac_inspection_count,
                    FAC_DATE_LAST_INSPECTION               AS fac_date_last_inspection,
                    FAC_FORMAL_ACTION_COUNT                AS fac_formal_action_count,
                    FAC_INFORMAL_COUNT                     AS fac_informal_count,
                    FAC_TOTAL_PENALTIES                    AS fac_total_penalties,
                    FAC_PENALTY_COUNT                      AS fac_penalty_count,
                    nullif(trim(FAC_MAJOR_FLAG),'')        AS fac_major_flag,
                    FAC_QTRS_WITH_NC                       AS fac_qtrs_with_nc
                FROM echo_rdr
                WHERE nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') IS NOT NULL
            """)
            con.unregister("echo_rdr")

            # NAICS / SIC pre-aggregated to one list per REGISTRY_ID (GROUP BY before any join).
            # Note: source code column is PGM_SYS_ACNRM (EPA's typo). primary_naics derived as the
            # min(NAICS_CODE) per facility (no FRS "primary" flag exists in the mirror).
            # Multi-value industry codes stored as PIPE-DELIMITED STRINGS (not Lance list<string>):
            # a sparse list<string> page (sic_codes) trips Lance v2.1's list-page StructArray decoder
            # on read-back (corrupt-column, proven this build). Delimited strings are queryable
            # (string_split / LIKE), compact, and decode reliably. NAICS/SIC sorted for determinism.
            _reg(con, "epa_frs_naics_codes", ["REGISTRY_ID", "NAICS_CODE"], "naics_rdr")
            con.execute("""
                CREATE TEMP TABLE naics AS
                SELECT registry_id,
                       string_agg(code, '|' ORDER BY code) AS naics_codes,
                       min(code)                           AS primary_naics
                FROM (
                    SELECT DISTINCT
                           nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') AS registry_id,
                           nullif(trim(CAST(NAICS_CODE AS VARCHAR)),'')  AS code
                    FROM naics_rdr
                ) WHERE registry_id IS NOT NULL AND code IS NOT NULL
                GROUP BY registry_id
            """)
            con.unregister("naics_rdr")
            _reg(con, "epa_frs_sic_codes", ["REGISTRY_ID", "SIC_CODE"], "sic_rdr")
            con.execute("""
                CREATE TEMP TABLE sic AS
                SELECT registry_id, string_agg(code, '|' ORDER BY code) AS sic_codes
                FROM (
                    SELECT DISTINCT
                           nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') AS registry_id,
                           nullif(trim(CAST(SIC_CODE AS VARCHAR)),'')    AS code
                    FROM sic_rdr
                ) WHERE registry_id IS NOT NULL AND code IS NOT NULL
                GROUP BY registry_id
            """)
            con.unregister("sic_rdr")

            # Program presence + program_count/acronyms from the universal crosswalk
            # (the authoritative source).
            _reg(con, "crosswalk_epa_registry_program",
                 ["REGISTRY_ID", "PGM_SYS_ACRNM"], "prog_rdr")
            con.execute("""
                CREATE TEMP TABLE prog AS
                SELECT registry_id,
                       count(DISTINCT acrnm)                  AS program_count,
                       string_agg(acrnm, '|' ORDER BY acrnm) AS program_acronyms
                FROM (
                    SELECT DISTINCT
                           nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') AS registry_id,
                           nullif(trim(CAST(PGM_SYS_ACRNM AS VARCHAR)),'') AS acrnm
                    FROM prog_rdr
                ) WHERE registry_id IS NOT NULL AND acrnm IS NOT NULL
                GROUP BY registry_id
            """)
            con.unregister("prog_rdr")

            # Per-program presence sets (distinct REGISTRY_ID in each per-program crosswalk).
            for prog, xw in [("npdes", "crosswalk_epa_registry_npdes"),
                             ("rcra", "crosswalk_epa_registry_rcra"),
                             ("sdwa", "crosswalk_epa_registry_sdwa"),
                             ("air", "crosswalk_epa_registry_air"),
                             ("enforcement", "crosswalk_epa_registry_enforcement")]:
                _reg(con, xw, ["REGISTRY_ID"], f"_{prog}_rdr")
                con.execute(f"""
                    CREATE TEMP TABLE pres_{prog} AS
                    SELECT DISTINCT nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') AS registry_id
                    FROM _{prog}_rdr
                    WHERE nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') IS NOT NULL
                """)
                con.unregister(f"_{prog}_rdr")

            # Final assembly: base ⨝ echo ⨝ aggregates; program-present filter (in prog OR echo).
            con.execute("""
                CREATE TEMP TABLE spine AS
                SELECT
                    f.registry_id,
                    f.fac_name, f.fac_street, f.fac_city, f.fac_state, f.fac_zip5,
                    f.fac_county, e.fac_fips, f.latitude, f.longitude,
                    n.naics_codes, n.primary_naics, s.sic_codes,
                    (pn.registry_id IS NOT NULL) AS has_npdes,
                    (pr.registry_id IS NOT NULL) AS has_rcra,
                    (ps.registry_id IS NOT NULL) AS has_sdwa,
                    (pa.registry_id IS NOT NULL) AS has_air,
                    (pe.registry_id IS NOT NULL) AS has_enforcement,
                    coalesce(p.program_count, 0) AS program_count,
                    p.program_acronyms,
                    e.fac_compliance_status, e.fac_programs_with_snc,
                    e.fac_inspection_count, e.fac_date_last_inspection,
                    e.fac_formal_action_count, e.fac_informal_count,
                    e.fac_total_penalties, e.fac_penalty_count, e.fac_major_flag,
                    (coalesce(e.fac_qtrs_with_nc,0) > 0
                     OR e.fac_compliance_status = 'Significant Violation') AS has_active_violation
                FROM fac f
                LEFT JOIN echo  e  USING (registry_id)
                LEFT JOIN naics n  USING (registry_id)
                LEFT JOIN sic   s  USING (registry_id)
                LEFT JOIN prog  p  USING (registry_id)
                LEFT JOIN pres_npdes       pn USING (registry_id)
                LEFT JOIN pres_rcra        pr USING (registry_id)
                LEFT JOIN pres_sdwa        ps USING (registry_id)
                LEFT JOIN pres_air         pa USING (registry_id)
                LEFT JOIN pres_enforcement pe USING (registry_id)
                WHERE p.registry_id IS NOT NULL OR e.registry_id IS NOT NULL
            """)

            # Gates computed in-SQL before write.
            total, distinct_rid, null_rid = con.execute(
                "SELECT count(*), count(DISTINCT registry_id), "
                "count(*) FILTER (WHERE registry_id IS NULL) FROM spine").fetchone()
            sig_viol = con.execute(
                "SELECT count(*) FILTER (WHERE fac_compliance_status='Significant Violation') "
                "FROM spine").fetchone()[0]
            # Presence-flag integrity (G2.4): has_<program> count == distinct RID in crosswalk ∩ spine.
            pres_counts = {}
            for prog in ("npdes", "rcra", "sdwa", "air", "enforcement"):
                spine_flag = con.execute(
                    f"SELECT count(*) FILTER (WHERE has_{prog}) FROM spine").fetchone()[0]
                xw_in_spine = con.execute(
                    f"SELECT count(*) FROM pres_{prog} x WHERE EXISTS "
                    f"(SELECT 1 FROM spine s WHERE s.registry_id = x.registry_id)").fetchone()[0]
                pres_counts[prog] = {"spine_flag": spine_flag, "xw_in_spine": xw_in_spine,
                                     "ok": spine_flag == xw_in_spine}
            gates = {
                "G2.1_row_floor": {"rows": total, "floor": 1_500_000, "ok": total >= 1_500_000},
                "G2.2_pk_unique": {"rows": total, "distinct": distinct_rid, "ok": total == distinct_rid},
                "G2.3_no_null_key": {"null_rid": null_rid, "ok": null_rid == 0},
                "G2.4_presence_integrity": pres_counts,
                "G2.5_sig_violation": {"count": sig_viol, "recon": SPINE_SIG_VIOLATION_RECON,
                                       "ok": abs(sig_viol - SPINE_SIG_VIOLATION_RECON)
                                       <= 0.01 * SPINE_SIG_VIOLATION_RECON},
            }
            if total != distinct_rid:
                raise RuntimeError(f"G2.2 PK uniqueness FAILED: {total} rows, {distinct_rid} distinct")
            if null_rid != 0:
                raise RuntimeError(f"G2.3 null key FAILED: {null_rid} null registry_id")
            if total < 1_500_000:
                raise RuntimeError(f"G2.1 row floor FAILED: {total} < 1,500,000")
            if not all(v["ok"] for v in pres_counts.values()):
                raise RuntimeError(f"G2.4 presence-flag integrity FAILED: {pres_counts}")
            if not gates["G2.5_sig_violation"]["ok"]:
                raise RuntimeError(f"G2.5 sig-violation anti-regression FAILED: {gates['G2.5_sig_violation']}")

            table = con.execute("SELECT * FROM spine ORDER BY registry_id").fetch_arrow_table()
        finally:
            con.close()

        # Wide list-bearing rows (naics/sic/program_acronyms) blow Lance's per-chunk BYTE cap at
        # 100k rows/chunk → smaller chunks keep each batch under the limit.
        reader = pa.Table.to_reader(table, max_chunksize=16_384)
        pub = _write_local_and_publish(name, reader, SPINE_BTREE, SPINE_BITMAP)
        built = pub["built"]
        rows = pub["rows"]

        # Provenance metadata AFTER the streaming write (the metadata-drop lesson).
        try:
            ds = lance.dataset(uri, storage_options=so)
            src_meta = {
                "source": "epa_facilities,epa_echo_exporter,epa_frs_naics_codes,"
                          "epa_frs_sic_codes,crosswalk_epa_registry_program+5",
                "spine_built_run_id": run_id,
            }
            existing = {(k.decode() if isinstance(k, bytes) else k):
                        (v.decode() if isinstance(v, bytes) else v)
                        for k, v in (ds.schema.metadata or {}).items()}
            ds.replace_schema_metadata({**existing, **src_meta})
            gates["G2.8_provenance"] = True
        except Exception as mexc:  # noqa: BLE001
            print(f"[{name}] provenance metadata write hiccup: {mexc}")
            gates["G2.8_provenance"] = f"hiccup: {mexc}"
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[{name}] FAILED: {error}")
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, phase="spine", artifact=name, dataset_uri=uri,
                    grain="1/REGISTRY_ID", rows_written=int(rows),
                    indices_built=",".join(built), gates=gates, status=status, error=error,
                    started_at=started, completed_at=completed)

    return {"artifact": name, "uri": uri, "rows": int(rows), "status": status,
            "indices": built, "gates": gates, "error": error}


# =========================================================================== #
# PHASE 3 — Per-program rollups (ride the REGISTRY_ID BTREE; 1 row / REGISTRY_ID)
# =========================================================================== #
def _set_provenance(uri: str, so: dict, meta: dict) -> None:
    """Write str-keyed provenance into the Lance schema metadata (post-write)."""
    import lance

    ds = lance.dataset(uri, storage_options=so)
    existing = {(k.decode() if isinstance(k, bytes) else k):
                (v.decode() if isinstance(v, bytes) else v)
                for k, v in (ds.schema.metadata or {}).items()}
    ds.replace_schema_metadata({**existing, **meta})


def _register_crosswalk_map(con, xw_name: str, key_col: str, so: dict) -> None:
    """Register a per-program crosswalk → temp `xmap(k, registry_id)` (string key → RID)."""
    import lance

    rdr = lance.dataset(ACTIVE_BASE + xw_name + "/", storage_options=so).scanner(
        columns=["REGISTRY_ID", key_col]).to_reader()
    con.register("_xw", rdr)
    con.execute(f"""
        CREATE TEMP TABLE xmap AS
        SELECT DISTINCT
            nullif(trim(CAST({key_col} AS VARCHAR)),'') AS k,
            nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') AS registry_id
        FROM _xw
        WHERE nullif(trim(CAST({key_col} AS VARCHAR)),'') IS NOT NULL
          AND nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') IS NOT NULL
    """)
    con.unregister("_xw")


def _register_detail(con, ds_name: str, cols: list[str], alias: str, so: dict) -> None:
    import lance

    rdr = lance.dataset(ACTIVE_BASE + ds_name + "/", storage_options=so).scanner(
        columns=cols).to_reader()
    con.register(alias, rdr)


def _finalize_rollup(con, name: str, btree: list[str], bitmap: list[str], so: dict,
                     run_id: str, sources: str, gates: dict, started,
                     recon: dict | None = None) -> dict:
    """Common tail: PK/null/⊆-spine gate, optional reconciliation gate, write local + publish,
    provenance, ledger row. `recon` (when set: {col, target, tol, gate}) asserts sum(col) ≈ target
    within tol on the PERSISTED rollup — the post-spine-join table actually written, not an
    intermediate."""
    import datetime as dt

    import pyarrow as pa

    uri = ACTIVE_BASE + name + "/"
    # rollup ⊆ spine (G3.3): the spine (built from the FRS master) is the resolvable universe.
    # A crosswalk RID sourced outside FRS (e.g. epa_case_facilities references facilities absent
    # from the current FRS download — verified 58,021 such non-FRS RIDs for enforcement) cannot
    # exist in the dimension. Inner-join the rollup to the spine RID set so the rollup carries
    # ONLY resolvable facilities; record the dropped non-FRS count for transparency.
    _register_detail(con, SPINE_NAME, ["registry_id"], "_spine", so)
    con.execute("CREATE TEMP TABLE spine_rid AS SELECT DISTINCT registry_id FROM _spine")
    con.unregister("_spine")
    pre_rows = con.execute("SELECT count(*) FROM roll").fetchone()[0]
    non_frs = con.execute(
        "SELECT count(*) FROM roll r WHERE NOT EXISTS "
        "(SELECT 1 FROM spine_rid s WHERE s.registry_id = r.registry_id)").fetchone()[0]
    if non_frs:
        con.execute("CREATE TEMP TABLE roll2 AS SELECT r.* FROM roll r "
                    "WHERE EXISTS (SELECT 1 FROM spine_rid s WHERE s.registry_id = r.registry_id)")
        con.execute("DROP TABLE roll")
        con.execute("ALTER TABLE roll2 RENAME TO roll")
    total, distinct_rid, null_rid = con.execute(
        "SELECT count(*), count(DISTINCT registry_id), "
        "count(*) FILTER (WHERE registry_id IS NULL) FROM roll").fetchone()
    orphans = con.execute(
        "SELECT count(*) FROM roll r WHERE NOT EXISTS "
        "(SELECT 1 FROM spine_rid s WHERE s.registry_id = r.registry_id)").fetchone()[0]
    gates["G3.2_grain"] = {"rows": total, "distinct": distinct_rid, "ok": total == distinct_rid}
    gates["G3.3_subset_of_spine"] = {"orphans": orphans, "non_frs_dropped": non_frs,
                                     "rows_pre_filter": pre_rows, "ok": orphans == 0}
    gates["G3.x_no_null_key"] = {"null_rid": null_rid, "ok": null_rid == 0}
    if total != distinct_rid:
        raise RuntimeError(f"G3.2 grain FAILED: {total} rows {distinct_rid} distinct")
    if null_rid != 0:
        raise RuntimeError(f"G3.x null registry_id: {null_rid}")
    if orphans != 0:
        raise RuntimeError(f"G3.3 rollup⊄spine FAILED: {orphans} orphan registry_ids")

    # Reconciliation gate (optional, per-rollup) — fires on the FINAL post-spine-join `roll`, the
    # exact table written below. Measuring the artifact that ships closes the false-pass window where
    # a pre-drop intermediate reconciled but the persisted rollup did not.
    if recon is not None:
        measured = con.execute(f"SELECT sum({recon['col']}) FROM roll").fetchone()[0] or 0
        measured = float(measured)
        ok = abs(measured - recon["target"]) <= recon["tol"] * recon["target"]
        gates[recon["gate"]] = {recon["col"]: measured, "recon": recon["target"],
                                "tol": recon["tol"], "ok": ok, "measured_on": "persisted_rollup"}
        if not ok:
            raise RuntimeError(f"{recon['gate']} FAILED (post-finalize): {gates[recon['gate']]}")

    table = con.execute("SELECT * FROM roll ORDER BY registry_id").fetch_arrow_table()
    reader = pa.Table.to_reader(table, max_chunksize=65_536)
    pub = _write_local_and_publish(name, reader, btree, bitmap)
    _set_provenance(uri, so, {"source": sources, "rollup_built_run_id": run_id})
    completed = dt.datetime.now(dt.timezone.utc)
    _record_run(run_id=run_id, phase="rollup", artifact=name, dataset_uri=uri,
                grain="1/REGISTRY_ID", rows_written=int(pub["rows"]),
                indices_built=",".join(pub["built"]), gates=gates, status="success",
                started_at=started, completed_at=completed)
    return {"artifact": name, "uri": uri, "rows": int(pub["rows"]), "status": "success",
            "indices": pub["built"], "gates": gates}


ROLLUP_BTREE = ["registry_id"]


@app.function(image=heavy_image, secrets=SECRETS, timeout=60 * 60 * 5,
              memory=65536, cpu=16.0, ephemeral_disk=524288, retries=1)
def build_rollup_npdes(run_id: str) -> dict:
    """rollup_epa_npdes — DMR 422M streaming aggregate (exceedances via VIOLATION_CODE),
    QNCR quarters-in-NC, inspections. Heavy profile; DMR read-only (floor re-asserted post)."""
    import datetime as dt

    import lance

    name = "rollup_epa_npdes"
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    gates: dict = {}
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    os.makedirs(SPILL_DIR, exist_ok=True)
    try:
        dmr_pre = lance.dataset(ACTIVE_BASE + "epa_npdes_dmrs/", storage_options=so).count_rows()
        con = _new_con(memory_limit="56GB", threads=16)
        try:
            _register_crosswalk_map(con, "crosswalk_epa_registry_npdes", "NPDES_ID", so)
            # DMR exceedance reach (G3.1): fraction of distinct EXTERNAL_PERMIT_NMBR resolving a RID.
            _register_detail(con, "epa_npdes_dmrs",
                             ["EXTERNAL_PERMIT_NMBR", "VIOLATION_CODE", "PARAMETER_CODE",
                              "MONITORING_PERIOD_END_DATE", "FISCAL_YEAR"], "_dmr", so)
            con.execute("""
                CREATE TEMP TABLE dmr_agg AS
                SELECT m.registry_id,
                       count(*) FILTER (WHERE d.viol IS NOT NULL)        AS dmr_exceedance_count,
                       count(DISTINCT d.permit)                          AS distinct_permits,
                       count(DISTINCT d.param) FILTER (WHERE d.viol IS NOT NULL)
                                                                         AS distinct_parameters_in_violation,
                       max(d.period_end) FILTER (WHERE d.viol IS NOT NULL) AS last_exceedance_period_end,
                       min(d.fy) AS first_dmr_fy, max(d.fy) AS last_dmr_fy
                FROM (
                    SELECT nullif(trim(CAST(EXTERNAL_PERMIT_NMBR AS VARCHAR)),'') AS permit,
                           nullif(trim(CAST(VIOLATION_CODE AS VARCHAR)),'')       AS viol,
                           nullif(trim(CAST(PARAMETER_CODE AS VARCHAR)),'')       AS param,
                           MONITORING_PERIOD_END_DATE AS period_end, FISCAL_YEAR AS fy
                    FROM _dmr
                ) d JOIN xmap m ON m.k = d.permit
                GROUP BY m.registry_id
            """)
            con.unregister("_dmr")

            # QNCR quarters-in-NC + inspections, joined via crosswalk (NPDES_ID) / inline RID.
            _register_detail(con, "epa_npdes_qncr_history",
                             ["NPDES_ID", "YEARQTR", "HLRNC", "NUME90Q", "NUMCVDT", "NUMSVCD"], "_qncr", so)
            con.execute("""
                CREATE TEMP TABLE qncr_agg AS
                SELECT m.registry_id,
                       count(*) FILTER (WHERE q.hlrnc IN ('S','D','E','T','X'))   AS qncr_quarters_in_nc,
                       max(q.yq) AS last_qncr_yearqtr
                FROM (
                    SELECT nullif(trim(CAST(NPDES_ID AS VARCHAR)),'') AS npdes_id,
                           nullif(trim(CAST(HLRNC AS VARCHAR)),'')    AS hlrnc,
                           CAST(YEARQTR AS VARCHAR) AS yq
                    FROM _qncr
                ) q JOIN xmap m ON m.k = q.npdes_id
                GROUP BY m.registry_id
            """)
            con.unregister("_qncr")
            _register_detail(con, "epa_npdes_inspections",
                             ["REGISTRY_ID", "NPDES_ID", "ACTUAL_END_DATE"], "_insp", so)
            con.execute("""
                CREATE TEMP TABLE insp_agg AS
                SELECT registry_id, count(*) AS inspection_count, max(end_date) AS last_inspection_date
                FROM (
                    SELECT coalesce(nullif(trim(CAST(i.REGISTRY_ID AS VARCHAR)),''), m.registry_id) AS registry_id,
                           i.ACTUAL_END_DATE AS end_date
                    FROM _insp i
                    LEFT JOIN xmap m ON m.k = nullif(trim(CAST(i.NPDES_ID AS VARCHAR)),'')
                ) WHERE registry_id IS NOT NULL
                GROUP BY registry_id
            """)
            con.unregister("_insp")

            # Reach gate: distinct DMR permits resolving a RID (re-scan permit-only is cheap vs full).
            _register_detail(con, "epa_npdes_dmrs", ["EXTERNAL_PERMIT_NMBR"], "_dmrp", so)
            denom, matched = con.execute("""
                WITH p AS (SELECT DISTINCT nullif(trim(CAST(EXTERNAL_PERMIT_NMBR AS VARCHAR)),'') AS permit
                           FROM _dmrp WHERE nullif(trim(CAST(EXTERNAL_PERMIT_NMBR AS VARCHAR)),'') IS NOT NULL)
                SELECT count(*), count(*) FILTER (WHERE EXISTS (SELECT 1 FROM xmap m WHERE m.k = p.permit)) FROM p
            """).fetchone()
            con.unregister("_dmrp")
            reach_pct = None if denom == 0 else matched / denom
            gates["G3.1_reach"] = {"denom": denom, "matched": matched, "reach_pct": reach_pct,
                                   "floor": 0.99, "ok": reach_pct is not None and reach_pct >= 0.99}

            # Assemble: union all RIDs, left join each agg.
            con.execute("""
                CREATE TEMP TABLE roll AS
                WITH rids AS (
                    SELECT registry_id FROM dmr_agg
                    UNION SELECT registry_id FROM qncr_agg
                    UNION SELECT registry_id FROM insp_agg
                )
                SELECT r.registry_id,
                       coalesce(d.dmr_exceedance_count,0)             AS dmr_exceedance_count,
                       coalesce(d.distinct_permits,0)                 AS distinct_permits,
                       coalesce(d.distinct_parameters_in_violation,0) AS distinct_parameters_in_violation,
                       d.last_exceedance_period_end,
                       coalesce(q.qncr_quarters_in_nc,0)              AS qncr_quarters_in_nc,
                       q.last_qncr_yearqtr,
                       coalesce(i.inspection_count,0)                 AS inspection_count,
                       i.last_inspection_date,
                       d.first_dmr_fy AS first_activity_year,
                       d.last_dmr_fy  AS last_activity_year,
                       (coalesce(d.dmr_exceedance_count,0) > 0)       AS has_dmr_exceedance,
                       CASE WHEN coalesce(q.qncr_quarters_in_nc,0) >= 4 THEN 'chronic'
                            WHEN coalesce(d.dmr_exceedance_count,0) > 0 THEN 'exceedance'
                            WHEN coalesce(i.inspection_count,0) > 0 THEN 'monitored'
                            ELSE 'clean' END                          AS npdes_compliance_tier
                FROM rids r
                LEFT JOIN dmr_agg  d USING (registry_id)
                LEFT JOIN qncr_agg q USING (registry_id)
                LEFT JOIN insp_agg i USING (registry_id)
            """)
            if not gates["G3.1_reach"]["ok"]:
                raise RuntimeError(f"G3.1 NPDES reach FAILED: {gates['G3.1_reach']}")
            out = _finalize_rollup(
                con, name, ROLLUP_BTREE, ["has_dmr_exceedance", "npdes_compliance_tier"], so,
                run_id, "epa_npdes_dmrs,epa_npdes_qncr_history,epa_npdes_inspections", gates, started)
        finally:
            con.close()
        # G3.7 — DMR untouched (count unchanged, >= floor).
        dmr_post = lance.dataset(ACTIVE_BASE + "epa_npdes_dmrs/", storage_options=so).count_rows()
        out["gates"]["G3.7_dmr_untouched"] = {"pre": dmr_pre, "post": dmr_post,
                                              "ok": dmr_post == dmr_pre and dmr_post >= DMR_HISTORICAL_FLOOR}
        if dmr_post != dmr_pre or dmr_post < DMR_HISTORICAL_FLOOR:
            raise RuntimeError(f"G3.7 DMR mutated/under-floor: pre={dmr_pre} post={dmr_post}")
        out["reach_pct"] = gates["G3.1_reach"]["reach_pct"]
        return out
    except Exception as exc:  # noqa: BLE001
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, phase="rollup", artifact=name,
                    dataset_uri=ACTIVE_BASE + name + "/", grain="1/REGISTRY_ID", gates=gates,
                    status="error", error=str(exc), started_at=started, completed_at=completed)
        print(f"[{name}] FAILED: {exc}")
        return {"artifact": name, "status": "error", "error": str(exc)}


@app.function(secrets=SECRETS, timeout=60 * 120, memory=49152, cpu=8.0)
def build_rollup_rcra(run_id: str) -> dict:
    """rollup_epa_rcra — violations, evaluations, enforcements (penalty $), viosnc SNC flags."""
    import datetime as dt

    name = "rollup_epa_rcra"
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    gates: dict = {}
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    try:
        con = _new_con()
        try:
            _register_crosswalk_map(con, "crosswalk_epa_registry_rcra", "ID_NUMBER", so)
            _register_detail(con, "epa_rcra_violations",
                             ["ID_NUMBER", "VIOLATION_TYPE", "DATE_VIOLATION_DETERMINED",
                              "ACTUAL_RTC_DATE"], "_v", so)
            con.execute("""
                CREATE TEMP TABLE v_agg AS
                SELECT m.registry_id,
                       count(*) AS rcra_violation_count,
                       count(*) FILTER (WHERE rtc IS NULL) AS rcra_open_violation_count,
                       max(det) AS last_violation_date
                FROM (
                    SELECT nullif(trim(CAST(ID_NUMBER AS VARCHAR)),'') AS idn,
                           nullif(trim(CAST(ACTUAL_RTC_DATE AS VARCHAR)),'') AS rtc,
                           try_strptime(nullif(trim(CAST(DATE_VIOLATION_DETERMINED AS VARCHAR)),''),'%m/%d/%Y') AS det
                    FROM _v
                ) x JOIN xmap m ON m.k = x.idn
                GROUP BY m.registry_id
            """)
            con.unregister("_v")
            _register_detail(con, "epa_rcra_evaluations",
                             ["ID_NUMBER", "EVALUATION_START_DATE", "FOUND_VIOLATION"], "_e", so)
            con.execute("""
                CREATE TEMP TABLE e_agg AS
                SELECT m.registry_id, count(*) AS rcra_evaluation_count,
                       max(ev) AS last_evaluation_date
                FROM (
                    SELECT nullif(trim(CAST(ID_NUMBER AS VARCHAR)),'') AS idn,
                           try_strptime(nullif(trim(CAST(EVALUATION_START_DATE AS VARCHAR)),''),'%m/%d/%Y') AS ev
                    FROM _e
                ) x JOIN xmap m ON m.k = x.idn
                GROUP BY m.registry_id
            """)
            con.unregister("_e")
            _register_detail(con, "epa_rcra_enforcements",
                             ["ID_NUMBER", "PMP_AMOUNT", "FMP_AMOUNT", "ENFORCEMENT_ACTION_DATE"], "_f", so)
            # Money in DOUBLE at line level; suppressed counter for null-money rows.
            con.execute("""
                CREATE TEMP TABLE f_agg AS
                SELECT m.registry_id,
                       count(*) AS rcra_enforcement_count,
                       sum(coalesce(pmp,0) + coalesce(fmp,0)) AS rcra_penalty_total_dbl,
                       count(*) FILTER (WHERE pmp IS NULL AND fmp IS NULL) AS rcra_penalty_lines_suppressed,
                       max(ed) AS last_enforcement_date
                FROM (
                    SELECT nullif(trim(CAST(ID_NUMBER AS VARCHAR)),'') AS idn,
                           TRY_CAST(replace(replace(nullif(trim(CAST(PMP_AMOUNT AS VARCHAR)),''),'$',''),',','') AS DOUBLE) AS pmp,
                           TRY_CAST(replace(replace(nullif(trim(CAST(FMP_AMOUNT AS VARCHAR)),''),'$',''),',','') AS DOUBLE) AS fmp,
                           try_strptime(nullif(trim(CAST(ENFORCEMENT_ACTION_DATE AS VARCHAR)),''),'%m/%d/%Y') AS ed
                    FROM _f
                ) x JOIN xmap m ON m.k = x.idn
                GROUP BY m.registry_id
            """)
            con.unregister("_f")
            _register_detail(con, "epa_rcra_viosnc_history",
                             ["ID_NUMBER", "VIO_FLAG", "SNC_FLAG"], "_s", so)
            con.execute("""
                CREATE TEMP TABLE s_agg AS
                SELECT m.registry_id,
                       bool_or(snc) AS rcra_snc_flag, bool_or(vio) AS has_rcra_violation_hist
                FROM (
                    SELECT nullif(trim(CAST(ID_NUMBER AS VARCHAR)),'') AS idn,
                           upper(trim(CAST(SNC_FLAG AS VARCHAR))) IN ('Y','1','TRUE') AS snc,
                           upper(trim(CAST(VIO_FLAG AS VARCHAR))) IN ('Y','1','TRUE') AS vio
                    FROM _s
                ) x JOIN xmap m ON m.k = x.idn
                GROUP BY m.registry_id
            """)
            con.unregister("_s")
            con.execute("""
                CREATE TEMP TABLE roll AS
                WITH rids AS (
                    SELECT registry_id FROM v_agg UNION SELECT registry_id FROM e_agg
                    UNION SELECT registry_id FROM f_agg UNION SELECT registry_id FROM s_agg
                )
                SELECT r.registry_id,
                       coalesce(v.rcra_violation_count,0)       AS rcra_violation_count,
                       coalesce(v.rcra_open_violation_count,0)  AS rcra_open_violation_count,
                       v.last_violation_date,
                       coalesce(e.rcra_evaluation_count,0)      AS rcra_evaluation_count,
                       e.last_evaluation_date,
                       coalesce(f.rcra_enforcement_count,0)     AS rcra_enforcement_count,
                       CAST(coalesce(f.rcra_penalty_total_dbl,0) AS DECIMAL(18,2)) AS rcra_penalty_total,
                       coalesce(f.rcra_penalty_lines_suppressed,0) AS rcra_penalty_lines_suppressed,
                       f.last_enforcement_date,
                       coalesce(s.rcra_snc_flag,false)          AS rcra_snc_flag,
                       (coalesce(v.rcra_violation_count,0) > 0 OR coalesce(s.has_rcra_violation_hist,false))
                                                                AS has_rcra_violation
                FROM rids r
                LEFT JOIN v_agg v USING (registry_id)
                LEFT JOIN e_agg e USING (registry_id)
                LEFT JOIN f_agg f USING (registry_id)
                LEFT JOIN s_agg s USING (registry_id)
            """)
            neg = con.execute("SELECT count(*) FROM roll WHERE rcra_penalty_total < 0 "
                              "OR rcra_violation_count < 0").fetchone()[0]
            gates["G3.6_no_negatives"] = {"neg": neg, "ok": neg == 0}
            if neg:
                raise RuntimeError(f"G3.6 negative aggregate: {neg}")
            return _finalize_rollup(
                con, name, ROLLUP_BTREE, ["rcra_snc_flag", "has_rcra_violation"], so, run_id,
                "epa_rcra_violations,epa_rcra_evaluations,epa_rcra_enforcements,epa_rcra_viosnc_history",
                gates, started)
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, phase="rollup", artifact=name,
                    dataset_uri=ACTIVE_BASE + name + "/", grain="1/REGISTRY_ID", gates=gates,
                    status="error", error=str(exc), started_at=started, completed_at=completed)
        print(f"[{name}] FAILED: {exc}")
        return {"artifact": name, "status": "error", "error": str(exc)}


@app.function(secrets=SECRETS, timeout=60 * 120, memory=49152, cpu=8.0)
def build_rollup_sdwa(run_id: str) -> dict:
    """rollup_epa_sdwa — violations_enforcement (health-based, status) + pub_water_systems
    (population served, system type). Population-served Σ is a signal floor (466.9M ±1%)."""
    import datetime as dt

    name = "rollup_epa_sdwa"
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    gates: dict = {}
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    try:
        con = _new_con()
        try:
            _register_crosswalk_map(con, "crosswalk_epa_registry_sdwa", "PWSID", so)
            _register_detail(con, "epa_sdwa_pub_water_systems",
                             ["PWSID", "POPULATION_SERVED_COUNT", "PWS_TYPE_CODE",
                              "PWS_ACTIVITY_CODE"], "_pws", so)
            # One row per PWSID in source (submissionyearquarter snapshot) → dedupe to max pop per PWSID.
            con.execute("""
                CREATE TEMP TABLE pws_agg AS
                SELECT m.registry_id,
                       sum(pop) AS population_served,
                       max(ptype) AS pws_type,
                       count(DISTINCT pwsid) AS distinct_pws,
                       bool_or(active) AS has_active_pws
                FROM (
                    SELECT pwsid, max(pop) AS pop, any_value(ptype) AS ptype, bool_or(active) AS active
                    FROM (
                        SELECT nullif(trim(CAST(PWSID AS VARCHAR)),'') AS pwsid,
                               TRY_CAST(nullif(trim(CAST(POPULATION_SERVED_COUNT AS VARCHAR)),'') AS BIGINT) AS pop,
                               nullif(trim(CAST(PWS_TYPE_CODE AS VARCHAR)),'') AS ptype,
                               upper(trim(CAST(PWS_ACTIVITY_CODE AS VARCHAR))) = 'A' AS active
                        FROM _pws
                    ) WHERE pwsid IS NOT NULL GROUP BY pwsid
                ) p JOIN xmap m ON m.k = p.pwsid
                GROUP BY m.registry_id
            """)
            con.unregister("_pws")
            _register_detail(con, "epa_sdwa_violations_enforcement",
                             ["PWSID", "IS_HEALTH_BASED_IND", "VIOLATION_STATUS",
                              "IS_MAJOR_VIOL_IND", "ENFORCEMENT_DATE"], "_ve", so)
            con.execute("""
                CREATE TEMP TABLE ve_agg AS
                SELECT m.registry_id,
                       count(*) FILTER (WHERE viol_id IS NOT NULL) AS sdwa_violation_rows,
                       bool_or(hb) AS has_health_based_violation,
                       count(*) FILTER (WHERE status IN ('Unaddressed','Addressed','Open')) AS sdwa_open_violations,
                       max(enf) AS last_enforcement_date
                FROM (
                    SELECT nullif(trim(CAST(PWSID AS VARCHAR)),'') AS pwsid,
                           PWSID AS viol_id,
                           upper(trim(CAST(IS_HEALTH_BASED_IND AS VARCHAR))) = 'Y' AS hb,
                           nullif(trim(CAST(VIOLATION_STATUS AS VARCHAR)),'') AS status,
                           try_strptime(nullif(trim(CAST(ENFORCEMENT_DATE AS VARCHAR)),''),'%m/%d/%Y') AS enf
                    FROM _ve
                ) x JOIN xmap m ON m.k = x.pwsid
                GROUP BY m.registry_id
            """)
            con.unregister("_ve")
            con.execute("""
                CREATE TEMP TABLE roll AS
                WITH rids AS (SELECT registry_id FROM pws_agg UNION SELECT registry_id FROM ve_agg)
                SELECT r.registry_id,
                       coalesce(p.population_served,0) AS population_served,
                       p.pws_type, coalesce(p.distinct_pws,0) AS distinct_pws,
                       coalesce(p.has_active_pws,false) AS has_active_pws,
                       coalesce(v.sdwa_violation_rows,0) AS sdwa_violation_rows,
                       coalesce(v.has_health_based_violation,false) AS has_health_based_violation,
                       coalesce(v.sdwa_open_violations,0) AS sdwa_open_violations,
                       v.last_enforcement_date
                FROM rids r
                LEFT JOIN pws_agg p USING (registry_id)
                LEFT JOIN ve_agg  v USING (registry_id)
            """)
            pop_total = con.execute("SELECT sum(population_served) FROM roll").fetchone()[0] or 0
            gates["G3.4_population_floor"] = {
                "population_served": int(pop_total), "recon": 466_900_000,
                "ok": abs(pop_total - 466_900_000) <= 0.01 * 466_900_000}
            if not gates["G3.4_population_floor"]["ok"]:
                raise RuntimeError(f"G3.4 SDWA population floor FAILED: {gates['G3.4_population_floor']}")
            return _finalize_rollup(
                con, name, ROLLUP_BTREE, ["has_health_based_violation", "pws_type"], so, run_id,
                "epa_sdwa_pub_water_systems,epa_sdwa_violations_enforcement", gates, started)
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, phase="rollup", artifact=name,
                    dataset_uri=ACTIVE_BASE + name + "/", grain="1/REGISTRY_ID", gates=gates,
                    status="error", error=str(exc), started_at=started, completed_at=completed)
        print(f"[{name}] FAILED: {exc}")
        return {"artifact": name, "status": "error", "error": str(exc)}


@app.function(secrets=SECRETS, timeout=60 * 120, memory=49152, cpu=8.0)
def build_rollup_air(run_id: str) -> dict:
    """rollup_epa_air — icis_air violation history (HPV) joined via crosswalk (PGM_SYS_ID)."""
    import datetime as dt

    name = "rollup_epa_air"
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    gates: dict = {}
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    try:
        con = _new_con()
        try:
            _register_crosswalk_map(con, "crosswalk_epa_registry_air", "PGM_SYS_ID", so)
            _register_detail(con, "epa_icis_air_violation_history",
                             ["PGM_SYS_ID", "HPV_DAYZERO_DATE", "HPV_RESOLVED_DATE",
                              "EARLIEST_FRV_DETERM_DATE"], "_vh", so)
            con.execute("""
                CREATE TEMP TABLE vh_agg AS
                SELECT m.registry_id,
                       count(*) AS air_violation_history_rows,
                       bool_or(hpv0 IS NOT NULL) AS caa_hpv_flag,
                       count(*) FILTER (WHERE hpv0 IS NOT NULL AND hpvr IS NULL) AS air_open_hpv_count,
                       max(frv) AS last_frv_determination_date
                FROM (
                    SELECT nullif(trim(CAST(PGM_SYS_ID AS VARCHAR)),'') AS pid,
                           nullif(trim(CAST(HPV_DAYZERO_DATE AS VARCHAR)),'') AS hpv0,
                           nullif(trim(CAST(HPV_RESOLVED_DATE AS VARCHAR)),'') AS hpvr,
                           try_strptime(nullif(trim(CAST(EARLIEST_FRV_DETERM_DATE AS VARCHAR)),''),'%m/%d/%Y') AS frv
                    FROM _vh
                ) x JOIN xmap m ON m.k = x.pid
                GROUP BY m.registry_id
            """)
            con.unregister("_vh")
            # AIR reach (G3.1): 100% of distinct violation-history PGM_SYS_ID should resolve.
            _register_detail(con, "epa_icis_air_violation_history", ["PGM_SYS_ID"], "_vp", so)
            denom, matched = con.execute("""
                WITH p AS (SELECT DISTINCT nullif(trim(CAST(PGM_SYS_ID AS VARCHAR)),'') AS pid
                           FROM _vp WHERE nullif(trim(CAST(PGM_SYS_ID AS VARCHAR)),'') IS NOT NULL)
                SELECT count(*), count(*) FILTER (WHERE EXISTS (SELECT 1 FROM xmap m WHERE m.k = p.pid)) FROM p
            """).fetchone()
            con.unregister("_vp")
            reach_pct = None if denom == 0 else matched / denom
            gates["G3.1_reach"] = {"denom": denom, "matched": matched, "reach_pct": reach_pct,
                                   "floor": 0.99, "ok": reach_pct is not None and reach_pct >= 0.99}
            con.execute("""
                CREATE TEMP TABLE roll AS
                SELECT registry_id,
                       air_violation_history_rows,
                       coalesce(caa_hpv_flag,false) AS caa_hpv_flag,
                       coalesce(air_open_hpv_count,0) AS air_open_hpv_count,
                       last_frv_determination_date,
                       (air_violation_history_rows > 0) AS has_air_violation
                FROM vh_agg
            """)
            if not gates["G3.1_reach"]["ok"]:
                raise RuntimeError(f"G3.1 AIR reach FAILED: {gates['G3.1_reach']}")
            out = _finalize_rollup(
                con, name, ROLLUP_BTREE, ["caa_hpv_flag", "has_air_violation"], so, run_id,
                "epa_icis_air_violation_history", gates, started)
            out["reach_pct"] = reach_pct
            return out
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, phase="rollup", artifact=name,
                    dataset_uri=ACTIVE_BASE + name + "/", grain="1/REGISTRY_ID", gates=gates,
                    status="error", error=str(exc), started_at=started, completed_at=completed)
        print(f"[{name}] FAILED: {exc}")
        return {"artifact": name, "status": "error", "error": str(exc)}


@app.function(secrets=SECRETS, timeout=60 * 120, memory=49152, cpu=8.0)
def build_rollup_enforcement(run_id: str) -> dict:
    """rollup_epa_enforcement — case_enforcements + case_penalties (FED_PENALTY $16.36B signal),
    joined via crosswalk (ACTIVITY_ID). Facility-attributed sums (many-to-many collapsed)."""
    import datetime as dt

    name = "rollup_epa_enforcement"
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    gates: dict = {}
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    try:
        con = _new_con()
        try:
            _register_crosswalk_map(con, "crosswalk_epa_registry_enforcement", "ACTIVITY_ID", so)
            # Constrain the enforcement edge to spine-resident facilities BEFORE attribution. The
            # enforcement crosswalk is built from the full epa_case_facilities edge (no spine filter),
            # so it carries 58,021 non-FRS REGISTRY_IDs the current FRS download cannot resolve.
            # _finalize_rollup inner-joins the rollup to the spine and DROPS those RIDs — but the
            # equal-split below divides each case penalty by the facility count. If that count includes
            # non-FRS facilities, their penalty shares are computed and then dropped with no
            # redistribution, stranding ~$5.98B of federal penalty mass (persisted sum collapses from
            # $16.36B to ~$10.38B). Filtering the edge to the spine HERE makes the split denominator and
            # the surviving facilities the same set, so a case's surviving facilities absorb its FULL
            # penalty. The persisted total = the MASS-CONSERVING attributable signal: Σ FED_PENALTY over
            # cases with >=1 spine-resident facility (~$13.84B), NOT $16.36B — ~$2.53B of penalties
            # (26,669 all-non-FRS cases) are correctly unattributable ($13.84B + $2.53B = $16.36B granular).
            _register_detail(con, SPINE_NAME, ["registry_id"], "_spine", so)
            con.execute("CREATE TEMP TABLE enf_spine_rid AS SELECT DISTINCT registry_id FROM _spine")
            con.unregister("_spine")
            non_frs_edge = con.execute(
                "SELECT count(DISTINCT registry_id) FROM xmap x WHERE NOT EXISTS "
                "(SELECT 1 FROM enf_spine_rid s WHERE s.registry_id = x.registry_id)").fetchone()[0]
            con.execute("DELETE FROM xmap WHERE NOT EXISTS "
                        "(SELECT 1 FROM enf_spine_rid s WHERE s.registry_id = xmap.registry_id)")
            gates["G3.4a_non_frs_edge_excluded"] = {"non_frs_registry_ids": non_frs_edge}
            # Facility count per case (ACTIVITY_ID) — the enforcement edge is many-to-many (plan D5,
            # avg 1.21 / max 834 facilities per case). A naive join multiplies each case penalty across
            # every facility on the case (→ $35.6B, 2.17x the granular $16.36B). Correct facility
            # attribution splits each case penalty EQUALLY across its distinct (spine-resident)
            # facilities, so the summed facility-level total reconciles to the granular FED_PENALTY
            # signal (plan D5: "a case-level penalty total is NOT a facility-level total").
            con.execute("CREATE TEMP TABLE fac_per_case AS "
                        "SELECT k AS aid, count(DISTINCT registry_id) AS nf FROM xmap GROUP BY k")
            # Penalties: FED_PENALTY is STRING in the mirror → TRY_CAST money to DOUBLE; suppressed counter.
            # De-dupe penalties to (aid, case_number) grain first (raw table is at that grain).
            _register_detail(con, "epa_case_penalties",
                             ["ACTIVITY_ID", "CASE_NUMBER", "FED_PENALTY", "ST_LCL_PENALTY"], "_p", so)
            con.execute("""
                CREATE TEMP TABLE p_agg AS
                SELECT m.registry_id,
                       sum(coalesce(pc.fed,0) / fpc.nf) AS fed_penalty_dbl,
                       sum(coalesce(pc.stl,0) / fpc.nf) AS st_lcl_penalty_dbl,
                       sum(CASE WHEN pc.fed IS NULL THEN 1 ELSE 0 END) AS penalty_lines_suppressed,
                       sum(CASE WHEN pc.fed IS NOT NULL AND pc.fed > 0 THEN 1 ELSE 0 END) AS penalty_line_count
                FROM (
                    SELECT aid, cn, any_value(fed) AS fed, any_value(stl) AS stl
                    FROM (
                        SELECT nullif(trim(CAST(ACTIVITY_ID AS VARCHAR)),'') AS aid,
                               nullif(trim(CAST(CASE_NUMBER AS VARCHAR)),'') AS cn,
                               TRY_CAST(replace(replace(nullif(trim(CAST(FED_PENALTY AS VARCHAR)),''),'$',''),',','') AS DOUBLE) AS fed,
                               TRY_CAST(replace(replace(nullif(trim(CAST(ST_LCL_PENALTY AS VARCHAR)),''),'$',''),',','') AS DOUBLE) AS stl
                        FROM _p
                    ) WHERE aid IS NOT NULL GROUP BY aid, cn
                ) pc
                JOIN fac_per_case fpc ON fpc.aid = pc.aid
                JOIN xmap m ON m.k = pc.aid
                GROUP BY m.registry_id
            """)
            # G3.4 mass-conservation target, recomputed from source each run (tracks data refreshes, not
            # a brittle hardcode). _p above is a ONE-SHOT reader already consumed by p_agg, so register a
            # FRESH penalty reader (_pg). The attributable total = Σ FED_PENALTY over de-duped (aid,case)
            # rows for cases with >=1 spine-resident facility (JOIN fac_per_case — the exact edge the
            # rollup splits over). The persisted rollup must CONSERVE this mass: a broken denominator or
            # facility map would make the split-resummed rollup diverge from this raw attributable sum.
            _register_detail(con, "epa_case_penalties",
                             ["ACTIVITY_ID", "CASE_NUMBER", "FED_PENALTY"], "_pg", so)
            enf_recon_target = con.execute("""
                SELECT coalesce(sum(d.fed), 0) FROM (
                    SELECT aid, cn, any_value(fed) AS fed FROM (
                        SELECT nullif(trim(CAST(ACTIVITY_ID AS VARCHAR)),'') AS aid,
                               nullif(trim(CAST(CASE_NUMBER AS VARCHAR)),'') AS cn,
                               TRY_CAST(replace(replace(nullif(trim(CAST(FED_PENALTY AS VARCHAR)),''),'$',''),',','') AS DOUBLE) AS fed
                        FROM _pg
                    ) WHERE aid IS NOT NULL GROUP BY aid, cn
                ) d JOIN fac_per_case fpc ON fpc.aid = d.aid
            """).fetchone()[0] or 0
            con.unregister("_pg")
            con.unregister("_p")
            _register_detail(con, "epa_case_enforcements",
                             ["ACTIVITY_ID", "FISCAL_YEAR", "ACTIVITY_STATUS_DATE",
                              "TOTAL_PENALTY_ASSESSED_AMT"], "_c", so)
            con.execute("""
                CREATE TEMP TABLE c_agg AS
                SELECT m.registry_id,
                       count(DISTINCT aid) AS federal_case_count,
                       min(fy) AS first_case_year, max(fy) AS last_case_year,
                       max(sd) AS last_case_status_date
                FROM (
                    SELECT nullif(trim(CAST(ACTIVITY_ID AS VARCHAR)),'') AS aid,
                           FISCAL_YEAR AS fy, ACTIVITY_STATUS_DATE AS sd
                    FROM _c
                ) x JOIN xmap m ON m.k = x.aid
                GROUP BY m.registry_id
            """)
            con.unregister("_c")
            con.execute("""
                CREATE TEMP TABLE roll AS
                WITH rids AS (SELECT registry_id FROM p_agg UNION SELECT registry_id FROM c_agg)
                SELECT r.registry_id,
                       coalesce(c.federal_case_count,0) AS federal_case_count,
                       CAST(coalesce(p.fed_penalty_dbl,0) AS DECIMAL(18,2)) AS fed_penalty_total,
                       CAST(coalesce(p.st_lcl_penalty_dbl,0) AS DECIMAL(18,2)) AS st_lcl_penalty_total,
                       coalesce(p.penalty_line_count,0) AS penalty_line_count,
                       coalesce(p.penalty_lines_suppressed,0) AS penalty_lines_suppressed,
                       c.first_case_year, c.last_case_year, c.last_case_status_date,
                       (coalesce(c.federal_case_count,0) > 0) AS has_federal_case,
                       (coalesce(p.fed_penalty_dbl,0) > 0) AS has_penalty
                FROM rids r
                LEFT JOIN p_agg p USING (registry_id)
                LEFT JOIN c_agg c USING (registry_id)
            """)
            bad_year = con.execute(
                "SELECT count(*) FROM roll WHERE last_case_year < first_case_year").fetchone()[0]
            gates["G3.6_year_order"] = {"bad": bad_year, "ok": bad_year == 0}
            if bad_year:
                raise RuntimeError(f"G3.6 year order FAILED: {bad_year}")
            # G3.4 fires INSIDE _finalize_rollup, on the post-spine-join rollup actually written to R2 —
            # never the pre-drop `roll` (which summed to a phantom $16.36B the spine drop then eroded;
            # gating the persisted artifact closed that false-pass window). The persisted total must
            # equal the recomputed attributable mass (enf_recon_target, ~$13.84B) within 1%.
            return _finalize_rollup(
                con, name, ROLLUP_BTREE, ["has_federal_case", "has_penalty"], so, run_id,
                "epa_case_enforcements,epa_case_penalties", gates, started,
                recon={"col": "fed_penalty_total", "target": enf_recon_target, "tol": 0.01,
                       "gate": "G3.4_fed_penalty_floor"})
        finally:
            con.close()
    except Exception as exc:  # noqa: BLE001
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, phase="rollup", artifact=name,
                    dataset_uri=ACTIVE_BASE + name + "/", grain="1/REGISTRY_ID", gates=gates,
                    status="error", error=str(exc), started_at=started, completed_at=completed)
        print(f"[{name}] FAILED: {exc}")
        return {"artifact": name, "status": "error", "error": str(exc)}


ROLLUP_FNS = {
    "rollup_epa_npdes": build_rollup_npdes,
    "rollup_epa_rcra": build_rollup_rcra,
    "rollup_epa_sdwa": build_rollup_sdwa,
    "rollup_epa_air": build_rollup_air,
    "rollup_epa_enforcement": build_rollup_enforcement,
}


@app.function(secrets=SECRETS, timeout=60 * 60 * 6, memory=2048)
def build_rollups(run_id: str, only: list[str] | None = None) -> dict:
    """Fan out the 5 rollups (each its own container; NPDES on the heavy profile)."""
    _ensure_ops_ledger()
    names = only or list(ROLLUP_FNS.keys())
    calls = [(n, ROLLUP_FNS[n].spawn(run_id)) for n in names]
    results = []
    for nm, call in calls:
        try:
            results.append(call.get())
        except Exception as exc:  # noqa: BLE001
            results.append({"artifact": nm, "status": "error", "error": str(exc)})
    bad = [r for r in results if r.get("status") != "success"]
    summary = {"run_id": run_id, "status": "success" if not bad else "error",
               "rollups": {r["artifact"]: {"rows": r.get("rows"), "reach_pct": r.get("reach_pct"),
                                           "status": r.get("status")} for r in results}}
    print(summary)
    if bad:
        raise RuntimeError(f"rollup build failed: {[r['artifact'] for r in bad]}")
    return summary


# =========================================================================== #
# PHASE 4 — spine_epa_facility_360 (capstone; spine LEFT JOIN every rollup)
# =========================================================================== #
CAPSTONE_NAME = "spine_epa_facility_360"
ROLLUP_NAMES = ["rollup_epa_npdes", "rollup_epa_rcra", "rollup_epa_sdwa",
                "rollup_epa_air", "rollup_epa_enforcement"]
# Per-rollup column→prefix map (program payload prefixing) + the rollup's own key.
ROLLUP_PREFIX = {"rollup_epa_npdes": "npdes", "rollup_epa_rcra": "rcra",
                 "rollup_epa_sdwa": "sdwa", "rollup_epa_air": "air",
                 "rollup_epa_enforcement": "enf"}
CAPSTONE_BTREE = ["registry_id", "fac_name", "fac_zip5"]
CAPSTONE_BITMAP = ["fac_state", "primary_naics", "fac_compliance_status", "program_count",
                   "has_npdes", "has_rcra", "has_sdwa", "has_air", "has_enforcement",
                   "npdes_has_dmr_exceedance", "rcra_rcra_snc_flag", "sdwa_has_health_based_violation",
                   "air_caa_hpv_flag", "enf_has_penalty"]


@app.function(secrets=SECRETS, timeout=60 * 120, memory=49152, cpu=8.0)
def build_spine_360(run_id: str) -> dict:
    """spine_epa_facility_360 — the base dimension LEFT JOIN every Phase-3 rollup (each rollup's
    payload prefixed by program). 1 row / REGISTRY_ID (LEFT JOINs cannot fan; every rollup is
    1-row-per-RID — gate G4.2 proves it). Wide → r2_safe."""
    import datetime as dt

    import lance
    import pyarrow as pa

    name = CAPSTONE_NAME
    uri = ACTIVE_BASE + name + "/"
    so = _r2_storage_options()
    started = dt.datetime.now(dt.timezone.utc)
    status, error, rows, built = "error", None, 0, []
    gates: dict = {}
    os.makedirs(SCRATCH_DIR, exist_ok=True)
    try:
        con = _new_con()
        try:
            # Read the spine as a full Arrow table (not a chunked scanner.to_reader()) — the
            # list columns (naics_codes/sic_codes/program_acronyms) trip Lance's chunked-decode
            # StructArray length check at a batch boundary when streamed through DuckDB's arrow_scan.
            spine_ds = lance.dataset(ACTIVE_BASE + SPINE_NAME + "/", storage_options=so)
            spine_tbl = spine_ds.to_table()
            con.register("_spine", spine_tbl)
            con.execute("CREATE TEMP TABLE base AS SELECT * FROM _spine")
            con.unregister("_spine")
            del spine_tbl
            base_rows = con.execute("SELECT count(*) FROM base").fetchone()[0]

            attach = {}
            select_parts = ["b.*"]
            join_parts = []
            for rname in ROLLUP_NAMES:
                pre = ROLLUP_PREFIX[rname]
                rds = lance.dataset(ACTIVE_BASE + rname + "/", storage_options=so)
                cols = [f.name for f in rds.schema if f.name != "registry_id"]
                con.register(f"_{pre}_r", rds.scanner().to_reader())
                # prefix every payload column
                proj = ", ".join(
                    [f"nullif(trim(CAST(registry_id AS VARCHAR)),'') AS registry_id"]
                    + [f"{c} AS {pre}_{c}" for c in cols])
                con.execute(f"CREATE TEMP TABLE r_{pre} AS SELECT {proj} FROM _{pre}_r")
                con.unregister(f"_{pre}_r")
                attach[rname] = {"rows": con.execute(f"SELECT count(*) FROM r_{pre}").fetchone()[0],
                                 "cols": [f"{pre}_{c}" for c in cols]}
                select_parts.append(f"r_{pre}.* EXCLUDE (registry_id)")
                join_parts.append(f"LEFT JOIN r_{pre} USING (registry_id)")

            con.execute(f"""
                CREATE TEMP TABLE cap AS
                SELECT {', '.join(select_parts)}
                FROM base b
                {' '.join(join_parts)}
            """)
            total, distinct_rid = con.execute(
                "SELECT count(*), count(DISTINCT registry_id) FROM cap").fetchone()
            # G4.3 attach rates: non-null prefix-key column per program == rollup row count.
            attach_rates = {}
            for rname in ROLLUP_NAMES:
                pre = ROLLUP_PREFIX[rname]
                probe_col = attach[rname]["cols"][0]
                nn = con.execute(
                    f"SELECT count(*) FILTER (WHERE {probe_col} IS NOT NULL) FROM cap").fetchone()[0]
                attach_rates[rname] = {"attached": nn, "rollup_rows": attach[rname]["rows"],
                                       "ok": nn == attach[rname]["rows"]}
            gates = {
                "G4.1_row_parity": {"cap": total, "base": base_rows, "ok": total == base_rows},
                "G4.2_no_fanout": {"rows": total, "distinct": distinct_rid, "ok": total == distinct_rid},
                "G4.3_attach_rates": attach_rates,
            }
            if total != base_rows:
                raise RuntimeError(f"G4.1 row parity FAILED: cap={total} base={base_rows}")
            if total != distinct_rid:
                raise RuntimeError(f"G4.2 fan-out FAILED: {total} rows {distinct_rid} distinct")
            if not all(v["ok"] for v in attach_rates.values()):
                raise RuntimeError(f"G4.3 attach rates FAILED: {attach_rates}")

            table = con.execute("SELECT * FROM cap ORDER BY registry_id").fetch_arrow_table()
        finally:
            con.close()

        reader = pa.Table.to_reader(table, max_chunksize=16_384)
        pub = _write_local_and_publish(name, reader, CAPSTONE_BTREE, CAPSTONE_BITMAP)
        built = pub["built"]
        rows = pub["rows"]
        _set_provenance(uri, so, {
            "source": ",".join([SPINE_NAME] + ROLLUP_NAMES), "capstone_built_run_id": run_id})
        gates["G4.5_provenance"] = True
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[{name}] FAILED: {error}")
    finally:
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(run_id=run_id, phase="capstone", artifact=name, dataset_uri=uri,
                    grain="1/REGISTRY_ID", rows_written=int(rows),
                    indices_built=",".join(built), gates=gates, status=status, error=error,
                    started_at=started, completed_at=completed)
    return {"artifact": name, "uri": uri, "rows": int(rows), "status": status,
            "indices": built, "gates": gates, "error": error}


# =========================================================================== #
# PHASE 5 — Orchestrator + published-layer re-gate (verify_epa_spine)
# =========================================================================== #
ALL_ARTIFACTS = (
    [s["name"] for s in CROSSWALK_SPECS]
    + [SPINE_NAME] + ROLLUP_NAMES + [CAPSTONE_NAME]
)


@app.function(secrets=R2_SECRET, timeout=60 * 30, memory=8192)
def verify_epa_spine() -> dict:
    """Published-layer re-gate (the _verify_published doctrine). Open every committed artifact
    FRESH from R2; assert rows, indices, PK-uniqueness, zero null keys, BTREE probe + Index-Scan
    pushdown, rollup⊆spine, and a fully-populated capstone point-read (the corpse detector G5.1)."""
    import lance

    so = _r2_storage_options()
    out: dict[str, dict] = {}
    fails: list[str] = []

    def _open(n):
        return lance.dataset(ACTIVE_BASE + n + "/", storage_options=so)

    # crosswalks + spine + rollups + capstone: structural gates
    for n in ALL_ARTIFACTS:
        try:
            ds = _open(n)
            idx = _index_names(ds)
            rows = ds.count_rows()
            key = "registry_id" if (n.startswith("spine_") or n.startswith("rollup_")) else None
            entry = {"rows": rows, "indices": idx, "n_cols": len(ds.schema.names)}
            if key:
                pk = ds.to_table(columns=[key])
                import pyarrow.compute as pc
                col = pk.column(key)
                distinct = len(pc.unique(col))
                nulls = col.null_count
                entry["pk_unique"] = (distinct == rows)
                entry["null_keys"] = nulls
                if distinct != rows:
                    fails.append(f"{n}: PK not unique ({distinct} distinct / {rows} rows)")
                if nulls != 0:
                    fails.append(f"{n}: {nulls} null keys")
                # BTREE probe + Index-Scan pushdown
                sample = pc.min(col).as_py()
                if sample is not None:
                    scn = ds.scanner(filter=f"{key} = '{sample}'", columns=[key],
                                     prefilter=True)
                    probe = scn.to_table().num_rows
                    entry["btree_probe_rows"] = probe
                    plan = scn.analyze_plan() if hasattr(scn, "analyze_plan") else str(scn.explain_plan())
                    pushed = ("ScalarIndexQuery" in plan or "IndexScan" in plan
                              or "index" in plan.lower())
                    entry["index_scan"] = pushed
                    if probe < 1:
                        fails.append(f"{n}: BTREE probe returned 0 rows for {sample}")
            out[n] = entry
        except Exception as exc:  # noqa: BLE001
            out[n] = {"error": str(exc)}
            fails.append(f"{n}: open/verify failed — {exc}")

    # rollup ⊆ spine (G3.3 re-gate on the published layer)
    try:
        import pyarrow.compute as pc
        spine_rid = set(pc.unique(_open(SPINE_NAME).to_table(columns=["registry_id"])
                                  .column("registry_id")).to_pylist())
        for rn in ROLLUP_NAMES:
            rrid = set(pc.unique(_open(rn).to_table(columns=["registry_id"])
                                 .column("registry_id")).to_pylist())
            orph = len(rrid - spine_rid)
            out[rn]["orphans_vs_spine"] = orph
            if orph:
                fails.append(f"{rn}: {orph} orphans not in spine")
    except Exception as exc:  # noqa: BLE001
        fails.append(f"rollup⊆spine re-gate failed: {exc}")

    # G5.1 — capstone fresh point-read returns a fully-populated row (the corpse detector).
    try:
        import pyarrow.compute as pc
        cap = _open(CAPSTONE_NAME)
        rid0 = pc.min(cap.to_table(columns=["registry_id"]).column("registry_id")).as_py()
        row = cap.scanner(filter=f"registry_id = '{rid0}'", prefilter=True).to_table()
        out[CAPSTONE_NAME]["g5_1_pointread_rows"] = row.num_rows
        out[CAPSTONE_NAME]["g5_1_pointread_cols"] = row.num_columns
        if row.num_rows != 1:
            fails.append(f"G5.1 capstone point-read returned {row.num_rows} rows")
    except Exception as exc:  # noqa: BLE001
        fails.append(f"G5.1 capstone point-read failed: {exc}")

    status = "success" if not fails else "error"
    _record_run(run_id=_now_run("verify"), phase="verify", artifact="__verify__",
                dataset_uri=ACTIVE_BASE, grain="published-regate", rows_written=0,
                gates={"artifacts": out, "failures": fails}, status=status,
                error=("; ".join(fails) if fails else None))
    print(f"[verify] status={status} failures={fails}")
    return {"status": status, "failures": fails, "artifacts": out}


def _now_run(tag: str) -> str:
    import datetime as dt

    return dt.datetime.now(dt.timezone.utc).strftime(f"epaspine_{tag}_%Y%m%dT%H%M%SZ")


@app.function(secrets=SECRETS, timeout=60 * 60 * 8, memory=4096)
def run_epa_spine(only: list[str] | None = None, skip_capstone: bool = False,
                  trigger_callback_url: str | None = None) -> dict:
    """Full orchestrator: ledger → preflight → crosswalks → spine → rollups (NPDES heavy,
    separate) → capstone → verify_epa_spine published re-gate."""
    import datetime as dt

    started = dt.datetime.now(dt.timezone.utc)
    run_id = started.strftime("epaspine_run_%Y%m%dT%H%M%SZ")
    _ensure_ops_ledger()

    pf = preflight.remote(run_id, None)
    if pf.get("status") != "success":
        raise RuntimeError(f"preflight FAILED: {pf.get('gates', {}).get('failures')}")

    xw = build_crosswalks.remote(run_id)
    sp = build_spine_facility.remote(run_id)
    rl = build_rollups.remote(run_id, only)
    cap = None if skip_capstone else build_spine_360.remote(run_id)
    ver = verify_epa_spine.remote()

    completed = dt.datetime.now(dt.timezone.utc)
    status = ver.get("status", "error")
    _record_run(run_id=run_id, phase="verify", artifact="__run__", dataset_uri=ACTIVE_BASE,
                grain="orchestrator", rows_written=0,
                gates={"preflight": pf.get("status"), "crosswalks": xw.get("status"),
                       "spine": sp.get("status"), "rollups": rl.get("status"),
                       "capstone": (cap or {}).get("status"), "verify": status},
                status=status, started_at=started, completed_at=completed)
    _post_callback(trigger_callback_url, {"status": status, "feed": FEED})
    summary = {"run_id": run_id, "status": status, "verify_failures": ver.get("failures")}
    print(summary)
    if status != "success":
        raise RuntimeError(f"run_epa_spine verify failed: {ver.get('failures')}")
    return summary


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def show_ledger(limit: int = 40) -> list:
    conn = _pg_connect()
    if conn is None:
        return []
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT recorded_at, phase, artifact, rows_written, reach_pct, status, "
                "left(coalesce(error,''),80) FROM ops.epa_spine_runs ORDER BY id DESC LIMIT %s",
                (limit,))
            return [list(r) for r in cur.fetchall()]
    finally:
        conn.close()


@app.function(secrets=R2_SECRET, timeout=600)
def probe_orphans() -> dict:
    """Are the enforcement-crosswalk REGISTRY_IDs that fall outside spine_epa_facility also
    outside epa_facilities (the FRS master)? If so they are non-FRS references, legitimately
    excluded from the dimension."""
    import lance

    so = _r2_storage_options()
    con = _new_con()
    try:
        for nm, cols, al in [
            ("crosswalk_epa_registry_enforcement", ["REGISTRY_ID"], "_x"),
            ("spine_epa_facility", ["registry_id"], "_s"),
            ("epa_facilities", ["REGISTRY_ID"], "_f"),
        ]:
            rdr = lance.dataset(ACTIVE_BASE + nm + "/", storage_options=so).scanner(columns=cols).to_reader()
            con.register(al, rdr)
        con.execute("CREATE TEMP TABLE xrid AS SELECT DISTINCT nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') AS rid FROM _x WHERE nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') IS NOT NULL")
        con.execute("CREATE TEMP TABLE srid AS SELECT DISTINCT registry_id AS rid FROM _s")
        con.execute("CREATE TEMP TABLE frid AS SELECT DISTINCT nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') AS rid FROM _f")
        xtot = con.execute("SELECT count(*) FROM xrid").fetchone()[0]
        not_in_spine = con.execute("SELECT count(*) FROM xrid x WHERE NOT EXISTS (SELECT 1 FROM srid s WHERE s.rid=x.rid)").fetchone()[0]
        not_in_frs = con.execute("SELECT count(*) FROM xrid x WHERE NOT EXISTS (SELECT 1 FROM frid f WHERE f.rid=x.rid)").fetchone()[0]
        in_frs_not_spine = con.execute("SELECT count(*) FROM xrid x WHERE EXISTS (SELECT 1 FROM frid f WHERE f.rid=x.rid) AND NOT EXISTS (SELECT 1 FROM srid s WHERE s.rid=x.rid)").fetchone()[0]
        return {"xwalk_distinct_rid": xtot, "not_in_spine": not_in_spine,
                "not_in_frs_master": not_in_frs, "in_frs_but_not_spine": in_frs_not_spine}
    finally:
        con.close()


@app.function(secrets=R2_SECRET, timeout=600)
def probe_spine_lists() -> dict:
    """Isolate which spine column(s) fail to decode from R2 (the StructArray list-decode bug)."""
    import lance

    so = _r2_storage_options()
    ds = lance.dataset(ACTIVE_BASE + SPINE_NAME + "/", storage_options=so)
    out = {}
    for c in ["registry_id", "naics_codes", "sic_codes", "program_acronyms"]:
        try:
            t = ds.to_table(columns=[c])
            out[c] = {"ok": True, "rows": t.num_rows}
        except Exception as exc:  # noqa: BLE001
            out[c] = {"ok": False, "error": str(exc)[:160]}
    return out


@app.function(secrets=R2_SECRET, timeout=600)
def probe_enforcement() -> dict:
    """Diagnose the enforcement penalty fan-out: raw granular FED_PENALTY sum (case grain) vs
    the facility-attributed sum, plus the ACTIVITY_ID→REGISTRY_ID cardinality."""
    import lance

    so = _r2_storage_options()
    con = _new_con()
    try:
        for nm, cols, al in [
            ("epa_case_penalties", ["ACTIVITY_ID", "CASE_NUMBER", "FED_PENALTY"], "_p"),
            ("crosswalk_epa_registry_enforcement", ["REGISTRY_ID", "ACTIVITY_ID"], "_x"),
        ]:
            rdr = lance.dataset(ACTIVE_BASE + nm + "/", storage_options=so).scanner(columns=cols).to_reader()
            con.register(al, rdr)
        con.execute("""CREATE TEMP TABLE pen AS
            SELECT nullif(trim(CAST(ACTIVITY_ID AS VARCHAR)),'') AS aid,
                   nullif(trim(CAST(CASE_NUMBER AS VARCHAR)),'') AS cn,
                   TRY_CAST(replace(replace(nullif(trim(CAST(FED_PENALTY AS VARCHAR)),''),'$',''),',','') AS DOUBLE) AS fed
            FROM _p""")
        con.execute("""CREATE TEMP TABLE xw AS
            SELECT DISTINCT nullif(trim(CAST(ACTIVITY_ID AS VARCHAR)),'') AS aid,
                   nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') AS rid FROM _x
            WHERE nullif(trim(CAST(ACTIVITY_ID AS VARCHAR)),'') IS NOT NULL
              AND nullif(trim(CAST(REGISTRY_ID AS VARCHAR)),'') IS NOT NULL""")
        raw_rows, raw_sum = con.execute("SELECT count(*), sum(fed) FROM pen WHERE fed IS NOT NULL").fetchone()
        # de-dupe penalty rows to (aid, cn) grain in case the penalties table itself has dupes
        case_sum = con.execute("""SELECT sum(fed) FROM (
            SELECT aid, cn, any_value(fed) AS fed FROM pen WHERE fed IS NOT NULL GROUP BY aid, cn)""").fetchone()[0]
        # facilities per activity_id (fan-out factor)
        fanout = con.execute("""SELECT avg(nf), max(nf) FROM (
            SELECT aid, count(DISTINCT rid) AS nf FROM xw GROUP BY aid)""").fetchone()
        # attributed once per (aid) — penalty joined to distinct facilities (the inflating path)
        attr_sum = con.execute("""SELECT sum(p.fed) FROM
            (SELECT aid, cn, any_value(fed) AS fed FROM pen WHERE fed IS NOT NULL GROUP BY aid,cn) p
            JOIN xw x ON x.aid = p.aid""").fetchone()[0]
        # penalties whose aid resolves a facility at all
        resolved_sum = con.execute("""SELECT sum(fed) FROM (
            SELECT aid, cn, any_value(fed) AS fed FROM pen WHERE fed IS NOT NULL GROUP BY aid,cn) p
            WHERE EXISTS (SELECT 1 FROM xw x WHERE x.aid = p.aid)""").fetchone()[0]
        return {"raw_rows": raw_rows, "raw_sum": float(raw_sum or 0),
                "case_grain_sum": float(case_sum or 0),
                "facility_attributed_sum": float(attr_sum or 0),
                "resolved_case_grain_sum": float(resolved_sum or 0),
                "avg_facilities_per_case": float(fanout[0] or 0), "max_facilities_per_case": int(fanout[1] or 0)}
    finally:
        con.close()


@app.function(secrets=R2_SECRET, timeout=600)
def dump_schemas(names: list[str]) -> dict:
    """Dump exact column names + dtypes for the given active/ datasets (schema recon)."""
    import lance

    so = _r2_storage_options()
    out: dict[str, dict] = {}
    for name in names:
        uri = ACTIVE_BASE + name + "/"
        try:
            ds = lance.dataset(uri, storage_options=so)
            out[name] = {f.name: str(f.type) for f in ds.schema}
        except Exception as exc:  # noqa: BLE001
            out[name] = {"error": str(exc)}
    return out


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=120)
def apply_state_schema() -> dict:
    conn = _pg_connect()
    if conn is None:
        raise RuntimeError("HQX_DB_URL_POOLED not set.")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(OPS_DDL)
    finally:
        conn.close()
    return {"status": "ok", "table": "ops.epa_spine_runs"}


# --------------------------------------------------------------------------- #
# Local entrypoints
# --------------------------------------------------------------------------- #
def _run_id(tag: str) -> str:
    import datetime as dt

    return dt.datetime.now(dt.timezone.utc).strftime(f"epaspine_{tag}_%Y%m%dT%H%M%SZ")


@app.local_entrypoint()
def init() -> None:
    print(apply_state_schema.remote())


@app.local_entrypoint()
def preflight_run(extra: str = "") -> None:
    import json

    names = [n for n in extra.split(",") if n] or None
    print(json.dumps(preflight.remote(_run_id("preflight"), names), indent=2, default=str))


@app.local_entrypoint()
def schemas(names: str) -> None:
    import json

    print(json.dumps(dump_schemas.remote([n for n in names.split(",") if n]), indent=2, default=str))


@app.local_entrypoint()
def probe_enf() -> None:
    import json

    print(json.dumps(probe_enforcement.remote(), indent=2, default=str))


@app.local_entrypoint()
def probe_orph() -> None:
    import json

    print(json.dumps(probe_orphans.remote(), indent=2, default=str))


@app.local_entrypoint()
def probe_lists() -> None:
    import json

    print(json.dumps(probe_spine_lists.remote(), indent=2, default=str))


@app.local_entrypoint()
def crosswalks(only: str = "") -> None:
    import json

    names = [n for n in only.split(",") if n] or None
    print(json.dumps(build_crosswalks.remote(_run_id("xwalk"), names), indent=2, default=str))


@app.local_entrypoint()
def spine() -> None:
    import json

    print(json.dumps(build_spine_facility.remote(_run_id("spine")), indent=2, default=str))


@app.local_entrypoint()
def rollups(only: str = "") -> None:
    import json

    names = [n for n in only.split(",") if n] or None
    print(json.dumps(build_rollups.remote(_run_id("rollup"), names), indent=2, default=str))


@app.local_entrypoint()
def capstone() -> None:
    import json

    print(json.dumps(build_spine_360.remote(_run_id("capstone")), indent=2, default=str))


@app.local_entrypoint()
def run(only: str = "", skip_capstone: bool = False) -> None:
    import json

    names = [n for n in only.split(",") if n] or None
    print(json.dumps(run_epa_spine.remote(only=names, skip_capstone=skip_capstone), indent=2, default=str))


@app.local_entrypoint()
def verify() -> None:
    import json

    print(json.dumps(verify_epa_spine.remote(), indent=2, default=str))


@app.local_entrypoint()
def ledger(limit: int = 40) -> None:
    for r in show_ledger.remote(limit):
        print(r)
