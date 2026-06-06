"""Compute worker — NUCC Health Care Provider Taxonomy reference (`nppes_taxonomy_ref`).

The immediate companion named by `nppes_analytical_implementation_plan.md` §11: a tiny static
ingest of the NUCC taxonomy crosswalk (code → grouping / classification / specialization /
display name) so the `taxonomy_code` axis of the analytical layer becomes human-readable.
Without it, `nppes_provider_taxonomy.taxonomy_code` / `nppes_provider.primary_taxonomy_code`
read as opaque codes (`106S00000X`); with it they join to `Behavior Technician`.

Clean-room data plane, identical doctrine to the rest of the fleet: the upstream CSV is
transport-only (streamed through the worker, never the SoR), DuckDB does 100% of the transform,
Lance on R2 is the system of record. Blast-radius isolated — a NEW Modal app that writes ONLY
`active/nppes_taxonomy_ref/`; it never touches the raw SoR or the analytical layer.

WHAT IT BUILDS — one flat (non-partitioned), append-only-by-overwrite Lance dataset:

    nppes_taxonomy_ref   1 row / NUCC taxonomy code   (883 @ v25.1)   ORDER BY taxonomy_code

It is a slowly-changing reference dimension (NUCC publishes ~2×/year: 25.0 → 25.1 → …), NOT
monthly-aligned with NPPES, so it is stored FLAT at `active/nppes_taxonomy_ref/` and overwritten
in place on each version bump. The NUCC version + source URL + ingest timestamp ride as schema
metadata (and the ledger), and Lance's own version history preserves prior editions if a vintage
join is ever needed.

JOIN CONTRACT (verified live against snapshot=2026-05):
  nppes_provider_taxonomy.taxonomy_code  →  nppes_taxonomy_ref.taxonomy_code   (LEFT JOIN)
  nppes_provider.primary_taxonomy_code   →  nppes_taxonomy_ref.taxonomy_code   (LEFT JOIN)
Coverage: 872/873 distinct live codes (11,952,805 / 11,952,809 = 100.0000% of taxonomy rows)
resolve to a name. The lone miss (`246ZS0400X`, 4 rows) is a NUCC-retired code NPPES still
carries — LEFT JOIN nulls it, by design. Use a LEFT JOIN, never INNER (an INNER would silently
drop retired-code rows).

WRITE TRANSPORT (the R2 multipart rule, diag §6.6): build the Lance dataset + scalar indices on
LOCAL disk, set provenance metadata explicitly (the streaming write drops Arrow KV metadata),
then boto3 publish (uniform parts). Tiny here (~0.5 MB), but identical to the proven path.

    modal run    pipelines/nppes/taxonomy_ref.py::init_state
    modal run    pipelines/nppes/taxonomy_ref.py::ingest
    modal run    pipelines/nppes/taxonomy_ref.py::verify
    modal run    pipelines/nppes/taxonomy_ref.py::show_ledger
    modal deploy pipelines/nppes/taxonomy_ref.py

`build_all` / `run_gate` are plain module-level functions so the §9.3 local dry-run can call them
in-process (uv venv + `doppler run`) and gate locally BEFORE any R2 write; the Modal functions
below are thin wrappers.
"""

from __future__ import annotations

import os

import modal

BUCKET = "data-sink"
FEED = "nppes_taxonomy_ref"

# Flat reference prefix (no snapshot partition — slowly-changing dimension). Env-overridable.
PREFIX = os.environ.get("NPPES_TAXONOMY_REF_PREFIX", "active/nppes_taxonomy_ref").strip("/")

# Current NUCC code set. The version is encoded in the filename (…_251.csv → 25.1).
CSV_URL = os.environ.get(
    "NUCC_TAXONOMY_CSV_URL",
    "https://nucc.org/images/stories/CSV/nucc_taxonomy_251.csv",
)
# Polite UA — some origins 403 a bare client.
HTTP_UA = os.environ.get("NUCC_HTTP_UA", "core-x-data-engine (compliance@substrate)")

SCRATCH_DIR = os.environ.get("NPPES_TAXONOMY_REF_SCRATCH", "/tmp/nppes_taxonomy_ref")
LOCAL_STAGE = os.path.join(SCRATCH_DIR, "nppes_taxonomy_ref_lance")

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576          # 883 rows → 1 fragment

# Scalar index plan. BTREE = the join/resolution key; BITMAP = the low-card rollup axes.
# (grouping NDV 29, section NDV 2; classification NDV ~245 is left unindexed — an 883-row scan
#  is sub-millisecond, so an index there is cosmetic.)
INDEX_PLAN = {"btree": ["taxonomy_code"], "bitmap": ["grouping", "section"]}
SORT_BY = "taxonomy_code"

# The 8 NUCC source columns → typed, snake_case projection (+ one derived best-label column).
# Empty strings → NULL; long free-text (definition/notes) kept verbatim for reference.
TRANSFORM_SQL = """
SELECT
  trim("Code")                              AS taxonomy_code,
  nullif(trim("Grouping"), '')              AS grouping,
  nullif(trim("Classification"), '')        AS classification,
  nullif(trim("Specialization"), '')        AS specialization,
  nullif(trim("Display Name"), '')          AS display_name,
  nullif(trim("Definition"), '')            AS definition,
  nullif(trim("Notes"), '')                 AS notes,
  nullif(trim("Section"), '')               AS section,
  -- single human-readable label for the common "show the specialty" path:
  -- NUCC's curated Display Name, falling back to Classification when blank.
  coalesce(nullif(trim("Display Name"), ''), nullif(trim("Classification"), '')) AS taxonomy_display
FROM read_csv(?, header=true, all_varchar=true)
WHERE "Code" IS NOT NULL AND trim("Code") <> ''
ORDER BY taxonomy_code
"""

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.nppes_taxonomy_ref_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,
    source_url          text,
    nucc_version        text,
    rows                bigint,
    dataset_uri         text,
    indices_built       text,
    coverage_pct        double precision,   -- % of live taxonomy rows that resolve to a name
    unmatched_codes     bigint,             -- distinct live codes absent from this edition
    gate                jsonb,
    status              text        NOT NULL,
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS nppes_taxonomy_ref_runs_recorded_idx ON ops.nppes_taxonomy_ref_runs (recorded_at DESC);
CREATE INDEX IF NOT EXISTS nppes_taxonomy_ref_runs_status_idx   ON ops.nppes_taxonomy_ref_runs (status);
"""

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "duckdb>=1.5,<2",
    "lancedb>=0.15",
    "pylance>=7",
    "pyarrow>=17",
    "boto3>=1.35",
    "requests>=2.32",
    "psycopg[binary]>=3.2",
)

app = modal.App("nppes-taxonomy-ref", image=image)


# --------------------------------------------------------------------------- #
# R2 / object-store (mirrored from pipelines/nppes/materialize_analytical.py)
# --------------------------------------------------------------------------- #
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID in the Modal secret / doppler config.")
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
                 response_checksum_validation="when_required",
                 retries={"max_attempts": 5, "mode": "standard"})
    return boto3.client("s3", endpoint_url=so["endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"],
                        region_name="auto", config=cfg)


def _uri() -> str:
    return f"s3://{BUCKET}/{PREFIX}/"


def _replace_r2_prefix(s3, prefix: str, local_dir: str) -> int:
    """Idempotent publish: wipe the prefix, then upload the local Lance dataset (boto3 =
    uniform-part multipart, R2-compliant). Returns files uploaded."""
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
# Fetch + version
# --------------------------------------------------------------------------- #
def derive_version(url: str) -> str:
    """nucc_taxonomy_251.csv → '25.1'. Falls back to the raw digit run, else 'unknown'."""
    import re

    m = re.search(r"(\d{3,4})\.csv$", url)
    if not m:
        return "unknown"
    d = m.group(1)
    return f"{d[:2]}.{d[2:]}" if len(d) == 3 else f"{d[:2]}.{d[2:]}"


def fetch_csv(url: str, dest_dir: str) -> str:
    """Stream the NUCC CSV to a local file (transport-only). Returns the local path."""
    import requests

    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, os.path.basename(url) or "nucc_taxonomy.csv")
    with requests.get(url, headers={"User-Agent": HTTP_UA}, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):
                f.write(chunk)
    if os.path.getsize(dest) < 10_000:
        raise RuntimeError(f"NUCC CSV at {url} is suspiciously small ({os.path.getsize(dest)} B).")
    return dest


# --------------------------------------------------------------------------- #
# Core build — CSV → local Lance stage → metadata → indices (no publish)
# --------------------------------------------------------------------------- #
def build_all(csv_path: str, *, source_url: str, nucc_version: str,
              local_stage: str = LOCAL_STAGE) -> dict:
    import datetime as dt
    import shutil

    import duckdb
    import lance

    shutil.rmtree(local_stage, ignore_errors=True)
    os.makedirs(os.path.dirname(local_stage), exist_ok=True)

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4; SET enable_progress_bar=false;")
    con.execute("SET preserve_insertion_order=true;")
    reader = con.execute(TRANSFORM_SQL, [csv_path]).fetch_record_batch(8192)
    lance.write_dataset(reader, local_stage, schema=reader.schema, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE)
    con.close()

    ds = lance.dataset(local_stage)
    rows = ds.count_rows()
    ds.update_schema_metadata({
        "source_url": source_url,
        "nucc_version": nucc_version,
        "pipeline": "taxonomy_ref",
        "ingested_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }, replace=True)

    built: list[str] = []
    ds = lance.dataset(local_stage)
    for col in INDEX_PLAN["btree"]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in INDEX_PLAN["bitmap"]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    print(f"[build] nppes_taxonomy_ref rows={rows} indices={built}")
    return {"rows": rows, "indices": built, "local": local_stage,
            "source_url": source_url, "nucc_version": nucc_version}


# --------------------------------------------------------------------------- #
# Acceptance gate
# --------------------------------------------------------------------------- #
def run_gate(*, local: str | None = None, taxonomy_uri: str | None = None) -> dict:
    """Correctness gate. Absolute: rows == distinct(taxonomy_code) (unique), rows >= 800,
    all 9 columns present, gate code 106S00000X resolves to a non-null display_name.
    Coverage vs the live taxonomy long table is RECORDED (a retired-code miss is expected,
    never gated)."""
    import lance

    so = None if local is not None else _r2_storage_options()
    ds = lance.dataset(local if local else _uri(), storage_options=so)
    cols = [f.name for f in ds.schema]
    rows = ds.count_rows()

    import duckdb
    con = duckdb.connect(":memory:")
    con.register("ref", ds)
    distinct = con.execute("SELECT count(DISTINCT taxonomy_code) FROM ref").fetchone()[0]
    sample = con.execute(
        "SELECT display_name FROM ref WHERE taxonomy_code='106S00000X'").fetchone()
    gate_code_ok = bool(sample and sample[0])

    coverage_pct = None
    unmatched = None
    if taxonomy_uri:
        try:
            tax = lance.dataset(taxonomy_uri, storage_options=_r2_storage_options())
            con.register("tax", tax)
            tot, match = con.execute(
                "SELECT count(*), count(*) FILTER (WHERE taxonomy_code IN (SELECT taxonomy_code FROM ref)) "
                "FROM tax").fetchone()
            coverage_pct = round(100.0 * match / tot, 6) if tot else None
            unmatched = con.execute(
                "SELECT count(*) FROM (SELECT DISTINCT taxonomy_code FROM tax "
                "WHERE taxonomy_code NOT IN (SELECT taxonomy_code FROM ref))").fetchone()[0]
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN coverage check skipped: {exc}")
    con.close()

    expected_cols = {"taxonomy_code", "grouping", "classification", "specialization",
                     "display_name", "definition", "notes", "section", "taxonomy_display"}
    cols_ok = expected_cols.issubset(set(cols))
    unique_ok = rows == distinct
    rows_ok = rows >= 800
    gates = {
        "rows_present": {"pass": rows_ok, "rows": rows},
        "code_unique": {"pass": unique_ok, "rows": rows, "distinct": distinct},
        "columns_present": {"pass": cols_ok, "missing": sorted(expected_cols - set(cols))},
        "gate_code_resolves": {"pass": gate_code_ok,
                               "106S00000X": (sample[0] if sample else None)},
    }
    passed = all(g["pass"] for g in gates.values())
    for gid, g in gates.items():
        print(f"  {gid}: {'PASS' if g['pass'] else 'FAIL'} {({k: v for k, v in g.items() if k != 'pass'})}")
    print(f"  coverage vs live taxonomy: {coverage_pct}% (unmatched distinct codes={unmatched})")
    return {"passed": passed, "gates": gates, "rows": rows,
            "coverage_pct": coverage_pct, "unmatched_codes": unmatched}


def publish() -> str:
    s3 = _s3_client()
    uploaded = _replace_r2_prefix(s3, PREFIX + "/", LOCAL_STAGE)
    print(f"[publish] nppes_taxonomy_ref: {uploaded} files → {_uri()}")
    return _uri()


# --------------------------------------------------------------------------- #
# ops ledger
# --------------------------------------------------------------------------- #
def _record_run(*, source_url, nucc_version, rows, dataset_uri, indices_built,
                coverage_pct, unmatched_codes, gate, status, error,
                started_at, completed_at) -> None:
    import psycopg
    from psycopg.types.json import Jsonb

    dsns = [d for d in (os.environ.get("HQX_DB_URL_POOLED"), os.environ.get("HQX_DB_URL_DIRECT")) if d]
    if not dsns:
        print("WARN: no HQX_DB_URL_POOLED/DIRECT; skipping ops.* write.")
        return
    params = (FEED, source_url, nucc_version, rows, dataset_uri,
              indices_built, coverage_pct, unmatched_codes,
              Jsonb(gate) if gate is not None else None, status, error,
              started_at, completed_at)
    insert = """
        INSERT INTO ops.nppes_taxonomy_ref_runs
            (feed, source_url, nucc_version, rows, dataset_uri, indices_built,
             coverage_pct, unmatched_codes, gate, status, error, started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
    last_exc = None
    for dsn in dsns:
        try:
            with psycopg.connect(dsn, connect_timeout=15) as conn, conn.cursor() as cur:
                cur.execute(OPS_DDL)
                cur.execute(insert, params)
                conn.commit()
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            print(f"WARN: ops.* write via {dsn.rsplit('@', 1)[-1][:32]} failed: {str(exc)[:120]}")
    print(f"WARN: ops.nppes_taxonomy_ref_runs write failed on all DSNs: {str(last_exc)[:160]}")


# --------------------------------------------------------------------------- #
# Modal workers
# --------------------------------------------------------------------------- #
@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_state_schema() -> dict:
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
        cur.execute("SELECT to_regclass('ops.nppes_taxonomy_ref_runs')")
        present = cur.fetchone()[0]
    print(f"ops.nppes_taxonomy_ref_runs present = {present}")
    return {"status": "success", "present": str(present)}


@app.function(
    secrets=[modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")],
    timeout=60 * 20, memory=4096, cpu=2.0,
)
def ingest(csv_url: str | None = None, publish_to_r2: bool = True) -> dict:
    """Fetch the NUCC CSV → build local Lance + indices → gate → publish → read-back gate → ops."""
    import datetime as dt

    url = csv_url or CSV_URL
    version = derive_version(url)
    started_at = dt.datetime.now(dt.timezone.utc)
    status, error, gate_result, b = "error", None, None, {}
    tax_uri = f"s3://{BUCKET}/active/nppes_provider_taxonomy/snapshot=2026-05/"
    try:
        csv_path = fetch_csv(url, SCRATCH_DIR)
        b = build_all(csv_path, source_url=url, nucc_version=version)
        print("[gate] local stage")
        local_gate = run_gate(local=b["local"], taxonomy_uri=tax_uri)
        if not local_gate["passed"]:
            raise RuntimeError(f"local gate failed: {local_gate['gates']}")
        if publish_to_r2:
            publish()
            print("[gate] published R2 layer")
            gate_result = run_gate(local=None, taxonomy_uri=tax_uri)
            if not gate_result["passed"]:
                raise RuntimeError(f"published gate failed: {gate_result['gates']}")
        else:
            gate_result = local_gate
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        print(f"[ingest] ERROR: {error}")
    finally:
        import shutil
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(source_url=url, nucc_version=version, rows=b.get("rows"),
                    dataset_uri=_uri(),
                    indices_built=",".join(b.get("indices", [])),
                    coverage_pct=(gate_result or {}).get("coverage_pct"),
                    unmatched_codes=(gate_result or {}).get("unmatched_codes"),
                    gate=(gate_result or {}).get("gates"), status=status, error=error,
                    started_at=started_at, completed_at=completed_at)
        shutil.rmtree(SCRATCH_DIR, ignore_errors=True)
    if status != "success":
        raise RuntimeError(f"nppes_taxonomy_ref ingest {status}: {error}")
    return {"feed": FEED, "nucc_version": version, "rows": b.get("rows"),
            "dataset_uri": _uri(), "indices": b.get("indices"),
            "coverage_pct": (gate_result or {}).get("coverage_pct"), "status": status}


@app.function(secrets=[modal.Secret.from_name("r2-credentials")], timeout=60 * 10)
def verify_published() -> dict:
    tax_uri = f"s3://{BUCKET}/active/nppes_provider_taxonomy/snapshot=2026-05/"
    return run_gate(local=None, taxonomy_uri=tax_uri)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def ledger(limit: int = 10) -> list:
    import psycopg

    dsn = os.environ["HQX_DB_URL_POOLED"]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, nucc_version, rows, coverage_pct, unmatched_codes, status, error, "
            "completed_at FROM ops.nppes_taxonomy_ref_runs ORDER BY id DESC LIMIT %s", (limit,))
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
def ingest_local(csv_url: str = "", publish_to_r2: bool = True) -> None:
    import json
    print(json.dumps(ingest.remote(csv_url=csv_url or None, publish_to_r2=publish_to_r2),
                     indent=2, default=str))


@app.local_entrypoint()
def verify() -> None:
    import json
    print(json.dumps(verify_published.remote(), indent=2, default=str))


@app.local_entrypoint()
def show_ledger(limit: int = 10) -> None:
    import json
    print(json.dumps(ledger.remote(limit), indent=2, default=str))
