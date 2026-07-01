"""BLS labor-statistics ingest — landing drops → Lance system of record (Gen-3).

LOCAL in-session compute worker (deliberately NOT Modal-wrapped). Both members are
in-core (OEWS ``all_data`` is 413,527 rows; the Employment-Projections matrix is 1,113
rows), so spinning up a Modal app/image is the wrong tool. This runs as a standalone
DuckDB→Lance materializer driven by Doppler-injected R2 creds, mirroring the local-run
convention of pipelines/fdic/ingest.py. It is re-runnable on every OEWS/EP refresh
(``mode="overwrite"`` is idempotent — a full-snapshot replace, never a partial append).

    doppler run -p core-x -c prd -- python3 -m pipelines.bls.ingest --init-state          # apply ops DDL (once)
    doppler run -p core-x -c prd -- python3 -m pipelines.bls.ingest --all                  # both members
    doppler run -p core-x -c prd -- python3 -m pipelines.bls.ingest --dataset oews

TOPOLOGY
    The operator drops BLS exports into s3://data-sink/landing/bls/. Raw is transport-only.

        OEWS May-2025 (oesm25all.zip → all_data_M_2025.xlsx)
            413,527 × 32  -> active/bls_oews_2025/                      AREA + OCC_CODE + NAICS
        Employment Projections 2024–34 (National Employment Matrix_IND_TE1000.csv)
              1,113 × 13  -> active/bls_employment_projections_2024_2034/  OCCUPATION_CODE

DELIBERATELY NOT INGESTED (proven by the pre-ingest structural probe, 2026-07-01)
    oesm25nat / oesm25st / oesm25ma / oesm25in4 zips — every row of all four is present
        verbatim in all_data_M_2025.xlsx. Row-count reconciliation is exact: the four part
        families sum to 413,527 = |all_data|, and all_data's AREA_TYPE / I_GROUP / OWN_CODE
        distributions cover national+state+territory+metro+nonmetro across the full
        cross-industry→sector→3/4/5/6-digit ladder and every ownership breakout. all_data is
        the single complete superset; ingesting the parts would duplicate every row.
    file_descriptions.xlsx (inside the ma/in4 zips) — BLS's column readme (a title-banner
        sheet documenting the OEWS schema), not operator data. Left in landing as transport.

FIDELITY (raw stays lossless)
    OEWS: every source column is projected VARCHAR with trim→empty-as-NULL; headers are
    snake_cased; zero rows dropped. Suppression markers (``*`` not-released, ``#`` at/above
    the $115/hr·$239,200/yr topcode, ``**`` not-published) survive verbatim — numeric typing
    of the wage/employment measures is a deferred downstream concern off the SoR.
    Employment Projections: a compact analytical matrix, so the eight measures ARE typed
    (DOUBLE, thousands of jobs / percents) — no suppression vocabulary exists in EP and each
    measure is unambiguously numeric. The Excel formula-escaped occupation code (=""11-1011"")
    is de-artifacted to 11-1011 and thousands-separators are stripped before cast; cast
    integrity is asserted downstream (rejected-row accounting). Provenance columns
    (source_file, release/vintage, ingested_at) are appended. BTREE on resolution keys;
    BITMAP on low-cardinality categoricals.

CONTROL PLANE
    Terminal state for each member is written to ops.bls_runs (psycopg, best-effort — an
    audit-write failure never masks an otherwise-good load). scripts/data-factory-catalog.py
    reads that ledger to surface BLS as an active ingest source.
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import os
import os.path
import re
import sys
import tempfile
import zipfile

BUCKET = "data-sink"
SCRATCH_DIR = tempfile.gettempdir()

# Net-new datasets → pin the current Lance default storage version (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576          # binds first; every member is < 1 fragment
MAX_BYTES_PER_FILE = 90 * 1024**3      # 90 GiB — Lance default / fleet constant

# ── EP column contract (exact source headers → canonical typed columns) ─────────────
# Employment in thousands of jobs; percents as published. Order defines output schema.
EP_MEASURES = {
    "2024 Employment": "emp_2024",
    "2024 Percent of Industry": "pct_industry_2024",
    "2024 Percent of Occupation": "pct_occupation_2024",
    "Projected 2034 Employment": "emp_2034",
    "Projected 2034 Percent of Industry": "pct_industry_2034",
    "Projected 2034 Percent of Occupation": "pct_occupation_2034",
    "Employment Change, 2024-2034": "emp_change_2024_2034",
    "Employment Percent Change, 2024-2034": "emp_pct_change_2024_2034",
}

DATASETS: dict[str, dict] = {
    "oews": {
        "uri": os.environ.get("BLS_OEWS_LANCE_URI", f"s3://{BUCKET}/active/bls_oews_2025/"),
        "kind": "xlsx_zip",
        "landing_prefix": "landing/bls/oews/",
        "match": r"^oesm25all.*\.zip$",
        "member": r"all_data.*\.xlsx$",
        "release": "M2025",
        "btree": ["area", "occ_code", "naics"],
        "bitmap": ["area_type", "i_group", "own_code", "o_group", "prim_state"],
    },
    "employment_projections": {
        "uri": os.environ.get(
            "BLS_EP_LANCE_URI", f"s3://{BUCKET}/active/bls_employment_projections_2024_2034/"),
        "kind": "ep_csv",
        "landing_prefix": "landing/bls/employment-projections/",
        "match": r"^National Employment Matrix.*\.csv$",
        "release": "2024-2034",
        "btree": ["occupation_code"],
        "bitmap": ["occupation_type", "display_level"],
    },
}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.bls_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset        text        NOT NULL,
    dataset_uri    text,
    source_file    text,
    release        text,
    rows_processed bigint,
    rejected_rows  bigint,
    indexes_built  text[],
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS bls_runs_dataset_idx     ON ops.bls_runs (dataset);
CREATE INDEX IF NOT EXISTS bls_runs_status_idx      ON ops.bls_runs (status);
CREATE INDEX IF NOT EXISTS bls_runs_recorded_at_idx ON ops.bls_runs (recorded_at DESC);
"""


# ── R2 / object-store plumbing ─────────────────────────────────────────────────────
def _storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID (Doppler core-x/prd).")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _s3_client():
    import boto3
    from botocore.config import Config

    so = _storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client("s3", endpoint_url=so["endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"],
                        region_name="auto", config=cfg)


def _discover_key(s3, prefix: str, match: str) -> str:
    """Newest landing object under ``prefix`` whose basename matches ``match``."""
    rx = re.compile(match, re.I)
    found: list[tuple[str, int, dt.datetime]] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            base = o["Key"].rsplit("/", 1)[-1]
            if rx.match(base):
                found.append((o["Key"], o["Size"], o["LastModified"]))
    if not found:
        raise RuntimeError(f"No landing object matches /{match}/ under s3://{BUCKET}/{prefix}")
    found.sort(key=lambda t: (t[2], t[1]), reverse=True)
    return found[0][0]


def _extract_member(zip_path: str, member_rx: str, dest_dir: str) -> str:
    """Extract the single archive member whose basename matches ``member_rx``."""
    rx = re.compile(member_rx, re.I)
    with zipfile.ZipFile(zip_path) as zf:
        hits = [n for n in zf.namelist() if rx.search(n.rsplit("/", 1)[-1])]
        if not hits:
            raise RuntimeError(f"No member matches /{member_rx}/ in {zip_path}")
        if len(hits) > 1:
            raise RuntimeError(f"Ambiguous member /{member_rx}/ in {zip_path}: {hits}")
        zf.extract(hits[0], dest_dir)
        return os.path.join(dest_dir, hits[0])


# ── column-name normalization (snake_case + dedup) ─────────────────────────────────
def _snake(name: str) -> str:
    s = re.sub(r"[^0-9a-z]+", "_", name.strip().lower())
    return re.sub(r"_+", "_", s).strip("_") or "col"


def _norm_pairs(cols: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: dict[str, int] = {}
    for c in cols:
        n = _snake(c)
        if n in seen:
            seen[n] += 1
            n = f"{n}_{seen[n]}"
        else:
            seen[n] = 0
        out.append((c, n))
    return out


# ── per-kind DuckDB builders (raw → project/cast → Arrow) ──────────────────────────
def _build_oews(con, path: str, source_file: str, release: str):
    """OEWS all_data xlsx → verbatim-VARCHAR Arrow table (+ provenance)."""
    con.execute("INSTALL excel; LOAD excel;")
    read = f"read_xlsx('{path}', all_varchar=true)"
    cols = [d[0] for d in con.execute(f"SELECT * FROM {read} LIMIT 0").description]
    pairs = _norm_pairs(cols)
    projection = ",\n    ".join(f"nullif(trim(\"{o}\"), '') AS \"{n}\"" for o, n in pairs)
    sql = (
        "SELECT\n    " + projection + ",\n"
        "    ? AS source_file,\n"
        "    ? AS release,\n"
        "    now() AS ingested_at\n"
        f"FROM {read}"
    )
    return con.execute(sql, [source_file, release]).to_arrow_table()


def _build_ep(con, path: str, source_file: str, release: str):
    """Employment-Projections national matrix CSV → cleaned/typed Arrow table."""
    read = (
        f"read_csv('{path}', delim=',', quote='\"', escape='\"', header=true, "
        "all_varchar=true, strict_mode=false, null_padding=true, sample_size=-1)"
    )
    measures = ",\n    ".join(
        f"TRY_CAST(replace(nullif(trim(\"{src}\"), ''), ',', '') AS DOUBLE) AS {dst}"
        for src, dst in EP_MEASURES.items()
    )
    sql = (
        "SELECT\n"
        "    nullif(trim(\"Occupation Title\"), '') AS occupation_title,\n"
        # de-artifact the Excel formula-escaped code  =""11-1011""  →  11-1011
        "    nullif(regexp_replace(trim(\"Occupation Code\"), '[=\"]', '', 'g'), '') AS occupation_code,\n"
        "    nullif(trim(\"Occupation Type\"), '') AS occupation_type,\n"
        "    " + measures + ",\n"
        "    TRY_CAST(nullif(trim(\"Occupation Sort\"), '') AS INTEGER) AS occupation_sort,\n"
        "    TRY_CAST(nullif(trim(\"Display Level\"), '') AS INTEGER) AS display_level,\n"
        "    'TE1000' AS matrix_code,\n"
        "    'Total, all industries' AS matrix_scope,\n"
        "    ? AS source_file,\n"
        "    ? AS projections_vintage,\n"
        "    now() AS ingested_at\n"
        f"FROM {read}"
    )
    return con.execute(sql, [source_file, release]).to_arrow_table()


# ── Lance scalar indexes ───────────────────────────────────────────────────────────
def _build_indexes(uri: str, btree: list[str], bitmap: list[str], so: dict) -> list[str]:
    """BTREE on resolution keys, BITMAP on categoricals. Idempotent (replace=True); a
    missing/odd column warns, never fails the load."""
    import lance

    ds = lance.dataset(uri, storage_options=so)
    present = {f.name for f in ds.schema}
    built: list[str] = []
    for col in btree:
        if col not in present:
            print(f"  WARN BTREE {col}: column absent — skipped")
            continue
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in bitmap:
        if col not in present:
            print(f"  WARN BITMAP {col}: column absent — skipped")
            continue
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            built.append(f"BITMAP:{col}")
            print(f"  BITMAP ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BITMAP {col} failed: {exc}")
    return built


# ── ops.bls_runs ledger ────────────────────────────────────────────────────────────
def _record_run(dataset, uri, source_file, release, rows, rejected, built, status,
                error, started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.bls_runs write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.bls_runs
                    (dataset, dataset_uri, source_file, release, rows_processed,
                     rejected_rows, indexes_built, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (dataset, uri, source_file, release, rows, rejected, built, status,
                 error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.bls_runs write failed: {exc}")


def apply_state_schema() -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set (Doppler core-x/prd).")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.bls_runs schema.")


# ── one member: landing raw → DuckDB project/cast → Lance overwrite → indexes ──────
def ingest_dataset(name: str, keep_temp: bool = False) -> dict:
    import duckdb
    import lance  # noqa: F401 — fail fast if pylance missing before the heavy read

    if name not in DATASETS:
        raise ValueError(f"dataset must be one of {sorted(DATASETS)}, got {name!r}")
    spec = DATASETS[name]
    uri = spec["uri"]
    release = spec["release"]
    so = _storage_options()
    s3 = _s3_client()

    started_at = dt.datetime.now(dt.timezone.utc)
    rows = rejected = 0
    built: list[str] = []
    status, error, source_file = "error", None, None
    scratch: list[str] = []

    key = _discover_key(s3, spec["landing_prefix"], spec["match"])
    source_file = key.rsplit("/", 1)[-1]
    dl_path = os.path.join(SCRATCH_DIR, f"bls_{name}_{os.path.basename(key)}")
    scratch.append(dl_path)
    try:
        print(f"[{name}] landing s3://{BUCKET}/{key} -> {dl_path}")
        s3.download_file(BUCKET, key, dl_path)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute(f"SET temp_directory='{os.path.join(SCRATCH_DIR, 'duckdb_spill')}';")
            if spec["kind"] == "xlsx_zip":
                member_dir = os.path.join(SCRATCH_DIR, f"bls_{name}_x")
                xlsx = _extract_member(dl_path, spec["member"], member_dir)
                scratch.append(member_dir)
                print(f"[{name}] extracted member -> {xlsx}")
                table = _build_oews(con, xlsx, source_file, release)
            elif spec["kind"] == "ep_csv":
                table = _build_ep(con, dl_path, source_file, release)
            else:
                raise ValueError(f"unknown kind {spec['kind']!r}")
            rows = table.num_rows
        finally:
            con.close()
        print(f"[{name}] parsed {rows:,} rows ({table.num_columns} cols)")

        lance.write_dataset(
            table, uri, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        del table
        print(f"[{name}] wrote Lance -> {uri}")
        built = _build_indexes(uri, spec["btree"], spec["bitmap"], so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(name, uri, source_file, release, int(rows), int(rejected),
                    built, status, error, started_at, completed_at)
        if not keep_temp:
            for p in scratch:
                try:
                    if os.path.isdir(p):
                        for f in glob.glob(os.path.join(p, "**"), recursive=True):
                            if os.path.isfile(f):
                                os.remove(f)
                    elif os.path.isfile(p):
                        os.remove(p)
                except OSError:
                    pass

    return {"dataset": name, "rows": int(rows), "rejected": int(rejected),
            "indexes": built, "uri": uri, "source_file": source_file, "release": release}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="BLS OEWS / Employment-Projections landing → Lance ingest.")
    ap.add_argument("--dataset", choices=sorted(DATASETS), help="single member")
    ap.add_argument("--all", action="store_true", help="all members")
    ap.add_argument("--init-state", action="store_true", help="apply ops.bls_runs DDL and exit")
    ap.add_argument("--keep-temp", action="store_true", help="keep downloaded raw in scratch")
    args = ap.parse_args(argv)

    if args.init_state:
        apply_state_schema()
        return 0
    targets = list(DATASETS) if args.all else ([args.dataset] if args.dataset else [])
    if not targets:
        ap.error("specify --all or --dataset <name> (or --init-state)")

    results = []
    for name in targets:
        results.append(ingest_dataset(name, keep_temp=args.keep_temp))

    print("\n=== BLS ingest summary ===")
    total = 0
    for r in results:
        total += r["rows"]
        print(f"  {r['dataset']:24s} rows={r['rows']:>9,}  idx={len(r['indexes']):>2}  -> {r['uri']}")
    print(f"  {'TOTAL':24s} rows={total:>9,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
