"""NCUA federally-insured credit-union roster ingest — landing ZIP → Lance SoR (Gen-3).

LOCAL in-session compute worker (not Modal-wrapped; the payload is ~1 MiB). Mirrors
pipelines/fdic/ingest.py: a standalone DuckDB→Lance materializer driven by Doppler-injected
R2 creds, re-runnable each quarter (``mode="overwrite"`` — full-snapshot replace).

    doppler run -p core-x -c prd -- python3 -m pipelines.ncua.ingest --init-state   # apply ops DDL (once)
    doppler run -p core-x -c prd -- python3 -m pipelines.ncua.ingest --run

TOPOLOGY
    The operator drops the NCUA quarterly roster ZIP into s3://data-sink/landing/credit-unions/.
    The ZIP holds a single workbook — FederallyInsuredCreditUnions_<YYYYqQ>.xlsx (one sheet,
    4,250 × 23) — the complete federally-insured credit-union list. It materializes to:

        active/ncua_credit_unions/   charter_number (PK), join_number (NCUA internal id)

    This is the NCUA companion to the FDIC bank universe — banks (FDIC) and credit unions
    (NCUA) are distinct regulators and were split into separate landing prefixes upstream.

FIDELITY
    Every column projected VARCHAR with trim→empty-as-NULL; the workbook's messy headers
    (newlines, parentheses, % signs) are snake_cased, with the two join keys given clean
    canonical names. Provenance columns (source_file, snapshot_date, ingested_at) appended.
    snapshot_date is the roster quarter-end (Q1 2026 → 2026-03-31), not the upload date.
    BTREE on charter_number + join_number; BITMAP on the categoricals (type, region, state,
    low-income designation). Numeric measures (assets, members, ratios) stay VARCHAR — typing
    is a deferred downstream concern off the SoR.

CONTROL PLANE
    Terminal state → ops.ncua_runs (psycopg, best-effort). scripts/data-factory-catalog.py
    reads that ledger to surface NCUA as an active ingest source.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import os.path
import re
import sys
import tempfile
import zipfile

BUCKET = "data-sink"
LANDING_PREFIX = "landing/credit-unions/"
SCRATCH_DIR = tempfile.gettempdir()

URI = os.environ.get("NCUA_CREDIT_UNIONS_LANCE_URI", f"s3://{BUCKET}/active/ncua_credit_unions/")
# Roster quarter-end. The FederallyInsuredCreditUnions_2026q1 workbook is the Q1-2026 list.
AS_OF_DEFAULT = "2026-03-31"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3

BTREE = ["charter_number", "join_number"]
BITMAP = ["credit_union_type", "ncua_region", "state_mailing_address", "low_income_designation"]

# Canonical names for the two resolution keys (the raw headers are verbose/parenthesized).
RENAME = {
    "Charter number": "charter_number",
    "NCUA internal ID (join_number)": "join_number",
}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.ncua_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset        text        NOT NULL,
    dataset_uri    text,
    source_file    text,
    as_of          date,
    rows_processed bigint,
    rejected_rows  bigint,
    indexes_built  text[],
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ncua_runs_dataset_idx     ON ops.ncua_runs (dataset);
CREATE INDEX IF NOT EXISTS ncua_runs_status_idx      ON ops.ncua_runs (status);
CREATE INDEX IF NOT EXISTS ncua_runs_recorded_at_idx ON ops.ncua_runs (recorded_at DESC);
"""


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


def _discover_zip(s3) -> str:
    found: list[tuple[str, int, dt.datetime]] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=LANDING_PREFIX):
        for o in page.get("Contents", []):
            if o["Key"].lower().endswith(".zip"):
                found.append((o["Key"], o["Size"], o["LastModified"]))
    if not found:
        raise RuntimeError(f"No .zip under s3://{BUCKET}/{LANDING_PREFIX}")
    found.sort(key=lambda t: (t[2], t[1]), reverse=True)
    return found[0][0]


def _snake(name: str) -> str:
    s = re.sub(r"[^0-9a-z]+", "_", name.strip().lower())
    return re.sub(r"_+", "_", s).strip("_") or "col"


def _norm_pairs(cols: list[str]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: dict[str, int] = {}
    for c in cols:
        n = RENAME.get(c) or _snake(c)
        if n in seen:
            seen[n] += 1
            n = f"{n}_{seen[n]}"
        else:
            seen[n] = 0
        out.append((c, n))
    return out


def _build_indexes(uri: str, so: dict) -> list[str]:
    import lance

    ds = lance.dataset(uri, storage_options=so)
    present = {f.name for f in ds.schema}
    built: list[str] = []
    for col in BTREE:
        if col not in present:
            print(f"  WARN BTREE {col}: column absent — skipped")
            continue
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
            built.append(f"BTREE:{col}")
            print(f"  BTREE  ✓ {col}")
        except Exception as exc:  # noqa: BLE001
            print(f"  WARN BTREE {col} failed: {exc}")
    for col in BITMAP:
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


def _record_run(uri, source_file, as_of, rows, rejected, built, status,
                error, started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.ncua_runs write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.ncua_runs
                    (dataset, dataset_uri, source_file, as_of, rows_processed,
                     rejected_rows, indexes_built, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                ("credit_unions", uri, source_file, as_of, rows, rejected, built,
                 status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        print(f"WARN: ops.ncua_runs write failed: {exc}")


def apply_state_schema() -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set (Doppler core-x/prd).")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.ncua_runs schema.")


def ingest(as_of: str = AS_OF_DEFAULT, keep_temp: bool = False) -> dict:
    import duckdb
    import lance  # noqa: F401

    so = _storage_options()
    s3 = _s3_client()
    started_at = dt.datetime.now(dt.timezone.utc)
    rows = rejected = 0
    built: list[str] = []
    status, error, source_file = "error", None, None

    zip_key = _discover_zip(s3)
    zip_path = os.path.join(SCRATCH_DIR, "ncua_roster.zip")
    extract_dir = os.path.join(SCRATCH_DIR, "ncua_extract")
    os.makedirs(extract_dir, exist_ok=True)
    xlsx_path = None
    try:
        print(f"[ncua] landing s3://{BUCKET}/{zip_key} -> {zip_path}")
        s3.download_file(BUCKET, zip_key, zip_path)
        with zipfile.ZipFile(zip_path) as zf:
            members = [n for n in zf.namelist() if n.lower().endswith((".xlsx", ".xls"))]
            if not members:
                raise RuntimeError(f"No .xlsx in {zip_key} (have: {zf.namelist()})")
            member = members[0]
            zf.extract(member, extract_dir)
            xlsx_path = os.path.join(extract_dir, member)
        source_file = member.rsplit("/", 1)[-1]
        print(f"[ncua] workbook -> {xlsx_path}")

        con = duckdb.connect(":memory:")
        try:
            con.execute("INSTALL excel; LOAD excel;")
            read = f"read_xlsx('{xlsx_path}', all_varchar=true, header=true)"
            cols = [d[0] for d in con.execute(f"SELECT * FROM {read} LIMIT 0").description]
            pairs = _norm_pairs(cols)
            projection = ",\n    ".join(
                f"nullif(trim(\"{o}\"), '') AS \"{n}\"" for o, n in pairs)
            sql = (
                "SELECT\n    " + projection + ",\n"
                "    ? AS source_file,\n"
                "    CAST(? AS DATE) AS snapshot_date,\n"
                "    now() AS ingested_at\n"
                f"FROM {read}"
            )
            table = con.execute(sql, [source_file, as_of]).to_arrow_table()
            rows = table.num_rows
        finally:
            con.close()
        print(f"[ncua] parsed {rows:,} rows ({table.num_columns} cols)")

        lance.write_dataset(
            table, URI, mode="overwrite",
            data_storage_version=DATA_STORAGE_VERSION,
            max_rows_per_file=MAX_ROWS_PER_FILE,
            max_bytes_per_file=MAX_BYTES_PER_FILE,
            storage_options=so,
        )
        del table
        print(f"[ncua] wrote Lance -> {URI}")
        built = _build_indexes(URI, so)
        status = "success"
    except Exception as exc:  # noqa: BLE001
        error = str(exc)
        status = "error"
        raise
    finally:
        completed_at = dt.datetime.now(dt.timezone.utc)
        _record_run(URI, source_file, as_of, int(rows), int(rejected), built,
                    status, error, started_at, completed_at)
        if not keep_temp:
            for p in (zip_path, xlsx_path):
                try:
                    if p:
                        os.remove(p)
                except OSError:
                    pass

    return {"dataset": "credit_unions", "rows": int(rows), "indexes": built,
            "uri": URI, "source_file": source_file, "as_of": as_of}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="NCUA credit-union roster landing → Lance ingest.")
    ap.add_argument("--run", action="store_true", help="execute the ingest")
    ap.add_argument("--as-of", default=AS_OF_DEFAULT, help="roster quarter-end (YYYY-MM-DD)")
    ap.add_argument("--init-state", action="store_true", help="apply ops.ncua_runs DDL and exit")
    ap.add_argument("--keep-temp", action="store_true", help="keep downloaded artifacts in scratch")
    args = ap.parse_args(argv)

    if args.init_state:
        apply_state_schema()
        return 0
    if not args.run:
        ap.error("specify --run (or --init-state)")

    r = ingest(as_of=args.as_of, keep_temp=args.keep_temp)
    print("\n=== NCUA ingest summary ===")
    print(f"  {r['dataset']:20s} rows={r['rows']:>9,}  idx={len(r['indexes']):>2}  -> {r['uri']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
