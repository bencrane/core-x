"""FDIC BankFind bulk ingest — landing CSVs → Lance system of record (Gen-3).

LOCAL in-session compute worker (deliberately NOT Modal-wrapped). The full FDIC
BankFind drop is ~150 MiB (largest member 56 MiB / 78k rows) — squarely in-core, so
spinning up a Modal app/image is the wrong tool. This runs as a standalone DuckDB→Lance
materializer driven by Doppler-injected R2 creds, mirroring the local-run convention of
scripts/data-factory-catalog.py. It is re-runnable on every quarterly BankFind refresh
(``mode="overwrite"`` is idempotent — a full-snapshot replace, never a partial append).

    doppler run -p core-x -c prd -- python3 -m pipelines.fdic.ingest --init-state   # apply ops DDL (once)
    doppler run -p core-x -c prd -- python3 -m pipelines.fdic.ingest --all          # all members
    doppler run -p core-x -c prd -- python3 -m pipelines.fdic.ingest --dataset institutions

TOPOLOGY
    The operator drops BankFind CSV exports into s3://data-sink/landing/fdic/. Raw is
    transport-only. Six load-bearing members materialize to one Lance dataset each, all
    joinable on CERT (the FDIC certificate number — the institution resolution key):

        Institutions  27,836 × 160  -> active/fdic_institutions/        CERT (PK)
        Financial      4,287 × 161  -> active/fdic_financial/           CERT + REPDTE
        Locations     78,298 × 38   -> active/fdic_locations/           UNINUM (PK), CERT (FK)
        SOD           76,120 × 81   -> active/fdic_sod/                 UNINUMBR (PK), CERT (FK)
        StructureChg   3,502 × 244  -> active/fdic_structure_changes/   CERT + TRANSNUM
        Failures           2 × 33   -> active/fdic_failures/            CERT + FAILDATE

DELIBERATELY NOT INGESTED (proven by the pre-ingest structural probe, 2026-06-21)
    Institutions_*-2.csv / Institutions_*-3.csv — a disjoint active/inactive partition
        of the base export: |-2|=4,269 ⊎ |-3|=23,567 = 27,836 = |base|, with -2∩-3=∅ and
        both strict CERT-subsets of the base, carrying *fewer* columns (151 / 158 vs 160)
        plus ragged trailing-comma artifacts. Ingesting them would triple-count the same
        institutions. The base Institutions export is the single complete superset.
    Summary_(Historical_Data_Aggregations)_*.csv — empty export (header only, 0 data rows).

FIDELITY (raw stays lossless)
    Every source column is projected VARCHAR with trim→empty-as-NULL; headers are
    snake_cased; zero rows are dropped (verified 0 rejects across all six members under a
    strict-mode-off / null-padded parse, with store_rejects quantifying any ragged rows).
    Identifiers (CERT, UNINUM, UNINUMBR, TRANSNUM, …) stay VARCHAR so cross-dataset joins
    are type-stable. Numeric / temporal typing of the wide financial columns is a deferred
    downstream concern off the SoR — there is no authoritative BankFind dtype map here, so
    coercing 160+ columns would risk silently corrupting the system of record. Provenance
    columns (source_file, snapshot_date, ingested_at) are appended. BTREE on resolution
    keys; BITMAP on low-cardinality categoricals.

CONTROL PLANE
    Terminal state for each member is written to ops.fdic_runs (psycopg, best-effort —
    an audit-write failure never masks an otherwise-good load). scripts/data-factory-catalog.py
    reads that ledger to surface FDIC as an active ingest source.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import os.path
import re
import sys
import tempfile

BUCKET = "data-sink"
LANDING_PREFIX = "landing/fdic/"
SCRATCH_DIR = tempfile.gettempdir()

# Export-date stamped in the BankFind filenames (=current snapshot). Override with --as-of.
AS_OF_DEFAULT = "2026-06-21"

# Net-new datasets → pin the current Lance default storage version (02_lancedb_storage.md §2.3).
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576          # binds first; every member is < 1 fragment
MAX_BYTES_PER_FILE = 90 * 1024**3      # 90 GiB — Lance default / fleet constant

# Member registry. ``match`` selects the latest landing object by basename (date-stamp
# agnostic); ``exclude`` removes the redundant active/inactive partition re-exports.
# btree = high-cardinality resolution keys (equality + range). bitmap = low-card categoricals.
DATASETS: dict[str, dict] = {
    "institutions": {
        "uri": os.environ.get("FDIC_INSTITUTIONS_LANCE_URI", f"s3://{BUCKET}/active/fdic_institutions/"),
        "match": r"^Institutions_.*\.csv$",
        "exclude": r"-[23]\.csv$",
        "btree": ["cert", "name"],
        "bitmap": ["active", "bkclass", "stalp", "stname", "fdicregn", "mutual", "trust", "specgrp"],
    },
    "financial": {
        "uri": os.environ.get("FDIC_FINANCIAL_LANCE_URI", f"s3://{BUCKET}/active/fdic_financial/"),
        "match": r"^Financial_.*\.csv$",
        "btree": ["cert", "repdte"],
        "bitmap": ["stalp", "bkclass", "active"],
    },
    "locations": {
        "uri": os.environ.get("FDIC_LOCATIONS_LANCE_URI", f"s3://{BUCKET}/active/fdic_locations/"),
        "match": r"^Locations_.*\.csv$",
        "btree": ["uninum", "cert"],
        "bitmap": ["stalp", "bkclass", "servtype", "mainoff"],
    },
    "sod": {
        "uri": os.environ.get("FDIC_SOD_LANCE_URI", f"s3://{BUCKET}/active/fdic_sod/"),
        "match": r"^SOD_.*\.csv$",
        "btree": ["uninumbr", "cert"],
        "bitmap": ["year", "stalpbr", "bkclass", "bkmo"],
    },
    "structure_changes": {
        "uri": os.environ.get("FDIC_STRUCTURE_CHANGES_LANCE_URI", f"s3://{BUCKET}/active/fdic_structure_changes/"),
        "match": r"^Historical_.*\.csv$",
        "btree": ["cert", "transnum"],
        "bitmap": ["changecode", "pstalp", "class"],
    },
    "failures": {
        "uri": os.environ.get("FDIC_FAILURES_LANCE_URI", f"s3://{BUCKET}/active/fdic_failures/"),
        "match": r"^Failures_.*\.csv$",
        "btree": ["cert", "faildate"],
        "bitmap": ["pstalp", "chclass", "restype"],
    },
}

OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.fdic_runs (
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
CREATE INDEX IF NOT EXISTS fdic_runs_dataset_idx     ON ops.fdic_runs (dataset);
CREATE INDEX IF NOT EXISTS fdic_runs_status_idx      ON ops.fdic_runs (status);
CREATE INDEX IF NOT EXISTS fdic_runs_recorded_at_idx ON ops.fdic_runs (recorded_at DESC);
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


def _discover_key(s3, match: str, exclude: str | None = None) -> str:
    """Newest landing object whose basename matches ``match`` (and not ``exclude``)."""
    rx = re.compile(match, re.I)
    ex = re.compile(exclude, re.I) if exclude else None
    found: list[tuple[str, int, dt.datetime]] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=LANDING_PREFIX):
        for o in page.get("Contents", []):
            base = o["Key"].rsplit("/", 1)[-1]
            if rx.match(base) and not (ex and ex.search(base)):
                found.append((o["Key"], o["Size"], o["LastModified"]))
    if not found:
        raise RuntimeError(f"No landing object matches /{match}/ under s3://{BUCKET}/{LANDING_PREFIX}")
    found.sort(key=lambda t: (t[2], t[1]), reverse=True)
    return found[0][0]


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


# ── Lance scalar indexes ───────────────────────────────────────────────────────────
def _build_indexes(uri: str, btree: list[str], bitmap: list[str], so: dict) -> list[str]:
    """BTREE on resolution keys, BITMAP on categoricals. create_scalar_index defaults to
    replace=True (idempotent). A missing/odd column must not fail the load."""
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


# ── ops.fdic_runs ledger ───────────────────────────────────────────────────────────
def _record_run(dataset, uri, source_file, as_of, rows, rejected, built, status,
                error, started_at, completed_at) -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.fdic_runs write.")
        return
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.fdic_runs
                    (dataset, dataset_uri, source_file, as_of, rows_processed,
                     rejected_rows, indexes_built, status, error, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (dataset, uri, source_file, as_of, rows, rejected, built, status,
                 error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        print(f"WARN: ops.fdic_runs write failed: {exc}")


def apply_state_schema() -> None:
    import psycopg

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set (Doppler core-x/prd).")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(OPS_DDL)
        conn.commit()
    print("Applied ops.fdic_runs schema.")


# ── one member: landing CSV → DuckDB project/cast → Lance overwrite → indexes ──────
def ingest_dataset(name: str, as_of: str = AS_OF_DEFAULT, keep_temp: bool = False) -> dict:
    import duckdb
    import lance  # noqa: F401 — fail fast if pylance missing before the heavy read

    if name not in DATASETS:
        raise ValueError(f"dataset must be one of {sorted(DATASETS)}, got {name!r}")
    spec = DATASETS[name]
    uri = spec["uri"]
    so = _storage_options()
    s3 = _s3_client()

    started_at = dt.datetime.now(dt.timezone.utc)
    rows = rejected = 0
    built: list[str] = []
    status, error, source_file = "error", None, None

    key = _discover_key(s3, spec["match"], spec.get("exclude"))
    source_file = key.rsplit("/", 1)[-1]
    csv_path = os.path.join(SCRATCH_DIR, f"fdic_{name}.csv")
    try:
        print(f"[{name}] landing s3://{BUCKET}/{key} -> {csv_path}")
        s3.download_file(BUCKET, key, csv_path)

        con = duckdb.connect(":memory:")
        try:
            con.execute("PRAGMA threads=8;")
            con.execute(f"SET temp_directory='{os.path.join(SCRATCH_DIR, 'duckdb_spill')}';")

            read = (
                "read_csv(?, delim=',', quote='\"', escape='\"', header=true, "
                "all_varchar=true, strict_mode=false, null_padding=true, "
                "sample_size=-1, max_line_size=40000000{rej})"
            )
            # header probe (no reject table)
            cols = [d[0] for d in con.execute(
                f"SELECT * FROM {read.format(rej='')} LIMIT 0", [csv_path]).description]
            pairs = _norm_pairs(cols)
            projection = ",\n    ".join(
                f"nullif(trim(\"{o}\"), '') AS \"{n}\"" for o, n in pairs)
            sql = (
                "SELECT\n    " + projection + ",\n"
                "    ? AS source_file,\n"
                "    CAST(? AS DATE) AS snapshot_date,\n"
                "    now() AS ingested_at\n"
                f"FROM {read.format(rej=', store_rejects=true')}"
            )
            table = con.execute(sql, [source_file, as_of, csv_path]).to_arrow_table()
            rows = table.num_rows
            try:
                rejected = int(con.execute(
                    "SELECT count(*) FROM reject_errors").fetchone()[0])
            except Exception:  # noqa: BLE001 — no reject table ⇒ zero rejects
                rejected = 0
        finally:
            con.close()
        print(f"[{name}] parsed {rows:,} rows ({table.num_columns} cols), {rejected:,} rejected")

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
        _record_run(name, uri, source_file, as_of, int(rows), int(rejected),
                    built, status, error, started_at, completed_at)
        if not keep_temp:
            try:
                os.remove(csv_path)
            except OSError:
                pass

    return {"dataset": name, "rows": int(rows), "rejected": int(rejected),
            "indexes": built, "uri": uri, "source_file": source_file, "as_of": as_of}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="FDIC BankFind landing → Lance ingest.")
    ap.add_argument("--dataset", choices=sorted(DATASETS), help="single member")
    ap.add_argument("--all", action="store_true", help="all members")
    ap.add_argument("--as-of", default=AS_OF_DEFAULT, help="snapshot date (YYYY-MM-DD)")
    ap.add_argument("--init-state", action="store_true", help="apply ops.fdic_runs DDL and exit")
    ap.add_argument("--keep-temp", action="store_true", help="keep downloaded CSVs in scratch")
    args = ap.parse_args(argv)

    if args.init_state:
        apply_state_schema()
        return 0
    targets = list(DATASETS) if args.all else ([args.dataset] if args.dataset else [])
    if not targets:
        ap.error("specify --all or --dataset <name> (or --init-state)")

    results = []
    for name in targets:
        results.append(ingest_dataset(name, as_of=args.as_of, keep_temp=args.keep_temp))

    print("\n=== FDIC ingest summary ===")
    total = 0
    for r in results:
        total += r["rows"]
        print(f"  {r['dataset']:20s} rows={r['rows']:>9,}  rejected={r['rejected']:>4}  "
              f"idx={len(r['indexes']):>2}  -> {r['uri']}")
    print(f"  {'TOTAL':20s} rows={total:>9,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
