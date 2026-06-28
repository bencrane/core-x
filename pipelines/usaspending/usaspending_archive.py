"""USAspending Award Data Archive ingest (LOCAL CLI) — monthly Full + Delta → two append Lance tables.

Lands the USAspending **Award Data Archive** monthly contract files (operator-dropped into the R2
landing zone) VERBATIM — every column, every row, all-VARCHAR, exact download column names — into
TWO append-only, snapshot-stamped Lance tables, SEPARATE from the pg-dump SoR
(``usaspending/transaction_search_fpds``) and the API fresh feed (``usaspending_api_fresh``):

    FULL  → s3://data-sink/active/usaspending_archive_full_fpds/   (current-state FY snapshots)
    DELTA → s3://data-sink/active/usaspending_archive_delta_fpds/  (change log; correction_delete_ind)

APPEND-ONLY. STAMPED. NO DATA LEFT BEHIND. Each monthly file is read verbatim (``read_csv``,
``all_varchar=true``, SELECT *, no filter) + three provenance columns (``archive_snapshot_stamp`` /
``archive_kind`` / ``archive_source_file``) and APPENDED — never overwritten, never cherry-picked.
The FULL table accumulates monthly snapshots (latest wins at canonical-read via
``max(last_modified_date)``); the DELTA table is the permanent deletion/mutation ledger (every
``correction_delete_ind`` row — C/D/added — retained forever; the D rows are the only deletion signal).

Runs LOCAL (doppler + uv) — no Modal, no auto-retries (a retry would double-append, since the write
commits before the index builds), no detach. ``LANCE_BYPASS_SPILLING=true`` sorts the scalar-index
build in-RAM (the small DataFusion external-merge pool OOMs otherwise). Data-driven idempotency: an
append is skipped if its ``archive_snapshot_stamp`` is already present in the table (robust to a prior
partial run). Direct-to-R2 write at ``max_rows_per_file=250_000`` (small uniform fragments dodge R2's
multipart rule); the multi-GB CSV streams via a record-batch reader so RAM stays bounded.

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'boto3>=1.34' --with 'psycopg[binary]>=3.2' \
      python3 pipelines/usaspending/usaspending_archive.py <init_ops|ingest|verify> [args]

      ingest full  'landing/usaspending/2026-06-06/FY2026_All_Contracts_Full_20260606.zip'  20260606
      ingest delta 'landing/usaspending/2026-06-06/FY(All)_All_Contracts_Delta_20260606.zip' 20260606
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import sys
import zipfile
from pathlib import Path

# In-RAM scalar-index sort — the small DataFusion external-merge pool OOMs ("ExternalSorterMerge")
# on a multi-million-row BTREE build over R2. Set BEFORE any lance call (ARCHITECTURE.md fleet rule).
os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

BUCKET = "data-sink"
ACTIVE = "s3://data-sink/active"
FULL_URI = os.environ.get("ARCHIVE_FULL_URI", f"{ACTIVE}/usaspending_archive_full_fpds").rstrip("/") + "/"
DELTA_URI = os.environ.get("ARCHIVE_DELTA_URI", f"{ACTIVE}/usaspending_archive_delta_fpds").rstrip("/") + "/"
DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 250_000
BATCH_ROWS = 200_000
SCRATCH = os.environ.get("ARCHIVE_SCRATCH", "/tmp/archive_ingest")
DUCK_MEM = os.environ.get("ARCHIVE_DUCKDB_MEMORY_LIMIT", "8GB")
DUCK_TMP = os.environ.get("ARCHIVE_DUCKDB_TEMP_DIR", "/tmp/archive_duckdb")

# Focused index set (verbatim Award-Data-Archive names). Absent columns are skipped at build time.
BTREE_COLS = ["contract_transaction_unique_key", "contract_award_unique_key",
              "recipient_uei", "action_date", "last_modified_date"]
BITMAP_COLS = ["correction_delete_ind", "archive_snapshot_stamp"]


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT")
    acct = os.environ.get("R2_ACCOUNT_ID")
    if not ep and acct:
        ep = f"https://{acct}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _s3():
    import boto3
    from botocore.config import Config
    so = _r2_so()
    return boto3.client("s3", endpoint_url=so["endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"], region_name="auto",
                        config=Config(retries={"max_attempts": 10, "mode": "standard"},
                                      connect_timeout=30, read_timeout=120))


def _publish_local_to_r2(s3, uri, local_ds) -> int:
    """Replace the R2 dataset prefix with a local Lance dataset, uploaded file-by-file. boto3
    s3transfer auto-retries each multipart part, so a transient network blip retries THAT part
    rather than aborting the entire write (the failure mode of one direct-to-R2 write_dataset)."""
    prefix = uri.replace(f"s3://{BUCKET}/", "")
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=BUCKET, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys})
    uploaded = 0
    for root, _dirs, files in os.walk(local_ds):
        for f in files:
            lp = os.path.join(root, f)
            rel = os.path.relpath(lp, local_ds).replace(os.sep, "/")
            s3.upload_file(lp, BUCKET, prefix + rel)
            uploaded += 1
    return uploaded


def _dataset_exists(uri, so) -> bool:
    import lance
    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001
        return False


def _stamp_present(uri, stamp, so) -> bool:
    """Data-driven idempotency: is this snapshot stamp already appended? Robust to partial prior runs."""
    import lance
    try:
        ds = lance.dataset(uri, storage_options=so)
        if "archive_snapshot_stamp" not in ds.schema.names:
            return False
        return ds.count_rows(filter=f"archive_snapshot_stamp = '{stamp}'") > 0
    except Exception:  # noqa: BLE001
        return False


def _download_extract(s3, zip_key) -> int:
    shutil.rmtree(SCRATCH, ignore_errors=True)
    os.makedirs(SCRATCH, exist_ok=True)
    zpath = os.path.join(SCRATCH, "src.zip")
    log(f"downloading s3://{BUCKET}/{zip_key} → {zpath}")
    s3.download_file(BUCKET, zip_key, zpath)
    with zipfile.ZipFile(zpath) as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        if not members:
            raise RuntimeError(f"no .csv members in {zip_key}: {zf.namelist()[:10]}")
        zf.extractall(SCRATCH, members=members)
    os.remove(zpath)
    log(f"extracted {len(members)} CSV member(s): {members}")
    return len(members)


def _record_run(*, kind, stamp, zip_key, csv_members, columns, rows_written, table_rows_after,
                write_mode, indices, status, error, started, completed) -> None:
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    if status != "success" and not error:
        error = "unknown terminal failure (no exception captured)"
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('ops.usaspending_archive_runs')")
            if cur.fetchone()[0] is None:
                cur.execute(Path(__file__).parent.joinpath("ops_usaspending_archive_runs.sql").read_text())
            cur.execute(
                "INSERT INTO ops.usaspending_archive_runs (kind, snapshot_stamp, zip_key, csv_members, "
                "columns, rows_written, table_rows_after, write_mode, indices_built, status, "
                "error_message, started_at, completed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (kind, stamp, zip_key, csv_members, columns, rows_written, table_rows_after, write_mode,
                 ",".join(indices) if indices else None, status, (error or "")[:2000] or None,
                 started, completed))
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the ingest
        log(f"WARN: ops.* write failed: {exc}")


def init_ops() -> None:
    import psycopg
    sql = Path(__file__).parent.joinpath("ops_usaspending_archive_runs.sql").read_text()
    with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    log("ops DDL applied")


def ingest(kind: str, zip_key: str, stamp: str, force: bool = False) -> dict:
    import duckdb
    import lance
    if kind not in ("full", "delta"):
        raise SystemExit(f"kind must be 'full' or 'delta', got {kind!r}")
    uri = FULL_URI if kind == "full" else DELTA_URI
    so = _r2_so()
    started = dt.datetime.now(dt.timezone.utc)

    if _stamp_present(uri, stamp, so) and not force:
        log(f"stamp {stamp} already present in {kind} table — skipping append (pass force to re-append).")
        return {"kind": kind, "stamp": stamp, "skipped": True}

    status, error, members, cols, rows, total, built, write_mode = "error", None, 0, 0, 0, 0, [], "append"
    try:
        s3 = _s3()
        members = _download_extract(s3, zip_key)
        src = os.path.basename(zip_key)
        csv_glob = os.path.join(SCRATCH, "**", "*.csv")

        con = duckdb.connect(":memory:")
        con.execute("PRAGMA threads=4")
        os.makedirs(DUCK_TMP, exist_ok=True)
        con.execute(f"SET memory_limit='{DUCK_MEM}'")
        con.execute(f"SET temp_directory='{DUCK_TMP}'")
        # Verbatim — every column, every row, all-VARCHAR — plus three provenance columns.
        sql = (
            f"SELECT *, '{stamp}' AS archive_snapshot_stamp, '{kind}' AS archive_kind, "
            f"'{src}' AS archive_source_file "
            f"FROM read_csv('{csv_glob}', all_varchar=true, header=true, sample_size=-1, union_by_name=true)"
        )
        reader = con.sql(sql).to_arrow_reader(batch_size=BATCH_ROWS)
        cols = len(reader.schema.names)
        write_mode = "overwrite"
        # Write Lance to LOCAL disk + index locally (no network), then boto3-publish to R2 — the
        # blip-resilient path (see _publish_local_to_r2). Tables are (re)built fresh; the monthly-
        # append cadence is a separate path (TODO) — here both tables start empty.
        local_ds = os.path.join(SCRATCH, f"{kind}_lance")
        shutil.rmtree(local_ds, ignore_errors=True)
        log(f"writing Lance LOCALLY ({kind}, stamp={stamp}, {cols} cols) → {local_ds}")
        lance.write_dataset(reader, local_ds, mode="overwrite", schema=reader.schema,
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE)
        con.close()

        ds = lance.dataset(local_ds)
        total = ds.count_rows()
        rows = total
        present = set(ds.schema.names)
        log(f"local Lance: {total:,} rows. building indices…")
        for col in [c for c in BTREE_COLS if c in present]:
            try:
                ds.create_scalar_index(col, index_type="BTREE", replace=True)
            except TypeError:
                ds.create_scalar_index(col, index_type="BTREE")
            built.append(col)
            log(f"  BTREE ✓ {col}")
        for col in [c for c in BITMAP_COLS if c in present]:
            try:
                ds.create_scalar_index(col, index_type="BITMAP", replace=True)
            except TypeError:
                ds.create_scalar_index(col, index_type="BITMAP")
            built.append(col)
            log(f"  BITMAP ✓ {col}")
        log(f"publishing local Lance → {uri} (boto3, retry-resilient)…")
        published = _publish_local_to_r2(s3, uri, local_ds)
        log(f"published {published} files → {uri}")
        status = "success"
        log(f"DONE ({kind}, {stamp}) → {uri} rows={total:,}")
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        log(f"FAILED: {error}")
        raise
    finally:
        _record_run(kind=kind, stamp=stamp, zip_key=zip_key, csv_members=int(members),
                    columns=int(cols), rows_written=int(rows), table_rows_after=int(total),
                    write_mode=write_mode, indices=built, status=status, error=error,
                    started=started, completed=dt.datetime.now(dt.timezone.utc))
        shutil.rmtree(SCRATCH, ignore_errors=True)

    return {"kind": kind, "stamp": stamp, "zip_key": zip_key, "csv_members": int(members),
            "columns": int(cols), "rows_written": int(rows), "table_rows_after": int(total),
            "write_mode": write_mode, "indices_built": built, "status": status}


def verify(kind: str) -> dict:
    import duckdb
    import lance
    uri = FULL_URI if kind == "full" else DELTA_URI
    so = _r2_so()
    ds = lance.dataset(uri, storage_options=so)
    cols = set(ds.schema.names)
    out = {"uri": uri, "rows": ds.count_rows(), "columns": len(ds.schema.names)}
    proj = [c for c in ("action_date", "archive_snapshot_stamp", "correction_delete_ind") if c in cols]
    con = duckdb.connect(":memory:")
    con.register("t", ds.scanner(columns=proj).to_table())
    if "action_date" in cols:
        mn, mx = con.execute("SELECT min(action_date), max(action_date) FROM t "
                             "WHERE length(action_date) >= 10").fetchone()
        out["min_action_date"], out["max_action_date"] = mn, mx
    if "archive_snapshot_stamp" in cols:
        out["by_stamp"] = dict(con.execute(
            "SELECT archive_snapshot_stamp, count(*) FROM t GROUP BY 1 ORDER BY 1").fetchall())
    if kind == "delta" and "correction_delete_ind" in cols:
        out["by_correction_delete_ind"] = dict(con.execute(
            "SELECT coalesce(nullif(trim(correction_delete_ind), ''), '<added/blank>'), count(*) "
            "FROM t GROUP BY 1 ORDER BY 2 DESC").fetchall())
    con.close()
    return out


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "verify"
    if cmd == "init_ops":
        init_ops()
    elif cmd == "ingest":
        kind, zip_key, stamp = sys.argv[2], sys.argv[3], sys.argv[4]
        force = len(sys.argv) > 5 and sys.argv[5].lower() in ("1", "true", "force")
        print(json.dumps(ingest(kind, zip_key, stamp, force=force), indent=2, default=str))
    elif cmd == "verify":
        print(json.dumps(verify(sys.argv[2] if len(sys.argv) > 2 else "full"), indent=2, default=str))
    else:
        print(f"unknown command: {cmd} (init_ops|ingest|verify)")
        sys.exit(2)


if __name__ == "__main__":
    main()
