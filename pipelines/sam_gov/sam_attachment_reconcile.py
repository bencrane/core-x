"""Reconcile the 90-day attachment harvest — standalone, re-runnable, idempotent.

Durability backstop for sam_attachment_download.py. Proves (and with --backfill,
RESTORES) consistency between the three durable artifacts, depending on NOTHING in the
local session:

  * CAS blobs   -> s3://data-sink/active/sam_attachment_blobs/<resource_id>   (the bytes)
  * Lance ledger-> s3://data-sink/active/sam_attachment_files/                (the catalog)
  * worklist    -> s3://data-sink/active/sam_attachment_worklist/             (resource_id -> file_name + meta)

The blob bytes are durable per-file the instant they land; the ledger commits every N files.
A crash between ledger flushes therefore leaves *orphan blobs* (bytes present, no catalog row)
— the bytes are never lost, only the row. Because the worklist snapshot carries file_name and
all lineage for every resource_id (and is itself in R2, immune to manifest re-snapshot), the
full ledger row for any orphan is reconstructable from R2 alone. That is the structural reason
the historical T4 lineage loss CANNOT recur here.

Report (read-only):
    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with boto3 \
      python pipelines/sam_gov/sam_attachment_reconcile.py

Heal orphans (single-writer only — run at completion or during a resume, never alongside a
live writer; Lance append is not concurrent-safe):
    ... python pipelines/sam_gov/sam_attachment_reconcile.py --backfill
"""

from __future__ import annotations

import argparse
import datetime as dt
import os

BLOB_PREFIX = os.environ.get("SAM_BLOB_PREFIX", "s3://data-sink/active/sam_attachment_blobs/")
LEDGER_URI = os.environ.get("SAM_LEDGER_URI", "s3://data-sink/active/sam_attachment_files/")
WORKLIST_URI = os.environ.get("SAM_WORKLIST_URI", "s3://data-sink/active/sam_attachment_worklist/")
_CONTENT_TYPE = {"pdf": "application/pdf",
                 "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                 "doc": "application/msword",
                 "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                 "txt": "text/plain", "zip": "application/zip"}


def _so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _split_s3(uri: str) -> tuple[str, str]:
    body = uri[len("s3://"):] if uri.startswith("s3://") else uri
    bucket, _, key = body.partition("/")
    if key and not key.endswith("/"):
        key += "/"
    return bucket, key


def _norm_mime(m: str | None) -> str:
    return (m or "").lstrip(".").lower()


def _s3():
    import boto3
    from botocore.config import Config
    so = _so()
    return boto3.client("s3", endpoint_url=so["endpoint"], aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"], region_name="auto",
                        config=Config(retries={"max_attempts": 3, "mode": "standard"},
                                      max_pool_connections=16))


def _ledger_schema():
    import pyarrow as pa
    return pa.schema([
        ("resource_id", pa.string()), ("file_name", pa.string()),
        ("mime_declared", pa.string()), ("mime_sniffed", pa.string()), ("mime_match", pa.bool_()),
        ("access_level", pa.string()),
        ("size_declared", pa.int64()), ("content_length", pa.int64()),
        ("size_downloaded", pa.int64()), ("size_match", pa.bool_()),
        ("sha256", pa.string()), ("http_status", pa.int32()), ("status", pa.string()),
        ("stored_uri", pa.string()), ("attempts", pa.int32()),
        ("notice_id", pa.string()), ("solicitation_number", pa.string()), ("naics_code", pa.string()),
        ("first_attempt_at", pa.timestamp("us", tz="UTC")),
        ("completed_at", pa.timestamp("us", tz="UTC")),
        ("error", pa.string()), ("run_id", pa.string()),
    ])


def _list_blobs(s3, blob_prefix: str) -> dict:
    bucket, prefix = _split_s3(blob_prefix)
    out: dict = {}
    tok = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            rid = o["Key"][len(prefix):]
            if rid:
                out[rid] = o["Size"]
        if not resp.get("IsTruncated"):
            break
        tok = resp.get("NextContinuationToken")
    return out


def _dataset_exists(uri: str, so: dict) -> bool:
    import lance
    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001
        return False


def reconcile(*, blob_prefix=BLOB_PREFIX, ledger_uri=LEDGER_URI, worklist_uri=WORKLIST_URI,
              backfill=False) -> dict:
    import lance
    import pyarrow as pa

    so = _so()
    s3 = _s3()
    bucket, key_prefix = _split_s3(blob_prefix)

    print("listing blobs (R2 CAS)...", flush=True)
    blobs = _list_blobs(s3, blob_prefix)
    print(f"  blobs: {len(blobs):,} objects, {sum(blobs.values()) / 1e9:.2f} GB", flush=True)

    ledger_dl: dict = {}
    if _dataset_exists(ledger_uri, so):
        rows = lance.dataset(ledger_uri, storage_options=so).to_table(
            columns=["resource_id", "status", "size_downloaded"]).to_pylist()
        for r in rows:
            if r["status"] == "downloaded":
                ledger_dl[r["resource_id"]] = r["size_downloaded"]
    print(f"  ledger downloaded rows: {len(ledger_dl):,}", flush=True)

    blob_rids = set(blobs)
    led_rids = set(ledger_dl)
    orphans = blob_rids - led_rids                 # bytes present, no catalog row
    missing = {r for r in led_rids if blobs.get(r, 0) == 0 and r not in blob_rids}  # row, no bytes
    size_mm = sum(1 for r in (blob_rids & led_rids)
                  if ledger_dl[r] is not None and blobs[r] != ledger_dl[r])
    print(f"  orphans (blob, no ledger row): {len(orphans):,}", flush=True)
    print(f"  missing  (ledger row, no blob): {len(missing):,}", flush=True)
    print(f"  size mismatches (blob != ledger): {size_mm:,}", flush=True)

    backfilled = 0
    if backfill and orphans:
        wmeta: dict = {}
        for r in lance.dataset(worklist_uri, storage_options=so).to_table(
                columns=["resource_id", "file_name", "mime_type", "size_bytes", "access_level",
                         "notice_id", "solicitation_number", "naics_code"]).to_pylist():
            wmeta[r["resource_id"]] = r
        now = dt.datetime.now(dt.timezone.utc)
        rows = []
        no_meta = 0
        for rid in orphans:
            m = wmeta.get(rid)
            if m is None:
                no_meta += 1
            size_dl = blobs[rid]
            decl = int((m or {}).get("size_bytes") or 0)
            rows.append({
                "resource_id": rid, "file_name": (m or {}).get("file_name"),
                "mime_declared": _norm_mime((m or {}).get("mime_type")) or None,
                "mime_sniffed": None, "mime_match": None,
                "access_level": (m or {}).get("access_level"),
                "size_declared": decl, "content_length": size_dl, "size_downloaded": size_dl,
                "size_match": (decl == size_dl) if decl else None, "sha256": None,
                "http_status": 200, "status": "downloaded",
                "stored_uri": f"s3://{bucket}/{key_prefix}{rid}", "attempts": 0,
                "notice_id": (m or {}).get("notice_id"),
                "solicitation_number": (m or {}).get("solicitation_number"),
                "naics_code": (m or {}).get("naics_code"),
                "first_attempt_at": now, "completed_at": now,
                "error": "reconcile-backfill", "run_id": "reconcile-backfill"})
        at = pa.Table.from_pylist(rows, schema=_ledger_schema())
        lance.write_dataset(at, ledger_uri, mode="append",
                            data_storage_version="2.1", storage_options=so)
        backfilled = len(rows)
        print(f"  backfilled {backfilled:,} orphan rows from worklist⨝CAS "
              f"({no_meta} had no worklist meta)", flush=True)
        lds = lance.dataset(ledger_uri, storage_options=so)
        for col, it in [("resource_id", "BTREE"), ("sha256", "BTREE"),
                        ("status", "BITMAP"), ("mime_declared", "BITMAP")]:
            try:
                lds.create_scalar_index(col, index_type=it, replace=True)
            except Exception as e:  # noqa: BLE001
                print(f"  index {col} skipped: {e}", flush=True)

    consistent = (len(orphans) == 0 or backfilled == len(orphans)) and len(missing) == 0
    print(f"RECONCILE: blobs={len(blobs):,} ledger_dl={len(ledger_dl):,} "
          f"orphans={len(orphans):,} missing={len(missing):,} size_mm={size_mm:,} "
          f"backfilled={backfilled:,} consistent={consistent}", flush=True)
    return {"blobs": len(blobs), "ledger_dl": len(ledger_dl), "orphans": len(orphans),
            "missing": len(missing), "size_mm": size_mm, "backfilled": backfilled,
            "consistent": consistent}


def _cli() -> None:
    p = argparse.ArgumentParser(description="Reconcile 90-day attachment harvest (blobs vs ledger).")
    p.add_argument("--backfill", action="store_true",
                   help="append ledger rows for orphan blobs from worklist⨝CAS (single-writer only)")
    p.add_argument("--blob-prefix", default=BLOB_PREFIX)
    p.add_argument("--ledger-uri", default=LEDGER_URI)
    p.add_argument("--worklist-uri", default=WORKLIST_URI)
    a = p.parse_args()
    reconcile(blob_prefix=a.blob_prefix, ledger_uri=a.ledger_uri,
              worklist_uri=a.worklist_uri, backfill=a.backfill)


if __name__ == "__main__":
    _cli()
