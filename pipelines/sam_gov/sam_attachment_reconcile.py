"""SAM.gov attachment download — reconciliation / audit closure (read-only, re-runnable).

Closes the audit loop between the R2 blob store and the Lance file-ledger, computed
on DISTINCT ``resource_id`` (one physical file):

  orphans = R2 object with no ``downloaded`` ledger row
  missing = ``downloaded`` ledger row with no R2 object (or a zero-byte object)
  corrupt = object size != ledger ``size_downloaded``  OR  sampled re-hash != ledger ``sha256``

Acceptance target: ``orphans = missing = corrupt = 0``. Exits non-zero on any drift so
it can gate CI / a launch script. Pure I/O — never mutates R2 or the ledger.

    doppler run --project core-x --config prd -- \
      uv run --with pylance --with pyarrow --with boto3 --with duckdb \
      python pipelines/sam_gov/sam_attachment_reconcile.py --rehash-sample 50

Smoke targets:
    ... python pipelines/sam_gov/sam_attachment_reconcile.py \
        --ledger-uri  s3://data-sink/active/_smoke_attach_files/ \
        --blob-prefix s3://data-sink/active/_smoke_blobs/
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import sys

LEDGER_URI = os.environ.get("SAM_ATTACH_LEDGER_URI", "s3://data-sink/active/sam_attachment_files/")
BLOB_PREFIX = os.environ.get("SAM_ATTACH_BLOB_PREFIX", "s3://data-sink/active/sam_attachment_blobs/")


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
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


def _split_s3(uri: str) -> tuple[str, str]:
    body = uri[len("s3://"):] if uri.startswith("s3://") else uri
    bucket, _, key = body.partition("/")
    if key and not key.endswith("/"):
        key += "/"
    return bucket, key


def reconcile(*, storage_options: dict, ledger_uri: str, blob_prefix: str,
              rehash_sample: int) -> bool:
    import duckdb
    import lance

    s3 = _s3_client()
    bucket, prefix = _split_s3(blob_prefix)

    # 1. R2 inventory: resource_id -> object size.
    objs: dict[str, int] = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            rid = o["Key"][len(prefix):]
            if rid:
                objs[rid] = o["Size"]
    print(f"R2: {len(objs):,} objects under {blob_prefix}", flush=True)

    # 2. ledger -> terminal row per distinct resource_id (downloaded preferred, then latest).
    tbl = lance.dataset(ledger_uri, storage_options=storage_options).to_table(
        columns=["resource_id", "status", "size_downloaded", "sha256", "completed_at"])
    con = duckdb.connect()
    con.register("l", tbl)
    n_rows = tbl.num_rows
    n_files = con.execute("SELECT count(DISTINCT resource_id) FROM l").fetchone()[0]
    dl_rows = con.execute("""
        SELECT resource_id, size_downloaded, sha256 FROM (
          SELECT *, row_number() OVER (PARTITION BY resource_id
                     ORDER BY (status = 'downloaded') DESC, completed_at DESC) AS rn
          FROM l
        ) WHERE rn = 1 AND status = 'downloaded'
    """).fetchall()
    led = {r[0]: (r[1], r[2]) for r in dl_rows}
    print(f"ledger: {n_rows:,} rows / {n_files:,} distinct files / {len(led):,} downloaded", flush=True)
    if n_rows != n_files:
        print(f"NOTE: {n_rows - n_files:,} duplicate ledger rows (resume retries) — terminal row used", flush=True)

    led_set, obj_set = set(led), set(objs)
    both = led_set & obj_set

    orphans = sorted(obj_set - led_set)
    missing = sorted(r for r in led_set if r not in obj_set or objs.get(r, 0) == 0)
    size_bad = [(r, objs[r], led[r][0]) for r in both if objs[r] != (led[r][0] if led[r][0] is not None else -1)]

    # 3. sampled content re-hash over size-clean files.
    pool = [r for r in both if objs[r] == (led[r][0] if led[r][0] is not None else -1)]
    sample = random.sample(pool, min(rehash_sample, len(pool))) if pool else []
    rehash_bad = []
    for rid in sample:
        body = s3.get_object(Bucket=bucket, Key=f"{prefix}{rid}")["Body"].read()
        if hashlib.sha256(body).hexdigest() != led[rid][1]:
            rehash_bad.append(rid)

    print("=" * 64)
    print(f"orphans  (object, no downloaded row) : {len(orphans):,}")
    print(f"missing  (downloaded row, no object) : {len(missing):,}")
    print(f"corrupt  (size mismatch)             : {len(size_bad):,}")
    print(f"corrupt  (rehash mismatch, n={len(sample)})    : {len(rehash_bad):,}")
    for label, lst in [("orphan", orphans[:10]), ("missing", missing[:10]),
                       ("size_bad", [x[0] for x in size_bad][:10]), ("rehash_bad", rehash_bad[:10])]:
        if lst:
            print(f"  sample {label}: {lst}")

    ok = not (orphans or missing or size_bad or rehash_bad)
    print(f"RECONCILE: {'PASS' if ok else 'FAIL'}")
    return ok


def _cli() -> None:
    p = argparse.ArgumentParser(description="SAM.gov attachment download reconciliation.")
    p.add_argument("--ledger-uri", default=LEDGER_URI)
    p.add_argument("--blob-prefix", default=BLOB_PREFIX)
    p.add_argument("--rehash-sample", type=int, default=50, help="objects to re-hash against the ledger")
    a = p.parse_args()
    ok = reconcile(storage_options=_r2_storage_options(), ledger_uri=a.ledger_uri,
                   blob_prefix=a.blob_prefix, rehash_sample=a.rehash_sample)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    _cli()
