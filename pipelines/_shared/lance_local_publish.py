"""Local-stage Lance publish — the R2-safe way to land an indexed dataset.

Lance's native R2 object writer streams adaptive-sized multipart parts, which R2 rejects
with 400 InvalidPart ("All non-trailing parts must have the same length") once an index's
page_data widens (observed from ~30M rows on VARCHAR BTREEs; reproduces on lance 7.x and
8.x). Data-file writes use uniform parts and survive; index writes do not.

Pattern (same as the award/FPDS canonical Modal wrappers): write the dataset to LOCAL
disk, create scalar indices against the local FS (no multipart), then upload the whole
tree file-by-file with boto3 s3transfer (uniform parts, per-part retries), wiping the
target prefix first. Data + indices land atomically from the reader's perspective — the
manifest uploads with everything else.

    from pipelines._shared.lance_local_publish import write_indexed_dataset
    write_indexed_dataset(reader, "s3://data-sink/active/my_ds/", [("uei", "BTREE")],
                          storage_options=so())
"""
from __future__ import annotations

import os
import shutil
import tempfile

import lance

BUCKET = "data-sink"


def _s3(storage_options: dict):
    import boto3
    return boto3.client("s3", endpoint_url=storage_options["endpoint"],
                        aws_access_key_id=storage_options["aws_access_key_id"],
                        aws_secret_access_key=storage_options["aws_secret_access_key"],
                        region_name="auto")


def write_indexed_dataset(reader, uri: str, indices: list[tuple[str, str]],
                          storage_options: dict, stage_root: str | None = None) -> "lance.LanceDataset":
    """Write `reader` + scalar `indices` to `uri` via local staging. Returns the remote dataset."""
    assert uri.startswith(f"s3://{BUCKET}/"), uri
    prefix = uri.replace(f"s3://{BUCKET}/", "")
    stage = tempfile.mkdtemp(prefix="lance_pub_", dir=stage_root or "/tmp")
    local_ds = os.path.join(stage, "ds")
    try:
        lance.write_dataset(reader, local_ds, mode="create")
        ds = lance.dataset(local_ds)  # LOCAL — no storage_options, no R2 writer
        for col, kind in indices:
            ds.create_scalar_index(col, kind)

        s3 = _s3(storage_options)
        # wipe target prefix (snapshot-overwrite semantics), then upload the staged tree
        keys: list[dict] = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
            keys.extend({"Key": o["Key"]} for o in page.get("Contents", []))
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys[i:i + 1000], "Quiet": True})
        uploaded = 0
        for root, _dirs, files in os.walk(local_ds):
            for f in files:
                lp = os.path.join(root, f)
                rel = os.path.relpath(lp, local_ds).replace(os.sep, "/")
                s3.upload_file(lp, BUCKET, prefix + rel)
                uploaded += 1
        remote = lance.dataset(uri, storage_options=storage_options)
        assert remote.count_rows() == ds.count_rows(), \
            f"publish row mismatch: remote {remote.count_rows()} != local {ds.count_rows()}"
        return remote
    finally:
        shutil.rmtree(stage, ignore_errors=True)
