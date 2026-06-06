#!/usr/bin/env python3
"""Ground-truth deep probe of the core-x Gen-3 Lance system of record (R2).

Read-only. Recursively discovers every LEAF Lance dataset under the served R2
prefixes, then for each emits: typed schema, row count, scalar/vector indices,
system staleness (latest manifest timestamp + version count) and data staleness
(MAX of the detected primary date column, returned as a STRING so DuckDB never
needs a tz->python conversion). Emits a JSON array to stdout.

This is the engine behind the federal-ingest ground-truth audit. It complements
``scripts/data-factory-catalog.py`` (which lists names only) by reading the
manifest/index/stat surface of every dataset.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/lance_ground_truth_probe.py > /tmp/lance_probe.json

Required env (Doppler core-x/prd): R2_ENDPOINT (or R2_ACCOUNT_ID),
R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY.

Notes
-----
* ``active/`` mixes leaf datasets with CONTAINER prefixes (``usaspending/``,
  ``fmcsa/``, ``ca_ucc/``, ``nppes/``) — discovery recurses to the manifest
  (``_versions/``) so containers expand to their true leaf datasets.
* ``usaspending_api_landings/`` is the USAspending daily-API landing tier and
  lives OUTSIDE ``active/``; it is partitioned by ``pull_date=`` sub-datasets.
* ``landing/`` is raw gz transport (NOT Lance) and is deliberately excluded.
* Giant tables cap the data-staleness scan at STALENESS_TIMEOUT_S; the cap is
  recorded in ``data_staleness_err`` rather than silently dropped.
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import re
import sys
import time

import boto3
import duckdb
import lance
import pyarrow as pa
from botocore.config import Config

BUCKET = "data-sink"
ROOTS = ["active/", "sam-gov-opps/", "usaspending_api_landings/"]
MAX_DEPTH = 4
STALENESS_TIMEOUT_S = 150

DATE_COL_PRIORITY = [
    "last_modified_date", "last_update_date", "action_date", "posted_date",
    "snapshot_date", "registration_date", "recorded_at", "updated_at",
    "harvested_at", "ingested_at", "period_of_performance_current_end_date",
    "filing_date", "date_filed", "period_end", "actual_date", "pull_date",
]
DATE_NAME_RE = re.compile(r"(_date$|_at$|_ts$|date_|timestamp|^date$|_dt$)", re.I)


def storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def s3_client():
    o = storage_options()
    cfg = Config(request_checksum_calculation="when_required",
                 response_checksum_validation="when_required")
    return boto3.client("s3", endpoint_url=o["endpoint"],
                        aws_access_key_id=o["aws_access_key_id"],
                        aws_secret_access_key=o["aws_secret_access_key"],
                        region_name="auto", config=cfg)


def is_temporal(t) -> bool:
    return pa.types.is_timestamp(t) or pa.types.is_date(t) or pa.types.is_time(t)


def is_vector(t) -> bool:
    return pa.types.is_fixed_size_list(t) and pa.types.is_floating(t.value_type)


def is_leaf(c, prefix: str) -> bool:
    return c.list_objects_v2(Bucket=BUCKET, Prefix=prefix + "_versions/",
                             MaxKeys=1).get("KeyCount", 0) > 0


def children(c, prefix: str) -> list[str]:
    out = []
    for page in c.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in (page.get("CommonPrefixes") or []):
            n = cp["Prefix"].removeprefix(prefix).rstrip("/")
            if n and not n.startswith("_"):
                out.append(n)
    return sorted(out)


def discover(c, prefix: str, depth: int = 0) -> list[str]:
    if is_leaf(c, prefix):
        return [prefix]
    if depth >= MAX_DEPTH:
        return []
    leaves = []
    for ch in children(c, prefix):
        leaves += discover(c, prefix + ch + "/", depth + 1)
    return leaves


def pick_date_col(fields):
    names = {n for n, _ in fields}
    for cand in DATE_COL_PRIORITY:
        if cand in names:
            return cand
    for n, t in fields:
        if is_temporal(t):
            return n
    for n, t in fields:
        if DATE_NAME_RE.search(n):
            return n
    return None


def data_staleness(uri, col, typ, so):
    def _run():
        ds = lance.dataset(uri, storage_options=so)
        reader = ds.scanner(columns=[col]).to_reader()
        con = duckdb.connect()
        con.execute("SET memory_limit='10GB'; SET temp_directory='/tmp/duck_spill';")
        con.register("t", reader)
        q = (f'SELECT CAST(max("{col}") AS VARCHAR) FROM t' if is_temporal(typ)
             else f'SELECT CAST(max(TRY_CAST("{col}" AS TIMESTAMP)) AS VARCHAR) FROM t')
        v = con.execute(q).fetchone()[0]
        con.close()
        return v
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_run)
        try:
            return fut.result(timeout=STALENESS_TIMEOUT_S), None
        except concurrent.futures.TimeoutError:
            return None, f"timeout>{STALENESS_TIMEOUT_S}s"
        except Exception as e:
            return None, f"{type(e).__name__}: {str(e).splitlines()[0][:160]}"


def probe_one(uri, so) -> dict:
    rec = {"uri": uri, "name": uri.rstrip("/").split("/", 1)[1]}
    try:
        ds = lance.dataset(uri, storage_options=so)
    except Exception as e:
        rec["error"] = f"open: {type(e).__name__}: {e}"
        return rec
    fields = [(f.name, f.type) for f in ds.schema]
    rec["n_cols"] = len(fields)
    rec["columns"] = [{"name": n, "type": str(t)} for n, t in fields]
    rec["vector_cols"] = [n for n, t in fields if is_vector(t)]
    rec["temporal_cols"] = [n for n, t in fields if is_temporal(t)]
    try:
        rec["rows"] = ds.count_rows()
    except Exception as e:
        rec["rows"] = None
        rec["rows_error"] = str(e)[:120]
    idx = []
    try:
        for i in ds.list_indices():
            d = dict(i) if not isinstance(i, dict) else i
            idx.append({"name": d.get("name"), "type": d.get("type") or d.get("index_type"),
                        "fields": d.get("fields") or d.get("columns")})
    except Exception as e:
        rec["indices_error"] = str(e)[:120]
    rec["indices"] = idx
    rec["index_summary"] = ", ".join(
        f'{(i.get("fields") or i.get("name"))}:{i.get("type")}' for i in idx) or "—"
    try:
        vs = ds.versions()
        rec["n_versions"] = len(vs)
        latest = vs[-1]
        ts = latest.get("timestamp") if isinstance(latest, dict) else getattr(latest, "timestamp", None)
        rec["latest_version"] = latest.get("version") if isinstance(latest, dict) else getattr(latest, "version", None)
        rec["system_staleness"] = ts.isoformat() if hasattr(ts, "isoformat") else (str(ts) if ts else None)
    except Exception as e:
        rec["system_staleness_error"] = str(e)[:120]
    col = pick_date_col(fields)
    rec["date_col"] = col
    if col:
        rec["data_staleness"], rec["data_staleness_err"] = data_staleness(uri, col, dict(fields)[col], so)
    else:
        rec["data_staleness"], rec["data_staleness_err"] = None, "no-date-col"
    return rec


def main() -> int:
    so = storage_options()
    c = s3_client()
    leaves = []
    for root in ROOTS:
        found = discover(c, root)
        print(f"[discover] {root} -> {len(found)} leaves", file=sys.stderr, flush=True)
        leaves += found
    leaves = sorted(set(leaves))
    print(f"[discover] total {len(leaves)} leaf datasets", file=sys.stderr, flush=True)
    results = []
    for i, uri in enumerate(leaves, 1):
        t0 = time.time()
        rec = probe_one(f"s3://{BUCKET}/{uri}", so)
        results.append(rec)
        print(f"[{i}/{len(leaves)}] {rec['name']:<52} rows={rec.get('rows')} "
              f"max={rec.get('data_staleness')} ({round(time.time()-t0,1)}s)",
              file=sys.stderr, flush=True)
    json.dump(results, sys.stdout, indent=2, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
