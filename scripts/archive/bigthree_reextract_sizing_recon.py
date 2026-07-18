#!/usr/bin/env python3
"""READ-ONLY schema recon for the Big-Three labor re-extraction sizing.

Dumps schema + rowcount for the four datasets the sizing join touches, plus the
distinct-value cardinality of the extraction-ledger state column and whether the
ledger carries a per-event timestamp (needed to dedup re-run rows to one per
resource). NO writes, NO full scans beyond count(*) and a tiny head.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/bigthree_reextract_sizing_recon.py
"""
from __future__ import annotations

import json
import os
import sys

import duckdb
import lance

A = "s3://data-sink/active"
DATASETS = {
    "gaa": f"{A}/govcon_active_awards/",
    "manifest_winners": f"{A}/sam_opps_attachment_manifest_winners/",
    "files": f"{A}/sam_attachment_files/",
    "extraction": f"{A}/sam_attachment_extraction/",
}


def r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def log(m: str) -> None:
    print(m, file=sys.stderr, flush=True)


def main() -> int:
    so = r2_so()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    out: dict = {}
    for name, uri in DATASETS.items():
        try:
            ds = lance.dataset(uri, storage_options=so)
            cols = [f.name for f in ds.schema]
            n = ds.count_rows()
            out[name] = {"uri": uri, "rows": n, "columns": cols}
            log(f"{name}: {n:,} rows, {len(cols)} cols")
        except Exception as exc:  # noqa: BLE001
            out[name] = {"uri": uri, "error": f"{type(exc).__name__}: {exc}"}
            log(f"{name}: ERROR {exc}")

    # extraction-ledger state distribution + dedup-key candidates (only if it loaded)
    ex = out.get("extraction", {})
    if "columns" in ex:
        con.register("ex", lance.dataset(DATASETS["extraction"], storage_options=so))
        cols = ex["columns"]
        if "state" in cols:
            ex["state_dist"] = con.execute(
                "SELECT state, count(*) n, count(DISTINCT resource_id) d "
                "FROM ex GROUP BY 1 ORDER BY n DESC").fetchall()
        ex["distinct_resource_ids"] = con.execute(
            "SELECT count(DISTINCT resource_id) FROM ex").fetchone()[0]
        ex["total_rows_vs_distinct"] = con.execute(
            "SELECT count(*), count(DISTINCT resource_id) FROM ex").fetchone()
        ts_candidates = [c for c in cols if any(
            k in c.lower() for k in ("_at", "ts", "time", "ingested", "landed", "event"))]
        ex["timestamp_candidates"] = ts_candidates
        chunk_candidates = [c for c in cols if any(
            k in c.lower() for k in ("chunk", "char", "yield", "byte", "size"))]
        ex["volume_candidates"] = chunk_candidates

    # gaa psc + active flags presence
    gaa = out.get("gaa", {})
    if "columns" in gaa:
        con.register("gaa", lance.dataset(DATASETS["gaa"], storage_options=so))
        for flag in ("active_current", "active_potential", "pop_unknown"):
            if flag in gaa["columns"]:
                gaa.setdefault("active_flag_counts", {})[flag] = con.execute(
                    f"SELECT count(*) FILTER (WHERE {flag}) FROM gaa").fetchone()[0]
        gaa["total_rows_check"] = con.execute("SELECT count(*) FROM gaa").fetchone()[0]

    # files status distribution
    fl = out.get("files", {})
    if "columns" in fl and "status" in fl["columns"]:
        con.register("fl", lance.dataset(DATASETS["files"], storage_options=so))
        fl["status_dist"] = con.execute(
            "SELECT status, count(*) n, count(DISTINCT resource_id) d "
            "FROM fl GROUP BY 1 ORDER BY n DESC").fetchall()

    json.dump(out, sys.stdout, indent=2, default=str)
    print("\nDONE", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
