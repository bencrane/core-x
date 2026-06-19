#!/usr/bin/env python
"""Subaward scope-enrichment lift — Phase E: append throwaway `_sublift_*` chunk rows into the SHARED
chunk sinks, idempotently by `chunk_id`.

This is the FIRST and ONLY shared-state write of the lift. Every preceding phase (route/expand/extract)
wrote only the throwaway `_sublift_*` namespace. Here, for each (throwaway -> shared) sink pair we:

  1. Read the throwaway chunk rows PROJECTION-ONLY, never the `embedding` column (it is NULL on the
     throwaway sinks anyway, and reading 1024-float vectors we are about to overwrite with NULL is
     pure waste; the embed refresh, Phase G, fills the shared NULL tail later).
  2. Re-add `embedding = NULL` for scope/unknown (pricing carries no embedding column — it has `cells`),
     reorder the columns to the LIVE shared schema's field order, and `cast` to it. The cast asserts
     column/type parity — Lance/pyarrow reject a `string`-vs-`large_string` mismatch on `text`/`cells`,
     so a drifted schema fails loudly rather than corrupting the sink.
  3. `merge_insert("chunk_id").when_matched_update_all().when_not_matched_insert_all()` under the SHARED
     sink's `SinkCommitLease`. `chunk_id = f"{rid}:{ix:04d}"` is deterministic, so a re-run rewrites
     identical values for matched rows => ZERO net row delta (idempotent). This is the same index-safe
     merge_insert path the embed module uses on these IVF_PQ-indexed sinks — Lance rewrites only matched
     fragments and the vector index keeps covering unmatched rows; the new NULL-embedding tail is
     brute-forced until the Phase G reindex.

Run (after Phase D extract + finalize complete, gate OFF, throwaway sinks populated):
    doppler run --project core-x --config prd -- <python-with-lance/pyarrow/boto3> \
        pipelines/sam_gov/subaward_scope_append.py

Idempotency / resumability: re-running is a no-op delta. A lost `$APPEND_CKPT` is harmless — the
merge_insert keys are deterministic, so the second run matches every row and changes nothing.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# repo root = .../pipelines/sam_gov/this_file -> parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import lance  # noqa: E402
import pyarrow as pa  # noqa: E402

from pipelines.sam_gov.sam_attachment_extract_90day import (  # noqa: E402
    SinkCommitLease,
    _dataset_exists,
    _pricing_schema,
    _r2_storage_options,
    _scope_schema,
    _unknown_schema,
)

# (throwaway_uri, shared_uri, schema_fn, sink_kind)
DEFAULT_PAIRS = [
    ("s3://data-sink/active/_sublift_scope/",   "s3://data-sink/active/govcon_scope_vectors", _scope_schema,   "scope"),
    ("s3://data-sink/active/_sublift_unknown/", "s3://data-sink/active/govcon_unknown",        _unknown_schema, "unknown"),
    ("s3://data-sink/active/_sublift_pricing/", "s3://data-sink/active/govcon_pricing",        _pricing_schema, "pricing"),
]
HOLDER = "subaward_scope_append"
LEASE_TTL_S = 24 * 60 * 60


def _non_embedding_cols(schema_fn) -> list[str]:
    """Column names of a sink schema EXCLUDING `embedding` (never read it)."""
    return [n for n in schema_fn().names if n != "embedding"]


def append_pair(tw: str, shared: str, schema_fn, kind: str, so: dict) -> dict:
    if not _dataset_exists(tw, so):
        print(f"{kind}: throwaway sink {tw} ABSENT — skipping (zero rows to append)", flush=True)
        return {"kind": kind, "skipped": True}
    cols = _non_embedding_cols(schema_fn)
    src = lance.dataset(tw, storage_options=so).to_table(columns=cols)   # projection, no embedding
    tgt_ds = lance.dataset(shared, storage_options=so)
    tgt_schema = tgt_ds.schema

    # Re-add embedding=NULL for the vector sinks (pricing has no embedding column).
    if "embedding" in tgt_schema.names:
        src = src.append_column(
            "embedding", pa.array([None] * src.num_rows, type=tgt_schema.field("embedding").type))

    # Parity guard: every shared-schema column must be present before the positional cast.
    missing = [n for n in tgt_schema.names if n not in src.schema.names]
    extra = [n for n in src.schema.names if n not in tgt_schema.names]
    if missing or extra:
        raise RuntimeError(f"{kind}: schema drift vs shared sink — missing={missing} extra={extra}")

    # Reorder to the shared schema's field order, then cast (large_string vs string, etc.).
    src = src.select(list(tgt_schema.names)).cast(tgt_schema)

    before = tgt_ds.count_rows()
    with SinkCommitLease(shared, holder=HOLDER, ttl_s=LEASE_TTL_S):
        tgt_ds = lance.dataset(shared, storage_options=so)  # re-open under the lease
        (tgt_ds.merge_insert("chunk_id")
               .when_matched_update_all()
               .when_not_matched_insert_all()
               .execute(src))
    after_ds = lance.dataset(shared, storage_options=so)
    after = after_ds.count_rows()
    # Uniqueness invariant after the merge.
    import duckdb
    con = duckdb.connect()
    con.register("t", after_ds.to_table(columns=["chunk_id"]))
    n, nd = con.execute("SELECT count(*), count(DISTINCT chunk_id) FROM t").fetchone()
    print(f"{kind}: +{after - before} rows ({before:,}->{after:,}); src={src.num_rows:,}; "
          f"count={n:,} distinct_chunk_id={nd:,} unique={n == nd}", flush=True)
    if n != nd:
        raise RuntimeError(f"{kind}: chunk_id uniqueness violated after merge ({n} != {nd})")
    return {"kind": kind, "delta": after - before, "src": src.num_rows,
            "before": before, "after": after, "unique": n == nd}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Phase E: append _sublift_* chunks into the shared sinks (idempotent by chunk_id).")
    p.add_argument("--only", default=None, help="restrict to one sink kind: scope|unknown|pricing")
    a = p.parse_args(argv)
    so = _r2_storage_options()
    results = []
    total = 0
    for tw, shared, schema_fn, kind in DEFAULT_PAIRS:
        if a.only and kind != a.only:
            continue
        r = append_pair(tw, shared, schema_fn, kind, so)
        results.append(r)
        total += r.get("delta", 0)
    print(f"RESULT: total_appended_rows={total:,} pairs={results}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
