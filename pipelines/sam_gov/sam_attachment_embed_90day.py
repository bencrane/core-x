"""Embed writer — PHASE 5 (spec §9): populate the `embedding` column on the chunk sinks with a
self-hosted BGE model, then build the cosine IVF_PQ + scalar indexes. The recall layer, attached
to consumers that already exist (gtm-mcp vector search; the Phase-4 capability_match upgrade).

MODEL (pinned, must match the query side apps/gtm_mcp/src/embeddings.py): BAAI/bge-large-en-v1.5,
D=1024, run LOCALLY — no external API (CUI-safe; marked rows embed too, the gate is CONSUMPTION not
embedding). BGE convention: passages embedded WITHOUT instruction, queries WITH the instruction
prefix; both L2-normalized → cosine. Vectors stored fixed_size_list<float32>[1024]. A produced dim
≠ EMBED_DIM fails loud (a writer/query model mismatch silently retrieves garbage neighbours).

WORKLIST = `embedding IS NULL` (NO char_len filter — the degenerate chunks are noise but filtering
them makes the §12 IS-NULL==0 gate unsatisfiable; anti-pattern #6). Free crash-resume: a re-run
re-selects only the rows still NULL. Writes are `merge_insert(chunk_id).when_matched_update_all()`
in flush batches with FULL-row sources (subset schemas are rejected). The single-committer lease
(SinkCommitLease) binds this writer so it can never race the extractor / phase_finalize on a sink.

ORDER (gates, exact — plan PHASE 5): embed both sinks → assert embedding IS NULL == 0 per sink →
compact_files → IVF_PQ (cosine, num_sub_vectors=64, partitions≈sqrt(n)) → scalar campaign
(BTREE resource_id/contract_award_unique_key, BITMAP naics_code/header_class, BITMAP lexicon_hit on
unknown only). NEVER BTREE(chunk_id) (anti-pattern #7 — nothing point-looks-up chunk_id and it
re-arms the #3177 window on every refresh).

THROUGHPUT (measured 2026-06-15): ~12 passages/s on this Mac's MPS → the full 2.39M-chunk corpus is
a ~55h local run; a rented A10G/4090 (or the Modal orchestrator) clears it in ~30-60 min for <$10.
Self-hosted either way — set EMBED_DEVICE=cuda on a GPU box. Resumable, so a local run can be
chunked across sessions via --limit.

    doppler run -p core-x -c prd -- .venv/bin/python pipelines/sam_gov/sam_attachment_embed_90day.py \
      <status|embed|index|verify> [--sink scope|unknown|both] [--limit N] [--flush 50000]
"""
from __future__ import annotations

import argparse
import datetime as dt
import math
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines.sam_gov.sam_attachment_extract_90day import (  # noqa: E402
    _r2_storage_options, _dataset_exists, SinkCommitLease)

ACTIVE = "s3://data-sink/active"
SCOPE_URI = os.environ.get("SAM90_EMBED_SCOPE_URI", f"{ACTIVE}/govcon_scope_vectors/")
UNKNOWN_URI = os.environ.get("SAM90_EMBED_UNKNOWN_URI", f"{ACTIVE}/govcon_unknown/")
SINKS = {"scope": SCOPE_URI, "unknown": UNKNOWN_URI}

# Pinned to the query side (apps/gtm_mcp/src/embeddings.py) via the SAME env vars — writer and query
# MUST stay in lockstep or the spaces mismatch.
EMBED_MODEL = os.environ.get("GTM_EMBED_MODEL", "BAAI/bge-large-en-v1.5")
EMBED_DIM = int(os.environ.get("GTM_EMBED_DIM", "1024"))
EMBED_DEVICE = os.environ.get("EMBED_DEVICE", "mps")     # mps (Apple) | cuda (GPU box) | cpu
ENCODE_BATCH = int(os.environ.get("EMBED_ENCODE_BATCH", "128"))
FLUSH_ROWS = int(os.environ.get("EMBED_FLUSH_ROWS", "50000"))
IVF_SUB_VECTORS = int(os.environ.get("EMBED_IVF_SUB_VECTORS", "64"))

# CUI bracket (mirrors the PHASE-2 marked-resource bracket): `content_marking ≠ []` chunks are
# deferred and embedded SEPARATELY under their own posture decision; the public bulk embeds now.
# `array_length(content_marking)` pushes down in Lance (verified live) — len() is DuckDB-only.
_MARKING_CLAUSE = {
    "unmarked": "array_length(content_marking) = 0",   # the non-CUI bulk
    "marked": "array_length(content_marking) > 0",      # the bracketed CUI subset
    "all": None,
}


def _worklist(marking: str) -> str:
    clause = _MARKING_CLAUSE[marking]
    return "embedding IS NULL" if clause is None else f"embedding IS NULL AND {clause}"


_model = None


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer  # heavy; function-local
        log(f"loading {EMBED_MODEL} on {EMBED_DEVICE} …")
        _model = SentenceTransformer(EMBED_MODEL, device=EMBED_DEVICE)
    return _model


def _embed_passages(texts):
    """BGE passages: NO instruction, L2-normalized, float32. Empty text still yields a vector
    (no char_len filter — the IS-NULL gate must stay satisfiable)."""
    import numpy as np
    clean = [t if isinstance(t, str) and t else " " for t in texts]
    vecs = _get_model().encode(clean, normalize_embeddings=True, batch_size=ENCODE_BATCH,
                               show_progress_bar=False).astype(np.float32)
    if vecs.shape[1] != EMBED_DIM:
        raise RuntimeError(f"model {EMBED_MODEL!r} produced dim {vecs.shape[1]} != EMBED_DIM {EMBED_DIM}")
    return vecs


def _fsl(arr):
    """(N, DIM) float32 → fixed_size_list<float32>[DIM] matching the sink's embedding column."""
    import pyarrow as pa
    flat = pa.array(arr.reshape(-1), type=pa.float32())
    return pa.FixedSizeListArray.from_arrays(flat, EMBED_DIM)


def _flush(ds_uri, buf, so):
    """Embed a buffered Arrow table's text and merge_insert the full rows (embedding filled)."""
    import lance
    vecs = _embed_passages(buf.column("text").to_pylist())
    src = buf.set_column(buf.schema.get_field_index("embedding"), "embedding", _fsl(vecs))
    ds = lance.dataset(ds_uri, storage_options=so)
    src = src.cast(ds.schema)                       # normalize child field names / nullability
    ds.merge_insert("chunk_id").when_matched_update_all().execute(src)
    return buf.num_rows


def embed_sink(name, *, marking="all", limit=None, flush_rows=FLUSH_ROWS):
    import lance
    import pyarrow as pa
    so = _r2_storage_options()
    uri = SINKS[name]
    worklist = _worklist(marking)
    run_id = dt.datetime.now(dt.timezone.utc).strftime("embed-%Y%m%dT%H%M%S")
    ds = lance.dataset(uri, storage_options=so)
    total_null = ds.count_rows(filter=worklist)
    log(f"[{name}] worklist ({marking}) = {total_null:,}{' limit=' + str(limit) if limit else ''}")
    if total_null == 0:
        log(f"[{name}] nothing to embed for marking={marking} — already complete")
        return {"sink": name, "marking": marking, "embedded": 0, "remaining_null": 0}
    # 24h lease ttl: size to the invocation (typically --limit-bounded), not the whole corpus.
    embedded = 0
    with SinkCommitLease(uri, holder=f"embed:{run_id}", ttl_s=24 * 60 * 60):
        scanner = ds.scanner(filter=worklist, batch_size=8192)
        buf_batches = []
        buf_rows = 0
        for batch in scanner.to_batches():
            buf_batches.append(batch)
            buf_rows += batch.num_rows
            if buf_rows >= flush_rows or (limit and embedded + buf_rows >= limit):
                tbl = pa.Table.from_batches(buf_batches)
                if limit:
                    tbl = tbl.slice(0, max(0, limit - embedded))
                embedded += _flush(uri, tbl, so)
                log(f"[{name}] embedded {embedded:,} / {min(total_null, limit or total_null):,}")
                buf_batches, buf_rows = [], 0
                if limit and embedded >= limit:
                    break
        if buf_batches and not (limit and embedded >= limit):
            tbl = pa.Table.from_batches(buf_batches)
            if limit:
                tbl = tbl.slice(0, max(0, limit - embedded))
            if tbl.num_rows:
                embedded += _flush(uri, tbl, so)
                log(f"[{name}] embedded {embedded:,} (final flush)")
    remaining = lance.dataset(uri, storage_options=so).count_rows(filter=worklist)
    log(f"[{name}] DONE embed pass ({marking}): +{embedded:,} this run · {remaining:,} still in worklist")
    return {"sink": name, "marking": marking, "embedded": embedded, "remaining_null": remaining}


def index_sink(name):
    """Gate + build indexes AFTER embedding is complete for the sink (embedding IS NULL == 0)."""
    import os
    import lance
    # pylance 7.0.0 sizes the scalar-index external-sorter pool to ~0 (the spill degenerates and the
    # merge OOMs even with ample RAM). Bypass spilling → in-memory sort against real RAM. Run scalar
    # builds on a high-memory host (≥32 GB free) accordingly.
    os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")
    so = _r2_storage_options()
    uri = SINKS[name]
    ds = lance.dataset(uri, storage_options=so)
    n = ds.count_rows()
    null_unmarked = ds.count_rows(filter=_worklist("unmarked"))
    null_marked = ds.count_rows(filter=_worklist("marked"))
    if null_unmarked != 0:
        raise RuntimeError(f"[{name}] {null_unmarked:,} UNMARKED rows still NULL — the public bulk embed "
                           f"must complete before indexing (anti-pattern #6: IS-NULL==0 is the completion "
                           f"contract for the un-bracketed set)")
    if null_marked:
        log(f"[{name}] NOTE: {null_marked:,} MARKED rows remain NULL (CUI bracket, deferred) — they are "
            f"excluded from the ANN index by construction (NULL vectors are not indexed); correct.")
    # compact_files is a best-effort optimization. pylance 7.0.0 trips internal failures compacting
    # the large_string `text` / list columns of these sinks in two distinct flavours: a normal
    # Exception ("Repetition buffer too large"), OR a pyo3 Rust panic surfaced as
    # pyo3_runtime.PanicException — which subclasses BaseException, NOT Exception, so a bare
    # `except Exception` lets it escape and abort the entire index command (run-record incident #9;
    # observed on the unknown sink). The IVF_PQ index does NOT require compaction, so skip on EITHER
    # failure and index the un-compacted fragments (a later Lance fix can compact separately). Catch
    # BaseException to also swallow the panic, but re-raise KeyboardInterrupt/SystemExit so an
    # operator interrupt is never eaten. (pyo3_runtime is not importable as a module here, so matching
    # the panic by its concrete type is not reliable — the BaseException catch is the robust path.)
    log(f"[{name}] compacting {n:,} rows (best-effort) …")
    try:
        ds.optimize.compact_files()
        ds = lance.dataset(uri, storage_options=so)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as exc:  # noqa: BLE001 — must also catch the pyo3 PanicException (BaseException)
        log(f"[{name}] compact skipped (non-fatal Lance error/panic): {str(exc)[:140]}")
        ds = lance.dataset(uri, storage_options=so)
    num_partitions = max(1, round(math.sqrt(n)))
    with SinkCommitLease(uri, holder=f"embed-index:{name}", ttl_s=6 * 60 * 60):
        log(f"[{name}] IVF_PQ cosine: partitions={num_partitions} sub_vectors={IVF_SUB_VECTORS} …")
        ds.create_index("embedding", index_type="IVF_PQ", num_partitions=num_partitions,
                        num_sub_vectors=IVF_SUB_VECTORS, metric="cosine", replace=True)
        cols = set(ds.schema.names)
        for col in ("resource_id", "contract_award_unique_key"):
            if col in cols:
                ds.create_scalar_index(col, index_type="BTREE", replace=True); log(f"[{name}] BTREE ✓ {col}")
        for col in ("naics_code", "header_class"):
            if col in cols:
                ds.create_scalar_index(col, index_type="BITMAP", replace=True); log(f"[{name}] BITMAP ✓ {col}")
        if name == "unknown" and "lexicon_hit" in cols:
            ds.create_scalar_index("lexicon_hit", index_type="BITMAP", replace=True)
            log(f"[{name}] BITMAP ✓ lexicon_hit")
    # NB: NEVER BTREE(chunk_id) — anti-pattern #7.
    log(f"[{name}] DONE index")
    return {"sink": name, "rows": n, "num_partitions": num_partitions}


def status():
    import json
    import lance
    so = _r2_storage_options()
    out = {}
    for name, uri in SINKS.items():
        if not _dataset_exists(uri, so):
            out[name] = {"uri": uri, "exists": False}
            continue
        ds = lance.dataset(uri, storage_options=so)
        n = ds.count_rows()
        nn = ds.count_rows(filter="embedding IS NULL")
        null_unmarked = ds.count_rows(filter=_worklist("unmarked"))
        null_marked = ds.count_rows(filter=_worklist("marked"))
        try:
            idx = sorted(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                         for i in ds.list_indices())
        except Exception:  # noqa: BLE001
            idx = []
        out[name] = {"uri": uri, "rows": n, "embedding_null": nn,
                     "null_unmarked_bulk": null_unmarked, "null_marked_bracketed": null_marked,
                     "embedded_pct": round(100 * (n - nn) / n, 2) if n else 0.0, "indices": idx}
    print(json.dumps({"model": EMBED_MODEL, "dim": EMBED_DIM, "device": EMBED_DEVICE,
                      "sinks": out}, indent=2))
    return out


def verify():
    """Gate report: per sink, embedding IS NULL must be 0 and a vector index must exist."""
    import json
    import lance
    so = _r2_storage_options()
    out = {}
    for name, uri in SINKS.items():
        ds = lance.dataset(uri, storage_options=so)
        nn = ds.count_rows(filter="embedding IS NULL")
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in ds.list_indices()]
        out[name] = {"embedding_null": nn, "is_null_gate": nn == 0,
                     "has_vector_index": any("embedding" in str(i) for i in idx), "indices": sorted(idx)}
    print(json.dumps(out, indent=2))
    return out


def main():
    p = argparse.ArgumentParser(description="PHASE 5 embed writer (BGE, self-hosted).")
    p.add_argument("cmd", choices=["status", "embed", "index", "verify"])
    p.add_argument("--sink", choices=["scope", "unknown", "both"], default="both")
    p.add_argument("--marking", choices=["unmarked", "marked", "all"], default="unmarked",
                   help="CUI bracket: 'unmarked' (default) = the non-CUI public bulk; 'marked' = the "
                        "deferred CUI subset (separate posture); 'all' = no bracket")
    p.add_argument("--limit", type=int, default=None, help="embed at most N rows this invocation")
    p.add_argument("--flush", type=int, default=FLUSH_ROWS, help="rows per merge_insert flush")
    a = p.parse_args()
    sinks = ["scope", "unknown"] if a.sink == "both" else [a.sink]
    if a.cmd == "status":
        status()
    elif a.cmd == "verify":
        verify()
    elif a.cmd == "embed":
        import json
        res = [embed_sink(s, marking=a.marking, limit=a.limit, flush_rows=a.flush) for s in sinks]
        print(json.dumps(res, indent=2))
    elif a.cmd == "index":
        import json
        res = [index_sink(s) for s in sinks]
        print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
