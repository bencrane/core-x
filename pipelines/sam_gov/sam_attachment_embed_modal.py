"""Modal GPU runner for the PHASE-5 embed — runs the non-CUI bulk (unmarked) embed + IVF_PQ on a
rented A10G, the in-stack equivalent of `sam_attachment_embed_90day.py` with EMBED_DEVICE=cuda.

Self-contained on purpose (no repo imports) so the container ships only its pip deps + this file —
the embed/index logic MIRRORS sam_attachment_embed_90day.py exactly (same model pin, same worklist,
same merge_insert motion, same IVF_PQ/scalar campaign, same CUI bracket). One container per sink ⇒
single-committer by construction (no lease needed). Marked (CUI) chunks are bracketed out and stay
NULL — excluded from the ANN index by construction (verified).

    doppler run -p core-x -c prd -- modal run pipelines/sam_gov/sam_attachment_embed_modal.py
    # options: --sink scope|unknown|both (default both) · --do-index/--no-do-index · --limit N (smoke)
"""
import modal

EMBED_MODEL = "BAAI/bge-large-en-v1.5"   # pinned — MUST match apps/gtm_mcp/src/embeddings.py
EMBED_DIM = 1024
# A10G measured ~99 passages/s on these (≤512-token) chunks; A100 ~3× cuts the 1.89M bulk to ~1h.
GPU = "A100"
ENCODE_BATCH = 384
SINKS = {
    "scope": "s3://data-sink/active/govcon_scope_vectors_90day/",
    "unknown": "s3://data-sink/active/govcon_unknown_90day/",
}
UNMARKED = "embedding IS NULL AND array_length(content_marking) = 0"   # the non-CUI bulk

image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install("torch", "sentence-transformers>=3", "pylance>=7", "pyarrow>=17", "numpy", "boto3")
)
app = modal.App("govcon-embed", image=image)


def _so():
    import os
    endpoint = os.environ.get("R2_ENDPOINT")
    acct = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and acct:
        endpoint = f"https://{acct}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": endpoint, "region": "auto"}


@app.function(gpu=GPU, secrets=[modal.Secret.from_name("r2-credentials")], timeout=6 * 60 * 60)
def embed_unmarked(name: str, flush_rows: int = 50000, limit: int | None = None):
    import time
    import lance
    import numpy as np
    import pyarrow as pa
    from sentence_transformers import SentenceTransformer

    so = _so()
    uri = SINKS[name]
    ds = lance.dataset(uri, storage_options=so)
    total = ds.count_rows(filter=UNMARKED)
    print(f"[{name}] unmarked worklist = {total:,}{' limit=' + str(limit) if limit else ''}", flush=True)
    if total == 0:
        return {"sink": name, "embedded": 0, "remaining": 0}

    model = SentenceTransformer(EMBED_MODEL, device="cuda")
    model = model.half()   # fp16 inference for throughput; vectors stored float32

    def flush(buf):
        vecs = model.encode([t or " " for t in buf.column("text").to_pylist()],
                            normalize_embeddings=True, batch_size=ENCODE_BATCH, show_progress_bar=False).astype(np.float32)
        if vecs.shape[1] != EMBED_DIM:
            raise RuntimeError(f"dim {vecs.shape[1]} != {EMBED_DIM}")
        fsl = pa.FixedSizeListArray.from_arrays(pa.array(vecs.reshape(-1), type=pa.float32()), EMBED_DIM)
        src = buf.set_column(buf.schema.get_field_index("embedding"), "embedding", fsl)
        d = lance.dataset(uri, storage_options=so)
        d.merge_insert("chunk_id").when_matched_update_all().execute(src.cast(d.schema))
        return buf.num_rows

    embedded, t0 = 0, time.time()
    batches, nrows = [], 0
    for batch in ds.scanner(filter=UNMARKED, batch_size=8192).to_batches():
        batches.append(batch); nrows += batch.num_rows
        if nrows >= flush_rows or (limit and embedded + nrows >= limit):
            tbl = pa.Table.from_batches(batches)
            if limit:
                tbl = tbl.slice(0, max(0, limit - embedded))
            embedded += flush(tbl)
            rate = embedded / max(1e-9, time.time() - t0)
            print(f"[{name}] {embedded:,}/{min(total, limit or total):,} ({rate:.0f}/s)", flush=True)
            batches, nrows = [], 0
            if limit and embedded >= limit:
                break
    if batches and not (limit and embedded >= limit):
        tbl = pa.Table.from_batches(batches)
        if limit:
            tbl = tbl.slice(0, max(0, limit - embedded))
        if tbl.num_rows:
            embedded += flush(tbl)
    remaining = lance.dataset(uri, storage_options=so).count_rows(filter=UNMARKED)
    print(f"[{name}] DONE embed: +{embedded:,} · {remaining:,} unmarked still NULL · "
          f"{time.time() - t0:.0f}s", flush=True)
    return {"sink": name, "embedded": embedded, "remaining": remaining}


@app.function(gpu=GPU, secrets=[modal.Secret.from_name("r2-credentials")], timeout=4 * 60 * 60)
def build_index(name: str):
    import math
    import lance
    so = _so()
    uri = SINKS[name]
    ds = lance.dataset(uri, storage_options=so)
    n = ds.count_rows()
    null_unmarked = ds.count_rows(filter=UNMARKED)
    null_marked = ds.count_rows(filter="embedding IS NULL AND array_length(content_marking) > 0")
    if null_unmarked != 0:
        raise RuntimeError(f"[{name}] {null_unmarked:,} UNMARKED still NULL — finish embed before index")
    print(f"[{name}] indexing {n:,} rows ({null_marked:,} marked rows remain NULL — bracketed, "
          f"excluded from ANN by construction)", flush=True)
    ds.optimize.compact_files()
    ds = lance.dataset(uri, storage_options=so)
    parts = max(1, round(math.sqrt(n)))
    print(f"[{name}] IVF_PQ cosine partitions={parts} sub_vectors=64 …", flush=True)
    ds.create_index("embedding", index_type="IVF_PQ", num_partitions=parts,
                    num_sub_vectors=64, metric="cosine", replace=True)
    cols = set(ds.schema.names)
    for c in ("resource_id", "contract_award_unique_key"):
        if c in cols:
            ds.create_scalar_index(c, index_type="BTREE", replace=True); print(f"[{name}] BTREE {c}", flush=True)
    for c in ("naics_code", "header_class"):
        if c in cols:
            ds.create_scalar_index(c, index_type="BITMAP", replace=True); print(f"[{name}] BITMAP {c}", flush=True)
    if name == "unknown" and "lexicon_hit" in cols:
        ds.create_scalar_index("lexicon_hit", index_type="BITMAP", replace=True)
    print(f"[{name}] DONE index", flush=True)
    return {"sink": name, "rows": n, "partitions": parts}


@app.local_entrypoint()
def main(sink: str = "both", do_index: bool = True, limit: int = 0):
    names = ["scope", "unknown"] if sink == "both" else [sink]
    lim = limit or None
    print(f"== embed (unmarked bulk) on Modal {GPU}: {names} ==", flush=True)
    res = list(embed_unmarked.starmap([(n, 50000, lim) for n in names]))
    print("embed results:", res, flush=True)
    if do_index and not lim:
        print(f"== build IVF_PQ + scalar indexes: {names} ==", flush=True)
        idx = list(build_index.map(names))
        print("index results:", idx, flush=True)
    else:
        print("(index skipped — limit smoke or --no-do-index)", flush=True)
