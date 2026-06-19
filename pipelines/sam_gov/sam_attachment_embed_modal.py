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
EMBED_MODEL_REV = "main"                 # HF revision pinned into sub_caps rows for provenance
EMBED_DIM = 1024
# A10G measured ~99 passages/s on these (≤512-token) chunks; A100 ~3× cuts the 1.89M bulk to ~1h.
GPU = "A100"
ENCODE_BATCH = 384
SINKS = {
    "scope": "s3://data-sink/active/govcon_scope_vectors/",
    "unknown": "s3://data-sink/active/govcon_unknown/",
    "sub_caps": "s3://data-sink/active/govcon_sub_capability_vectors/",
}
UNMARKED = "embedding IS NULL AND array_length(content_marking) = 0"   # the non-CUI bulk (chunk sinks)


def _worklist(name: str) -> str:
    """Per-sink embed worklist. The chunk sinks carry a CUI `content_marking` bracket (marked chunks
    stay NULL, excluded from the ANN index by construction). `sub_caps` carries NO marking column —
    `subaward_description` is the firm's own SAM subaward report (sub-self-reported, not solicitation
    CUI) — so its worklist is the bare NULL set."""
    return "embedding IS NULL" if name == "sub_caps" else UNMARKED

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
    wl = _worklist(name)
    ds = lance.dataset(uri, storage_options=so)
    total = ds.count_rows(filter=wl)
    print(f"[{name}] worklist ({wl}) = {total:,}{' limit=' + str(limit) if limit else ''}", flush=True)
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
        # sub_caps carries model provenance columns (pin them); chunk sinks have none → untouched.
        if name == "sub_caps":
            n = src.num_rows
            for col, val in (("model_id", EMBED_MODEL), ("model_revision", EMBED_MODEL_REV)):
                if col in src.schema.names:
                    src = src.set_column(src.schema.get_field_index(col), col,
                                         pa.array([val] * n, pa.string()))
        d = lance.dataset(uri, storage_options=so)
        d.merge_insert("chunk_id").when_matched_update_all().execute(src.cast(d.schema))
        return buf.num_rows

    embedded, t0 = 0, time.time()
    batches, nrows = [], 0
    for batch in ds.scanner(filter=wl, batch_size=8192).to_batches():
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
    remaining = lance.dataset(uri, storage_options=so).count_rows(filter=wl)
    print(f"[{name}] DONE embed: +{embedded:,} · {remaining:,} still NULL · "
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
    wl = _worklist(name)
    null_remaining = ds.count_rows(filter=wl)
    null_marked = (0 if name == "sub_caps"
                   else ds.count_rows(filter="embedding IS NULL AND array_length(content_marking) > 0"))
    if null_remaining != 0:
        raise RuntimeError(f"[{name}] {null_remaining:,} worklist rows still NULL — finish embed before index")
    print(f"[{name}] indexing {n:,} rows ({null_marked:,} marked rows remain NULL — bracketed, "
          f"excluded from ANN by construction)", flush=True)
    # compact_files is a best-effort optimization: pylance 7.0.0 trips an internal encoding bug
    # ("Repetition buffer too large") compacting the large_string text / list columns. The IVF_PQ
    # index does not require compaction — skip on failure and index the un-compacted fragments.
    try:
        ds.optimize.compact_files()
        ds = lance.dataset(uri, storage_options=so)
        print(f"[{name}] compacted", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"[{name}] compact skipped (non-fatal): {str(e)[:140]}", flush=True)
        ds = lance.dataset(uri, storage_options=so)
    parts = max(1, round(math.sqrt(n)))
    print(f"[{name}] IVF_PQ cosine partitions={parts} sub_vectors=64 …", flush=True)
    try:
        ds.create_index("embedding", index_type="IVF_PQ", num_partitions=parts,
                        num_sub_vectors=64, metric="cosine", accelerator="cuda", replace=True)
    except Exception as e:  # noqa: BLE001 — fall back to CPU kmeans if the GPU accelerator path errors
        print(f"[{name}] cuda accelerator unavailable ({str(e)[:80]}); CPU kmeans", flush=True)
        ds.create_index("embedding", index_type="IVF_PQ", num_partitions=parts,
                        num_sub_vectors=64, metric="cosine", replace=True)
    print(f"[{name}] DONE IVF_PQ (scalars built separately under high memory)", flush=True)
    return {"sink": name, "rows": n, "partitions": parts}


# Scalar indexes run in a SEPARATE high-memory CPU function: create_scalar_index's external sorter
# auto-sizes its pool from available RAM, and after the IVF_PQ build (or on a small box) it OOMs
# ("Resources exhausted … total pool"). 64 GB + a fresh process (no resident vectors) is the proven
# pattern (matches pipelines/ingest_epa). No GPU needed for scalar indexes.
@app.function(cpu=4.0, memory=65536, secrets=[modal.Secret.from_name("r2-credentials")], timeout=4 * 60 * 60)
def build_scalars(name: str):
    import os
    # pylance 7.0.0 sizes the scalar-index external-sorter pool to ~0 (verified: "4.2 KB total pool"
    # even in a 64 GB container), so the spill degenerates and the merge OOMs. Bypassing spilling
    # does the sort in-memory against the real 64 GB — the actual fix (RAM, not pool, is the bound).
    os.environ["LANCE_BYPASS_SPILLING"] = "true"
    import lance
    so = _so()
    uri = SINKS[name]
    ds = lance.dataset(uri, storage_options=so)
    cols = set(ds.schema.names)
    have = {(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i)))
            for i in ds.list_indices()}
    if name == "sub_caps":
        # sub_caps grain is (subawardee_uei, description_chunk_ix); the only prefilter key is
        # BTREE(subawardee_uei) — already built by build_sub_capability_vectors.py (idempotent skip).
        plan = [("subawardee_uei", "BTREE")]
    else:
        plan = [("resource_id", "BTREE"), ("contract_award_unique_key", "BTREE"),
                ("naics_code", "BITMAP"), ("header_class", "BITMAP")]
        if name == "unknown" and "lexicon_hit" in cols:
            plan.append(("lexicon_hit", "BITMAP"))
    for col, kind in plan:
        if col not in cols or f"{col}_idx" in have:
            continue
        lance.dataset(uri, storage_options=so).create_scalar_index(col, index_type=kind, replace=True)
        print(f"[{name}] {kind} {col}", flush=True)
    final = sorted(i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
                   for i in lance.dataset(uri, storage_options=so).list_indices())
    print(f"[{name}] DONE scalars · indices={final}", flush=True)
    return {"sink": name, "indices": final}


@app.local_entrypoint()
def main(sink: str = "both", do_index: bool = True, limit: int = 0, mode: str = "all"):
    names = ["scope", "unknown"] if sink == "both" else [sink]
    lim = limit or None
    if mode != "scalars":
        print(f"== embed (unmarked bulk) on Modal {GPU}: {names} ==", flush=True)
        print("embed results:", list(embed_unmarked.starmap([(n, 50000, lim) for n in names])), flush=True)
        if do_index and not lim:
            print(f"== IVF_PQ (GPU): {names} ==", flush=True)
            print("ivf results:", list(build_index.map(names)), flush=True)
    if mode in ("all", "scalars") and not lim:
        print(f"== scalar indexes (high-mem CPU): {names} ==", flush=True)
        print("scalar results:", list(build_scalars.map(names)), flush=True)
