"""Query-embedding helper for the gtm-mcp vector-search surface.

WHY A LOCAL MODEL, NOT AN EXTERNAL API. The vectors stored in
``s3://data-sink/active/govcon_scope_vectors/`` (column ``embedding`` =
``list_<float32>[1024]``) are written by the Phase-4 embedder of
``pipelines/sam_gov/sam_attachment_extract_90day.py``, specified in
``docs/reference/SAM_90DAY_EXTRACTION_PIPELINE_SPEC_V2.md`` §9 as a **self-hosted
instruction-retrieval model, ``BAAI/bge-large-en-v1.5`` (D=1024), run locally — no
external API** (nothing is sent off-host). A query vector is comparable to the stored
passage vectors ONLY if it comes from the SAME model at the SAME dimension; calling a
different provider would return a dimension/space mismatch that silently retrieves
garbage neighbours. So this helper runs the established model locally — it is the
"established embedding provider for this environment," interpreted faithfully.

BGE RETRIEVAL CONVENTION. bge-*-en-v1.5 embeds passages WITHOUT an instruction and
prepends a short query instruction to QUERIES only; the pair is then compared by cosine
(so vectors are L2-normalized). The instruction default below matches the model card; it
is configurable so it can be pinned to whatever Phase 4 actually used.

LIFECYCLE. The model is a lazy, process-resident singleton built on FIRST use (inside
``search_govcon_scopes``), NEVER at import or at server boot — so the existing
``main.py`` warm-up (registry + DuckDB connection) and the ``database.py`` singletons
(``_con`` / ``_registry`` / ``_handle_cache``) are untouched (zero-regression). The
``sentence_transformers`` / ``torch`` import is function-local for the same reason: a
gateway that never calls the vector tool never pays the load.

MEMORY NOTE. ``bge-large-en-v1.5`` (~335M params) + torch is a heavyweight resident in
the serving process; on a memory-bound box it competes with the warm Lance handle cache.
``GTM_EMBED_MODEL`` allows pinning a smaller sibling (e.g. ``bge-base-en-v1.5``, D=768)
IF the writer dimension is changed to match — the two MUST stay in lockstep.
"""

from __future__ import annotations

import functools
import os
import threading

# Established writer model + retrieval convention (overridable, but writer and query MUST match).
EMBED_MODEL = os.environ.get("GTM_EMBED_MODEL", "BAAI/bge-large-en-v1.5")
EMBED_DIM = int(os.environ.get("GTM_EMBED_DIM", "1024"))
QUERY_INSTRUCTION = os.environ.get(
    "GTM_EMBED_QUERY_PREFIX",
    "Represent this sentence for searching relevant passages: ",
)
EMBED_CACHE_SIZE = int(os.environ.get("GTM_EMBED_CACHE_SIZE", "2048"))

_model = None
_model_lock = threading.Lock()


def _get_model():
    """Lazy process-resident singleton SentenceTransformer. Built on first call, under a
    lock (double-checked) so concurrent first-callers share one instance. Heavy imports are
    deferred to here so module import / server boot never load torch."""
    global _model
    if _model is None:
        with _model_lock:
            if _model is None:
                from sentence_transformers import SentenceTransformer  # heavy; function-local on purpose

                _model = SentenceTransformer(EMBED_MODEL)
    return _model


@functools.lru_cache(maxsize=EMBED_CACHE_SIZE)
def embed_query(text: str) -> tuple[float, ...]:
    """Embed a raw query string into the stored vectors' space and return the vector as a
    tuple of floats (hashable → safe as an ``lru_cache`` value; identical queries within a
    process are served from cache, no recompute).

    Applies the bge query instruction and L2-normalizes (cosine metric). Raises ``ValueError``
    on empty input and ``RuntimeError`` if the produced dimension does not match the writer's
    ``EMBED_DIM`` — a model/writer mismatch must fail loud, never return mismatched vectors."""
    t = (text or "").strip()
    if not t:
        raise ValueError("query text is empty")
    vec = _get_model().encode(QUERY_INSTRUCTION + t, normalize_embeddings=True)
    out = tuple(float(x) for x in vec)
    if len(out) != EMBED_DIM:
        raise RuntimeError(
            f"embedding dimension {len(out)} != expected {EMBED_DIM} for model {EMBED_MODEL!r}; "
            "the query model and the govcon_scope_vectors writer must use the same model/dim."
        )
    return out


def cache_info() -> dict:
    """Expose the lru_cache hit/miss counters (for the search tool's diagnostics)."""
    ci = embed_query.cache_info()
    return {"hits": ci.hits, "misses": ci.misses, "maxsize": ci.maxsize, "currsize": ci.currsize}
