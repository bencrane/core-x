"""GovCon scope vector search — hybrid (scalar filter → ANN) retrieval over the
90-day SAM.gov solicitation scope corpus.

Dataset: ``s3://data-sink/active/govcon_scope_vectors/`` (auto-registered by the
dynamic registry as ``govcon_scope_vectors``). Written by Phase 2 of
``pipelines/sam_gov/sam_attachment_extract_90day.py`` — one row per ~1,200-char scope
chunk (SOW/PWS/SOO text) with the join keys and the Phase-4 ``embedding``
(``list<float32>[1024]``, ``BAAI/bge-large-en-v1.5``, cosine, IVF_PQ).

WHY A TYPED TOOL (vs. raw ``execute_audience_query``). The vector mirror's PURPOSE is
similarity retrieval — "find scopes that mean SCIF / sensitive compartmented information
facility" — which keyword/NAICS filtering provably cannot find (``GOVCON_90DAY_TRIGGER_
DIAGNOSTIC.md`` §4: ``SCIF`` appears in ≤0.15% of award descriptions; the security intent
lives in the attachment text). DuckDB-over-a-registered-Lance relation engages NEITHER the
scalar index NOR ANN (``RECALL_ARCHITECTURE_DIAGNOSTIC.md`` §6.1), so the only way to use
the vectors is a Lance scanner that pushes the scalar prefilter down and runs ``nearest``
ANN against a query vector. This tool is that path.

HYBRID EXECUTION. ``scanner(filter=<scalar predicate>, prefilter=True, nearest={...})`` —
``prefilter=True`` applies the NAICS / header_class / notice / date predicate FIRST (BTREE
pushdown), then runs ANN only over the surviving rows (filter-then-ANN, not ANN-then-filter).

GRACEFUL DEGRADATION. Phase 4 (embedding + IVF_PQ) is a separate pipeline stage; until it
runs, ``embedding`` is NULL and no vector index exists. Rather than let a NULL-embedding
brute-force kNN scan the multi-GB lake, this tool checks for the vector index first and
returns a clear ``vector_index_absent`` status — never a pathological full scan.

State: reuses ``database.open_dataset`` (the warm ``_handle_cache``) and
``embeddings.embed_query`` (the lru-cached local embedder). It introduces NO new
connection / registry / handle-cache state.
"""

from __future__ import annotations

import re
from typing import Any

from .. import database, embeddings

DATASET = "govcon_scope_vectors"

_RETURN_COLUMNS = [
    "chunk_id", "resource_id", "notice_id", "solicitation_number", "naics_code",
    "contract_award_unique_key", "header_class", "chunk_ix", "char_len", "text",
]

_NAICS_RE = re.compile(r"^[0-9]{2,6}$")          # NAICS codes are 2–6 digit strings
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")    # ISO date for the created_at window

_DEFAULT_K = 20
_MAX_K = 200
_DEFAULT_NPROBES = 20
_REFINE_FACTOR = 10


def _sql_str(value: str) -> str:
    """Single-quoted, escaped string literal for a Lance scanner predicate."""
    return "'" + value.replace("'", "''") + "'"


def _vector_index_present(ds) -> bool:
    """True iff a vector index covers the ``embedding`` column (Phase-4 IVF_PQ). Tolerant of
    the differing key names pylance versions use for the indexed columns."""
    try:
        for idx in ds.list_indices():
            cols = idx.get("fields") or idx.get("columns") or idx.get("column") or []
            if isinstance(cols, str):
                cols = [cols]
            if "embedding" in cols:
                return True
    except Exception:  # noqa: BLE001 — absence/probe failure → treat as "no usable index"
        return False
    return False


def _build_filter(naics, header_class, notice_id, solicitation_number, date_after, date_before) -> str:
    """Compose the scalar prefilter pushed down BEFORE the ANN search. Every leg is an
    indexed/cheap scalar column on the real schema; values are validated + escaped."""
    clauses: list[str] = []

    if naics:
        codes = [naics] if isinstance(naics, str) else list(naics)
        clean = [c.strip() for c in codes if c and _NAICS_RE.match(c.strip())]
        if not clean:
            raise ValueError("naics must be 2–6 digit NAICS code string(s)")
        clauses.append("naics_code IN (" + ", ".join(_sql_str(c) for c in clean) + ")")

    if header_class:  # default 'scope' — the signal-bearing class; pass None to search all classes
        clauses.append(f"header_class = {_sql_str(header_class.strip())}")

    if notice_id:
        clauses.append(f"notice_id = {_sql_str(notice_id.strip())}")

    if solicitation_number:
        clauses.append(f"solicitation_number = {_sql_str(solicitation_number.strip())}")

    for bound, op in ((date_after, ">="), (date_before, "<=")):
        if bound:
            b = bound.strip()
            if not _DATE_RE.match(b):
                raise ValueError(f"date bound {bound!r} must be ISO 'YYYY-MM-DD'")
            # created_at is the extraction timestamp (UTC); this windows on when the chunk was extracted.
            clauses.append(f"created_at {op} timestamp '{b} 00:00:00'")

    return " AND ".join(clauses)


def search_govcon_scopes(
    query: str,
    naics: list[str] | None = None,
    header_class: str | None = "scope",
    notice_id: str | None = None,
    solicitation_number: str | None = None,
    date_after: str | None = None,
    date_before: str | None = None,
    k: int = _DEFAULT_K,
) -> dict[str, Any]:
    """Semantic search over 90-day SAM.gov solicitation SCOPE text (SOW/PWS/SOO chunks).

    Use this to find opportunities by what the work MEANS, not by keyword — e.g. "secure
    compartmented facility / ICD-705 / SCIF build-out", "domestic structural steel mandate",
    "bridge seismic retrofit". The query is embedded with the same model that wrote the corpus
    and matched by cosine ANN; an optional scalar prefilter (NAICS, notice/solicitation id, or
    extraction-date window, plus a ``header_class`` lane) is pushed down FIRST so ANN runs only
    over the surviving rows.

    Args:
      query: natural-language description of the scope/work to find (required).
      naics: optional NAICS code(s) (2–6 digit strings) — restrict to these sectors.
      header_class: chunk lane to search; defaults to ``"scope"`` (the SOW/PWS signal). Pass
        ``None`` to search every class (scope/pricing/boilerplate/unknown).
      notice_id / solicitation_number: optional exact id pins (one notice's chunks).
      date_after / date_before: optional ISO ``YYYY-MM-DD`` window on the chunk's extraction
        timestamp (``created_at``, UTC) — NOT the award action date.
      k: number of nearest chunks to return (default 20, capped at 200).

    Returns ``{"status", "query", "filter", "k", "match_count", "matches": [...],
    "embed_cache"}``. Each match carries the chunk + join keys (``notice_id``,
    ``solicitation_number``, ``naics_code``, ``contract_award_unique_key``) and ``_distance``
    (cosine; smaller = closer). Non-``ok`` ``status`` values: ``dataset_unavailable`` (corpus
    not yet landed in the active sink), ``vector_index_absent`` (Phase-4 embedding/IVF_PQ not
    yet built — vectors are not searchable), ``embedder_unavailable`` (the local embedding
    model could not be loaded). Join a match back to the federal-spend resume via
    ``contract_award_unique_key`` or hydrate contacts via the ``notice_id``.
    """
    q = (query or "").strip()
    if not q:
        raise ValueError("query is required")
    k = max(1, min(int(k), _MAX_K))

    # 1) Resolve the corpus via the warm handle cache (no new state). Absent → not yet landed.
    try:
        ds = database.open_dataset(DATASET)
    except KeyError:
        return {
            "status": "dataset_unavailable",
            "query": q,
            "detail": (
                f"{DATASET!r} is not registered in the active sink yet — the 90-day scope "
                "vector corpus has not landed. It auto-registers once committed under "
                "active/; call list_datasets to confirm."
            ),
        }

    # 2) Guard: the Phase-4 IVF_PQ vector index must exist. Without it, embeddings are NULL and
    #    an ANN query would degrade to a brute-force scan of the whole lake — refuse instead.
    if not _vector_index_present(ds):
        return {
            "status": "vector_index_absent",
            "query": q,
            "detail": (
                "the embedding column / IVF_PQ vector index is not built yet (Phase 4 of "
                "sam_attachment_extract_90day pending) — scopes are not semantically searchable. "
                "Scalar columns remain queryable via execute_audience_query in the meantime."
            ),
        }

    # 3) Build the scalar prefilter (validated + escaped) and the query vector (lru-cached).
    pred = _build_filter(naics, header_class, notice_id, solicitation_number, date_after, date_before)
    try:
        qv = embeddings.embed_query(q)
    except (ImportError, ModuleNotFoundError) as exc:
        return {
            "status": "embedder_unavailable",
            "query": q,
            "detail": (
                f"the local embedding model ({embeddings.EMBED_MODEL}) could not be loaded: {exc}. "
                "Ensure sentence-transformers is installed on the gtm-mcp service."
            ),
        }

    # 4) Hybrid execution: scalar prefilter pushed down, THEN cosine ANN over the survivors.
    scanner = ds.scanner(
        columns=_RETURN_COLUMNS,
        filter=pred or None,
        prefilter=True,
        nearest={
            "column": "embedding",
            "q": list(qv),
            "k": k,
            "nprobes": _DEFAULT_NPROBES,
            "refine_factor": _REFINE_FACTOR,
        },
    )
    rows = scanner.to_table().to_pylist()
    return {
        "status": "ok",
        "query": q,
        "filter": pred or None,
        "k": k,
        "match_count": len(rows),
        "matches": rows,
        "embed_cache": embeddings.cache_info(),
    }


def register(mcp) -> None:
    """Mount the govcon vector-search tool onto the FastMCP server. The function signature +
    docstring become the tool's input schema + agent-facing contract."""
    mcp.add_tool(search_govcon_scopes)
