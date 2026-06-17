"""Subawardee capability vector search — semantic retrieval over what subawardees ACTUALLY DID,
from their own SAM subaward reports.

Dataset: ``s3://data-sink/active/govcon_sub_capability_vectors_90day/`` (auto-registered by the
dynamic registry as ``govcon_sub_capability_vectors_90day``). Written by
``pipelines/sam_gov/build_sub_capability_vectors.py`` (text substrate) + the
``sam_attachment_embed_modal.py`` ``sub_caps`` sink (``embedding`` = ``list<float32>[1024]``,
``BAAI/bge-large-en-v1.5``, cosine, IVF_PQ). One row per (subawardee_uei, distinct-description)
≤1,200-char chunk of the deduped ``subaward_description`` text.

WHY A TYPED TOOL (vs. raw ``execute_audience_query``). The structured sub conjunction — "subs that
do X ∧ hold clearance ≥ Y ∧ team with prime Z" — is already served by the
``build_subawardee_capability_profiles.py`` ``query`` CLI and the raw-SQL audience path over the
profiles. What neither can do is OPEN-VOCABULARY similarity: "find subs whose past work means
'substation switchgear install' / 'avionics depot repair'" — the intent lives in free-text the
firm self-reported, and keyword SQL provably misses paraphrase. This tool is that path (the sub-side
mirror of ``search_govcon_scopes``).

HYBRID EXECUTION. ``scanner(filter=<sub_uei/date predicate or None>, prefilter=True,
nearest={...})`` — the optional scalar prefilter (a sub_uei pin or extraction-date window) is pushed
down FIRST (BTREE), then cosine ANN runs over the survivors. Results are collapsed to DISTINCT subs
(best distance per sub) and enriched from ``govcon_subawardee_capability_profiles`` (name, subaward
count/$, capability_tags, clearance) so the caller gets a ranked target list, not raw chunks.

GRACEFUL DEGRADATION. Until the embed + IVF_PQ stage runs, ``embedding`` is NULL and no vector index
exists; the tool returns ``vector_index_absent`` rather than brute-force-scanning the lake. Profile
enrichment is best-effort — if the profiles dataset is unavailable, chunk matches are still returned.

State: reuses ``database.open_dataset`` (warm ``_handle_cache``) and ``embeddings.embed_query``
(lru-cached local embedder). No new connection / registry / handle-cache state.
"""

from __future__ import annotations

import re
from typing import Any

from .. import database, embeddings

DATASET = "govcon_sub_capability_vectors_90day"
# Identity (name + subaward count/$ + distinct primes) — UNIVERSAL: the vectors cover all ~25,449 subs
# in contract_subaward, far more than the 6,586 bridge subs in the profiles, so identity must come from
# the fact table or most results would be bare UEIs.
SUBAWARD_FACT_DATASET = "usaspending_api_fresh/contract_subaward"
# Capability axis (tags/clearance/POC) — BONUS: only the ~6,586 bridge subs carry it (the profiles are
# scoped to subs that teamed under tracked solicitations). Absent → fields null, `profiled` = false.
PROFILES_DATASET = "govcon_subawardee_capability_profiles"

# vectors-table columns (the ANN survivors); no NAICS/clearance here — those live on the profiles.
_RETURN_COLUMNS = [
    "subawardee_uei", "description_chunk_ix", "chunk_id", "text", "char_len", "n_source_subawards",
]
_FACT_COLUMNS = ["subawardee_uei", "subawardee_name", "subaward_amount", "prime_awardee_uei"]
_PROFILE_COLUMNS = [
    "sub_uei", "capability_tags", "requires_clearance", "req_clearance_level_max", "poc_available",
    # Path B: self-reported capability tags (the sub's own descriptions) + provenance
    "self_reported_capability_tags", "tag_source",
    # GEO: HQ (sub address) + place of performance
    "hq_state", "hq_city", "pop_state", "pop_states",
]

_UEI_RE = re.compile(r"^[A-Za-z0-9]{12}$")       # SAM UEI: 12 alnum chars
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")    # ISO date for the created_at window

_DEFAULT_K = 25
_MAX_K = 200
# ~7% of the sub_caps IVF_PQ's ~321 partitions (vs. govcon's 1.7%) — a small partition count starves
# recall at the default; pin a higher floor (verified: nprobes=24 returns clean electrical neighbors).
_DEFAULT_NPROBES = 24
_REFINE_FACTOR = 10
# pull extra chunk hits so the distinct-sub collapse still yields ~k subs when one sub has many chunks.
_CHUNK_OVERFETCH = 4


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _vector_index_present(ds) -> bool:
    """True iff a vector index covers the ``embedding`` column. Tolerant of pylance key-name drift."""
    try:
        for idx in ds.list_indices():
            cols = idx.get("fields") or idx.get("columns") or idx.get("column") or []
            if isinstance(cols, str):
                cols = [cols]
            if "embedding" in cols:
                return True
    except Exception:  # noqa: BLE001
        return False
    return False


def _build_filter(subawardee_uei, date_after, date_before) -> str:
    """Scalar prefilter pushed down BEFORE ANN. sub_uei is the BTREE pin; created_at windows the
    extraction timestamp. Values are validated + escaped."""
    clauses: list[str] = []
    if subawardee_uei:
        u = subawardee_uei.strip()
        if not _UEI_RE.match(u):
            raise ValueError("subawardee_uei must be a 12-char alphanumeric SAM UEI")
        clauses.append(f"subawardee_uei = {_sql_str(u)}")
    for bound, op in ((date_after, ">="), (date_before, "<=")):
        if bound:
            b = bound.strip()
            if not _DATE_RE.match(b):
                raise ValueError(f"date bound {bound!r} must be ISO 'YYYY-MM-DD'")
            clauses.append(f"created_at {op} timestamp '{b} 00:00:00'")
    return " AND ".join(clauses)


def _enrich_identity(sub_ueis: list[str]) -> dict[str, dict]:
    """UNIVERSAL identity from contract_subaward (covers all subs the vectors cover): per sub_uei →
    sub_name, n_subawards, total_subaward_amount, n_distinct_primes_subaward. Best-effort → {} on miss."""
    if not sub_ueis:
        return {}
    try:
        fact = database.open_dataset(SUBAWARD_FACT_DATASET)
    except KeyError:
        return {}
    pred = "subawardee_uei IN (" + ", ".join(_sql_str(u) for u in sub_ueis) + ")"
    try:
        rows = fact.scanner(columns=_FACT_COLUMNS, filter=pred).to_table().to_pylist()
    except Exception:  # noqa: BLE001 — enrichment is non-fatal
        return {}
    agg: dict[str, dict] = {}
    for r in rows:
        u = r["subawardee_uei"]
        a = agg.setdefault(u, {"sub_name": None, "n_subawards": 0, "total": 0.0, "primes": set()})
        a["n_subawards"] += 1
        if a["sub_name"] is None and r.get("subawardee_name"):
            a["sub_name"] = r["subawardee_name"]
        try:
            a["total"] += float(r.get("subaward_amount") or 0)
        except (TypeError, ValueError):
            pass
        if r.get("prime_awardee_uei"):
            a["primes"].add(r["prime_awardee_uei"])
    return {u: {"sub_name": a["sub_name"], "n_subawards": a["n_subawards"],
                "total_subaward_amount": round(a["total"], 2),
                "n_distinct_primes_subaward": len(a["primes"])}
            for u, a in agg.items()}


def _enrich_profile(sub_ueis: list[str]) -> dict[str, dict]:
    """BONUS capability axis from the profiles BTREE (only the ~6,586 bridge subs). {} on miss."""
    if not sub_ueis:
        return {}
    try:
        prof = database.open_dataset(PROFILES_DATASET)
    except KeyError:
        return {}
    pred = "sub_uei IN (" + ", ".join(_sql_str(u) for u in sub_ueis) + ")"
    try:
        rows = prof.scanner(columns=_PROFILE_COLUMNS, filter=pred).to_table().to_pylist()
    except Exception:  # noqa: BLE001 — enrichment is non-fatal
        return {}
    return {r["sub_uei"]: r for r in rows}


def search_subawardee_capabilities(
    query: str,
    subawardee_uei: str | None = None,
    date_after: str | None = None,
    date_before: str | None = None,
    k: int = _DEFAULT_K,
) -> dict[str, Any]:
    """Semantic search over SUBAWARDEE capability — find subcontractors by what their past work MEANS.

    Each subawardee's self-reported ``subaward_description`` text (the work they performed under prime
    contracts) is embedded; this matches a free-text query by cosine ANN and returns a ranked list of
    DISTINCT subawardees, each enriched with name, subaward count/$, controlled-vocab capability_tags,
    and clearance from ``govcon_subawardee_capability_profiles``. Use it for "who can do X" sourcing —
    e.g. "substation switchgear and power distribution installation", "aircraft avionics depot repair",
    "SCIF / sensitive compartmented facility construction" — where keyword/NAICS filtering misses
    paraphrase. For STRUCTURED conjunction (capability_tag ∧ clearance ∧ teaming-prime), use the
    profiles query path instead; this tool is the open-vocabulary recall leg.

    Args:
      query: natural-language description of the capability/work to find (required).
      subawardee_uei: optional 12-char UEI pin — restrict the search to one sub's own descriptions.
      date_after / date_before: optional ISO ``YYYY-MM-DD`` window on the chunk extraction timestamp
        (``created_at``, UTC) — NOT the subaward action date.
      k: number of DISTINCT subawardees to return (default 25, capped at 200).

    Returns ``{"status", "query", "filter", "k", "sub_count", "subs": [...], "embed_cache"}``. Each
    ``subs`` entry: ``{subawardee_uei, sub_name, best_distance (cosine; smaller=closer),
    matched_description, n_chunks_matched, n_subawards, total_subaward_amount,
    n_distinct_primes_subaward, capability_tags, requires_clearance, req_clearance_level_max,
    poc_available}``. Non-``ok`` ``status``: ``dataset_unavailable`` (vectors not landed),
    ``vector_index_absent`` (embed/IVF_PQ not built), ``embedder_unavailable`` (local model missing).
    Drill a returned ``subawardee_uei`` via the catalyst_api ``/entities/{uei}/subaward-profile`` route
    or the profiles dataset for full prime-contract history.
    """
    q = (query or "").strip()
    if not q:
        raise ValueError("query is required")
    k = max(1, min(int(k), _MAX_K))

    try:
        ds = database.open_dataset(DATASET)
    except KeyError:
        return {
            "status": "dataset_unavailable",
            "query": q,
            "detail": (
                f"{DATASET!r} is not registered in the active sink yet — the subawardee capability "
                "vector corpus has not landed. It auto-registers once committed under active/; call "
                "list_datasets to confirm."
            ),
        }

    if not _vector_index_present(ds):
        return {
            "status": "vector_index_absent",
            "query": q,
            "detail": (
                "the embedding column / IVF_PQ vector index is not built yet — subawardees are not "
                "semantically searchable. Run sam_attachment_embed_modal.py --sink sub_caps. The "
                "profiles dataset remains queryable via execute_audience_query in the meantime."
            ),
        }

    pred = _build_filter(subawardee_uei, date_after, date_before)
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

    # Hybrid: scalar prefilter pushed down, THEN cosine ANN. Over-fetch chunks so the distinct-sub
    # collapse still yields ~k subs when a single prolific sub contributes many top chunks.
    scanner = ds.scanner(
        columns=_RETURN_COLUMNS,
        filter=pred or None,
        prefilter=True,
        nearest={
            "column": "embedding",
            "q": list(qv),
            "k": min(_MAX_K, k * _CHUNK_OVERFETCH),
            "nprobes": _DEFAULT_NPROBES,
            "refine_factor": _REFINE_FACTOR,
        },
    )
    chunks = scanner.to_table().to_pylist()

    # collapse to distinct subs, keeping the best (smallest) distance + its matched description
    best: dict[str, dict] = {}
    for r in chunks:
        u = r["subawardee_uei"]
        d = r.get("_distance")
        cur = best.get(u)
        if cur is None:
            best[u] = {"best_distance": d, "matched_description": r.get("text"), "n_chunks_matched": 1}
        else:
            cur["n_chunks_matched"] += 1
            if d is not None and (cur["best_distance"] is None or d < cur["best_distance"]):
                cur["best_distance"] = d
                cur["matched_description"] = r.get("text")
    ranked = sorted(best.items(), key=lambda kv: (kv[1]["best_distance"] is None, kv[1]["best_distance"]))[:k]

    matched_ueis = [u for u, _ in ranked]
    ident = _enrich_identity(matched_ueis)     # universal name/$/count
    prof = _enrich_profile(matched_ueis)       # bonus capability axis (bridge subs only)
    subs = []
    for u, m in ranked:
        idr = ident.get(u, {})
        p = prof.get(u, {})
        subs.append({
            "subawardee_uei": u,
            "sub_name": idr.get("sub_name"),
            "best_distance": m["best_distance"],
            "matched_description": m["matched_description"],
            "n_chunks_matched": m["n_chunks_matched"],
            "n_subawards": idr.get("n_subawards"),
            "total_subaward_amount": idr.get("total_subaward_amount"),
            "n_distinct_primes_subaward": idr.get("n_distinct_primes_subaward"),
            "capability_tags": p.get("capability_tags"),                              # scope-derived (bridge)
            "self_reported_capability_tags": p.get("self_reported_capability_tags"),   # Path B (own descriptions)
            "tag_source": p.get("tag_source"),                                         # scope|self_reported|both|none
            "requires_clearance": p.get("requires_clearance"),
            "req_clearance_level_max": p.get("req_clearance_level_max"),
            "poc_available": p.get("poc_available"),
            "hq_state": p.get("hq_state"), "hq_city": p.get("hq_city"),     # sub HQ
            "pop_state": p.get("pop_state"), "pop_states": p.get("pop_states"),  # place of performance
            "profiled": u in prof,    # true → in the profiles table (now ~full universe); see tag_source for which axis
        })
    return {
        "status": "ok",
        "query": q,
        "filter": pred or None,
        "k": k,
        "sub_count": len(subs),
        "subs": subs,
        "embed_cache": embeddings.cache_info(),
    }


def register(mcp) -> None:
    """Mount the subawardee capability vector-search tool onto the FastMCP server."""
    mcp.add_tool(search_subawardee_capabilities)
