#!/usr/bin/env python3
"""Materialize sca_soc_crosswalk — the permanent SCA↔SOC occupation bridge.

Puts each SCA 5-digit occupation_code and its canonical SOC soc_code on ONE row so
sca_wd_rates.hourly_wage (Axis 2, statutory floor) can be compared to soc_state_wage.h_median /
soc_priced_skilled decile ladder (Axis 1, OEWS market) for the SAME role — the single-row
wage-arbitrage comparison that is structurally BLOCKED without this crosswalk
(see docs/reference/SCA_SOC_OCCUPATION_CROSSWALK_DIAGNOSTIC.md).

PRECISION IS GUARD-ENFORCED, NOT SAMPLED. False positives are unacceptable; unmatched (NULL) is
the guarded default, never a guessed SOC. The model can only ever emit a frozen candidate or null.

STAGES (fail-closed, resume via the frozen manifest — never re-derived from live data):
  manifest  Stage 0 (four deterministic candidate generators → bounded per-code enum, frozen)
            + Stage 1 (FPDS-dollar-weighted deterministic T1 collapse) → _sca_soc_crosswalk_manifest
            + emits the Stage-2 adjudication worklist (tie-out ∪ non-co-resident) as batch files.
  retrieve  Stage 3 (read --agent-results, apply the 9 publish guards G1–G9, write MAIN 1:1 +
            SIDECAR N:M) + Stage 4 (standalone in-Lance verification: G7 grain, G4 no-guess).

Classification (Stage 2) runs ONLY as in-session Opus 4.8 subagents (waves), ZERO Anthropic API
spend — the naics_psc_labor_profile house precedent. `manifest` renders the enum-confined worklist;
the orchestrator fans it to subagents; `retrieve` validates + materializes.

  # Stage 0/1 — build the frozen manifest + T1 + worklist (needs embedding + bm25 deps):
  doppler run -p core-x -c prd -- uv run --no-project \
    --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.1' --with 'psycopg[binary]>=3.2' \
    --with 'sentence-transformers>=3,<4' --with 'torch' --with 'numpy' --with 'rank-bm25' \
    python3 pipelines/reference/materialize_sca_soc_crosswalk.py manifest

  # (adjudicate the worklist in-session, Opus 4.8, enum-confined → agent_results.json)

  # Stage 3/4 — publish + verify:
  doppler run -p core-x -c prd -- uv run --no-project \
    --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.1' --with 'psycopg[binary]>=3.2' \
    python3 pipelines/reference/materialize_sca_soc_crosswalk.py retrieve --agent-results agent_results.json
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys

# ---------------------------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------------------------
BASE = "s3://data-sink/active/"
SCA_RATES_URI = BASE + "sca_wd_rates"
SCA_DICT_URI = BASE + "dol_sca_occupations"
CATEGORIES_URI = BASE + "naics_psc_labor_profile_categories"
PROFILE_URI = BASE + "naics_psc_labor_profile"
SOC_PRICED_URI = BASE + "soc_priced_skilled"
ONET_TITLES_URI = BASE + "onet_job_titles"

MANIFEST_URI = BASE + "_sca_soc_crosswalk_manifest"
MAIN_URI = BASE + "sca_soc_crosswalk"
SIDECAR_URI = BASE + "sca_soc_candidates"

DATA_STORAGE_VERSION = "2.1"
MODEL_ID = "claude-opus-4-8:in-session"
PROMPT_VERSION = "sca_soc_v1"
EMBED_MODEL_VERSION = "BAAI/bge-large-en-v1.5@1024"
SOURCE = "SCA_SOC_CROSSWALK"

# tuning knobs (frozen, stamped on rows / ledger)
K_CANDIDATES = 12          # bounded candidate enum cap per SCA code
DOMINANCE_MULT = 2.0       # T1 requires rank1 dollar_weight >= this * rank2
OFF_PATTERN_MAX = 0.5      # T1 requires primary off_pattern_share <= this
BGE_TOPK = 8               # BGE cosine recall net top-k
BM25_TOPK = 6              # family-scoped BM25 top-k
OVERLAP_MIN = 0.03         # T2 lexical-corroboration floor (garbage catch; enum+LLM are the semantic gates)
EMBED_DIM = 1024
BGE_QUERY_PREFIX = "Represent this sentence for searching relevant passages: "
CONF_WEIGHT = {"high": 3, "medium": 2, "low": 1}

# Hand-verified SCA-family (2-digit) -> allowed SOC major groups (2-digit). Generous but
# domain-correct: it is NOT the numeric-prefix identity (SCA family 23 Mechanics is mapped to the
# trade/repair SOC majors, NEVER SOC major 23 = Legal). Serves BM25 candidate focus AND the G6
# false-friend guard. Family titles (SCADD 5th ed.) in comments.
SCA_FAMILY_ALLOWED_SOC_MAJOR = {
    "01": {"11", "13", "15", "25", "27", "41", "43"},        # Administrative Support & Clerical
    "05": {"47", "49", "51", "53"},                          # Automotive Service
    "06": {"47", "49", "51"},                                # (rare; generous trade/repair)
    "07": {"35", "51"},                                      # Food Preparation & Service
    "08": {"19", "33", "37", "45", "53"},                    # (observed spread; forestry/fire/service)
    "09": {"47", "49", "51", "53"},                          # Furniture Maintenance & Repair
    "11": {"37", "39", "45", "53"},                          # General Services & Support
    "12": {"21", "29", "31", "43", "51", "53"},              # Health
    "13": {"23", "25", "27", "43", "47", "49", "51"},        # Information & Arts (legal-support ⇒ 23 allowed)
    "14": {"15", "17", "49"},                                # Information Technology
    "15": {"13", "15", "25", "27"},                          # Instructional
    "16": {"37", "51"},                                      # Laundry / Dry-Cleaning / Pressing
    "19": {"47", "49", "51"},                                # Machine Tool Operation & Repair
    "21": {"13", "43", "51", "53"},                          # Materials Handling & Packing
    "23": {"15", "17", "27", "47", "49", "51", "53"},        # Mechanics & Maintenance/Repair (NOT 23 Legal)
    "24": {"21", "31", "39"},                                # Personal Needs
    "25": {"47", "49", "51"},                                # Plant & System Operation
    "27": {"33", "43"},                                      # Protective Service
    "28": {"27", "33", "39"},                                # Recreation
    "29": {"53"},                                            # Stevedoring / Longshoremen
    "30": {"15", "17", "19", "27", "29", "51", "53"},        # Technical (NOT 23 Legal)
    "31": {"33", "53"},                                      # Transportation / Mobile Equipment
    "47": {"47", "49", "53"},                                # Water Transportation
    "91": {"19", "29", "45"},                                # Wildlife Mgmt & Animal Care
    "99": {"17", "31", "37", "39", "41", "43", "45", "47", "49", "51", "53"},  # Miscellaneous (broad)
}


def _family_of(sca_code: str, dict_family: str | None) -> str:
    """Family 2-digit prefix: dictionary family_code if present, else derived from the code."""
    fam = (dict_family or "")[:2]
    if not fam:
        fam = (sca_code or "")[:2]
    return fam


def _allowed_majors(fam2: str) -> set[str]:
    return SCA_FAMILY_ALLOWED_SOC_MAJOR.get(fam2, set())


# ---------------------------------------------------------------------------------------------
# Shared helpers (Lance / R2 / ledger)
# ---------------------------------------------------------------------------------------------
def _so() -> dict:
    from pipelines.bls.ingest import _storage_options
    return _storage_options()


def _norm(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"[^A-Z0-9]+", " ", s.upper()).strip()


def _tokens(s: str | None) -> set[str]:
    return {t for t in _norm(s).split() if len(t) > 2}


def _overlap(a: str | None, b: str | None) -> float:
    """Overlap coefficient |A∩B| / min(|A|,|B|) over content tokens."""
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / min(len(ta), len(tb))


def _write(tbl, uri: str, btree: list[str], bitmap: list[str], so: dict) -> list[str]:
    import lance
    from pipelines.bls.ingest import _build_indexes
    lance.write_dataset(tbl, uri, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION, storage_options=so)
    return _build_indexes(uri, btree, bitmap, so)


def _record_run(stage, dataset, uri, *, manifest_sha=None, agent_sha=None, sca_universe=None,
                t1=None, t2=None, unmatched=None, register_only=None, guard_failures=None,
                built=None, metrics=None, status="success", error=None,
                started_at=None, completed_at=None) -> None:
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set; skipping ops.sca_soc_crosswalk_runs write.")
        return
    try:
        import psycopg
        from psycopg.types.json import Jsonb
        ddl = open(os.path.join(os.path.dirname(__file__), "ops_sca_soc_crosswalk_runs.sql")).read()
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(ddl)
            cur.execute(
                """INSERT INTO ops.sca_soc_crosswalk_runs
                   (stage, dataset, dataset_uri, prompt_version, model_id, embed_model_version,
                    manifest_sha256, agent_results_sha256, sca_universe, t1_rows, t2_rows,
                    unmatched_rows, register_only_rows, guard_failures, indexes_built, metrics,
                    status, error, started_at, completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (stage, dataset, uri, PROMPT_VERSION, MODEL_ID, EMBED_MODEL_VERSION,
                 manifest_sha, agent_sha, sca_universe, t1, t2, unmatched, register_only,
                 guard_failures, built, Jsonb(metrics) if metrics is not None else None,
                 status, error, started_at, completed_at),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        print(f"WARN: ops.sca_soc_crosswalk_runs write failed: {exc}")


def log(msg: str) -> None:
    print(msg, flush=True)


# ---------------------------------------------------------------------------------------------
# STAGE 0 — candidate generators + STAGE 1 — deterministic T1
# ---------------------------------------------------------------------------------------------
def build_manifest() -> dict:
    import duckdb
    import lance
    import pyarrow as pa

    started = dt.datetime.now(dt.timezone.utc)
    so = _so()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8;")
    for name, uri in [("rates", SCA_RATES_URI), ("dict", SCA_DICT_URI), ("cats", CATEGORIES_URI),
                      ("profile", PROFILE_URI), ("soc", SOC_PRICED_URI), ("onet", ONET_TITLES_URI)]:
        con.register(name, lance.dataset(uri, storage_options=so))
    log("registered rates + dict + cats + profile + soc + onet Lance scanners")

    # ---- SCA universe = the 424 priced occupation_code (the load-bearing wage-arbitrage set) ----
    sca = con.execute("""
        SELECT r.occupation_code AS sca_code,
               any_value(r.title)              AS rate_title,
               any_value(d.occupation_title)   AS dict_title,
               any_value(d.occupation_definition) AS definition,
               any_value(d.family_code)        AS dict_family,
               any_value(d.family_title)       AS family_title,
               (max(d.occupation_code) IS NOT NULL) AS in_dict
        FROM rates r
        LEFT JOIN dict d ON r.occupation_code = d.occupation_code
        GROUP BY r.occupation_code
        ORDER BY r.occupation_code
    """).fetchall()
    sca_cols = [c[0] for c in con.description]
    SCA = [dict(zip(sca_cols, row)) for row in sca]
    log(f"SCA universe: {len(SCA)} priced codes ({sum(1 for s in SCA if s['in_dict'])} in dict, "
        f"{sum(1 for s in SCA if not s['in_dict'])} register-only)")

    # ---- SOC universe (830) — the enum + referential-integrity target ----
    soc_rows = con.execute("""
        SELECT soc_code, soc_title, coalesce(onet_description, onet_title, soc_title) AS soc_doc,
               onet_description
        FROM soc WHERE soc_code IS NOT NULL
    """).fetchall()
    SOC = {r[0]: {"soc_title": r[1], "soc_doc": r[2], "onet_description": r[3]} for r in soc_rows}
    SOC_UNIVERSE = set(SOC)
    log(f"SOC universe: {len(SOC_UNIVERSE)} codes")

    # ================= Generator 1: co-classification (distinct soc per sca) =================
    g1 = con.execute("""
        SELECT sca_code, soc_code, count(*) AS edges
        FROM cats
        WHERE sca_code IS NOT NULL AND sca_code <> '' AND soc_code IS NOT NULL AND soc_code <> ''
        GROUP BY sca_code, soc_code
    """).fetchall()
    coclass: dict[str, set[str]] = {}
    for sca_code, soc_code, _ in g1:
        coclass.setdefault(sca_code, set()).add(soc_code)
    log(f"G1 co-classification: {len(coclass)} SCA codes carry candidates")

    # ================= Generator 2: O*NET exact normalized title hit =================
    # normalize SCA dict title, match to onet job_title universe -> soc
    onet = con.execute("""
        SELECT DISTINCT trim(regexp_replace(upper(job_title), '[^A-Z0-9]+', ' ', 'g')) AS nt, soc_code
        FROM onet WHERE job_title IS NOT NULL AND soc_code IS NOT NULL
    """).fetchall()
    onet_map: dict[str, set[str]] = {}
    for nt, soc_code in onet:
        onet_map.setdefault(nt, set()).add(soc_code)
    g2: dict[str, set[str]] = {}
    for s in SCA:
        nt = _norm(s["dict_title"] or s["rate_title"])
        # also try a skill-level-stripped base title
        base = re.sub(r"\b(I{1,3}|IV|V|VI)\b|\(OCCUPATIONAL BASE\)", "", nt).strip()
        hits = set(onet_map.get(nt, set())) | set(onet_map.get(base, set()))
        if hits:
            g2[s["sca_code"]] = hits
    log(f"G2 O*NET exact-title: {len(g2)} SCA codes hit an alt-title")

    # ================= Generator 4: BGE-large-en cosine (recall net) =================
    # (run before BM25 so both use the same SOC doc order)
    import numpy as np
    soc_codes = list(SOC.keys())
    soc_docs = [f"{SOC[c]['soc_title']} — {SOC[c]['soc_doc']}" for c in soc_codes]
    sca_docs = [BGE_QUERY_PREFIX + f"{(s['dict_title'] or s['rate_title'] or '')} — "
                f"{(s['definition'] or s['dict_title'] or s['rate_title'] or '')}" for s in SCA]
    log(f"BGE: embedding {len(soc_docs)} SOC passages + {len(sca_docs)} SCA queries "
        f"({EMBED_MODEL_VERSION})…")
    from sentence_transformers import SentenceTransformer
    device = os.environ.get("EMBED_DEVICE", "mps")
    model = SentenceTransformer("BAAI/bge-large-en-v1.5", device=device)
    soc_vecs = model.encode(soc_docs, normalize_embeddings=True, batch_size=128,
                            show_progress_bar=False).astype(np.float32)
    sca_vecs = model.encode(sca_docs, normalize_embeddings=True, batch_size=128,
                            show_progress_bar=False).astype(np.float32)
    if soc_vecs.shape[1] != EMBED_DIM:
        raise RuntimeError(f"BGE produced dim {soc_vecs.shape[1]} != {EMBED_DIM}")
    sims = sca_vecs @ soc_vecs.T  # cosine (both L2-normalized)
    g4: dict[str, list[tuple[str, float]]] = {}
    for i, s in enumerate(SCA):
        order = np.argsort(-sims[i])[:BGE_TOPK]
        g4[s["sca_code"]] = [(soc_codes[j], float(sims[i][j])) for j in order]
    cosim = {s["sca_code"]: {soc_codes[j]: float(sims[i][j]) for j in range(len(soc_codes))}
             for i, s in enumerate(SCA)}
    log(f"BGE cosine: top-{BGE_TOPK} for {len(g4)} SCA codes")

    # ================= Generator 3: family-scoped BM25 =================
    from rank_bm25 import BM25Okapi
    soc_major = {c: c[:2] for c in soc_codes}
    tokenized = [_norm(d).split() for d in soc_docs]
    bm25 = BM25Okapi(tokenized)
    g3: dict[str, list[str]] = {}
    for s in SCA:
        fam2 = _family_of(s["sca_code"], s["dict_family"])
        allowed = _allowed_majors(fam2)
        query = _norm(f"{s['dict_title'] or s['rate_title'] or ''} {s['definition'] or ''}").split()
        if not query:
            continue
        scores = bm25.get_scores(query)
        ranked = np.argsort(-scores)
        picks = []
        for j in ranked:
            c = soc_codes[j]
            if allowed and soc_major[c] not in allowed:
                continue
            if scores[j] <= 0:
                continue
            picks.append(c)
            if len(picks) >= BM25_TOPK:
                break
        if picks:
            g3[s["sca_code"]] = picks
    log(f"G3 family-scoped BM25: {len(g3)} SCA codes with candidates")

    # ================= Union → family-filter → cap K (priority-ordered) =================
    def build_candidates(s: dict) -> list[dict]:
        sca_code = s["sca_code"]
        fam2 = _family_of(sca_code, s["dict_family"])
        allowed = _allowed_majors(fam2)
        seen: dict[str, dict] = {}
        # priority order: co_class, onet_exact, bge, bm25
        ordered: list[tuple[str, str]] = []
        for c in sorted(coclass.get(sca_code, set())):
            ordered.append((c, "co_class"))
        for c in sorted(g2.get(sca_code, set())):
            ordered.append((c, "onet_exact"))
        for c, _sim in g4.get(sca_code, []):
            ordered.append((c, "bge"))
        for c in g3.get(sca_code, []):
            ordered.append((c, "bm25"))
        for soc_code, src in ordered:
            if soc_code not in SOC_UNIVERSE:      # referential integrity: must be a real priced SOC
                continue
            if allowed and soc_code[:2] not in allowed:   # G6 family false-friend filter
                continue
            if soc_code in seen:
                continue
            seen[soc_code] = {
                "soc_code": soc_code,
                "soc_title": SOC[soc_code]["soc_title"],
                "onet_desc": (SOC[soc_code]["onet_description"] or "")[:280],
                "src": src,
                "cosine": round(cosim.get(sca_code, {}).get(soc_code, 0.0), 4),
            }
            if len(seen) >= K_CANDIDATES:
                break
        return list(seen.values())

    # ================= STAGE 1 — deterministic FPDS-dollar-weighted T1 =================
    dw = con.execute("""
        SELECT c.sca_code, c.soc_code,
               sum(coalesce(p.total_dollars_obligated, 0)) AS dollar_weight,
               sum(coalesce(p.n_awards, 0))                AS award_weight,
               count(DISTINCT c.naics_code || '|' || c.psc_code) AS combo_support,
               avg(CASE c.confidence WHEN 'high' THEN 3 WHEN 'medium' THEN 2
                                     WHEN 'low' THEN 1 ELSE NULL END) AS mean_conf,
               avg(CASE WHEN c.off_pattern THEN 1.0 WHEN c.off_pattern IS NULL THEN NULL
                        ELSE 0.0 END) AS off_pattern_share
        FROM cats c
        LEFT JOIN profile p ON c.naics_code = p.naics_code AND c.psc_code = p.psc_code
        WHERE c.sca_code IS NOT NULL AND c.sca_code <> '' AND c.soc_code IS NOT NULL AND c.soc_code <> ''
        GROUP BY c.sca_code, c.soc_code
    """).fetchall()
    dw_cols = [d[0] for d in con.description]
    weights: dict[str, list[dict]] = {}
    for row in dw:
        r = dict(zip(dw_cols, row))
        weights.setdefault(r["sca_code"], []).append(r)
    con.close()

    def rank_key(r: dict):
        return (-(r["dollar_weight"] or 0.0), -(r["award_weight"] or 0), -(r["combo_support"] or 0),
                (r["off_pattern_share"] if r["off_pattern_share"] is not None else 1.0),
                -(r["mean_conf"] or 0.0), r["soc_code"])

    # ================= Assemble manifest rows =================
    man_rows = []
    for s in SCA:
        sca_code = s["sca_code"]
        cands = build_candidates(s)
        cand_codes = {c["soc_code"] for c in cands}
        cand_json = json.dumps(cands, sort_keys=True)
        cand_sha = hashlib.sha256(cand_json.encode()).hexdigest()

        # T1 decision
        ws = sorted(weights.get(sca_code, []), key=rank_key)
        t1_soc = None
        t1_method = None
        dominance = None
        primary_dollar = 0.0
        primary_award = 0
        primary_combo = 0
        primary_offshare = None
        primary_conf = None
        tie_out = True
        if ws:
            r1 = ws[0]
            r2 = ws[1] if len(ws) > 1 else None
            d1 = r1["dollar_weight"] or 0.0
            d2 = (r2["dollar_weight"] or 0.0) if r2 else 0.0
            dominance = (d1 / d2) if d2 > 0 else (float("inf") if d1 > 0 else None)
            primary_dollar, primary_award = d1, int(r1["award_weight"] or 0)
            primary_combo = int(r1["combo_support"] or 0)
            primary_offshare = r1["off_pattern_share"]
            primary_conf = r1["mean_conf"]
            offshare_ok = (primary_offshare is None) or (primary_offshare <= OFF_PATTERN_MAX)
            dom_ok = dominance is not None and (dominance >= DOMINANCE_MULT)
            in_enum = r1["soc_code"] in cand_codes
            if dom_ok and offshare_ok and d1 > 0 and in_enum:
                t1_soc = r1["soc_code"]
                t1_method = "fpds_weighted_majority"
                tie_out = False
        # register-only / non-co-resident codes have no weights -> tie_out stays True (→ worklist)
        co_resident = bool(ws)

        man_rows.append({
            "sca_code": sca_code,
            "rate_title": s["rate_title"],
            "dict_title": s["dict_title"],
            "definition": s["definition"],
            "family_code": (s["dict_family"] or (sca_code[:2] + "000")),
            "family_title": s["family_title"],
            "in_scadd": bool(s["in_dict"]),
            "register_only": not bool(s["in_dict"]),
            "co_resident": co_resident,
            "candidates_json": cand_json,
            "candidates_sha256": cand_sha,
            "candidate_count": len(cands),
            "t1_soc_code": t1_soc,
            "t1_method": t1_method,
            "dominance_ratio": (None if dominance in (None, float("inf")) else round(dominance, 3)),
            "dominance_inf": (dominance == float("inf")),
            "primary_dollar_weight": float(primary_dollar),
            "primary_award_weight": int(primary_award),
            "primary_combo_support": int(primary_combo),
            "primary_off_pattern_share": (None if primary_offshare is None else float(primary_offshare)),
            "primary_mean_conf": (None if primary_conf is None else float(primary_conf)),
            "tie_out": tie_out,
            "prompt_version": PROMPT_VERSION,
            "embed_model_version": EMBED_MODEL_VERSION,
        })

    # manifest_sha over the ordered frozen candidate shas + T1 picks (resume/version key)
    manifest_sha = hashlib.sha256(
        json.dumps([(r["sca_code"], r["candidates_sha256"], r["t1_soc_code"]) for r in man_rows],
                   sort_keys=True).encode()).hexdigest()

    tbl = pa.Table.from_pylist(man_rows)
    built = _write(tbl, MANIFEST_URI,
                   btree=["sca_code", "family_code", "t1_soc_code"],
                   bitmap=["tie_out", "co_resident", "register_only", "in_scadd"], so=so)

    n_t1 = sum(1 for r in man_rows if r["t1_soc_code"])
    n_worklist = sum(1 for r in man_rows if r["tie_out"] and r["candidate_count"] > 0)
    n_no_cand = sum(1 for r in man_rows if r["candidate_count"] == 0)
    log(f"manifest: {len(man_rows)} rows | T1={n_t1} | worklist(tie_out & has-cand)={n_worklist} "
        f"| no-candidate(→unmatched by construction)={n_no_cand}")

    # ---- emit Stage-2 adjudication worklist as batch files ----
    worklist = [r for r in man_rows if r["tie_out"] and r["candidate_count"] > 0]
    outdir = os.environ.get("SCA_SOC_WORKDIR", "/tmp/audit/adj")
    os.makedirs(outdir, exist_ok=True)
    BATCH = int(os.environ.get("SCA_SOC_BATCH", "9"))
    batches = [worklist[i:i + BATCH] for i in range(0, len(worklist), BATCH)]
    for bi, batch in enumerate(batches):
        items = [{
            "occupation_code": r["sca_code"],
            "sca_title": r["dict_title"] or r["rate_title"],
            "sca_definition": (r["definition"] or "")[:900] or None,
            "family_title": r["family_title"],
            "candidates": json.loads(r["candidates_json"]),
        } for r in batch]
        json.dump(items, open(os.path.join(outdir, f"batch_{bi:03d}.json"), "w"), indent=1)
    json.dump({"n_batches": len(batches), "n_codes": len(worklist), "dir": outdir,
               "manifest_sha256": manifest_sha},
              open(os.path.join(outdir, "index.json"), "w"), indent=1)
    log(f"worklist: {len(worklist)} codes → {len(batches)} batch files in {outdir}")

    completed = dt.datetime.now(dt.timezone.utc)
    _record_run("manifest", "_sca_soc_crosswalk_manifest", MANIFEST_URI, manifest_sha=manifest_sha,
                sca_universe=len(man_rows), t1=n_t1, built=built,
                metrics={"worklist_codes": len(worklist), "no_candidate": n_no_cand,
                         "co_resident": sum(1 for r in man_rows if r["co_resident"]),
                         "batches": len(batches)},
                started_at=started, completed_at=completed)
    return {"manifest_rows": len(man_rows), "t1": n_t1, "worklist": len(worklist),
            "batches": len(batches), "manifest_sha256": manifest_sha, "workdir": outdir}


# ---------------------------------------------------------------------------------------------
# STAGE 3 — publish (guards G1–G9) + STAGE 4 — verify
# ---------------------------------------------------------------------------------------------
def retrieve(agent_results_path: str) -> dict:
    import duckdb
    import lance
    import pyarrow as pa

    started = dt.datetime.now(dt.timezone.utc)
    so = _so()

    raw = open(agent_results_path, "rb").read()
    agent_sha = hashlib.sha256(raw).hexdigest()
    picks_in = json.loads(raw)
    # accept either a flat array of {occupation_code, soc_code, confidence, rationale}
    # or {results:[...]}
    if isinstance(picks_in, dict):
        picks_in = picks_in.get("results", picks_in.get("picks", []))
    adj: dict[str, dict] = {}
    for e in picks_in:
        oc = str(e.get("occupation_code", "")).strip()
        if oc:
            adj[oc] = e
    log(f"agent-results: {len(adj)} adjudicated codes (sha {agent_sha[:12]})")

    man = list(lance.dataset(MANIFEST_URI, storage_options=so).to_table().to_pylist())
    by_code = {r["sca_code"]: r for r in man}
    soc_universe = set(lance.dataset(SOC_PRICED_URI, storage_options=so)
                       .to_table(columns=["soc_code"]).column("soc_code").to_pylist())
    soc_meta = {r["soc_code"]: r for r in lance.dataset(SOC_PRICED_URI, storage_options=so)
                .to_table(columns=["soc_code", "soc_title", "onet_description"]).to_pylist()}
    log(f"manifest: {len(man)} rows | soc universe: {len(soc_universe)}")

    gen_at = dt.datetime.now(dt.timezone.utc).isoformat()
    main_rows, side_rows = [], []
    failures: list[str] = []

    for r in man:
        sca = r["sca_code"]
        fam2 = _family_of(sca, r["family_code"])
        allowed = _allowed_majors(fam2)
        cands = json.loads(r["candidates_json"])
        cand_codes = {c["soc_code"] for c in cands}
        cand_by_code = {c["soc_code"]: c for c in cands}
        soc_pick, tier, method, corroborator, defoverlap, confidence, rationale, cosine = (
            None, "unmatched", None, None, None, None, None, None)

        # ---- T1 (deterministic, frozen) ----
        if r["t1_soc_code"]:
            soc_pick = r["t1_soc_code"]
            tier, method = "T1", "fpds_weighted_majority"
            corroborator = "dollar_dominance"
            confidence = "high"
            cosine = cand_by_code.get(soc_pick, {}).get("cosine")
        else:
            # ---- T2 (LLM adjudication, enum-confined, guarded) ----
            e = adj.get(sca)
            if e is not None:
                raw_soc = e.get("soc_code")
                raw_soc = str(raw_soc).strip() if raw_soc not in (None, "", "null") else None
                if raw_soc:
                    # G1 enum provenance (server-side re-validation), G2/G3 handled at guard pass
                    if raw_soc not in cand_codes:
                        failures.append(f"{sca}: LLM soc {raw_soc!r} off frozen enum → forced unmatched")
                    elif allowed and raw_soc[:2] not in allowed:
                        failures.append(f"{sca}: LLM soc {raw_soc!r} in forbidden major → forced unmatched")
                    elif raw_soc not in soc_universe:
                        failures.append(f"{sca}: LLM soc {raw_soc!r} not in soc_priced_skilled → unmatched")
                    else:
                        soc_pick = raw_soc
                        tier, method = "T2", "llm_definition_adjudicated"
                        corroborator = cand_by_code[raw_soc]["src"]
                        confidence = (e.get("confidence") or "medium").lower()
                        rationale = (e.get("rationale") or "")[:400] or None
                        cosine = cand_by_code[raw_soc].get("cosine")
                        defoverlap = _overlap(r["definition"], soc_meta.get(raw_soc, {}).get("onet_description"))

        soc_title = soc_meta.get(soc_pick, {}).get("soc_title") if soc_pick else None
        main_rows.append({
            "occupation_code": sca,
            "occupation_title": r["dict_title"] or r["rate_title"],
            "occupation_definition": r["definition"],
            "family_code": r["family_code"],
            "family_title": r["family_title"],
            "soc_code": soc_pick,
            "soc_title": soc_title,
            "tier": tier,
            "method": method,
            "corroborator_source": corroborator,
            "confidence": confidence,
            "definition_overlap": defoverlap,
            "dominance_ratio": r["dominance_ratio"],
            "dominance_inf": r["dominance_inf"],
            "primary_dollar_weight": r["primary_dollar_weight"],
            "primary_award_weight": r["primary_award_weight"],
            "primary_off_pattern_share": r["primary_off_pattern_share"],
            "cosine_sim": cosine,
            "candidate_soc_count": r["candidate_count"],
            "co_resident": r["co_resident"],
            "in_scadd": r["in_scadd"],
            "register_only": r["register_only"],
            "in_soc_priced_skilled": (soc_pick in soc_universe) if soc_pick else False,
            "rationale": rationale,
            "prompt_version": PROMPT_VERSION,
            "embed_model_version": EMBED_MODEL_VERSION,
            "model_id": MODEL_ID,
            "source": SOURCE,
            "generated_at": gen_at,
        })
        # ---- SIDECAR: every candidate, with selection provenance ----
        for rank, c in enumerate(cands, 1):
            side_rows.append({
                "occupation_code": sca,
                "candidate_soc_code": c["soc_code"],
                "candidate_soc_title": c["soc_title"],
                "rank": rank,
                "source_generator": c["src"],
                "cosine_sim": c.get("cosine"),
                "is_selected": (c["soc_code"] == soc_pick),
                "is_primary": (c["soc_code"] == soc_pick),
                "tier": tier if c["soc_code"] == soc_pick else None,
                "source": SOURCE,
                "generated_at": gen_at,
            })

    # ============================ FAIL-CLOSED PUBLISH GATE (G1–G9) ============================
    def is_soc(x): return bool(x) and re.fullmatch(r"[0-9]{2}-[0-9]{4}", x) is not None
    def is_sca_shaped(x): return bool(x) and re.fullmatch(r"[0-9]{5}", x) is not None

    matched = [m for m in main_rows if m["tier"] in ("T1", "T2")]
    unmatched = [m for m in main_rows if m["tier"] == "unmatched"]
    guard_fail = 0

    # G7 grain (MAIN 1:1, SIDECAR (code, cand) unique)
    if len({m["occupation_code"] for m in main_rows}) != len(main_rows):
        failures.append("G7: MAIN occupation_code not unique"); guard_fail += 1
    if len({(s["occupation_code"], s["candidate_soc_code"]) for s in side_rows}) != len(side_rows):
        failures.append("G7: SIDECAR (occupation_code, candidate_soc_code) not unique"); guard_fail += 1
    # G4 no-guess invariant
    for m in matched:
        if not m["soc_code"]:
            failures.append(f"G4: {m['occupation_code']} matched but NULL soc_code"); guard_fail += 1
    for m in unmatched:
        if m["soc_code"]:
            failures.append(f"G4: {m['occupation_code']} unmatched but non-null soc_code"); guard_fail += 1
    # G2 namespace disjointness
    for m in matched:
        if not is_soc(m["soc_code"]) or is_sca_shaped(m["soc_code"]):
            failures.append(f"G2: {m['occupation_code']} soc {m['soc_code']!r} not dd-dddd / SCA-shaped"); guard_fail += 1
    # G3 referential integrity (MAIN + SIDECAR)
    for m in matched:
        if m["soc_code"] not in soc_universe:
            failures.append(f"G3: {m['occupation_code']} soc {m['soc_code']!r} absent from soc_priced_skilled"); guard_fail += 1
    for s in side_rows:
        if s["candidate_soc_code"] not in soc_universe:
            failures.append(f"G3: sidecar {s['occupation_code']} cand {s['candidate_soc_code']!r} absent from soc_priced_skilled"); guard_fail += 1
    # G1 enum provenance: every published soc has is_selected in its sidecar
    sel = {(s["occupation_code"], s["candidate_soc_code"]) for s in side_rows if s["is_selected"]}
    for m in matched:
        if (m["occupation_code"], m["soc_code"]) not in sel:
            failures.append(f"G1: {m['occupation_code']} published soc {m['soc_code']!r} has no selected sidecar lineage"); guard_fail += 1
    # G5 corroboration
    for m in matched:
        if m["tier"] == "T1":
            dom_ok = m["dominance_inf"] or (m["dominance_ratio"] is not None and m["dominance_ratio"] >= DOMINANCE_MULT)
            off_ok = (m["primary_off_pattern_share"] is None) or (m["primary_off_pattern_share"] <= OFF_PATTERN_MAX)
            if not (dom_ok and off_ok):
                failures.append(f"G5: T1 {m['occupation_code']} fails dominance/off-pattern"); guard_fail += 1
        else:  # T2 must carry a corroborator source
            if not m["corroborator_source"]:
                failures.append(f"G5: T2 {m['occupation_code']} lacks corroborator"); guard_fail += 1
    # G6 false-friend (allowed-major)
    for m in matched:
        fam2 = _family_of(m["occupation_code"], m["family_code"])
        allowed = _allowed_majors(fam2)
        if allowed and m["soc_code"][:2] not in allowed:
            failures.append(f"G6: {m['occupation_code']} fam {fam2} soc major {m['soc_code'][:2]} forbidden"); guard_fail += 1
    # G8 anchors (hand-verified ground truth)
    ANCHORS = {"23370": "49-9071", "11150": "37-2011"}
    picks_by_code = {m["occupation_code"]: m["soc_code"] for m in main_rows}
    for oc, expect in ANCHORS.items():
        if oc in picks_by_code and picks_by_code[oc] not in (expect, None):
            failures.append(f"G8: anchor {oc} → {picks_by_code[oc]} != expected {expect}"); guard_fail += 1
    # G9 coverage bands (regression trip: collapse-to-~0 or runaway)
    n = len(main_rows)
    n_match = len(matched)
    if n_match < 0.30 * n:
        failures.append(f"G9: matched {n_match}/{n} below 30% floor (collapse regression)"); guard_fail += 1
    if n_match > 0.98 * n:
        failures.append(f"G9: matched {n_match}/{n} above 98% (runaway regression)"); guard_fail += 1

    hard_fail = guard_fail > 0
    if hard_fail:
        log("PUBLISH GATE FAILED — writing nothing:")
        for f in failures[:60]:
            log(f"  ✗ {f}")
        _record_run("retrieve", "sca_soc_crosswalk", MAIN_URI, agent_sha=agent_sha,
                    manifest_sha=None, sca_universe=n, guard_failures=guard_fail,
                    status="gap", error=f"{guard_fail} guard failures",
                    started_at=started, completed_at=dt.datetime.now(dt.timezone.utc))
        raise SystemExit(f"FAIL-CLOSED: {guard_fail} guard failures; nothing written.")
    # non-fatal informational failures (off-enum LLM picks forced to unmatched) are fine
    if failures:
        log(f"note: {len(failures)} LLM picks were forced to unmatched (guard-safe):")
        for f in failures[:20]:
            log(f"  · {f}")

    # ============================ WRITE MAIN + SIDECAR ============================
    main_tbl = pa.Table.from_pylist(main_rows)
    side_tbl = pa.Table.from_pylist(side_rows)
    built_main = _write(main_tbl, MAIN_URI,
                        btree=["occupation_code", "soc_code", "family_code"],
                        bitmap=["tier", "confidence", "register_only"], so=so)
    built_side = _write(side_tbl, SIDECAR_URI,
                        btree=["occupation_code", "candidate_soc_code"],
                        bitmap=["is_selected", "source_generator"], so=so)
    log(f"WROTE {MAIN_URI} ({main_tbl.num_rows} rows) idx={built_main}")
    log(f"WROTE {SIDECAR_URI} ({side_tbl.num_rows} rows) idx={built_side}")

    n_t1 = sum(1 for m in matched if m["tier"] == "T1")
    n_t2 = sum(1 for m in matched if m["tier"] == "T2")
    n_un = len(unmatched)
    n_reg = sum(1 for m in main_rows if m["register_only"])
    _record_run("retrieve", "sca_soc_crosswalk", MAIN_URI, agent_sha=agent_sha,
                sca_universe=n, t1=n_t1, t2=n_t2, unmatched=n_un, register_only=n_reg,
                guard_failures=0, built=built_main + built_side,
                metrics={"sidecar_rows": side_tbl.num_rows,
                         "forced_unmatched_llm": len(failures)},
                status="success", started_at=started,
                completed_at=dt.datetime.now(dt.timezone.utc))

    result = {"main_rows": main_tbl.num_rows, "sidecar_rows": side_tbl.num_rows,
              "T1": n_t1, "T2": n_t2, "unmatched": n_un, "register_only": n_reg}
    log(f"RESULT {json.dumps(result)}")
    # Stage 4 verification
    verify()
    return result


def verify() -> dict:
    """STAGE 4 — standalone, in-Lance, read-only. Asserts G7 grain + G4 no-guess; fails loud."""
    import lance
    so = _so()
    main = lance.dataset(MAIN_URI, storage_options=so).to_table().to_pylist()
    side = lance.dataset(SIDECAR_URI, storage_options=so).to_table(
        columns=["occupation_code", "candidate_soc_code", "is_selected"]).to_pylist()
    errs = []
    # G7 grain
    if len({m["occupation_code"] for m in main}) != len(main):
        errs.append("G7: MAIN occupation_code not 1:1")
    if len({(s["occupation_code"], s["candidate_soc_code"]) for s in side}) != len(side):
        errs.append("G7: SIDECAR grain not unique")
    # G4 no-guess
    for m in main:
        if m["tier"] in ("T1", "T2") and not m["soc_code"]:
            errs.append(f"G4: {m['occupation_code']} matched w/ NULL soc")
        if m["tier"] == "unmatched" and m["soc_code"]:
            errs.append(f"G4: {m['occupation_code']} unmatched w/ soc")
        if m["soc_code"] and not re.fullmatch(r"[0-9]{2}-[0-9]{4}", m["soc_code"]):
            errs.append(f"G4/G2: {m['occupation_code']} soc bad shape {m['soc_code']!r}")
    if errs:
        for e in errs[:40]:
            log(f"  ✗ VERIFY {e}")
        raise SystemExit(f"STANDALONE VERIFY FAILED: {len(errs)} violations")
    tiers = {}
    for m in main:
        tiers[m["tier"]] = tiers.get(m["tier"], 0) + 1
    log(f"VERIFY ✓ MAIN {len(main)} rows 1:1, SIDECAR {len(side)} rows unique, tiers={tiers}")
    return {"main": len(main), "sidecar": len(side), "tiers": tiers}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("manifest", help="Stage 0/1 — freeze candidates + T1 + emit worklist")
    rp = sub.add_parser("retrieve", help="Stage 3/4 — publish MAIN+SIDECAR + verify")
    rp.add_argument("--agent-results", required=True,
                    help="JSON array of {occupation_code, soc_code|null, confidence, rationale} "
                         "from the in-session Opus/xhigh enum-confined adjudication subagents")
    sub.add_parser("verify", help="Stage 4 — standalone in-Lance verification")
    args = ap.parse_args()
    if args.cmd == "manifest":
        print(json.dumps(build_manifest(), indent=2))
    elif args.cmd == "retrieve":
        print(json.dumps(retrieve(args.agent_results), indent=2))
    elif args.cmd == "verify":
        print(json.dumps(verify(), indent=2))


if __name__ == "__main__":
    main()
