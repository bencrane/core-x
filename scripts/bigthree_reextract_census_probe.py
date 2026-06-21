#!/usr/bin/env python3
"""READ-ONLY — EXACT LLM-lane token census for the Big-Three labor re-extraction,
using the pipeline's OWN selection functions (no replication drift).

Corrects the naive "feed the whole 101,950-chunk inventory" model: the LLM lane
stages a Decision-A input set (scope-ALL ∪ pricing-ALL ∪ (unknown ∩ lexicon/regex
hit)) MINUS marked resources, and caps each doc at DOC_TOKEN_BUDGET (8,000 tok) via
tiered select_chunks(). Imports select_chunks / compute_input_set / estimate_tokens
from sam_labor_demand_extract_90day so the numbers match a real `--phase census`.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/bigthree_reextract_census_probe.py > /tmp/bigthree_census.json
"""
from __future__ import annotations

import datetime as dt
import importlib
import json
import os
import sys

import duckdb
import lance

sys.path.insert(0, "pipelines/sam_gov")
sys.path.insert(0, "pipelines")
sys.path.insert(0, ".")
M = importlib.import_module("sam_labor_demand_extract_90day")

A = "s3://data-sink/active"
GAA = f"{A}/govcon_active_awards/"
MAN = f"{A}/sam_opps_attachment_manifest_winners/"
FILES = f"{A}/sam_attachment_files/"
LEDGER = M.EXTRACT_LEDGER_URI
SINKS = {"scope": M.SCOPE_URI, "pricing": M.PRICING_URI, "unknown": M.UNKNOWN_URI}


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
    os.makedirs("/tmp/duck_spill_cen", exist_ok=True)
    con.execute("SET memory_limit='12GB'")
    con.execute("SET temp_directory='/tmp/duck_spill_cen'")
    con.register("gaa", lance.dataset(GAA, storage_options=so))
    con.register("man", lance.dataset(MAN, storage_options=so))
    con.register("fl", lance.dataset(FILES, storage_options=so))

    # cohort downloaded resource set
    log("cohort downloaded resources")
    con.execute("""
        CREATE TEMP TABLE dl AS
        SELECT DISTINCT f.resource_id AS rid
        FROM fl f
        JOIN (SELECT DISTINCT m.resource_id
              FROM man m
              JOIN (SELECT contract_award_unique_key AS k FROM gaa
                    WHERE upper(trim(psc_code)) IN ('DA01','R425','R499')
                      AND contract_award_unique_key IS NOT NULL) bt
                ON m.contract_award_unique_key = bt.k
              WHERE m.resource_id IS NOT NULL) c
          ON f.resource_id = c.resource_id
        WHERE f.status='downloaded'
    """)
    cohort = set(r[0] for r in con.execute("SELECT rid FROM dl").fetchall())
    log(f"cohort downloaded files: {len(cohort):,}")

    # pull cohort chunk rows per sink (resource_id, chunk_ix, text, content_marking)
    chunks_by_sink: dict[str, dict[str, list[dict]]] = {}
    sink_counts: dict[str, dict[str, int]] = {}
    marked: set[str] = set()
    for name, uri in SINKS.items():
        log(f"sink {name}: pulling cohort chunks")
        ds = lance.dataset(uri, storage_options=so)
        cols = [c for c in ("resource_id", "chunk_ix", "text", "content_marking")
                if c in ds.schema.names]
        con.register("sink_src", ds.scanner(columns=cols).to_reader())
        con.execute("CREATE TEMP TABLE st AS SELECT * FROM sink_src")
        con.unregister("sink_src")
        rows = con.execute(
            "SELECT s.resource_id, s.chunk_ix, s.text, s.content_marking "
            "FROM st s JOIN dl ON s.resource_id = dl.rid").fetchall()
        con.execute("DROP TABLE st")
        by_res: dict[str, list[dict]] = {}
        cnt: dict[str, int] = {}
        for rid, cix, text, cm in rows:
            by_res.setdefault(rid, []).append({"chunk_ix": cix, "text": text})
            cnt[rid] = cnt.get(rid, 0) + 1
            if cm is not None and len(cm) > 0:
                marked.add(rid)
        chunks_by_sink[name] = by_res
        sink_counts[name] = cnt
        log(f"  {name}: {len(by_res):,} cohort resources, {sum(cnt.values()):,} chunks")

    # ledger rows for input-set determination (lexicon_hit_fullbody / n_requirements_regex)
    log("ledger")
    led = lance.dataset(LEDGER, storage_options=so)
    led_cols = [c for c in ("resource_id", "lexicon_hit_fullbody", "n_requirements_regex")
                if c in led.schema.names]
    con.register("led_src", led.scanner(columns=led_cols).to_reader())
    con.execute("CREATE TEMP TABLE lt AS SELECT * FROM led_src")
    con.unregister("led_src")
    ledger_rows = [dict(zip(led_cols, r)) for r in
                   con.execute(f"SELECT {','.join(led_cols)} FROM lt l "
                               "JOIN dl ON l.resource_id = dl.rid").fetchall()]
    log(f"  ledger rows for cohort: {len(ledger_rows):,}")

    # Decision-A input set (exact pipeline function), then intersect cohort, minus marked
    input_set = M.compute_input_set(sink_counts, ledger_rows)
    staged = (input_set & cohort) - marked
    log(f"input_set∩cohort: {len(input_set & cohort):,}  marked(excluded): "
        f"{len(marked & (input_set & cohort)):,}  staged: {len(staged):,}")

    # run the REAL select_chunks per staged resource → exact selected tokens
    res_to_sink = {}
    for name, by_res in chunks_by_sink.items():
        for rid in by_res:
            res_to_sink[rid] = name  # disjoint across sinks
    tot_sel_tokens = tot_sel_chunks = tot_elig_tokens = tot_total_tokens = 0
    n_trunc = n_over = 0
    per_sink_tok = {"scope": 0, "pricing": 0, "unknown": 0}
    per_sink_docs = {"scope": 0, "pricing": 0, "unknown": 0}
    doc_tokens: list[int] = []
    for rid in staged:
        name = res_to_sink.get(rid)
        if name is None:
            continue
        rows = chunks_by_sink[name].get(rid, [])
        if not rows:
            continue
        sel = M.select_chunks(rows)
        tot_sel_tokens += sel["est_tokens_selected"]
        tot_sel_chunks += sel["n_chunks_selected"]
        tot_elig_tokens += sel["est_tokens_eligible"]
        tot_total_tokens += sel["est_tokens_total"]
        n_trunc += int(sel["coverage_truncated"])
        n_over += int(sel["over_budget"])
        per_sink_tok[name] += sel["est_tokens_selected"]
        per_sink_docs[name] += 1
        doc_tokens.append(sel["est_tokens_selected"])

    n_staged = len([d for d in doc_tokens])
    fixed_overhead_tok_per_doc = (3610 + 5459 + 3073 + 3 - 1) // 4  # prompt+vocab+schema, chars/4
    overhead_total = fixed_overhead_tok_per_doc * n_staged
    # selected-chunk JSON wrapper ~ {"chunk_id","text"} ≈ 12 tok/chunk
    scaffold_total = tot_sel_chunks * 12 + n_staged * 60

    out = {
        "as_of": dt.datetime.now(dt.timezone.utc).isoformat(),
        "doc_token_budget": M.DOC_TOKEN_BUDGET,
        "chars_per_token": M.CHARS_PER_TOKEN,
        "cohort_downloaded_files": len(cohort),
        "marked_excluded_in_cohort": len(marked & (input_set & cohort)),
        "input_set_in_cohort": len(input_set & cohort),
        "staged_docs": n_staged,
        "staged_by_sink": per_sink_docs,
        "selected": {
            "total_selected_chunks": tot_sel_chunks,
            "total_selected_tokens": tot_sel_tokens,
            "total_eligible_tokens": tot_elig_tokens,
            "total_inventory_tokens": tot_total_tokens,
            "selected_tokens_by_sink": per_sink_tok,
            "docs_coverage_truncated": n_trunc,
            "docs_over_budget": n_over,
            "avg_selected_tokens_per_doc": round(tot_sel_tokens / max(1, n_staged), 1),
            "max_selected_tokens_per_doc": max(doc_tokens) if doc_tokens else 0,
        },
        "input_token_model": {
            "chunk_selected_tokens": tot_sel_tokens,
            "fixed_overhead_tok_per_doc": fixed_overhead_tok_per_doc,
            "fixed_overhead_total": overhead_total,
            "scaffold_total": scaffold_total,
            "TOTAL_INPUT_TOKENS": tot_sel_tokens + overhead_total + scaffold_total,
        },
    }
    json.dump(out, sys.stdout, indent=2, default=str)
    print("\nDONE", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
