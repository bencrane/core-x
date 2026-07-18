#!/usr/bin/env python3
"""Read-only reconnaissance of the CA + CO UCC Lance datasets (Gen-3 SoR on R2).

Maps the secured-party (lender) surface for a GTM evaluation targeting alternative
lenders / equipment financiers. For every CA and CO UCC Lance dataset under
``s3://data-sink/active/`` it emits, per dataset:

  * typed schema (column name + Arrow type)
  * row count (Lance manifest metadata — O(1), no scan)
  * committed scalar indices (BTREE / BITMAP)
  * provenance / LLM-artifact column classification (hard-lineage vs inferred)
  * frequency GROUP BYs on every low-cardinality classification column present
    (secured_party_type, filing_type, action_type, alt_designation_type,
     debtor_type, transaction_type, document_type, record_status, organization_type …)
  * ingest vintage (DISTINCT as_of / snapshot_date) and, where a filing-date
    column exists, the min/max coverage window (best-effort, timeout-bounded)

Strictly NON-MUTATING: only ``lance.dataset(...).scanner()/count_rows()/list_indices()``
+ DuckDB ``SELECT``. Zero writes — no datasets, no indexes, no ops.* rows, no R2 puts.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/ucc_ca_co_recon_probe.py > /tmp/ucc_recon.json

Required env (Doppler core-x/prd): R2_ENDPOINT (or R2_ACCOUNT_ID),
R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY.
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import re
import sys
import time

import duckdb
import lance
import pyarrow as pa

BUCKET = "data-sink"
GROUP_TIMEOUT_S = 90
TOP_N = 60  # cap on returned groups per column (full distinct count always reported)

# ── Dataset registry — every CA + CO UCC Lance dataset, with the columns worth a
#    frequency profile and the column that carries ingest/event vintage. ──────────
# label, uri, state, role, classify[], vintage_event_col(optional)
DATASETS = [
    # ── California — ca_ucc/ namespace (5 datasets) ──
    ("ca_ucc/filings", "active/ca_ucc/filings/", "CA", "filing event (master)",
     ["filing_type", "action_type", "alt_designation_type"], "filing_date"),
    ("ca_ucc/debtors", "active/ca_ucc/debtors/", "CA", "debtor (N/filing)",
     ["debtor_type", "state", "country"], None),
    ("ca_ucc/secured_parties", "active/ca_ucc/secured_parties/", "CA", "SECURED PARTY / LENDER (N/filing)",
     ["secured_party_type", "state", "country"], None),
    ("ca_ucc/filing_amendments", "active/ca_ucc/filing_amendments/", "CA", "UCC3→UCC1 amendment bridge",
     ["action_type"], None),
    ("ca_ucc/debtor_index", "active/ca_ucc/debtor_index/", "CA", "derived debtor firmographic view",
     ["debtor_type", "debtor_state", "action_type", "filing_type", "alt_designation_type"], "filing_date"),
    # ── Colorado — flat active/ namespace (1 ledger + 3 companions) ──
    ("co_ucc_transactions", "active/co_ucc_transactions/", "CO", "filing transaction ledger (no party data)",
     ["transaction_type", "filing_type", "document_type", "financial_statement_type",
      "continuation", "termination_flag", "manufactured_home_transactions"], "filing_date"),
    ("ucc_co_debtors", "active/ucc_co_debtors/", "CO", "debtor companion",
     ["action_type", "record_status", "state", "organization_type", "country", "party_role"], None),
    ("ucc_co_secured_parties", "active/ucc_co_secured_parties/", "CO", "SECURED PARTY / LENDER companion",
     ["action_type", "record_status", "state", "country", "party_role"], None),
    ("ucc_co_collateral", "active/ucc_co_collateral/", "CO", "collateral companion",
     ["action_type", "record_status", "farm_product_flag", "county"], None),
]

# Column-name signal classes for the provenance / classification audit.
LLM_RE = re.compile(r"(_llm|llm_|_confidence|confidence|_extracted|extract|_inferred|"
                    r"_score|_prob|probability|_model|_reason|_rationale|_embedding|vector)", re.I)
LENDER_CLASS_RE = re.compile(r"(lender|is_bank|bank_flag|naics|\bsic\b|category|categor|classif|"
                             r"segment|fico|institution_type|party_class|lender_type)", re.I)
HARD_PROV = {"source_file", "as_of", "ingested_at", "snapshot_date", "source_key"}


def storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def is_temporal(t) -> bool:
    return pa.types.is_timestamp(t) or pa.types.is_date(t) or pa.types.is_time(t)


def is_nested(t) -> bool:
    return (pa.types.is_list(t) or pa.types.is_large_list(t)
            or pa.types.is_fixed_size_list(t) or pa.types.is_struct(t)
            or pa.types.is_map(t))


def classify_columns(fields) -> dict:
    """Bucket every column by provenance signal: hard-lineage vs LLM/inferred vs
    lender-classification vs nested. This is the heart of the provenance audit —
    it proves whether lender categorization is a stored attribute at all."""
    out = {"hard_provenance": [], "llm_or_inferred": [], "lender_classification": [],
           "nested": []}
    for n, t in fields:
        if n in HARD_PROV:
            out["hard_provenance"].append(n)
        if LLM_RE.search(n):
            out["llm_or_inferred"].append(n)
        if LENDER_CLASS_RE.search(n):
            out["lender_classification"].append(n)
        if is_nested(t):
            out["nested"].append({"name": n, "type": str(t)})
    return out


def _con():
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'; SET temp_directory='/tmp/duck_spill_ucc';")
    con.execute("PRAGMA threads=8;")
    return con


def profile_dataset(label, uri, classify_cols, vintage_col, so) -> dict:
    """Single column-group scan per dataset: materialize the present classification
    + vintage columns into a DuckDB temp table, then run all GROUP BYs / min-max
    against it. Bounded by GROUP_TIMEOUT_S in a worker thread."""
    full = f"s3://{BUCKET}/{uri}"
    ds = lance.dataset(full, storage_options=so)
    fields = [(f.name, f.type) for f in ds.schema]
    names = {n for n, _ in fields}

    rec = {
        "label": label, "uri": full,
        "rows": ds.count_rows(),
        "n_cols": len(fields),
        "schema": [{"name": n, "type": str(t)} for n, t in fields],
        "provenance_audit": classify_columns(fields),
        "indices": [],
        "frequencies": {},
        "vintage": {},
    }

    # committed scalar indices
    try:
        for i in ds.list_indices():
            d = dict(i) if not isinstance(i, dict) else i
            rec["indices"].append({
                "name": d.get("name"),
                "type": d.get("type") or d.get("index_type"),
                "fields": d.get("fields") or d.get("columns"),
            })
    except Exception as e:  # noqa: BLE001
        rec["indices_error"] = str(e)[:160]

    present_class = [c for c in classify_cols if c in names]
    has_vintage = bool(vintage_col and vintage_col in names)
    prov_dates = [c for c in ("as_of", "snapshot_date") if c in names]
    proj = sorted(set(present_class + prov_dates + ([vintage_col] if has_vintage else [])))
    if not proj:
        return rec

    def _run():
        reader = ds.scanner(columns=proj).to_reader()
        con = _con()
        try:
            con.register("r", reader)
            con.execute("CREATE TEMP TABLE t AS SELECT * FROM r")
            for col in present_class:
                rows = con.execute(
                    f'SELECT COALESCE(CAST("{col}" AS VARCHAR), \'∅ NULL\') AS v, '
                    f'count(*) AS n FROM t GROUP BY 1 ORDER BY n DESC'
                ).fetchall()
                ndist = con.execute(
                    f'SELECT count(DISTINCT "{col}") FROM t'
                ).fetchone()[0]
                rec["frequencies"][col] = {
                    "distinct": int(ndist),
                    "groups": [{"value": v, "rows": int(n)} for v, n in rows[:TOP_N]],
                    "truncated": len(rows) > TOP_N,
                }
            for pc in prov_dates:
                vals = con.execute(
                    f'SELECT CAST("{pc}" AS VARCHAR) AS v, count(*) n FROM t '
                    f'GROUP BY 1 ORDER BY n DESC LIMIT 12'
                ).fetchall()
                rec["vintage"][pc] = [{"value": v, "rows": int(n)} for v, n in vals]
            if has_vintage:
                lo, hi = con.execute(
                    f'SELECT CAST(min("{vintage_col}") AS VARCHAR), '
                    f'CAST(max("{vintage_col}") AS VARCHAR) FROM t'
                ).fetchone()
                rec["vintage"][f"{vintage_col}_min"] = lo
                rec["vintage"][f"{vintage_col}_max"] = hi
        finally:
            con.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_run)
        try:
            fut.result(timeout=GROUP_TIMEOUT_S)
        except concurrent.futures.TimeoutError:
            rec["frequencies_error"] = f"timeout>{GROUP_TIMEOUT_S}s"
        except Exception as e:  # noqa: BLE001
            rec["frequencies_error"] = f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
    return rec


def main() -> int:
    so = storage_options()
    results = []
    for label, uri, state, role, classify_cols, vintage_col in DATASETS:
        t0 = time.time()
        try:
            rec = profile_dataset(label, uri, classify_cols, vintage_col, so)
            rec["state"], rec["role"] = state, role
        except Exception as e:  # noqa: BLE001
            rec = {"label": label, "uri": f"s3://{BUCKET}/{uri}", "state": state,
                   "role": role, "error": f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"}
        results.append(rec)
        print(f"[{state}] {label:<26} rows={rec.get('rows')!s:>10} "
              f"cols={rec.get('n_cols')!s:>3} idx={len(rec.get('indices', []))} "
              f"({round(time.time() - t0, 1)}s)", file=sys.stderr, flush=True)

    ca = sum(r.get("rows") or 0 for r in results if r.get("state") == "CA")
    co = sum(r.get("rows") or 0 for r in results if r.get("state") == "CO")
    summary = {"ca_total_rows": ca, "co_total_rows": co, "grand_total_rows": ca + co,
               "n_datasets": len(results)}
    print(f"[TOTAL] CA={ca:,}  CO={co:,}  ALL={ca + co:,}", file=sys.stderr, flush=True)
    json.dump({"summary": summary, "datasets": results}, sys.stdout, indent=2, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
