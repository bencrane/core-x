#!/usr/bin/env python3
"""PE thesis query — top-compensated physicians by specialty × state (live Lance join).

Executes the core Private-Equity targeting thesis over the Gen-3 R2 system of record:
rank the highest-compensated individual physicians in a given specialty (NUCC taxonomy
code) and state by total CMS Open Payments received (general + research), decorated with
NPPES provider identity. **Read-only. No dataset is materialized.**

Every architectural choice traces to `docs/cms_nppes_relational_diagnostic.md`:

  • Finding A.1 — `covered_recipient_npi` is 96.39% NULL in research; the resolution key
    is `coalesce(covered_recipient_npi, principal_investigator_1_npi)`. The research
    aggregate keys on that coalesce and the research scanner prefilters on BOTH NPI
    columns, or 96% of research payments would silently vanish.

  • Finding D.2 — a LIVE join over the raw Lance tables; NO flattened/materialized view.
    The three datasets are opened fresh and joined in one DuckDB statement.

  • Finding D.3 — the selective predicate is specialty+state on NPPES, not NPI on CMS, so
    the join is DRIVEN FROM THE PROVIDER SIDE: the provider scanner prefilters on the
    BITMAP axes → a bounded candidate-NPI set. CMS NPI is BTREE-indexed but NOT clustered
    (no fragment pruning), so payments are resolved by pushing the candidate set into the
    CMS scanners as a row-level BTREE `IN`-list (surgical) when bounded; for pathologically
    large cells the module falls back to a 2-column streamed scan + hash semijoin (the
    §6.2 scan floor). Both are live joins; neither materializes a table.

  • Finding B.2 — every filter axis used here is scalar-indexed (BITMAP on the provider
    categoricals `practice_state` / `primary_taxonomy_code` / `entity_type_code`; BTREE on
    every NPI), so the prefilters fire `ScalarIndexQuery` pushdown rather than full scans.

Run (ad-hoc):
  doppler run -p core-x -c prd -- uv run --no-project \
    --with 'duckdb>=1.5,<2' --with 'pylance>=7' --with pyarrow \
    python3 -m apps.gtm_mcp.pe_thesis_query --state TX --taxonomy 207X00000X --min-year 2020

Import:
  from apps.gtm_mcp.pe_thesis_query import top_compensated_providers
  out = top_compensated_providers("TX", "207X00000X", min_year=2020)
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from typing import Any

BUCKET = "data-sink"
DEFAULT_SNAPSHOT = "2026-05"

# Above this candidate-NPI count, skip the IN-list pushdown and stream the two CMS
# payment columns with a hash semijoin (finding D.3 — CMS NPI is row-level BTREE, not
# clustered; a giant IN-list is slower to plan/transport than a bounded 2-column scan).
PUSHDOWN_NPI_THRESHOLD = 10_000

_STATE_RE = re.compile(r"^[A-Z]{2}$")          # clean_state() output is 2-letter USPS
_TAXONOMY_RE = re.compile(r"^[0-9A-Z]{10}$")   # NUCC taxonomy codes are 10 chars


# ── R2 credentials (house convention, mirrored from pipelines/* _r2_storage_options) ──
def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError(
            "Set R2_ENDPOINT or R2_ACCOUNT_ID — credentials come from Doppler core-x/prd."
        )
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _lit(s: str) -> str:
    """Single-quoted SQL string literal with internal quotes doubled (defense in depth;
    inputs are already charset-gated by the regexes above)."""
    return "'" + s.replace("'", "''") + "'"


def _uris(snapshot: str) -> dict[str, str]:
    return {
        "general":  f"s3://{BUCKET}/active/cms_general_payments/",
        "research": f"s3://{BUCKET}/active/cms_research_payments/",
        "provider": f"s3://{BUCKET}/active/nppes_provider/snapshot={snapshot}/",
    }


# The ranking statement. `gen` / `res` / `prov` are bound at runtime (Arrow tables on the
# pushdown path; the live LanceDataset on the streamed-fallback path — both re-scannable).
# {year_pred} is `payment_year >= N` or `TRUE`; {limit} is the leaderboard depth.
_RANKING_SQL = """
WITH gen_sum AS (
    SELECT covered_recipient_npi AS npi,
           SUM(total_amount_of_payment_usdollars) AS general_total,
           COUNT(*)                               AS general_txns
    FROM gen
    WHERE covered_recipient_npi IS NOT NULL
      AND covered_recipient_npi IN (SELECT npi FROM prov)
      AND ({year_pred})
    GROUP BY 1
),
res_sum AS (
    -- FINDING A.1: covered_recipient_npi is 96.39% NULL in research; the resolution key
    -- is coalesce(covered_recipient_npi, principal_investigator_1_npi).
    SELECT coalesce(covered_recipient_npi, principal_investigator_1_npi) AS npi,
           SUM(total_amount_of_payment_usdollars) AS research_total,
           COUNT(*)                               AS research_txns
    FROM res
    WHERE coalesce(covered_recipient_npi, principal_investigator_1_npi) IS NOT NULL
      AND coalesce(covered_recipient_npi, principal_investigator_1_npi) IN (SELECT npi FROM prov)
      AND ({year_pred})
    GROUP BY 1
),
pay AS (
    SELECT coalesce(g.npi, r.npi)        AS npi,
           coalesce(g.general_total, 0)  AS general_total,
           coalesce(r.research_total, 0) AS research_total,
           coalesce(g.general_txns, 0)   AS general_txns,
           coalesce(r.research_txns, 0)  AS research_txns
    FROM gen_sum g FULL OUTER JOIN res_sum r USING (npi)
)
SELECT
    ROW_NUMBER() OVER (ORDER BY (pay.general_total + pay.research_total) DESC, p.npi) AS rank,
    p.npi,
    p.provider_name,
    p.last_name,
    p.first_name,
    p.credential,
    p.primary_taxonomy_code,
    p.practice_city,
    p.practice_state,
    p.practice_zip5,
    (pay.general_total + pay.research_total)::DECIMAL(18,2) AS total_compensation_usd,
    pay.general_total::DECIMAL(18,2)                        AS general_payments_usd,
    pay.research_total::DECIMAL(18,2)                       AS research_payments_usd,
    pay.general_txns                                        AS general_transactions,
    pay.research_txns                                       AS research_transactions,
    (pay.general_txns + pay.research_txns)                  AS total_transactions
FROM pay
JOIN prov p ON p.npi = pay.npi
ORDER BY total_compensation_usd DESC, p.npi
LIMIT {limit}
"""


def top_compensated_providers(
    state: str,
    taxonomy_code: str,
    *,
    snapshot: str = DEFAULT_SNAPSHOT,
    min_year: int | None = None,
    limit: int = 50,
    active_only: bool = True,
    memory_limit: str = "8GB",
    threads: int = 8,
) -> dict[str, Any]:
    """Rank the top `limit` highest-compensated individual physicians in `taxonomy_code`
    practising in `state`, by total CMS Open Payments (general + research), joined live to
    `nppes_provider`. Returns a dict with the parameters, the resolution path taken, and a
    `results` list of per-provider records. Read-only; mutates nothing.

    Args:
        state: 2-letter USPS code (case-insensitive); matches the clean_state-normalized
            `practice_state`.
        taxonomy_code: 10-char NUCC taxonomy code (case-insensitive); matches the
            provider's `primary_taxonomy_code`.
        snapshot: NPPES snapshot partition (default the current `2026-05`).
        min_year: optional inclusive floor on `payment_year` (e.g. 2020 for recent comp).
        limit: leaderboard depth (default 50).
        active_only: restrict to `is_active = true` providers (default True).
    """
    import duckdb  # local import: keeps the module importable without the query deps present
    import lance

    state_u = state.strip().upper()
    tax_u = taxonomy_code.strip().upper()
    if not _STATE_RE.match(state_u):
        raise ValueError(f"state must be a 2-letter USPS code, got {state!r}")
    if not _TAXONOMY_RE.match(tax_u):
        raise ValueError(f"taxonomy_code must be a 10-char NUCC code, got {taxonomy_code!r}")
    if min_year is not None and not (2013 <= int(min_year) <= 2100):
        raise ValueError(f"min_year out of range (Open Payments starts 2013): {min_year!r}")
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit!r}")

    so = _r2_storage_options()
    uris = _uris(snapshot)
    year_pred = f"payment_year >= {int(min_year)}" if min_year is not None else "TRUE"
    t0 = time.time()

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={int(threads)}")
    con.execute(f"SET memory_limit='{memory_limit}'")
    os.makedirs("/tmp/pe_thesis_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/pe_thesis_spill'")
    con.execute("SET preserve_insertion_order=false")

    try:
        # ── candidate providers — selective predicate pushed into the BITMAP-indexed
        #    provider scanner (D.3 drive-from-selective-side; B.2 indexed axes) ─────────
        prov_filter = (
            f"practice_state = {_lit(state_u)} "
            f"AND primary_taxonomy_code = {_lit(tax_u)} "
            f"AND entity_type_code = '1'"  # individuals (physicians), never organizations
        )
        if active_only:
            prov_filter += " AND is_active = true"
        prov_tbl = (
            lance.dataset(uris["provider"], storage_options=so)
            .scanner(
                columns=["npi", "provider_name", "last_name", "first_name", "credential",
                         "primary_taxonomy_code", "practice_city", "practice_state",
                         "practice_zip5"],
                filter=prov_filter,
                prefilter=True,
            )
            .to_table()
        )
        con.register("prov", prov_tbl)
        candidate_npis = prov_tbl.column("npi").to_pylist()
        n_cand = len(candidate_npis)

        if n_cand == 0:
            return {
                "state": state_u, "taxonomy_code": tax_u, "snapshot": snapshot,
                "min_year": min_year, "active_only": active_only,
                "candidate_providers": 0, "resolution_path": "none", "results": [],
                "elapsed_sec": round(time.time() - t0, 2),
                "note": "no matching individual providers for this specialty/state",
            }

        gen_ds = lance.dataset(uris["general"], storage_options=so)
        res_ds = lance.dataset(uris["research"], storage_options=so)

        # ── CMS payments — surgical IN-list BTREE pushdown when bounded, else streamed
        #    2-column scan + hash semijoin (D.3) ──────────────────────────────────────
        if n_cand <= PUSHDOWN_NPI_THRESHOLD:
            resolution_path = "btree_in_list_pushdown"
            in_list = "(" + ",".join(_lit(n) for n in candidate_npis) + ")"
            year_clause = f" AND payment_year >= {int(min_year)}" if min_year is not None else ""
            gen_cols = ["covered_recipient_npi", "total_amount_of_payment_usdollars"]
            res_cols = ["covered_recipient_npi", "principal_investigator_1_npi",
                        "total_amount_of_payment_usdollars"]
            if min_year is not None:
                gen_cols.append("payment_year")
                res_cols.append("payment_year")
            con.register(
                "gen",
                gen_ds.scanner(
                    columns=gen_cols,
                    filter=f"covered_recipient_npi IN {in_list}{year_clause}",
                    prefilter=True,
                ).to_table(),
            )
            con.register(
                "res",
                res_ds.scanner(
                    columns=res_cols,
                    # A.1: prefilter BOTH NPI columns or coalesce-keyed rows are missed.
                    filter=(f"(covered_recipient_npi IN {in_list} "
                            f"OR principal_investigator_1_npi IN {in_list}){year_clause}"),
                    prefilter=True,
                ).to_table(),
            )
        else:
            resolution_path = "streamed_scan_semijoin"
            # Register the LanceDataset itself (NOT a one-shot RecordBatchReader, which
            # DuckDB exhausts after the first scan → silent empty on the FULL OUTER reuse).
            # DuckDB re-scans lazily and pushes the 2-column projection into Lance; the
            # `IN (SELECT npi FROM prov)` semijoin in _RANKING_SQL bounds the aggregate.
            con.register("gen", gen_ds)
            con.register("res", res_ds)

        sql = _RANKING_SQL.format(year_pred=year_pred, limit=int(limit))
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    finally:
        con.close()

    money_cols = {"total_compensation_usd", "general_payments_usd", "research_payments_usd"}
    results: list[dict[str, Any]] = []
    for row in rows:
        rec = dict(zip(cols, row))
        for k in money_cols:
            rec[k] = round(float(rec[k]), 2) if rec[k] is not None else 0.0
        results.append(rec)

    return {
        "state": state_u,
        "taxonomy_code": tax_u,
        "snapshot": snapshot,
        "min_year": min_year,
        "active_only": active_only,
        "candidate_providers": n_cand,
        "resolution_path": resolution_path,
        "result_count": len(results),
        "results": results,
        "elapsed_sec": round(time.time() - t0, 2),
    }


# ── CLI ──────────────────────────────────────────────────────────────────────────────
def _print_table(out: dict[str, Any]) -> None:
    hdr = (f"specialty {out['taxonomy_code']} × {out['state']} "
           f"| snapshot {out['snapshot']} | min_year {out['min_year']} "
           f"| candidates {out['candidate_providers']} | path {out['resolution_path']} "
           f"| {out['elapsed_sec']}s")
    print(hdr)
    print("-" * len(hdr))
    if not out["results"]:
        print(out.get("note", "(no results)"))
        return
    print(f"{'#':>3}  {'NPI':<10}  {'total_$':>14}  {'gen_$':>14}  {'res_$':>13}  "
          f"{'txns':>5}  provider")
    for r in out["results"]:
        name = r["provider_name"] or f"{r['last_name']}, {r['first_name']}"
        cred = f" ({r['credential']})" if r.get("credential") else ""
        print(f"{r['rank']:>3}  {r['npi']:<10}  {r['total_compensation_usd']:>14,.2f}  "
              f"{r['general_payments_usd']:>14,.2f}  {r['research_payments_usd']:>13,.2f}  "
              f"{r['total_transactions']:>5}  {name}{cred} — {r['practice_city']}, {r['practice_state']}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--state", required=True, help="2-letter USPS state code (e.g. TX)")
    ap.add_argument("--taxonomy", required=True, help="10-char NUCC taxonomy code (e.g. 207X00000X)")
    ap.add_argument("--snapshot", default=DEFAULT_SNAPSHOT, help=f"NPPES snapshot (default {DEFAULT_SNAPSHOT})")
    ap.add_argument("--min-year", type=int, default=None, help="inclusive payment_year floor (e.g. 2020)")
    ap.add_argument("--limit", type=int, default=50, help="leaderboard depth (default 50)")
    ap.add_argument("--include-inactive", action="store_true", help="include deactivated NPIs")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = ap.parse_args(argv)

    out = top_compensated_providers(
        args.state, args.taxonomy,
        snapshot=args.snapshot, min_year=args.min_year, limit=args.limit,
        active_only=not args.include_inactive,
    )
    if args.json:
        print(json.dumps(out, indent=2, default=str))
    else:
        _print_table(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
