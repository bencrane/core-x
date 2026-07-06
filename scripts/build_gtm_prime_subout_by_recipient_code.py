#!/usr/bin/env python3
"""gtm_prime_subout_by_recipient_code — the two-sided sub-out history cube.

SoR  s3://data-sink/active/gtm_prime_subout_by_recipient_code/
     (Lance; derived, snapshot-overwrite; BTREE prime_awardee_uei / recipient_code /
      context_code)

WHAT THIS IS
Pre-aggregated history answering BOTH directions as one indexed lookup:
  prime → "under what work does this prime sub out, to firms of what code profile?"
  code  → "which primes sub out into codes like the target's?" — THE demo direction:
          probe with the target's code list (any lens, inferred included; lens choice is
          the CALLER's parameter) and get the primes whose sub-out history hits it.

Raw history only — counts, sums, dates. No weights, no propensity score, no threshold
(read-time recipes own those, per the pre-weighting doctrine).

GRAIN  1 row / (prime_awardee_uei, context_code_type, context_code,
                recipient_code_source, recipient_code_type, recipient_code)
  context_code           the PRIME AWARD's code on the edges (what the work was)
  recipient_code_*       what the receiving firms demonstrably were/declared —
                         code_source values from gtm_subaward_recipient_code_evidence
                         (recipients' INFERRED codes are deliberately absent there:
                         past dollar flows are characterized by demonstration, never
                         by inference — inference belongs on the PROBE side)
MEASURES
  subaward_edge_ct          edges behind the cell
  subaward_amt_total        Σ subaward_amount over those edges
  distinct_recipient_ct     distinct subawardee UEIs behind the cell
  last_subaward_action_date recency

SOURCE  gtm_subaward_recipient_code_evidence (92.3M rows) — pure GROUP BY, context codes
taken from the edge's prime_award naics + psc (each contributes its own context rows).

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_prime_subout_by_recipient_code.py [--verify]
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

import duckdb
import lance

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipelines._shared.lance_local_publish import write_indexed_dataset  # noqa: E402

A = "s3://data-sink/active"
EVIDENCE_URI = f"{A}/gtm_subaward_recipient_code_evidence/"
OUT = f"{A}/gtm_prime_subout_by_recipient_code/"
PARAM_SET_ID = "v1"
BTREE_COLS = ["prime_awardee_uei", "recipient_code", "context_code"]


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def build() -> int:
    opt = so()
    as_of = date.today().isoformat()
    con = duckdb.connect()
    con.execute("SET memory_limit='24GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")

    ev = lance.dataset(EVIDENCE_URI, storage_options=opt)
    con.register("_e", ev.scanner(columns=[
        "prime_awardee_uei", "subawardee_uei", "subaward_unique_key", "subaward_amount",
        "subaward_action_date", "prime_award_naics_code",
        "prime_award_product_or_service_code", "code_source", "code_type", "code",
    ], filter="prime_awardee_uei IS NOT NULL").to_reader())
    con.execute("CREATE TABLE ev AS SELECT * FROM _e")
    con.unregister("_e")
    print(f"evidence loaded: {con.execute('SELECT COUNT(*) FROM ev').fetchone()[0]:,}",
          flush=True)

    # Each edge contributes one context row per context code it carries (naics and psc).
    con.execute("""
    CREATE TABLE cube AS
    WITH ctx AS (
        SELECT prime_awardee_uei, subawardee_uei, subaward_unique_key, subaward_amount,
               subaward_action_date, code_source AS recipient_code_source,
               code_type AS recipient_code_type, code AS recipient_code,
               'naics' AS context_code_type, prime_award_naics_code AS context_code
        FROM ev WHERE prime_award_naics_code IS NOT NULL
        UNION ALL
        SELECT prime_awardee_uei, subawardee_uei, subaward_unique_key, subaward_amount,
               subaward_action_date, code_source, code_type, code,
               'psc', prime_award_product_or_service_code
        FROM ev WHERE prime_award_product_or_service_code IS NOT NULL
    )
    SELECT prime_awardee_uei, context_code_type, context_code,
           recipient_code_source, recipient_code_type, recipient_code,
           COUNT(DISTINCT subaward_unique_key) AS subaward_edge_ct,
           SUM(subaward_amount)                AS subaward_amt_total,
           COUNT(DISTINCT subawardee_uei)      AS distinct_recipient_ct,
           MAX(subaward_action_date)           AS last_subaward_action_date
    FROM ctx
    GROUP BY 1, 2, 3, 4, 5, 6
    """)
    con.execute("DROP TABLE ev")

    n = con.execute("SELECT COUNT(*) FROM cube").fetchone()[0]
    by = con.execute("SELECT recipient_code_source, COUNT(*), SUM(subaward_amt_total) "
                     "FROM cube GROUP BY 1 ORDER BY 2 DESC").fetchall()
    print(f"cube rows: {n:,}", flush=True)
    for s, c, amt in by:
        print(f"  {s}: {c:,} cells  ${(amt or 0)/1e9:,.1f}B", flush=True)
    dup = con.execute("""SELECT COUNT(*) FROM (
        SELECT prime_awardee_uei, context_code_type, context_code,
               recipient_code_source, recipient_code_type, recipient_code
        FROM cube GROUP BY 1,2,3,4,5,6 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup == 0, f"grain not unique: {dup} dups"

    built_from = f"gtm_subaward_recipient_code_evidence:v{ev.version}"
    res = con.execute(f"""
        SELECT *, DATE '{as_of}' AS as_of, '{built_from}' AS built_from_version,
               '{PARAM_SET_ID}' AS param_set_id FROM cube""")
    reader = res.to_arrow_reader(65536) if hasattr(res, "to_arrow_reader") else res.fetch_record_batch(65536)
    ds = write_indexed_dataset(reader, OUT, [(c, "BTREE") for c in BTREE_COLS],
                               storage_options=opt)
    print(f"wrote {OUT}  v{ds.version}  rows={ds.count_rows():,}  "
          f"indices={[i['name'] for i in ds.list_indices()]}", flush=True)
    return 0


def verify() -> int:
    opt = so()
    ds = lance.dataset(OUT, storage_options=opt)
    rows = ds.count_rows()
    idx = [i["name"] for i in ds.list_indices()]
    demonstrated = ds.count_rows(
        filter="recipient_code_source = 'awarded_prime_contracts_in_code'")
    ok = rows > 1_000_000 and demonstrated > 0 and all(
        any(c in n for n in idx) for c in BTREE_COLS)
    print(f"{OUT}: rows={rows:,} demonstrated_cells={demonstrated:,} indices={idx} "
          f"-> {'OK' if ok else 'FAIL'}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
