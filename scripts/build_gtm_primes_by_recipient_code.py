#!/usr/bin/env python3
"""gtm_primes_by_recipient_code — the code→prime marginal of the sub-out cube, serving-shaped.

SoR  s3://data-sink/active/gtm_primes_by_recipient_code/
     (Lance; derived, snapshot-overwrite; BTREE recipient_code / prime_awardee_uei)

WHAT THIS IS
The exact aggregate the subout_opportunities hot path needs at boot: for each
(recipient_code_type, recipient_code), which primes have subbed out to firms carrying
that code, with how much. Pre-aggregating here (~1.7M rows) removes an 11.8M-row
in-process aggregation from the API's cache build — boot becomes a plain columnar load
(seconds, no transient memory peak). Collapses recipient_code_source and context_code;
the full cube (gtm_prime_subout_by_recipient_code) remains the drill-down surface.

GRAIN  1 row / (recipient_code_type, recipient_code, prime_awardee_uei)
MEASURES  subaward_edge_ct (Σ), subaward_amt_total (Σ), distinct_recipient_ct (MAX over
          collapsed cells — an upper-bound proxy, NOT a distinct union across cells),
          last_subaward_action_date (MAX)

    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=8' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' --with boto3 \
      python3 scripts/build_gtm_primes_by_recipient_code.py [--verify]
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
CUBE_URI = f"{A}/gtm_prime_subout_by_recipient_code/"
OUT = f"{A}/gtm_primes_by_recipient_code/"
PARAM_SET_ID = "v1"
BTREE_COLS = ["recipient_code", "prime_awardee_uei"]


def so() -> dict:
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            "region": "auto"}


def build() -> int:
    opt = so()
    as_of = date.today().isoformat()
    con = duckdb.connect()
    con.execute("SET memory_limit='16GB'; SET threads TO 4; SET preserve_insertion_order=false;")
    os.makedirs("/tmp/duck_spill", exist_ok=True)
    con.execute("SET temp_directory='/tmp/duck_spill';")

    cube = lance.dataset(CUBE_URI, storage_options=opt)
    con.register("_c", cube.scanner(columns=[
        "recipient_code_type", "recipient_code", "prime_awardee_uei",
        "subaward_edge_ct", "subaward_amt_total", "distinct_recipient_ct",
        "last_subaward_action_date"]).to_reader())
    con.execute("""
    CREATE TABLE marginal AS
    SELECT recipient_code_type, recipient_code, prime_awardee_uei,
           SUM(subaward_edge_ct)            AS subaward_edge_ct,
           SUM(subaward_amt_total)          AS subaward_amt_total,
           MAX(distinct_recipient_ct)       AS distinct_recipient_ct,
           MAX(last_subaward_action_date)   AS last_subaward_action_date
    FROM _c
    GROUP BY 1, 2, 3
    """)
    con.unregister("_c")
    n = con.execute("SELECT COUNT(*) FROM marginal").fetchone()[0]
    print(f"marginal rows: {n:,}", flush=True)
    dup = con.execute("""SELECT COUNT(*) FROM (
        SELECT recipient_code_type, recipient_code, prime_awardee_uei
        FROM marginal GROUP BY 1,2,3 HAVING COUNT(*) > 1)""").fetchone()[0]
    assert dup == 0, f"grain not unique: {dup} dups"

    built_from = f"gtm_prime_subout_by_recipient_code:v{cube.version}"
    res = con.execute(f"""
        SELECT *, DATE '{as_of}' AS as_of, '{built_from}' AS built_from_version,
               '{PARAM_SET_ID}' AS param_set_id FROM marginal""")
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
    ok = 500_000 < rows < 10_000_000 and all(any(c in n for n in idx) for c in BTREE_COLS)
    print(f"{OUT}: rows={rows:,} indices={idx} -> {'OK' if ok else 'FAIL'}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(verify() if "--verify" in sys.argv else build())
