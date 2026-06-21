#!/usr/bin/env python3
"""Pre-flight for the Big-Three v2 free-form labor re-run. Generates the cohort
allow-list (chunk-bearing cohort resources, sink-present) and reports their current
ledger llm_state so the operator knows whether reset-llm is required. Writes ONLY
the allow-list file (/tmp); read-only against R2.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/bigthree_reextract_preflight.py
"""
from __future__ import annotations

import json
import os
import sys

import duckdb
import lance

A = "s3://data-sink/active"
GAA = f"{A}/govcon_active_awards/"
MAN = f"{A}/sam_opps_attachment_manifest_winners/"
FILES = f"{A}/sam_attachment_files/"
SINKS = {"scope": f"{A}/govcon_scope_vectors/", "pricing": f"{A}/govcon_pricing/",
         "unknown": f"{A}/govcon_unknown/"}
LEDGER = f"{A}/govcon_requirements_extract_ledger/"
ALLOWLIST = os.environ.get("BIGTHREE_ALLOWLIST", "/tmp/bigthree_cohort.ids")


def r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def main() -> int:
    so = r2_so()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=8")
    con.register("gaa", lance.dataset(GAA, storage_options=so))
    con.register("man", lance.dataset(MAN, storage_options=so))
    con.register("fl", lance.dataset(FILES, storage_options=so))

    # cohort downloaded resources
    con.execute("""
        CREATE TEMP TABLE dl AS
        SELECT DISTINCT f.resource_id AS rid
        FROM fl f
        JOIN (SELECT DISTINCT m.resource_id FROM man m
              JOIN (SELECT contract_award_unique_key AS k FROM gaa
                    WHERE upper(trim(psc_code)) IN ('DA01','R425','R499')
                      AND contract_award_unique_key IS NOT NULL) bt
                ON m.contract_award_unique_key = bt.k
              WHERE m.resource_id IS NOT NULL) c
          ON f.resource_id = c.resource_id
        WHERE f.status='downloaded'
    """)

    # chunk-bearing cohort = cohort ∩ (any sink)
    cohort_chunk = set()
    for name, uri in SINKS.items():
        ds = lance.dataset(uri, storage_options=so)
        con.register("sink_src", ds.scanner(columns=["resource_id"]).to_reader())
        con.execute("CREATE TEMP TABLE st AS SELECT DISTINCT resource_id FROM sink_src")
        con.unregister("sink_src")
        for (rid,) in con.execute(
                "SELECT s.resource_id FROM st s JOIN dl ON s.resource_id=dl.rid").fetchall():
            cohort_chunk.add(rid)
        con.execute("DROP TABLE st")
    ids = sorted(cohort_chunk)

    # ledger llm_state for the cohort
    led = lance.dataset(LEDGER, storage_options=so)
    led_cols = led.schema.names
    state_col = "llm_state" if "llm_state" in led_cols else None
    con.register("led_src", led.scanner(
        columns=[c for c in ("resource_id", "llm_state") if c in led_cols]).to_reader())
    con.execute("CREATE TEMP TABLE lt AS SELECT * FROM led_src")
    con.unregister("led_src")
    con.execute("CREATE TEMP TABLE allow AS SELECT unnest(?::VARCHAR[]) AS rid", [ids])
    if state_col:
        dist = con.execute("""
            SELECT coalesce(l.llm_state,'<no_ledger_row>') AS llm_state, count(*) n
            FROM allow a LEFT JOIN lt l ON a.rid = l.resource_id
            GROUP BY 1 ORDER BY n DESC""").fetchall()
    else:
        dist = [("<no_llm_state_col>", len(ids))]

    with open(ALLOWLIST, "w") as f:
        f.write("\n".join(ids) + "\n")

    out = {
        "cohort_chunk_bearing": len(ids),
        "allowlist_file": ALLOWLIST,
        "ledger_llm_state_distribution": [{"llm_state": s, "n": n} for s, n in dist],
        "need_reset": sum(n for s, n in dist if s not in ("pending",)),
    }
    json.dump(out, sys.stdout, indent=2)
    print("\nDONE", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
