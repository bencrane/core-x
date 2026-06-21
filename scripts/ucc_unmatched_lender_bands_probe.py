#!/usr/bin/env python3
"""Frequency-band distribution of UNMATCHED UCC lender names, CA+CO combined.

Of the secured-party (lender) organization names that fail the canonical-registry join
(sba_7a / hmda_panels / sec_adv_part1, exact _name_norm), how are they distributed by
appearance ('txn') volume — so the long tail can be cut and only the high-frequency
unmatched head sent to an LLM. Appearances are summed ACROSS CA and CO (same normalized
name → one combined row), per the operator's framing.

Emits: combined matched/unmatched totals, a per-band histogram, cumulative ≥N counts,
and the top unmatched names by combined appearances (the actionable classification head).
Strictly read-only.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/ucc_unmatched_lender_bands_probe.py > /tmp/bands.json
"""
from __future__ import annotations

import json
import os
import sys
import time

import duckdb
import lance

BUCKET = "data-sink"
NAME_NORM = r"""
CREATE OR REPLACE MACRO nn(x) AS
  nullif(trim(regexp_replace(regexp_replace(upper(CAST(x AS VARCHAR)),
         '[^A-Z0-9 ]+', '', 'g'), '\s+', ' ', 'g')), '');
"""
CUMULATIVE_THRESHOLDS = [2, 3, 5, 10, 15, 25, 50, 100, 250]


def so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def rdr(label, cols, opts, filt=None):
    ds = lance.dataset(f"s3://{BUCKET}/active/{label}/", storage_options=opts)
    return ds.scanner(columns=cols, filter=filt).to_reader()


def main() -> int:
    opts = so()
    con = duckdb.connect()
    con.execute("SET memory_limit='20GB'; SET temp_directory='/tmp/duck_spill_bd'; PRAGMA threads=8;")
    con.execute(NAME_NORM)
    t0 = time.time()

    # ── ref(nln): distinct canonical lender names (same three registries) ────────
    con.register("sba", rdr("sba_7a", ["bank_name"], opts))
    con.register("hmda", rdr("hmda_panels", ["respondent_name"], opts))
    con.register("adv", rdr("sec_adv_part1", ["legal_name", "primary_business_name"], opts))
    con.execute("""
        CREATE TEMP TABLE ref AS
        SELECT DISTINCT nln FROM (
            SELECT nn(bank_name) nln FROM sba
            UNION SELECT nn(respondent_name) FROM hmda
            UNION SELECT nn(legal_name) FROM adv
            UNION SELECT nn(primary_business_name) FROM adv
        ) WHERE nln IS NOT NULL
    """)

    # ── combined UCC org-lender universe: nln → summed appearances across CA+CO ──
    con.register("ca", rdr("ca_ucc/secured_parties", ["org_name", "secured_party_type"], opts,
                           "secured_party_type = 'Organization'"))
    con.register("co", rdr("ucc_co_secured_parties", ["organization_name"], opts))
    con.execute("""
        CREATE TEMP TABLE ucc AS
        SELECT nln, sum(app) AS app FROM (
            SELECT nn(org_name) nln, count(*) app FROM ca WHERE org_name IS NOT NULL GROUP BY 1
            UNION ALL
            SELECT nn(organization_name) nln, count(*) app FROM co WHERE organization_name IS NOT NULL GROUP BY 1
        ) WHERE nln IS NOT NULL GROUP BY 1
    """)

    # ── unmatched = not in ref ──────────────────────────────────────────────────
    con.execute("""
        CREATE TEMP TABLE un AS
        SELECT u.nln, u.app FROM ucc u
        LEFT JOIN ref r USING (nln) WHERE r.nln IS NULL
    """)

    tot_names, tot_app = con.execute("SELECT count(*), sum(app) FROM ucc").fetchone()
    m_names = con.execute("SELECT count(*) FROM ucc u JOIN ref r USING(nln)").fetchone()[0]
    un_names, un_app = con.execute("SELECT count(*), sum(app) FROM un").fetchone()
    print(f"[combined] {tot_names:,} distinct names · matched {m_names:,} · "
          f"unmatched {un_names:,} ({round(time.time()-t0,1)}s)", file=sys.stderr, flush=True)

    bands = con.execute("""
        WITH b AS (
            SELECT CASE
                WHEN app = 1 THEN '01:  =1'
                WHEN app BETWEEN 2 AND 4 THEN '02:  2-4'
                WHEN app BETWEEN 5 AND 9 THEN '03:  5-9'
                WHEN app BETWEEN 10 AND 14 THEN '04:  10-14'
                WHEN app BETWEEN 15 AND 24 THEN '05:  15-24'
                WHEN app BETWEEN 25 AND 49 THEN '06:  25-49'
                WHEN app BETWEEN 50 AND 99 THEN '07:  50-99'
                WHEN app BETWEEN 100 AND 249 THEN '08:  100-249'
                WHEN app BETWEEN 250 AND 999 THEN '09:  250-999'
                ELSE '10:  1000+' END AS band, app
            FROM un)
        SELECT band AS band, count(*) AS names, sum(app) AS appearances
        FROM b GROUP BY band ORDER BY band
    """).fetchall()

    cum = []
    for thr in CUMULATIVE_THRESHOLDS:
        n, a = con.execute("SELECT count(*), sum(app) FROM un WHERE app >= ?", [thr]).fetchone()
        cum.append({"threshold": thr, "names": int(n or 0), "appearances": int(a or 0)})

    top = con.execute("SELECT nln, app FROM un ORDER BY app DESC LIMIT 40").fetchall()

    out = {
        "combined_distinct_names": int(tot_names), "matched_names": int(m_names),
        "unmatched_names": int(un_names), "unmatched_appearances": int(un_app),
        "bands": [{"band": b, "names": int(n), "appearances": int(a)} for b, n, a in bands],
        "cumulative_at_or_above": cum,
        "top_unmatched": [{"nln": n, "appearances": int(a)} for n, a in top],
    }
    json.dump(out, sys.stdout, indent=2, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
