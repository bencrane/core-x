#!/usr/bin/env python3
"""How much of the UNMATCHED bank-looking residual is recovered by CANONICALIZATION
alone (no new registry) vs is genuinely novel (real FDIC/NCUA-ingest headroom)?

Applies a light canonicalizer to BOTH the canonical ref names and the unmatched
bank/CU-looking UCC names, then re-joins. Splits the unmatched depository residual into:
  * recovered-by-canon  → a name variant of an institution ALREADY in SBA/HMDA/ADV
                          (fix with an alias fold, NOT by ingesting FDIC/NCUA)
  * still-novel         → not in any current registry even after canon
                          (the actual FDIC/NCUA-ingest headroom)

Canon = fold NATIONAL ASSOCIATION→(drop), strip trailing role/agent/DBA/division/
successor/charter tails, drop trailing legal-form + NA/FSB/SSB tokens. Read-only.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/archive/ucc_bank_canon_recovery_probe.py > /tmp/canon.json
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
# Light canonicalizer: drop role/agent/dba/division/successor/charter tails, then peel
# trailing depository/legal-form tokens, then collapse whitespace.
CANON = r"""
CREATE OR REPLACE MACRO canon(x) AS
  nullif(trim(regexp_replace(regexp_replace(regexp_replace(
    x,
    '( AS (ADMINISTRATIVE |COLLATERAL |SUB |SUCCESSOR )?AGENT.*| AS REPRESENTATIVE.*| AS TRUSTEE.*| AS CUSTODIAN.*| AS SECURED PARTY.*| DBA .*| A DIVISION OF .*| DIVISION OF .*| SUCCESSOR( BY MERGER)?.*| FORMERLY.*| A [A-Z]+ (STATE )?BANKING CORPORATION.*| A [A-Z]+ CORPORATION.*| CO .*FINANCIAL.*)$',
    ''),
    '( NATIONAL ASSOCIATION| NATIONAL ASSN| NA| FSB| SSB| FLCA| PCA| NATIONAL TRUST AND SAVINGS ASSOCIATION| NATIONAL BANK| NB| INC| LLC| LP| CORPORATION| CORP| COMPANY| CO| THE)$',
    ''),
    '\s+', ' ', 'g')), '');
"""
BANK_PAT = (r'(^| )BANK( |$)|(^| )BANKING( |$)|BANCORP|BANCORPORATION|BANCSHARES|'
            r'(^| )BANC( |$)|(^| )BANCO( |$)|SAVINGS|(^| )FSB( |$)|(^| )SSB( |$)|'
            r'THRIFT|NATIONAL ASSOCIATION')
CU_PAT = r'CREDIT UNION|(^| )FCU( |$)'


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
    con.execute("SET memory_limit='20GB'; SET temp_directory='/tmp/duck_spill_cn'; PRAGMA threads=8;")
    con.execute(NAME_NORM)
    con.execute(CANON)
    t0 = time.time()

    con.register("sba", rdr("sba_7a", ["bank_name"], opts))
    con.register("hmda", rdr("hmda_panels", ["respondent_name"], opts))
    con.register("adv", rdr("sec_adv_part1", ["legal_name", "primary_business_name"], opts))
    con.execute("""
        CREATE TEMP TABLE refn AS SELECT DISTINCT nln FROM (
            SELECT nn(bank_name) nln FROM sba UNION SELECT nn(respondent_name) FROM hmda
            UNION SELECT nn(legal_name) FROM adv UNION SELECT nn(primary_business_name) FROM adv
        ) WHERE nln IS NOT NULL
    """)
    # canon-folded ref key set
    con.execute("CREATE TEMP TABLE refc AS SELECT DISTINCT canon(nln) AS c FROM refn WHERE canon(nln) IS NOT NULL")

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
    # Unmatched (exact) depository-looking names, with canon recovery flag.
    con.execute(f"""
        CREATE TEMP TABLE un_dep AS
        SELECT u.nln, u.app,
            CASE WHEN regexp_matches(u.nln, '{CU_PAT}') THEN 'CREDIT_UNION' ELSE 'BANK' END AS lexclass,
            (rc.c IS NOT NULL) AS recovered_by_canon
        FROM ucc u
        LEFT JOIN refn r  ON r.nln = u.nln
        LEFT JOIN refc rc ON rc.c = canon(u.nln)
        WHERE r.nln IS NULL
          AND (regexp_matches(u.nln, '{BANK_PAT}') OR regexp_matches(u.nln, '{CU_PAT}'))
    """)
    print(f"[built] ({round(time.time()-t0,1)}s)", file=sys.stderr, flush=True)

    rows = con.execute("""
        SELECT lexclass, recovered_by_canon, count(*) AS names, sum(app) AS appearances
        FROM un_dep GROUP BY 1,2 ORDER BY 1,2
    """).fetchall()
    out = {"split": [{"lexclass": l, "recovered_by_canon": bool(rb),
                      "names": int(n), "appearances": int(a)} for l, rb, n, a in rows]}
    out["still_novel_top"] = {}
    for lex in ("BANK", "CREDIT_UNION"):
        out["still_novel_top"][lex] = [
            {"nln": n, "appearances": int(a)} for n, a in con.execute(f"""
                SELECT nln, app FROM un_dep
                WHERE lexclass='{lex}' AND recovered_by_canon=false
                ORDER BY app DESC LIMIT 30""").fetchall()]
    json.dump(out, sys.stdout, indent=2, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
