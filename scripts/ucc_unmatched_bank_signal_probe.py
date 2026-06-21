#!/usr/bin/env python3
"""Do the UNMATCHED UCC lenders 'look like banks'? — FDIC/NCUA-ingest ROI test.

For every CA+CO org secured-party name, classify by LEXICAL depository signal
(BANK / CREDIT_UNION / NEITHER) and cross-tab against canonical-match status. Answers:
  * Of bank/CU-looking APPEARANCES, what % did SBA+HMDA already capture deductively?
  * Of the UNMATCHED set, how much looks like a depository at all (vs captive finance / fintech)?
  * Of unmatched bank-looking names, how much is a 'pure depository' (real FDIC/NCUA headroom)
    vs a finance-arm/division variant (a canonicalization problem, not an ingest problem)?

Lexical only — name-token heuristic, deliberately conservative. Read-only.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/ucc_unmatched_bank_signal_probe.py > /tmp/banksig.json
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
# Whole-word (space-normalized) depository tokens.
CU_PAT = r'CREDIT UNION|(^| )FCU( |$)'
BANK_PAT = (r'(^| )BANK( |$)|(^| )BANKING( |$)|BANCORP|BANCORPORATION|BANCSHARES|'
            r'(^| )BANC( |$)|(^| )BANCO( |$)|SAVINGS|(^| )FSB( |$)|(^| )SSB( |$)|'
            r'THRIFT|NATIONAL ASSOCIATION')
# A bank-looking name that ALSO carries one of these is a captive finance arm / division,
# not the depository itself (e.g. US BANK EQUIPMENT FINANCE) — a canonicalization target,
# not something an FDIC depository directory would resolve as the operative lender.
FINARM_PAT = (r'EQUIPMENT|VENDOR|LEASING|(^| )LEASE|FINANCE|FINANCIAL|(^| )CAPITAL( |$)|'
              r'MORTGAGE|FUNDING|ACCEPTANCE|CREDIT CORP')


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
    con.execute("SET memory_limit='20GB'; SET temp_directory='/tmp/duck_spill_bs'; PRAGMA threads=8;")
    con.execute(NAME_NORM)
    t0 = time.time()

    con.register("sba", rdr("sba_7a", ["bank_name"], opts))
    con.register("hmda", rdr("hmda_panels", ["respondent_name"], opts))
    con.register("adv", rdr("sec_adv_part1", ["legal_name", "primary_business_name"], opts))
    con.execute("""
        CREATE TEMP TABLE ref AS SELECT DISTINCT nln FROM (
            SELECT nn(bank_name) nln FROM sba UNION SELECT nn(respondent_name) FROM hmda
            UNION SELECT nn(legal_name) FROM adv UNION SELECT nn(primary_business_name) FROM adv
        ) WHERE nln IS NOT NULL
    """)
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
    con.execute(f"""
        CREATE TEMP TABLE univ AS
        SELECT u.nln, u.app, (r.nln IS NOT NULL) AS matched,
            CASE WHEN regexp_matches(u.nln, '{CU_PAT}')   THEN 'CREDIT_UNION'
                 WHEN regexp_matches(u.nln, '{BANK_PAT}') THEN 'BANK'
                 ELSE 'NEITHER' END AS lexclass,
            regexp_matches(u.nln, '{FINARM_PAT}') AS finarm
        FROM ucc u LEFT JOIN ref r USING (nln)
    """)
    print(f"[univ] built ({round(time.time()-t0,1)}s)", file=sys.stderr, flush=True)

    out = {}
    # 1. lexclass × matched cross-tab
    rows = con.execute("""
        SELECT lexclass, matched, count(*) AS names, sum(app) AS appearances
        FROM univ GROUP BY 1,2 ORDER BY 1,2
    """).fetchall()
    out["crosstab"] = [{"lexclass": l, "matched": bool(m), "names": int(n), "appearances": int(a)}
                       for l, m, n, a in rows]
    # 2. deductive capture rate per depository class
    cap = con.execute("""
        SELECT lexclass,
               sum(app) AS total_app,
               sum(app) FILTER (WHERE matched) AS matched_app,
               count(*) AS total_names,
               count(*) FILTER (WHERE matched) AS matched_names
        FROM univ WHERE lexclass IN ('BANK','CREDIT_UNION') GROUP BY 1
    """).fetchall()
    out["capture_rate"] = [{
        "lexclass": l, "total_app": int(ta), "matched_app": int(ma or 0),
        "captured_pct_app": round(100*(ma or 0)/ta, 1) if ta else 0,
        "total_names": int(tn), "matched_names": int(mn or 0),
        "captured_pct_names": round(100*(mn or 0)/tn, 1) if tn else 0,
    } for l, ta, ma, tn, mn in cap]
    # 3. unmatched depository-looking: pure vs finance-arm
    split = con.execute("""
        SELECT lexclass, finarm, count(*) AS names, sum(app) AS appearances
        FROM univ WHERE matched = false AND lexclass IN ('BANK','CREDIT_UNION')
        GROUP BY 1,2 ORDER BY 1,2
    """).fetchall()
    out["unmatched_depository_split"] = [
        {"lexclass": l, "finance_arm": bool(f), "names": int(n), "appearances": int(a)}
        for l, f, n, a in split]
    # 4. top unmatched PURE-depository bank names (the real FDIC headroom)
    out["top_unmatched_pure_bank"] = [
        {"nln": n, "appearances": int(a)} for n, a in con.execute("""
            SELECT nln, app FROM univ
            WHERE matched=false AND lexclass='BANK' AND finarm=false
            ORDER BY app DESC LIMIT 35""").fetchall()]
    out["top_unmatched_finarm_bank"] = [
        {"nln": n, "appearances": int(a)} for n, a in con.execute("""
            SELECT nln, app FROM univ
            WHERE matched=false AND lexclass='BANK' AND finarm=true
            ORDER BY app DESC LIMIT 20""").fetchall()]
    out["top_unmatched_credit_union"] = [
        {"nln": n, "appearances": int(a)} for n, a in con.execute("""
            SELECT nln, app FROM univ
            WHERE matched=false AND lexclass='CREDIT_UNION'
            ORDER BY app DESC LIMIT 30""").fetchall()]

    json.dump(out, sys.stdout, indent=2, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
