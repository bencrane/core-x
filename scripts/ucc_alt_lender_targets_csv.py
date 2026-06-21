#!/usr/bin/env python3
"""Export the ACTIVE alt-lender target list to CSV for website/LinkedIn enrichment.

Population: CA+CO org secured parties with >5 appearances in the trailing 24 months
(filing_date via join; CA secured_parties→filings, CO →co_ucc_transactions, min date/key).

Strip ONLY by name pattern: government filers, filing agents, obvious banks, obvious
credit unions. KEEP everything else — equipment/specialty/fintech/solar finance AND
medical-provider liens (operator wants those). No filing_type filter.

Lean columns: company_name (modal raw spelling), normalized_name, states, appearances_24mo,
appearances_all_time. Sorted by 24mo volume desc. Read-only; writes one CSV.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/ucc_alt_lender_targets_csv.py /tmp/ucc_alt_lender_targets.csv
"""
from __future__ import annotations

import os
import sys

import duckdb
import lance

BUCKET = "data-sink"
CUT_24 = "2024-06-01"   # trailing 24 months vs ref 2026-06-01
NAME_NORM = r"""
CREATE OR REPLACE MACRO nn(x) AS
  nullif(trim(regexp_replace(regexp_replace(upper(CAST(x AS VARCHAR)),
         '[^A-Z0-9 ]+', '', 'g'), '\s+', ' ', 'g')), '');
"""
# Strip buckets (whole-word, space-normalized UPPER names) — deliberately conservative:
# over-stripping loses a real alt-lender, so each pattern targets unambiguous tokens.
BANK_PAT = (r'(^| )BANK( |$)|(^| )BANKING( |$)|BANCORP|BANCORPORATION|BANCSHARES|'
            r'(^| )BANC( |$)|(^| )BANCO( |$)|SAVINGS|(^| )FSB( |$)|(^| )SSB( |$)|'
            r'THRIFT|NATIONAL ASSOCIATION')
CU_PAT = r'CREDIT UNION|(^| )FCU( |$)'
GOV_PAT = (r'EMPLOYMENT DEVELOPMENT DEPARTMENT|FRANCHISE TAX BOARD|INTERNAL REVENUE|'
           r'(^| )IRS( |$)|IRSOHIO|TAX AND FEE ADMINISTRATION|DEPARTMENT OF TAX|'
           r'BOARD OF EQUALIZATION|DEPARTMENT OF REVENUE|SMALL BUSINESS ADMINISTRATION|'
           r'SECRETARY OF STATE|STATE BOARD OF|DEPARTMENT OF LABOR|DIVISION OF UNEMPLOYMENT|'
           r'DEPARTMENT OF THE TREASURY|(^| )TREASURER( |$)|DEPARTMENT OF INDUSTRIAL|'
           r'WORKFORCE (COMMISSION|SERVICES|DEVELOPMENT)|UNEMPLOYMENT INSURANCE')
AGENT_PAT = (r'AS REPRESENTATIVE|CORPORATION SERVICE COMPANY|(^| )C T CORPORATION|'
             r'(^| )CT CORPORATION|FIRST CORPORATE SOLUTIONS|(^| )CHTD COMPANY|'
             r'COGENCY GLOBAL|NATIONAL REGISTERED AGENTS|(^| )NRAI( |$)|CT LIEN SOLUTIONS|'
             r'WOLTERS KLUWER|(^| )LIEN SOLUTIONS|(^| )MIDDESK|INCORP SERVICES|'
             r'CAPITOL SERVICES|PARACORP|REGISTERED AGENT SOLUTIONS|GLOBAL REGISTERED AGENTS|'
             r'(^| )CSC( |$)|REGISTERED AGENTS INC')


def so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def rdr(label, cols, opts, filt=None):
    ds = lance.dataset(f"s3://{BUCKET}/active/{label}/", storage_options=opts)
    return ds.scanner(columns=cols, filter=filt).to_reader()


def main() -> int:
    out_csv = sys.argv[1] if len(sys.argv) > 1 else "/tmp/ucc_alt_lender_targets.csv"
    opts = so()
    con = duckdb.connect()
    con.execute("SET memory_limit='24GB'; SET temp_directory='/tmp/duck_spill_csv'; PRAGMA threads=8;")
    con.execute(NAME_NORM)

    con.register("ca_f", rdr("ca_ucc/filings", ["ucc1_num", "ucc3_num", "filing_date"], opts))
    con.execute("CREATE TEMP TABLE fd_ca AS SELECT ucc1_num, ucc3_num, min(filing_date) AS fd FROM ca_f GROUP BY 1,2")
    con.register("ca_sp", rdr("ca_ucc/secured_parties",
                              ["org_name", "secured_party_type", "ucc1_num", "ucc3_num"], opts,
                              "secured_party_type = 'Organization'"))
    con.execute("""
        CREATE TEMP TABLE app AS
        SELECT nn(s.org_name) AS nln, s.org_name AS raw, 'CA' AS st, CAST(f.fd AS DATE) AS fd
        FROM ca_sp s LEFT JOIN fd_ca f ON s.ucc1_num = f.ucc1_num AND s.ucc3_num = f.ucc3_num
        WHERE s.org_name IS NOT NULL
    """)
    con.register("co_t", rdr("co_ucc_transactions", ["file_id", "filing_date"], opts))
    con.execute("CREATE TEMP TABLE fd_co AS SELECT file_id, min(filing_date) AS fd FROM co_t GROUP BY 1")
    con.register("co_sp", rdr("ucc_co_secured_parties", ["organization_name", "file_id"], opts))
    con.execute("""
        INSERT INTO app
        SELECT nn(s.organization_name) AS nln, s.organization_name AS raw, 'CO' AS st, CAST(f.fd AS DATE) AS fd
        FROM co_sp s LEFT JOIN fd_co f ON s.file_id = f.file_id
        WHERE s.organization_name IS NOT NULL
    """)

    con.execute(f"""
        CREATE TEMP TABLE agg AS
        SELECT nln,
            count(*) AS app_all,
            count(*) FILTER (WHERE fd >= DATE '{CUT_24}') AS app_24,
            string_agg(DISTINCT st, ';') AS states
        FROM app WHERE nln IS NOT NULL GROUP BY 1
    """)
    con.execute("""
        CREATE TEMP TABLE disp AS
        SELECT nln, arg_max(raw, c) AS company_name
        FROM (SELECT nln, raw, count(*) AS c FROM app WHERE nln IS NOT NULL GROUP BY 1,2)
        GROUP BY 1
    """)
    con.execute(f"""
        CREATE TEMP TABLE target AS
        SELECT d.company_name, a.nln AS normalized_name, a.states,
               a.app_24 AS appearances_24mo, a.app_all AS appearances_all_time
        FROM agg a JOIN disp d USING (nln)
        WHERE a.app_24 > 5
          AND NOT regexp_matches(a.nln, '{BANK_PAT}')
          AND NOT regexp_matches(a.nln, '{CU_PAT}')
          AND NOT regexp_matches(a.nln, '{GOV_PAT}')
          AND NOT regexp_matches(a.nln, '{AGENT_PAT}')
    """)

    # reporting counts
    base = con.execute(f"SELECT count(*) FROM agg WHERE app_24 > 5").fetchone()[0]
    kept = con.execute("SELECT count(*) FROM target").fetchone()[0]
    strip = {}
    for nm, pat in [("bank", BANK_PAT), ("credit_union", CU_PAT), ("gov", GOV_PAT), ("agent", AGENT_PAT)]:
        strip[nm] = con.execute(
            f"SELECT count(*) FROM agg WHERE app_24 > 5 AND regexp_matches(nln, '{pat}')").fetchone()[0]
    print(f"[csv] base(>5 in 24mo)={base:,}  kept={kept:,}  "
          f"stripped: bank={strip['bank']:,} cu={strip['credit_union']:,} "
          f"gov={strip['gov']:,} agent={strip['agent']:,}", file=sys.stderr, flush=True)

    con.execute(f"""
        COPY (SELECT * FROM target ORDER BY appearances_24mo DESC, company_name)
        TO '{out_csv}' (HEADER, DELIMITER ',', QUOTE '"')
    """)
    print(f"[csv] wrote {kept:,} rows -> {out_csv}", file=sys.stderr, flush=True)
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
