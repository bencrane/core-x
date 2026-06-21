#!/usr/bin/env python3
"""Time-bound the lender appearance bands. The secured-party tables are DATELESS, so
attach filing_date by join (CA secured_parties→filings on ucc1_num/ucc3_num; CO
ucc_co_secured_parties→co_ucc_transactions on file_id, min date per key), then re-band
the per-name appearance counts on trailing windows (12 / 24 / 36 months vs 2026-06-01).

Answers: of the 'more than X appearances' heads, how many survive a recency filter — i.e.
how many lenders are actually *active*, not just historically present. Read-only.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/ucc_lender_recency_bands_probe.py > /tmp/recency.json
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
CUT_12, CUT_24, CUT_36 = "2025-06-01", "2024-06-01", "2023-06-01"  # vs ref 2026-06-01


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
    con.execute("SET memory_limit='24GB'; SET temp_directory='/tmp/duck_spill_rc'; PRAGMA threads=8;")
    con.execute(NAME_NORM)
    t0 = time.time()

    # ref (matched set)
    con.register("sba", rdr("sba_7a", ["bank_name"], opts))
    con.register("hmda", rdr("hmda_panels", ["respondent_name"], opts))
    con.register("adv", rdr("sec_adv_part1", ["legal_name", "primary_business_name"], opts))
    con.execute("""
        CREATE TEMP TABLE ref AS SELECT DISTINCT nln FROM (
            SELECT nn(bank_name) nln FROM sba UNION SELECT nn(respondent_name) FROM hmda
            UNION SELECT nn(legal_name) FROM adv UNION SELECT nn(primary_business_name) FROM adv
        ) WHERE nln IS NOT NULL
    """)

    # CA: filing_date per (ucc1_num,ucc3_num); attach to org secured parties
    con.register("ca_f", rdr("ca_ucc/filings", ["ucc1_num", "ucc3_num", "filing_date"], opts))
    con.execute("CREATE TEMP TABLE fd_ca AS SELECT ucc1_num, ucc3_num, min(filing_date) AS fd FROM ca_f GROUP BY 1,2")
    con.register("ca_sp", rdr("ca_ucc/secured_parties", ["org_name", "secured_party_type", "ucc1_num", "ucc3_num"],
                              opts, "secured_party_type = 'Organization'"))
    con.execute("""
        CREATE TEMP TABLE app_ca AS
        SELECT nn(s.org_name) AS nln, CAST(f.fd AS DATE) AS fd
        FROM ca_sp s LEFT JOIN fd_ca f ON s.ucc1_num = f.ucc1_num AND s.ucc3_num = f.ucc3_num
        WHERE s.org_name IS NOT NULL
    """)

    # CO: filing_date per file_id; attach to secured parties
    con.register("co_t", rdr("co_ucc_transactions", ["file_id", "filing_date"], opts))
    con.execute("CREATE TEMP TABLE fd_co AS SELECT file_id, min(filing_date) AS fd FROM co_t GROUP BY 1")
    con.register("co_sp", rdr("ucc_co_secured_parties", ["organization_name", "file_id"], opts))
    con.execute("""
        CREATE TEMP TABLE app_co AS
        SELECT nn(s.organization_name) AS nln, CAST(f.fd AS DATE) AS fd
        FROM co_sp s LEFT JOIN fd_co f ON s.file_id = f.file_id
        WHERE s.organization_name IS NOT NULL
    """)

    con.execute("""
        CREATE TEMP TABLE app AS
        SELECT nln, fd FROM app_ca WHERE nln IS NOT NULL
        UNION ALL SELECT nln, fd FROM app_co WHERE nln IS NOT NULL
    """)
    print(f"[app] built ({round(time.time()-t0,1)}s)", file=sys.stderr, flush=True)

    # date coverage / recency mix
    cov = con.execute(f"""
        SELECT count(*) AS total, count(fd) AS dated,
               CAST(min(fd) AS VARCHAR) AS min_fd, CAST(max(fd) AS VARCHAR) AS max_fd,
               count(*) FILTER (WHERE fd >= DATE '{CUT_12}') AS a12,
               count(*) FILTER (WHERE fd >= DATE '{CUT_24}') AS a24,
               count(*) FILTER (WHERE fd >= DATE '{CUT_36}') AS a36
        FROM app
    """).fetchone()
    out = {"coverage": {"total_appearances": int(cov[0]), "dated": int(cov[1]),
                        "null_fd_pct": round(100*(cov[0]-cov[1])/cov[0], 2),
                        "min_fd": cov[2], "max_fd": cov[3],
                        "appx_last_12mo": int(cov[4]), "appx_last_24mo": int(cov[5]),
                        "appx_last_36mo": int(cov[6]),
                        "pct_last_24mo": round(100*cov[5]/cov[0], 1)}}

    # per-name counts: all-time and per-window
    con.execute(f"""
        CREATE TEMP TABLE byname AS
        SELECT a.nln,
            count(*) AS app_all,
            count(*) FILTER (WHERE a.fd >= DATE '{CUT_12}') AS app_12,
            count(*) FILTER (WHERE a.fd >= DATE '{CUT_24}') AS app_24,
            count(*) FILTER (WHERE a.fd >= DATE '{CUT_36}') AS app_36,
            (r.nln IS NOT NULL) AS matched
        FROM app a LEFT JOIN ref r ON r.nln = a.nln
        GROUP BY a.nln, (r.nln IS NOT NULL)
    """)

    # band the heads on each window, split matched/unmatched
    def head(colexpr, thr, matched=None):
        w = f"{colexpr} >= {thr}"
        if matched is not None:
            w += f" AND matched = {str(matched).lower()}"
        return con.execute(f"SELECT count(*), coalesce(sum({colexpr}),0) FROM byname WHERE {w}").fetchone()

    out["bands"] = []
    for thr in (5, 10, 15, 25, 50):
        row = {"threshold": thr}
        for col, key in [("app_all", "all_time"), ("app_36", "last_36mo"),
                         ("app_24", "last_24mo"), ("app_12", "last_12mo")]:
            n, a = head(col, thr)
            nm, _ = head(col, thr, matched=True)
            nu, _ = head(col, thr, matched=False)
            row[key] = {"names": int(n), "matched_names": int(nm), "unmatched_names": int(nu)}
        out["bands"].append(row)

    # top unmatched names by RECENT (24mo) volume — the active alt-lender head
    out["top_unmatched_24mo"] = [
        {"nln": n, "app_24mo": int(a24), "app_all": int(aall)} for n, a24, aall in con.execute("""
            SELECT nln, app_24, app_all FROM byname
            WHERE matched = false AND app_24 > 0 ORDER BY app_24 DESC LIMIT 30""").fetchall()]

    json.dump(out, sys.stdout, indent=2, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
