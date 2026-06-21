#!/usr/bin/env python3
"""Coverage probe: what fraction of CA+CO UCC secured parties (lenders) can be
classified by a CANONICAL government designation already in the Lance SoR — no LLM.

Builds a reference name→designation set from three canonical registries:
  * sba_7a           — bank_name → bank_fdic_number / bank_ncua_number (FDIC cert / NCUA charter)
  * hmda_panels      — respondent_name → agency_code (1 OCC / 2 FRS / 3 FDIC / 5 NCUA / 7 HUD / 9 CFPB)
  * sec_adv_part1    — legal_name / primary_business_name → SEC-registered investment adviser

…then equi-joins (exact _name_norm match — the SAME macro that built the SoS spine, so it
matches the blueprint's name-block discipline) against the UCC secured-party org names and
reports distinct-name and APPEARANCE-weighted match rates, broken out by resulting class.

Exact _name_norm match is a FLOOR (no alias canonicalization, no NA↔NATIONAL ASSOCIATION
fold, no filing-agent unmasking). Strictly read-only.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/ucc_lender_designation_overlap_probe.py > /tmp/overlap.json
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


def so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def reader(label, cols, opts, filt=None):
    ds = lance.dataset(f"s3://{BUCKET}/active/{label}/", storage_options=opts)
    return ds.scanner(columns=cols, filter=filt).to_reader()


def main() -> int:
    opts = so()
    con = duckdb.connect()
    con.execute("SET memory_limit='20GB'; SET temp_directory='/tmp/duck_spill_ov'; PRAGMA threads=8;")
    con.execute(NAME_NORM)

    t0 = time.time()
    # ── Reference registries → ref(nln, class, src) ──────────────────────────────
    con.register("sba", reader("sba_7a", ["bank_name", "bank_fdic_number", "bank_ncua_number"], opts))
    con.execute("""
        CREATE TEMP TABLE ref_sba AS
        SELECT nn(bank_name) AS nln,
               CASE WHEN max(bank_fdic_number) IS NOT NULL THEN 'BANK (FDIC cert)'
                    WHEN max(bank_ncua_number) IS NOT NULL THEN 'CREDIT_UNION (NCUA)'
                    ELSE 'BANK/lender (SBA, no cert)' END AS class
        FROM sba WHERE bank_name IS NOT NULL GROUP BY 1
    """)
    # HMDA agency_code is the SUPERVISORY agency, not the charter type:
    #   1 OCC · 2 FRS · 3 FDIC → depository bank (unambiguous)
    #   5 NCUA               → credit union (unambiguous)
    #   7 (HUD-legacy)       → independent non-depository mortgage company (e.g. GoodLeap)
    #   9 CFPB               → AMBIGUOUS: large depositories >$10B (Chase/Wells NA) AND large non-banks
    con.register("hmda", reader("hmda_panels", ["respondent_name", "agency_code"], opts))
    con.execute("""
        CREATE TEMP TABLE ref_hmda AS
        SELECT nn(respondent_name) AS nln,
               CASE WHEN list_contains(list(DISTINCT agency_code), '5') THEN 'CREDIT_UNION (NCUA agency 5)'
                    WHEN list_has_any(list(DISTINCT agency_code), ['1','2','3']) THEN 'BANK (OCC/FRS/FDIC agency 1/2/3)'
                    WHEN list_contains(list(DISTINCT agency_code), '7') THEN 'NONBANK_MORTGAGE (HMDA agency 7)'
                    ELSE 'CFPB_SUPERVISED — bank or large nonbank (agency 9)' END AS class
        FROM hmda WHERE respondent_name IS NOT NULL GROUP BY 1
    """)
    con.register("adv", reader("sec_adv_part1", ["legal_name", "primary_business_name"], opts))
    con.execute("""
        CREATE TEMP TABLE ref_adv AS
        SELECT nln, 'INVESTMENT_ADVISER (SEC ADV)' AS class FROM (
            SELECT nn(legal_name) AS nln FROM adv WHERE legal_name IS NOT NULL
            UNION
            SELECT nn(primary_business_name) FROM adv WHERE primary_business_name IS NOT NULL
        ) GROUP BY 1, 2
    """)
    # Merge with class priority: FDIC/NCUA cert beats HMDA reg beats adviser.
    con.execute("""
        CREATE TEMP TABLE ref AS
        WITH u AS (
            SELECT nln, class, 1 AS pr FROM ref_sba WHERE nln IS NOT NULL
            UNION ALL SELECT nln, class, 2 FROM ref_hmda WHERE nln IS NOT NULL
            UNION ALL SELECT nln, class, 3 FROM ref_adv  WHERE nln IS NOT NULL
        )
        SELECT nln, arg_min(class, pr) AS class FROM u GROUP BY 1
    """)
    ref_n = con.execute("SELECT count(*) FROM ref").fetchone()[0]
    by_src = {
        "ref_sba": con.execute("SELECT count(*) FROM ref_sba WHERE nln IS NOT NULL").fetchone()[0],
        "ref_hmda": con.execute("SELECT count(*) FROM ref_hmda WHERE nln IS NOT NULL").fetchone()[0],
        "ref_adv": con.execute("SELECT count(*) FROM ref_adv WHERE nln IS NOT NULL").fetchone()[0],
        "ref_unique": ref_n,
    }
    print(f"[ref] built {ref_n:,} distinct canonical lender names ({round(time.time()-t0,1)}s)",
          file=sys.stderr, flush=True)

    # ── UCC lender universes (org secured parties) → name + appearances ──────────
    out = {"ref_sizes": by_src, "states": {}}
    for state, label, namecol, filt in [
        ("CA", "ca_ucc/secured_parties", "org_name", "secured_party_type = 'Organization'"),
        ("CO", "ucc_co_secured_parties", "organization_name", None),
    ]:
        t1 = time.time()
        con.register("sp", reader(label, [namecol] + (["secured_party_type"] if state == "CA" else []),
                                  opts, filt))
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE ucc AS
            SELECT nn({namecol}) AS nln, count(*) AS appearances
            FROM sp WHERE {namecol} IS NOT NULL GROUP BY 1
        """)
        tot_names, tot_app = con.execute(
            "SELECT count(*), sum(appearances) FROM ucc WHERE nln IS NOT NULL").fetchone()
        m_names, m_app = con.execute("""
            SELECT count(*), sum(u.appearances)
            FROM ucc u JOIN ref r USING (nln) WHERE u.nln IS NOT NULL
        """).fetchone()
        by_class = con.execute("""
            SELECT r.class, count(*) AS names, sum(u.appearances) AS appearances
            FROM ucc u JOIN ref r USING (nln)
            GROUP BY 1 ORDER BY appearances DESC
        """).fetchall()
        top_matched = con.execute("""
            SELECT u.nln, r.class, u.appearances
            FROM ucc u JOIN ref r USING (nln)
            ORDER BY u.appearances DESC LIMIT 25
        """).fetchall()
        out["states"][state] = {
            "distinct_names": int(tot_names), "appearances": int(tot_app),
            "matched_names": int(m_names or 0), "matched_appearances": int(m_app or 0),
            "match_pct_names": round(100 * (m_names or 0) / tot_names, 2) if tot_names else 0,
            "match_pct_appearances": round(100 * (m_app or 0) / tot_app, 2) if tot_app else 0,
            "by_class": [{"class": c, "names": int(n), "appearances": int(a)} for c, n, a in by_class],
            "top_matched": [{"nln": n, "class": c, "appearances": int(a)} for n, c, a in top_matched],
        }
        print(f"[{state}] {label}: matched {out['states'][state]['match_pct_appearances']}% of "
              f"appearances, {out['states'][state]['match_pct_names']}% of distinct names "
              f"({round(time.time()-t1,1)}s)", file=sys.stderr, flush=True)

    con.close()
    json.dump(out, sys.stdout, indent=2, default=str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
