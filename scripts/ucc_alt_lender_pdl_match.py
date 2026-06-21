#!/usr/bin/env python3
"""Match the 3,294 alt-lender target names against the PDL company dataset (35.4M) and
enrich with domain + LinkedIn. Tests legal-name → PDL business-name coverage.

Two tiers, SAME _name_norm macro on BOTH sides (never trust a foreign norm):
  * exact   : nn(lender) = nn(pdl.company_name)
  * suffix  : strip_suffix(nn(lender)) = strip_suffix(nn(pdl.company_name))  [legal-form peel]

Per matched key, picks ONE best PDL record: a domain↔name CONSISTENCY check wins first
(the chosen domain's root must share a token with the name — kills GOODLEAP→fixtureskb.com
style false positives), then non-generic-domain, then has-linkedin. Emits a confidence
flag (high/medium/low) so the output can be trust-filtered, plus domain + LinkedIn.
Read-only.

    doppler run -p core-x -c prd -- \
      uv run --no-project --with boto3 --with pylance --with duckdb \
      python3 scripts/ucc_alt_lender_pdl_match.py <in_csv> <out_csv>
"""
from __future__ import annotations

import os
import sys

import duckdb
import lance

BUCKET = "data-sink"
MACROS = r"""
CREATE OR REPLACE MACRO nn(x) AS
  nullif(trim(regexp_replace(regexp_replace(upper(CAST(x AS VARCHAR)),
         '[^A-Z0-9 ]+', '', 'g'), '\s+', ' ', 'g')), '');
CREATE OR REPLACE MACRO strip_suffix(x) AS
  nullif(trim(regexp_replace(CAST(x AS VARCHAR),
    ' (INCORPORATED|CORPORATION|COMPANY|LIMITED|LLLP|PLLC|LLP|LLC|CORP|INC|LTD|LP|CO|PC|PA|NA|USA)$',
    '')), '');
-- domain↔key consistency: chosen domain root shares a ≥4-char token with the matched name.
CREATE OR REPLACE MACRO domroot(d) AS
  regexp_replace(lower(split_part(coalesce(CAST(d AS VARCHAR),''), '.', 1)), '[^a-z0-9]', '', 'g');
CREATE OR REPLACE MACRO cons(key, d) AS (
  CASE WHEN d IS NULL THEN 0
       WHEN length(domroot(d)) >= 4 AND strpos(lower(replace(key,' ','')), domroot(d)) > 0 THEN 1
       WHEN length(split_part(key,' ',1)) >= 4 AND strpos(domroot(d), lower(split_part(key,' ',1))) > 0 THEN 1
       ELSE 0 END);
"""


PDL_CACHE = "/tmp/pdl_hit_alt_lenders.parquet"


def so() -> dict:
    ep = os.environ.get("R2_ENDPOINT") or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto",
            "timeout": "300s", "connect_timeout": "30s"}


def main() -> int:
    in_csv, out_csv = sys.argv[1], sys.argv[2]
    opts = so()
    con = duckdb.connect()
    con.execute("SET memory_limit='28GB'; SET temp_directory='/tmp/duck_spill_pdl'; PRAGMA threads=8;")
    con.execute(MACROS)

    con.execute(f"""
        CREATE TEMP TABLE target AS
        SELECT *, nn(normalized_name) AS k, strip_suffix(nn(normalized_name)) AS kss
        FROM read_csv('{in_csv}', header=true, all_varchar=true)
    """)
    con.execute("ALTER TABLE target ALTER appearances_24mo TYPE BIGINT")
    n_target = con.execute("SELECT count(*) FROM target").fetchone()[0]
    con.execute("CREATE TEMP TABLE tk AS SELECT DISTINCT k FROM target WHERE k IS NOT NULL")
    con.execute("CREATE TEMP TABLE tkss AS SELECT DISTINCT kss FROM target WHERE kss IS NOT NULL")

    if os.path.exists(PDL_CACHE):
        con.execute(f"CREATE TEMP TABLE pdl_hit AS SELECT * FROM read_parquet('{PDL_CACHE}')")
        print(f"[pdl] loaded cached pdl_hit from {PDL_CACHE}", file=sys.stderr, flush=True)
    else:
        con.register("pdl", lance.dataset("s3://data-sink/active/pdl_normalized_companies/", storage_options=opts)
                     .scanner(columns=["company_name", "normalized_domain", "is_generic_domain",
                                       "linkedin_slug", "industry", "employee_size_range"]).to_reader())
        con.execute("""
            CREATE TEMP TABLE pdl_hit AS
            WITH p AS (
                SELECT company_name AS pdl_name, normalized_domain, is_generic_domain,
                       linkedin_slug, industry, employee_size_range, nn(company_name) AS k
                FROM pdl
            )
            SELECT p.*, strip_suffix(k) AS kss,
                   ((normalized_domain IS NOT NULL AND NOT coalesce(is_generic_domain,false))::INT * 2
                    + (linkedin_slug IS NOT NULL)::INT) AS q,
                   cons(k, normalized_domain) AS cons_k,
                   cons(strip_suffix(k), normalized_domain) AS cons_kss
            FROM p
            WHERE k IN (SELECT k FROM tk) OR strip_suffix(k) IN (SELECT kss FROM tkss)
        """)
        con.execute(f"COPY pdl_hit TO '{PDL_CACHE}' (FORMAT parquet)")
    n_hit = con.execute("SELECT count(*) FROM pdl_hit").fetchone()[0]
    print(f"[pdl] {n_hit:,} PDL rows hit the {n_target:,} target keys", file=sys.stderr, flush=True)

    con.execute("""
        CREATE TEMP TABLE best_exact AS
        SELECT k, pdl_name, normalized_domain, linkedin_slug, industry, employee_size_range,
               n_cand, cons_k AS chosen_cons
        FROM (SELECT *, count(*) OVER (PARTITION BY k) AS n_cand,
                     row_number() OVER (PARTITION BY k ORDER BY cons_k DESC, q DESC, normalized_domain) AS rn
              FROM pdl_hit) WHERE rn = 1
    """)
    con.execute("""
        CREATE TEMP TABLE best_ss AS
        SELECT kss, pdl_name, normalized_domain, linkedin_slug, industry, employee_size_range,
               n_cand, cons_kss AS chosen_cons
        FROM (SELECT *, count(*) OVER (PARTITION BY kss) AS n_cand,
                     row_number() OVER (PARTITION BY kss ORDER BY cons_kss DESC, q DESC, normalized_domain) AS rn
              FROM pdl_hit) WHERE rn = 1
    """)

    con.execute("""
        CREATE TEMP TABLE enriched AS
        WITH j AS (
            SELECT t.company_name, t.normalized_name, t.states, t.appearances_24mo,
                   t.appearances_all_time, t.dominant_filing_type, t.filing_type_mix,
                   CASE WHEN be.k IS NOT NULL THEN 'exact'
                        WHEN bs.kss IS NOT NULL THEN 'suffix' ELSE 'none' END AS tier,
                   coalesce(be.pdl_name, bs.pdl_name) AS pdl_company_name,
                   coalesce(be.normalized_domain, bs.normalized_domain) AS pdl_domain,
                   coalesce(be.linkedin_slug, bs.linkedin_slug) AS slug,
                   coalesce(be.industry, bs.industry) AS pdl_industry,
                   coalesce(be.employee_size_range, bs.employee_size_range) AS pdl_employee_size,
                   coalesce(be.n_cand, bs.n_cand) AS pdl_candidate_count,
                   coalesce(be.chosen_cons, bs.chosen_cons) AS chosen_cons
            FROM target t
            LEFT JOIN best_exact be ON be.k = t.k
            LEFT JOIN best_ss   bs ON bs.kss = t.kss AND be.k IS NULL
        )
        SELECT company_name, normalized_name, states, appearances_24mo, appearances_all_time,
               dominant_filing_type, filing_type_mix, pdl_company_name, pdl_domain,
               CASE WHEN slug IS NOT NULL THEN 'https://www.linkedin.com/company/' || slug END AS pdl_linkedin_url,
               pdl_industry, pdl_employee_size, tier AS pdl_match_tier, pdl_candidate_count,
               -- ambiguous suffix matches (c>=2) are demoted to low even if a brand token
               -- agrees: a shared token cannot tell the lender from a same-named unrelated firm.
               CASE WHEN tier='none' THEN 'none'
                    WHEN tier='exact'  AND pdl_candidate_count=1 THEN 'high'
                    WHEN tier='suffix' AND pdl_candidate_count=1 AND chosen_cons=1 THEN 'high'
                    WHEN tier='exact'  AND chosen_cons=1 THEN 'medium'
                    WHEN tier='suffix' AND pdl_candidate_count=1 THEN 'medium'
                    ELSE 'low' END AS pdl_match_confidence
        FROM j
    """)

    tot_n, tot_a = con.execute("SELECT count(*), sum(appearances_24mo) FROM enriched").fetchone()

    def stat(w):
        n, a = con.execute(f"SELECT count(*), coalesce(sum(appearances_24mo),0) FROM enriched WHERE {w}").fetchone()
        return f"names {n:>5,} ({100*n/tot_n:4.1f}%)   appx-wtd {a:>9,} ({100*a/tot_a:4.1f}%)"
    print("  -- tiers --", file=sys.stderr, flush=True)
    for lbl, w in [("exact", "pdl_match_tier='exact'"), ("suffix", "pdl_match_tier='suffix'"),
                   ("ANY match", "pdl_match_tier<>'none'"), ("no match", "pdl_match_tier='none'")]:
        print(f"  {lbl:<12} {stat(w)}", file=sys.stderr, flush=True)
    print("  -- confidence --", file=sys.stderr, flush=True)
    for c in ("high", "medium", "low"):
        line = stat("pdl_match_confidence='" + c + "'")
        print(f"  {c:<12} {line}", file=sys.stderr, flush=True)
    print(f"  has domain    {stat('pdl_domain IS NOT NULL')}", file=sys.stderr, flush=True)
    print(f"  has linkedin  {stat('pdl_linkedin_url IS NOT NULL')}", file=sys.stderr, flush=True)
    hm = stat("pdl_match_confidence IN ('high','medium')")
    print(f"  HIGH or MED   {hm}", file=sys.stderr, flush=True)

    con.execute(f"""
        COPY (SELECT * FROM enriched ORDER BY appearances_24mo DESC, company_name)
        TO '{out_csv}' (HEADER, DELIMITER ',', QUOTE '"')
    """)
    print(f"[csv] wrote {tot_n:,} rows -> {out_csv}", file=sys.stderr, flush=True)
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
