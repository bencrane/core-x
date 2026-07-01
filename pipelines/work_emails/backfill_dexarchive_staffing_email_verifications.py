#!/usr/bin/env python3
"""Backfill — land the dex-archive staffing work emails + MillionVerifier verdicts into the HQX
email SoR arm ops.email_verifications, so the existing materialize_work_emails.py projects them
into active/work_emails on its next full-overwrite run.

WHY THIS ARM (not a direct Lance write). active/work_emails is a FULL-OVERWRITE projection of
ops.email_resolutions ∪ ops.email_verifications (materialize_work_emails.py::run, most-recent-wins
per person_id). A direct append to the Lance dataset would be wiped on the next run. The durable
SoR is the HQX pair; ops.email_verifications is the "supplied email, MV-validated, no finder" arm
— the exact shape of the dex staffing emails (supplied target_people_emails + a MillionVerifier
verdict, no icypeas/leadmagic/blitz finder).

SOURCE (live DEX Postgres, read-only):
    entities.target_people           (staffing scope: target_company_id ∈ demand_company_target_verticals)
    entities.target_people_emails    (target_person_id → target_people.id; the supplied email)
    entities.millionverifier_results (target_people_email_id → email.id; result/quality/subresult/raw)
TARGET (HQX control-plane Postgres):
    ops.email_verifications  (PK contact_id)

KEY ALIGNMENT. contact_id = target_people.id = the person_id already in active/people →
materialize_work_emails projects contact_id→person_id, so work_emails joins the spine cleanly.

MAPPING (mirrors the live mv_result→verification_status / mv_resultcode scheme, learned from the
existing rows across both arms):
    ok         → verified   / 1        catch_all → risky      / 2
    unknown    → risky      / 3        invalid   → unresolved / 6
    disposable → unresolved / 5 (MV-standard slot; HQX-unused; 3 rows)
    verified_at → resolved_at (watermark) · raw_response → mv_raw · source='dexarchive_staffing_agencies'

IDEMPOTENT. Transactional: DELETE WHERE source='dexarchive_staffing_agencies' (removes only this
cohort's prior rows), then INSERT ... ON CONFLICT (contact_id) DO NOTHING (never clobbers a
fresher row owned by another source). Re-runs converge.

RUN:
    doppler run -p core-x -c prd -- uv run --no-project --with 'psycopg[binary]' \
        python3 pipelines/work_emails/backfill_dexarchive_staffing_email_verifications.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os

import psycopg
from psycopg.types.json import Jsonb

SOURCE = "dexarchive_staffing_agencies"
BATCH_LABEL = "dexarchive_staffing_agencies_2026-07-01"
ATTEMPTS_STUB = [{"stage": "supplied", "source": SOURCE, "verified": True}]

# DEX-side canonical domain normalization (== companies.normalized_domain rule, Postgres syntax).
_DOM = (
    "nullif(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
    "lower(trim(company_domain)),'^https?://','','g'),'^www\\.','','g'),"
    "'/.*$','','g'),'\\.+$','','g'),'')"
)

# Full projection, scoped to staffing, 1 row per person (most-recent MV wins), transforms inline.
DEX_SQL = f"""
WITH vt AS (SELECT DISTINCT target_company_id FROM entities.demand_company_target_verticals),
     sp AS (
        SELECT id, {_DOM} AS company_domain, person_linkedin_url
        FROM entities.target_people
        WHERE target_company_id IN (SELECT target_company_id FROM vt)
     )
SELECT DISTINCT ON (sp.id)
    CAST(sp.id AS text)                                                       AS contact_id,
    e.email                                                                   AS email,
    CASE mv.result WHEN 'ok' THEN 'verified' WHEN 'catch_all' THEN 'risky'
         WHEN 'unknown' THEN 'risky' WHEN 'invalid' THEN 'unresolved'
         WHEN 'disposable' THEN 'unresolved' ELSE 'unresolved' END            AS verification_status,
    CASE mv.result WHEN 'ok' THEN 1 WHEN 'catch_all' THEN 2 WHEN 'unknown' THEN 3
         WHEN 'invalid' THEN 6 WHEN 'disposable' THEN 5 ELSE NULL END         AS mv_resultcode,
    mv.result                                                                 AS mv_result,
    mv.quality                                                                AS mv_quality,
    mv.subresult                                                              AS mv_subresult,
    sp.company_domain                                                         AS company_domain,
    mv.raw_response                                                           AS mv_raw,
    mv.verified_at                                                            AS resolved_at,
    sp.person_linkedin_url                                                    AS person_linkedin_url
FROM sp
JOIN entities.target_people_emails e     ON e.target_person_id = sp.id
JOIN entities.millionverifier_results mv ON mv.target_people_email_id = e.id
WHERE e.email IS NOT NULL AND mv.verified_at IS NOT NULL
ORDER BY sp.id, mv.verified_at DESC NULLS LAST
"""

INSERT_SQL = """
INSERT INTO ops.email_verifications
    (contact_id, email, verification_status, mv_resultcode, mv_result, mv_quality, mv_subresult,
     source, company_domain, mv_raw, attempts, batch_label, resolved_at, person_linkedin_url)
VALUES
    (%(contact_id)s, %(email)s, %(verification_status)s, %(mv_resultcode)s, %(mv_result)s,
     %(mv_quality)s, %(mv_subresult)s, %(source)s, %(company_domain)s, %(mv_raw)s, %(attempts)s,
     %(batch_label)s, %(resolved_at)s, %(person_linkedin_url)s)
ON CONFLICT (contact_id) DO NOTHING
"""


def _dsn(key: str) -> str:
    dsn = os.environ[key]
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    dex_dsn = _dsn("DEX_DB_POOLER_URL") if os.environ.get("DEX_DB_POOLER_URL") else _dsn("DEX_DB_DIRECT_URL")
    with psycopg.connect(dex_dsn) as dex:
        dex.read_only = True
        with dex.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(DEX_SQL)
            rows = cur.fetchall()
    n = len(rows)
    from collections import Counter
    vs = Counter(r["verification_status"] for r in rows)
    print(f"dex staffing email+MV rows projected : {n:,}")
    print(f"  verification_status: {dict(vs)}")
    print(f"  verified (deliverable) = {vs.get('verified', 0):,}")

    if args.dry_run:
        print("\n[dry-run] no HQX write. sample:")
        for r in rows[:6]:
            print(f"    {str(r['email'])[:34]:34} {r['mv_result']:10} → {r['verification_status']:10} "
                  f"dom={r['company_domain']}")
        return 0

    for r in rows:
        r["source"] = SOURCE
        r["batch_label"] = BATCH_LABEL
        r["attempts"] = Jsonb(ATTEMPTS_STUB)
        r["mv_raw"] = Jsonb(r["mv_raw"]) if r["mv_raw"] is not None else None

    with psycopg.connect(_dsn("HQX_DB_URL_POOLED")) as hqx:
        with hqx.cursor() as cur:
            cur.execute("SELECT count(*) FROM ops.email_verifications")
            before = cur.fetchone()[0]
            cur.execute("DELETE FROM ops.email_verifications WHERE source = %s", (SOURCE,))
            deleted = cur.rowcount
            cur.executemany(INSERT_SQL, rows)
            cur.execute("SELECT count(*) FROM ops.email_verifications WHERE source = %s", (SOURCE,))
            cohort_after = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM ops.email_verifications")
            after = cur.fetchone()[0]
        hqx.commit()

    print(f"\nops.email_verifications: {before:,} → {after:,}  (cohort rows now {cohort_after:,}; "
          f"deleted {deleted:,} prior; skipped-on-conflict {n - cohort_after:,})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
