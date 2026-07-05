#!/usr/bin/env python3
"""Enroller — SAM-mart DM LinkedIn queue → LeadMagic mobile finder.

Cohort (operator-directed 2026-07-04, fired after the 1,159 email-first run
returned 54.8%): the ≥$1M LinkedIn queue (identity-resolved, no owned mobile,
NOT in the ruled-email cohort), constrained to DECISION-MAKERS:

    FFATA officer (is_exec_officer_prime|sub)
    ∨ DM title on gtm_sam_people.best_title
    ∨ DM title on the matched clay/blitz source row
    …with the same size rule (<500 confirmed ∨ unknown <$100M) and
    money24 = greatest(sub-$24m, prime-$24m) ≥ $1M.

DEDUPED BY HUMAN: one contact per person_linkedin_url_norm (the same human at
N in-queue UEIs enrolls once — keep the highest-money24 sam_person_id as
contact_id; sibling rows inherit the mobile at query time via the LinkedIn
join). Exclusions: slugs in the ruled-email enrolled cohort AND slugs already
FOUND in live ops.phone_resolutions (covers the 635 just landed).

Payload: person_linkedin_url always; vendor work_email attached where owned
(work_emails via slug); company_domain/name, first/last from the mart.

RUN:
    doppler run -- python3 pipelines/enrichment_leadmagic/enroll_sam_dm_li_mobile.py --dry-run
    doppler run -- python3 pipelines/enrichment_leadmagic/enroll_sam_dm_li_mobile.py
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys

import duckdb
import lance

ACTIVE = os.environ.get("GTM_ACTIVE_ROOT", "s3://data-sink/active")
TRIGGER_API_BASE = os.environ.get("TRIGGER_API_URL", "https://api.trigger.dev").rstrip("/")
RESOLVE_TASK_ID = "leadmagic-phone-finder-resolve"
WINDOW = os.environ.get("GTM_AWARD_WINDOW", "2024-07-04")

_SLUG = r"linkedin\.com/in/([^/?#]+)"
_LARGE = ("contains({c},'501') OR contains({c},'1001') OR contains({c},'5001') "
          "OR contains({c},'10001') OR contains({c},'1,001')")
_DM = ("(owner|founder|ceo|chief executive|president|principal"
       "|managing (member|partner|director)|partner|coo|cfo|cto|cio|chief"
       "|vice president|vp |evp|svp)")


def _storage_options() -> dict:
    ep = os.environ.get("R2_ENDPOINT")
    if not ep and os.environ.get("R2_ACCOUNT_ID"):
        ep = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": ep,
        "region": "auto",
    }


def _rule(t: str) -> None:
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}", flush=True)


def _ops_found_slugs() -> set[str]:
    """Live exclusion: slugs already resolved FOUND in ops.phone_resolutions
    (the Lance mirror lags; this covers same-day finds)."""
    import psycopg
    import re

    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        print("WARN: HQX_DB_URL_POOLED not set — ops-side exclusion skipped.")
        return set()
    out: set[str] = set()
    with psycopg.connect(dsn) as pg, pg.cursor() as cur:
        cur.execute("SELECT person_linkedin_url FROM ops.phone_resolutions "
                    "WHERE phone_status = 'found' AND person_linkedin_url IS NOT NULL")
        for (u,) in cur.fetchall():
            m = re.search(r"linkedin\.com/in/([^/?#]+)", (u or "").lower())
            if m:
                out.add("linkedin.com/in/" + m.group(1))
    return out


def _build_contacts(limit: int | None) -> tuple[list[dict], dict]:
    so = _storage_options()
    con = duckdb.connect()
    con.execute("SET memory_limit='16GB'; PRAGMA threads=8;")

    def reg(a, n, cols, flt=None):
        con.register(a, lance.dataset(f"{ACTIVE}/{n}/", storage_options=so)
                     .scanner(columns=cols, filter=flt).to_reader())

    sl = (f"CASE WHEN regexp_extract(lower(coalesce(person_linkedin_url,'')), '{_SLUG}', 1) <> '' "
          f"THEN 'linkedin.com/in/' || regexp_extract(lower(person_linkedin_url), '{_SLUG}', 1) END")

    reg("ent", "gtm_sam_entities",
        ["uei", "normalized_domain", "legal_business_name"], "is_subawardee")
    con.execute("CREATE TABLE e AS SELECT * FROM ent")
    reg("sub", "usaspending_subaward_canonical",
        ["subawardee_uei", "subaward_action_date", "subaward_amount"])
    con.execute(f"""CREATE TABLE s AS SELECT subawardee_uei AS uei,
        max(subaward_action_date) AS last_sub,
        coalesce(sum(subaward_amount) FILTER (
            WHERE subaward_action_date >= DATE '{WINDOW}'), 0) AS sub24
        FROM sub GROUP BY 1""")
    reg("txn", "usaspending_fpds_canonical_txn",
        ["recipient_uei", "action_date", "federal_action_obligation"],
        f"recipient_uei IS NOT NULL AND action_date >= DATE '{WINDOW}'")
    con.execute("""CREATE TABLE p24 AS SELECT recipient_uei AS uei,
        coalesce(sum(federal_action_obligation), 0) AS prime24 FROM txn GROUP BY 1""")
    con.execute(f"""CREATE TABLE aud AS
        SELECT e.uei, e.normalized_domain, e.legal_business_name,
               greatest(coalesce(s.sub24,0), coalesce(p.prime24,0)) AS money24
        FROM e LEFT JOIN s USING (uei) LEFT JOIN p24 p USING (uei)
        WHERE (s.last_sub >= DATE '{WINDOW}' OR coalesce(p.prime24,0) <> 0)
          AND greatest(coalesce(s.sub24,0), coalesce(p.prime24,0)) >= 1_000_000""")

    reg("fb", "firmographics_blitz", ["uei", "employee_size_band"])
    con.execute(f"""CREATE TABLE szsu AS SELECT DISTINCT uei FROM fb
        WHERE uei IS NOT NULL AND employee_size_band IS NOT NULL
          AND NOT ({_LARGE.format(c='employee_size_band')})""")
    con.execute("CREATE TABLE szau AS SELECT DISTINCT uei FROM fb "
                "WHERE uei IS NOT NULL AND employee_size_band IS NOT NULL")
    reg("pdl", "pdl_normalized_companies", ["normalized_domain", "employee_size_range"],
        "normalized_domain IS NOT NULL AND NOT is_generic_domain "
        "AND employee_size_range IS NOT NULL")
    con.execute(f"""CREATE TABLE szsd AS
        SELECT normalized_domain AS dom FROM pdl GROUP BY 1
        HAVING count(DISTINCT employee_size_range) = 1
           AND NOT ({_LARGE.format(c='min(employee_size_range)')})""")
    con.execute("CREATE TABLE szad AS SELECT DISTINCT normalized_domain AS dom FROM pdl")

    reg("ppl", "gtm_sam_people",
        ["sam_person_id", "uei", "first_name", "last_name", "best_title",
         "is_exec_officer_prime", "is_exec_officer_sub"])
    con.execute("CREATE TABLE p AS SELECT * FROM ppl WHERE uei IN (SELECT uei FROM aud)")
    reg("idn", "gtm_sam_person_identity",
        ["sam_person_id", "person_linkedin_url_norm", "match_source"])
    con.execute("CREATE TABLE i AS SELECT * FROM idn")
    reg("ph", "phone_resolutions", ["person_linkedin_url", "phone"], "phone IS NOT NULL")
    con.execute(f"CREATE TABLE vph AS SELECT DISTINCT {sl} AS s FROM ph "
                "WHERE person_linkedin_url IS NOT NULL")
    reg("rul", "gtm_sam_person_firm_emails", ["sam_person_id", "match_tier"])
    con.execute("CREATE TABLE r AS SELECT * FROM rul")
    reg("clay", "clay_find_people",
        ["record_id", "matched_job_title", "latest_experience_title"])
    con.execute("CREATE TABLE c AS SELECT * FROM clay")
    reg("bl", "blitz_find_people", ["record_id", "headline"])
    con.execute("CREATE TABLE b AS SELECT * FROM bl")
    reg("we", "work_emails", ["person_linkedin_url", "email"], "email IS NOT NULL")
    con.execute(f"""CREATE TABLE vem AS SELECT {sl} AS s, min(email) AS email
        FROM we WHERE person_linkedin_url IS NOT NULL GROUP BY 1""")

    # ruled-email enrolled cohort slugs (the email-first tranche)
    con.execute("""CREATE TABLE enr AS
        SELECT DISTINCT i.person_linkedin_url_norm AS slug
        FROM r JOIN i ON i.sam_person_id = r.sam_person_id
        WHERE r.match_tier IN ('tier1_full_name','tier2_first_initial_plus_surname',
                               'tier2_first_name_plus_surname_initial')
          AND i.person_linkedin_url_norm IS NOT NULL""")

    rows = con.execute(f"""
        WITH q AS (
            SELECT p.sam_person_id, p.uei, a.money24,
                   i.person_linkedin_url_norm AS slug,
                   a.normalized_domain AS dom, a.legal_business_name AS cname,
                   p.first_name, p.last_name,
                   (p.is_exec_officer_prime OR p.is_exec_officer_sub) AS officer,
                   lower(coalesce(p.best_title,'')) AS bt,
                   lower(coalesce(nullif(trim(c.matched_job_title),''),
                                  nullif(trim(c.latest_experience_title),''),
                                  nullif(trim(b.headline),''), '')) AS st,
                   vem.email AS vendor_email
            FROM p
            JOIN aud a USING (uei)
            JOIN i USING (sam_person_id)
            LEFT JOIN vph v ON v.s = i.person_linkedin_url_norm
            LEFT JOIN r ON r.sam_person_id = p.sam_person_id
            LEFT JOIN c ON c.record_id = regexp_extract(i.match_source, 'clay:([^;]+)', 1)
            LEFT JOIN b ON b.record_id = regexp_extract(i.match_source, 'blitz:([^;]+)', 1)
            LEFT JOIN vem ON vem.s = i.person_linkedin_url_norm
            WHERE i.person_linkedin_url_norm IS NOT NULL
              AND v.s IS NULL
              AND r.sam_person_id IS NULL
              AND ((p.uei IN (SELECT uei FROM szsu)
                    OR a.normalized_domain IN (SELECT dom FROM szsd))
                   OR (NOT (p.uei IN (SELECT uei FROM szau)
                            OR a.normalized_domain IN (SELECT dom FROM szad))
                       AND a.money24 < 100_000_000))),
        dm AS (
            SELECT * FROM q
            WHERE officer OR bt ~ '{_DM}' OR st ~ '{_DM}'),
        dedup AS (
            SELECT * FROM dm
            WHERE slug NOT IN (SELECT slug FROM enr)
            QUALIFY row_number() OVER (
                PARTITION BY slug ORDER BY money24 DESC, sam_person_id) = 1)
        SELECT sam_person_id, slug, dom, cname, first_name, last_name,
               vendor_email, money24, officer
        FROM dedup ORDER BY money24 DESC, sam_person_id
    """).fetchall()
    con.close()

    ops_found = _ops_found_slugs()
    stats = {"officer": 0, "with_vendor_email": 0, "excluded_ops_found": 0}
    contacts: list[dict] = []
    for (pid, slug, dom, cname, fn, ln, vemail, money24, officer) in rows:
        if slug in ops_found:
            stats["excluded_ops_found"] += 1
            continue
        c: dict = {"contact_id": pid, "country_code": "US",
                   "person_linkedin_url": f"https://www.{slug}"}
        if vemail:
            c["work_email"] = vemail
            stats["with_vendor_email"] += 1
        if dom:
            c["company_domain"] = dom
        if cname:
            c["company_name"] = cname
        if fn:
            c["first_name"] = fn
        if ln:
            c["last_name"] = ln
        if officer:
            stats["officer"] += 1
        contacts.append(c)

    if limit is not None:
        contacts = contacts[:limit]
    return contacts, stats


def _trigger_batch(contacts: list[dict], batch_label: str, force: bool, chunk_size: int) -> str:
    import requests

    key = os.environ.get("TRIGGER_SECRET_KEY")
    if not key:
        raise SystemExit("TRIGGER_SECRET_KEY not set (expected from doppler core-x/prd).")
    url = f"{TRIGGER_API_BASE}/api/v1/tasks/{RESOLVE_TASK_ID}/trigger"
    body = {"payload": {"contacts": contacts, "batchLabel": batch_label,
                        "priority": "low", "force": force, "chunkSize": chunk_size}}
    resp = requests.post(url, headers={"Authorization": f"Bearer {key}",
                                       "Content-Type": "application/json"},
                         json=body, timeout=60)
    if resp.status_code // 100 != 2:
        raise SystemExit(f"Trigger POST {resp.status_code}: {resp.text[:400]}")
    run_id = (resp.json() or {}).get("id")
    if not run_id:
        raise SystemExit(f"Trigger accepted but no run id: {resp.text[:400]}")
    return run_id


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--batch-size", type=int, default=1500)
    ap.add_argument("--chunk-size", type=int, default=250)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    contacts, stats = _build_contacts(args.limit)
    n = len(contacts)
    stamp = dt.date.today().isoformat()

    _rule("SAM DM LinkedIn queue → LeadMagic mobile finder (slug-deduped)")
    print(f"    task              = {RESOLVE_TASK_ID}", flush=True)
    print(f"    contacts          = {n:,}"
          + (f"  (canary --limit {args.limit})" if args.limit is not None else ""), flush=True)
    print(f"    ffata officers    = {stats['officer']:,}", flush=True)
    print(f"    with vendor email = {stats['with_vendor_email']:,}", flush=True)
    print(f"    excluded (already found in ops) = {stats['excluded_ops_found']:,}", flush=True)
    print(f"    force={args.force}  chunk_size={args.chunk_size}  batch_size={args.batch_size}", flush=True)

    if n == 0:
        print("\nNothing to enroll — idempotent no-op.", flush=True)
        return 0
    if args.dry_run:
        print("\n[dry-run] sample contacts:", flush=True)
        for c in contacts[:6]:
            print("      " + json.dumps(c, ensure_ascii=False), flush=True)
        print(f"\n[dry-run] would fire {(n + args.batch_size - 1) // args.batch_size} "
              f"trigger(s). No trigger sent.", flush=True)
        return 0

    _rule(f"trigger {RESOLVE_TASK_ID}")
    run_ids: list[str] = []
    n_batches = (n + args.batch_size - 1) // args.batch_size
    for idx0 in range(0, n, args.batch_size):
        batch = contacts[idx0:idx0 + args.batch_size]
        idx = idx0 // args.batch_size
        label = f"sam-dm-li-mobile-{stamp}" + (f"#{idx}" if n_batches > 1 else "")
        run_id = _trigger_batch(batch, label, args.force, args.chunk_size)
        run_ids.append(run_id)
        print(f"    batch {idx + 1}/{n_batches} ({len(batch):,}) → run {run_id} [{label}]", flush=True)

    _rule("enrolled")
    print(f"    {n:,} contacts across {len(run_ids)} run(s); results land async in "
          f"ops.phone_resolutions (contact_id = sam_person_id).", flush=True)
    print("    run ids: " + ", ".join(run_ids), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
