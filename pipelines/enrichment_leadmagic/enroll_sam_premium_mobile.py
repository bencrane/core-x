#!/usr/bin/env python3
"""Enroller — SAM-mart premium tranche → LeadMagic mobile finder.

Cohort (operator-directed 2026-07-04): the DSBS email→mobile waterfall queue
(gtm_sam_person_firm_emails rulings, no owned mobile), constrained to:

    money24 = greatest(sub-$ 24m, prime-$ 24m) >= $1M            (either side)
    ∧ any firm-email ruling (all tiers — each is already the unique best
      match at its entity; tier carried as confidence metadata only)
    ∧ ( confirmed <500 employees (blitz uei-direct ∨ PDL unique-domain)
        OR size UNKNOWN ∧ money24 < $100M )

prime-$ 24m is the SUM-safe windowed measure: L1 spine federal_action_obligation
summed over action_date >= window (never award-state snapshots).

Payload per contact (leadmagic-phone-finder-resolve contract — worker accepts
any of profile_url / work_email / personal_email):
    contact_id           = sam_person_id  (mart vocabulary; new to
                           ops.phone_resolutions, joins back to gtm_sam_people
                           directly — person_id doctrine respected)
    work_email           = the ruled DSBS email (verbatim)   [personal_email
                           instead when the domain is a personal provider]
    person_linkedin_url  = from gtm_sam_person_identity where resolved
                           (email-first is fine; LI attached where owned)
    company_domain / company_name / first_name / last_name from the mart.

Results land in ops.phone_resolutions (misses free, 5 credits/hit) and flow
through materialize_phone_resolutions → active/phone_resolutions unchanged.

RUN (dry-run is the DEFAULT — nothing fires without --apply):
    doppler run -- python3 pipelines/enrichment_leadmagic/enroll_sam_premium_mobile.py           # preview
    doppler run -- python3 pipelines/enrichment_leadmagic/enroll_sam_premium_mobile.py --apply   # fire
    [--limit N] [--batch-size 1500] [--chunk-size 250] [--force]
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

PERSONAL_PROVIDERS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com", "msn.com",
    "live.com", "comcast.net", "me.com", "sbcglobal.net", "ymail.com", "protonmail.com",
    "att.net", "verizon.net", "bellsouth.net",
}

_SLUG = r"linkedin\.com/in/([^/?#]+)"
_LARGE = ("contains({c},'501') OR contains({c},'1001') OR contains({c},'5001') "
          "OR contains({c},'10001') OR contains({c},'1,001')")


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


def _build_contacts(limit: int | None) -> tuple[list[dict], dict]:
    so = _storage_options()
    con = duckdb.connect()
    con.execute("SET memory_limit='16GB'; PRAGMA threads=8;")

    def reg(a, n, cols, flt=None):
        con.register(a, lance.dataset(f"{ACTIVE}/{n}/", storage_options=so)
                     .scanner(columns=cols, filter=flt).to_reader())

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

    # ALL rulings are eligible (operator decision 2026-07-05): the builder's
    # unambiguity gate already made each ruling the unique best match at its
    # entity. match_tier rides along as recorded confidence, never as a wall.
    reg("rul", "gtm_sam_person_firm_emails",
        ["sam_person_id", "uei", "email", "match_tier"])
    con.execute("CREATE TABLE r AS SELECT * FROM rul WHERE uei IN (SELECT uei FROM aud)")
    reg("ppl", "gtm_sam_people", ["sam_person_id", "first_name", "last_name"])
    con.execute("CREATE TABLE pp AS SELECT * FROM ppl "
                "WHERE sam_person_id IN (SELECT sam_person_id FROM r)")
    reg("idn", "gtm_sam_person_identity", ["sam_person_id", "person_linkedin_url_norm"])
    con.execute("CREATE TABLE i AS SELECT * FROM idn")
    reg("ph", "phone_resolutions", ["person_linkedin_url", "phone"], "phone IS NOT NULL")
    con.execute(f"""CREATE TABLE vph AS SELECT DISTINCT
        CASE WHEN regexp_extract(lower(coalesce(person_linkedin_url,'')), '{_SLUG}', 1) <> ''
             THEN 'linkedin.com/in/' || regexp_extract(lower(person_linkedin_url), '{_SLUG}', 1)
        END AS s FROM ph WHERE person_linkedin_url IS NOT NULL""")

    reg("fb", "firmographics_blitz", ["uei", "employee_size_band"])
    con.execute(f"""CREATE TABLE sz_small_u AS SELECT DISTINCT uei FROM fb
        WHERE uei IS NOT NULL AND employee_size_band IS NOT NULL
          AND NOT ({_LARGE.format(c='employee_size_band')})""")
    con.execute(f"""CREATE TABLE sz_any_u AS SELECT DISTINCT uei FROM fb
        WHERE uei IS NOT NULL AND employee_size_band IS NOT NULL""")
    reg("pdl", "pdl_normalized_companies", ["normalized_domain", "employee_size_range"],
        "normalized_domain IS NOT NULL AND NOT is_generic_domain "
        "AND employee_size_range IS NOT NULL")
    con.execute(f"""CREATE TABLE sz_small_d AS
        SELECT normalized_domain AS dom FROM pdl GROUP BY 1
        HAVING count(DISTINCT employee_size_range) = 1
           AND NOT ({_LARGE.format(c='min(employee_size_range)')})""")
    con.execute("""CREATE TABLE sz_any_d AS
        SELECT DISTINCT normalized_domain AS dom FROM pdl""")

    rows = con.execute("""
        WITH q AS (
            SELECT r.sam_person_id, r.uei, r.email, a.money24,
                   a.normalized_domain AS dom, a.legal_business_name AS company_name,
                   pp.first_name, pp.last_name,
                   i.person_linkedin_url_norm AS li_slug,
                   (r.uei IN (SELECT uei FROM sz_small_u)
                    OR a.normalized_domain IN (SELECT dom FROM sz_small_d)) AS lt500,
                   (r.uei IN (SELECT uei FROM sz_any_u)
                    OR a.normalized_domain IN (SELECT dom FROM sz_any_d)) AS size_known
            FROM r
            JOIN aud a USING (uei)
            LEFT JOIN pp USING (sam_person_id)
            LEFT JOIN i USING (sam_person_id)
            LEFT JOIN vph v ON v.s = i.person_linkedin_url_norm
            WHERE v.s IS NULL)
        SELECT sam_person_id, email, dom, company_name, first_name, last_name,
               li_slug, money24, lt500, size_known
        FROM q
        WHERE lt500 OR (NOT size_known AND money24 < 100_000_000)
        ORDER BY money24 DESC, sam_person_id
    """).fetchall()
    con.close()

    stats = {"lt500": 0, "size_unknown": 0, "with_li": 0,
             "work_email": 0, "personal_email": 0}
    contacts: list[dict] = []
    for (pid, email, dom, cname, fn, ln, li, money24, lt500, size_known) in rows:
        c: dict = {"contact_id": pid, "country_code": "US"}
        addr = (email or "").strip()
        edom = addr.split("@", 1)[1].lower() if "@" in addr else ""
        if edom in PERSONAL_PROVIDERS:
            c["personal_email"] = addr
            stats["personal_email"] += 1
        else:
            c["work_email"] = addr
            stats["work_email"] += 1
        if li:
            c["person_linkedin_url"] = f"https://www.{li}"
            stats["with_li"] += 1
        if dom:
            c["company_domain"] = dom
        if cname:
            c["company_name"] = cname
        if fn:
            c["first_name"] = fn
        if ln:
            c["last_name"] = ln
        stats["lt500" if lt500 else "size_unknown"] += 1
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
    import requests as rq
    resp = rq.post(url, headers={"Authorization": f"Bearer {key}",
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
    ap.add_argument("--apply", action="store_true",
                    help="actually fire triggers; without it this is a dry-run")
    ap.add_argument("--dry-run", action="store_true",
                    help="(default behavior; kept for compatibility)")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--batch-size", type=int, default=1500)
    ap.add_argument("--chunk-size", type=int, default=250)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    contacts, stats = _build_contacts(args.limit)
    n = len(contacts)
    stamp = dt.date.today().isoformat()

    _rule("SAM premium tranche → LeadMagic mobile finder (email-first, LI where owned)")
    print(f"    task           = {RESOLVE_TASK_ID}", flush=True)
    print(f"    contacts       = {n:,}"
          + (f"  (canary --limit {args.limit})" if args.limit is not None else ""), flush=True)
    print(f"    <500 employees = {stats['lt500']:,}   size-unknown(<100M) = {stats['size_unknown']:,}", flush=True)
    print(f"    work_email     = {stats['work_email']:,}   personal_email = {stats['personal_email']:,}", flush=True)
    print(f"    with linkedin  = {stats['with_li']:,} / {n:,}", flush=True)
    print(f"    force={args.force}  chunk_size={args.chunk_size}  batch_size={args.batch_size}", flush=True)

    if n == 0:
        print("\nNothing to enroll — idempotent no-op.", flush=True)
        return 0
    if not args.apply:
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
        label = f"sam-premium-mobile-{stamp}" + (f"#{idx}" if n_batches > 1 else "")
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
