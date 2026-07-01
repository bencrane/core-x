#!/usr/bin/env python3
"""Enroller — resolve DSBS-POC mobile phones via the LeadMagic mobile-finder (trigger.dev + Modal).

Builds the mobile-target cohort from active/dsbs_poc_people (still needs a mobile) and triggers the
existing, deployed `leadmagic-phone-finder-resolve` Trigger.dev task (LeadMagic /mobile-finder; no
gateway, misses free, 5 credits/hit). Results land in ops.phone_resolutions keyed contact_id =
person_id — the same loop-closure as the work-email rail — then flow through
materialize_phone_resolutions → active/phone_resolutions and flip dsbs_poc_people.has_mobile.

EMAIL HINT (LeadMagic: profile_url + email = highest match rate). Per person, exactly one email is
attached, sourced by identity-correctness so LeadMagic is never fed a mismatched address:
    1. resolved work_email (from the Icypeas→LeadMagic cascade, now in dsbs_poc_people.work_email)
       → work_email                                          [applies to any poc_type once resolved]
    2. else, ONLY for poc_type in (contact_person, both): the DSBS firm email (belongs to the
       contact_person) → personal_email if a personal-provider domain, else work_email
    3. else (current_principal without a resolved email): profile_url only — the firm email is the
       contact_person's, not theirs, so it is deliberately withheld.

CONTACT (leadmagic-phone-finder-resolve contract, src/trigger/leadmagic_phone_finder.ts):
    {contact_id=person_id, person_linkedin_url, work_email?|personal_email?, company_domain,
     country_code='US', first_name?, last_name?, company_name?}

IDEMPOTENT. The worker skips contacts already resolved to a FOUND phone by ANY vendor (force=False).

RUN:
    doppler run -p core-x -c prd -- uv run --no-project \\
        --with pylance --with duckdb --with pyarrow --with requests \\
        python3 pipelines/enrichment_leadmagic/enroll_dsbs_poc_mobile.py \\
        [--dry-run] [--limit N] [--batch-size 1500] [--chunk-size 250] [--force]
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
POC_PEOPLE_URI = os.environ.get("DSBS_POC_PEOPLE_URI", f"{ACTIVE}/dsbs_poc_people/")
DSBS_FIRMS_URI = os.environ.get("SBA_DSBS_FIRMS_URI", f"{ACTIVE}/sba_dsbs_certified_firms/")

TRIGGER_API_BASE = os.environ.get("TRIGGER_API_URL", "https://api.trigger.dev").rstrip("/")
RESOLVE_TASK_ID = "leadmagic-phone-finder-resolve"

PERSONAL_PROVIDERS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com", "icloud.com", "msn.com",
    "live.com", "comcast.net", "me.com", "sbcglobal.net", "ymail.com", "protonmail.com",
    "att.net", "verizon.net", "bellsouth.net",
}


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
    con.execute("SET memory_limit='6GB'; PRAGMA threads=4;")
    con.register("poc", lance.dataset(POC_PEOPLE_URI, storage_options=so).scanner(
        columns=["person_id", "uei", "poc_type", "has_mobile", "has_work_email", "work_email",
                 "best_domain", "legal_business_name", "person_full_name",
                 "person_linkedin_url"]).to_reader())
    con.register("fm", lance.dataset(DSBS_FIRMS_URI, storage_options=so).scanner(
        columns=["uei", "email"]).to_reader())
    con.execute("CREATE TABLE pocp AS SELECT * FROM poc")
    con.execute("""CREATE TABLE fmail AS SELECT uei, lower(trim(email)) email
                   FROM fm WHERE email IS NOT NULL AND trim(email) <> '' AND email LIKE '%@%'""")

    # One row per NEW-mobile-needing person_id, carrying the single best email hint. hint_rank:
    #   0 = a resolved work_email  ·  1 = firm email for contact_person/both  ·  2 = no hint.
    rows = con.execute("""
        WITH base AS (
            SELECT
                p.person_id, p.uei, p.poc_type,
                lower(nullif(trim(p.best_domain), ''))     AS company_domain,
                nullif(trim(p.legal_business_name), '')    AS company_name,
                nullif(trim(p.person_full_name), '')       AS full_name,
                nullif(trim(p.person_linkedin_url), '')    AS person_linkedin_url,
                nullif(trim(p.work_email), '')             AS resolved_email,
                fmail.email                                AS firm_email
            FROM pocp p
            LEFT JOIN fmail ON fmail.uei = p.uei
            WHERE NOT p.has_mobile
              AND nullif(trim(p.person_linkedin_url), '') IS NOT NULL
              AND person_id IS NOT NULL
        ),
        hinted AS (
            SELECT *,
                CASE
                    WHEN resolved_email IS NOT NULL THEN 'work:' || resolved_email
                    WHEN poc_type IN ('contact_person', 'both') AND firm_email IS NOT NULL
                        THEN 'firm:' || firm_email
                    ELSE NULL
                END AS hint,
                CASE
                    WHEN resolved_email IS NOT NULL THEN 0
                    WHEN poc_type IN ('contact_person', 'both') AND firm_email IS NOT NULL THEN 1
                    ELSE 2
                END AS hint_rank
            FROM base
        ),
        ranked AS (
            SELECT *, row_number() OVER (PARTITION BY person_id ORDER BY hint_rank, uei) AS rn
            FROM hinted
        )
        SELECT person_id, full_name, company_domain, company_name, person_linkedin_url, hint
        FROM ranked WHERE rn = 1
        ORDER BY person_id
    """).fetchall()
    con.close()

    stats = {"resolved_work": 0, "firm_work": 0, "firm_personal": 0, "linkedin_only": 0}
    contacts: list[dict] = []
    for person_id, full_name, company_domain, company_name, li, hint in rows:
        if not li:
            continue
        c: dict = {"contact_id": person_id, "person_linkedin_url": li, "country_code": "US"}
        if company_domain:
            c["company_domain"] = company_domain
        if company_name:
            c["company_name"] = company_name
        if full_name:
            parts = full_name.split()
            if parts:
                c["first_name"] = parts[0]
                if len(parts) > 1:
                    c["last_name"] = " ".join(parts[1:])
        # email hint → work_email or personal_email
        if hint and hint.startswith("work:"):
            c["work_email"] = hint[5:]
            stats["resolved_work"] += 1
        elif hint and hint.startswith("firm:"):
            addr = hint[5:]
            dom = addr.split("@", 1)[1] if "@" in addr else ""
            if dom in PERSONAL_PROVIDERS:
                c["personal_email"] = addr
                stats["firm_personal"] += 1
            else:
                c["work_email"] = addr
                stats["firm_work"] += 1
        else:
            stats["linkedin_only"] += 1
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
        raise SystemExit(f"Trigger POST {resp.status_code} for {RESOLVE_TASK_ID}: {resp.text[:400]}")
    run_id = (resp.json() or {}).get("id")
    if not run_id:
        raise SystemExit(f"Trigger accepted but returned no run id: {resp.text[:400]}")
    return run_id


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="build + sample the cohort; trigger nothing")
    ap.add_argument("--limit", type=int, default=None, help="canary: enroll only the first N contacts")
    ap.add_argument("--batch-size", type=int, default=1500, help="contacts per resolve-task trigger")
    ap.add_argument("--chunk-size", type=int, default=250, help="contacts per Modal worker (resolve payload)")
    ap.add_argument("--force", action="store_true", help="re-resolve contacts already found by any vendor")
    args = ap.parse_args()

    contacts, stats = _build_contacts(args.limit)
    n = len(contacts)
    stamp = dt.date.today().isoformat()
    with_email = sum(1 for c in contacts if c.get("work_email") or c.get("personal_email"))

    _rule("DSBS-POC → LeadMagic mobile finder (profile_url + email hint)")
    print(f"    task            = {RESOLVE_TASK_ID}", flush=True)
    print(f"    contacts        = {n:,}"
          + (f"  (canary --limit {args.limit})" if args.limit is not None else ""), flush=True)
    print(f"    with linkedin   = {sum(1 for c in contacts if c.get('person_linkedin_url')):,} / {n:,}", flush=True)
    print(f"    with email hint = {with_email:,} / {n:,}", flush=True)
    print(f"      resolved work_email = {stats['resolved_work']:,}", flush=True)
    print(f"      firm work_email     = {stats['firm_work']:,}", flush=True)
    print(f"      firm personal_email = {stats['firm_personal']:,}", flush=True)
    print(f"      linkedin only       = {stats['linkedin_only']:,}", flush=True)
    print(f"    force           = {args.force}   chunk_size = {args.chunk_size}   batch_size = {args.batch_size}", flush=True)

    if n == 0:
        print("\nNothing to enroll — idempotent no-op.", flush=True)
        return 0

    if args.dry_run:
        print("\n[dry-run] sample contacts (exact trigger payload shape):", flush=True)
        for c in contacts[:6]:
            print("      " + json.dumps(c, ensure_ascii=False), flush=True)
        print(f"\n[dry-run] would fire {(n + args.batch_size - 1) // args.batch_size} trigger(s) to "
              f"{RESOLVE_TASK_ID}. No trigger sent.", flush=True)
        return 0

    _rule(f"trigger {RESOLVE_TASK_ID}")
    run_ids: list[str] = []
    n_batches = (n + args.batch_size - 1) // args.batch_size
    for i in range(0, n, args.batch_size):
        batch = contacts[i:i + args.batch_size]
        idx = i // args.batch_size
        label = f"dsbs-poc-mobile-{stamp}" + (f"#{idx}" if n_batches > 1 else "")
        run_id = _trigger_batch(batch, label, args.force, args.chunk_size)
        run_ids.append(run_id)
        print(f"    batch {idx + 1}/{n_batches}  ({len(batch):,} contacts)  →  run {run_id}  [{label}]", flush=True)

    _rule("enrolled")
    print(f"    {n:,} contacts across {len(run_ids)} run(s). Results land async in "
          f"ops.phone_resolutions (contact_id=person_id).", flush=True)
    print("    run ids: " + ", ".join(run_ids), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
