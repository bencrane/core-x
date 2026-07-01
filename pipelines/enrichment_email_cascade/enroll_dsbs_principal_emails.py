#!/usr/bin/env python3
"""Enroller — resolve current-principal DSBS-POC work emails via the Icypeas → LeadMagic cascade.

Builds the contact cohort from active/dsbs_poc_people and triggers the existing, deployed
`enrichment-email-cascade-resolve` Trigger.dev task (Icypeas → LeadMagic → MillionVerifier; Blitz
removed). Results land in ops.email_resolutions keyed on contact_id = person_id — the same
loop-closure convention as the phone rail, so a downstream materializer rolls them into Lance and
they join active/people on person_id.

WHY. These DSBS current-principals had the DSBS firm email suppressed from their mobile lookup —
that address belongs to the firm's contact_person, not the principal. Resolving each principal's
OWN verified work email both delivers a work email AND, downstream, gives the LeadMagic mobile
finder a VALID `work_email` hint for their mobile lookup (the "profile_url + email = highest match
rate" path) instead of a mismatched-identity address.

COHORT (active/dsbs_poc_people):
    poc_type = 'current_principal'      -- the suppressed-email cohort
    AND NOT has_mobile                  -- still in the mobile-enrichment target
    AND NOT has_work_email              -- no work email yet (worker also skips 'verified' live)
    AND best_domain IS NOT NULL         -- company_domain for the finder
    AND person_full_name has 2+ tokens  -- first + last for the finder
  Deduped to ONE contact per person_id (a principal at multiple firms → one deterministic firm).

CONTACT (the resolve-task contract, src/trigger/enrichment_email_cascade.ts):
    {contact_id=person_id, first_name, last_name, company_domain=best_domain,
     company_name=legal_business_name, person_linkedin_url}

IDEMPOTENT. The worker skips any contact already 'verified' (force=False). Safe to re-run; a
canary batch is not re-spent on the full run.

RUN:
    doppler run -p core-x -c prd -- uv run --no-project \\
        --with pylance --with duckdb --with pyarrow --with requests \\
        python3 pipelines/enrichment_email_cascade/enroll_dsbs_principal_emails.py \\
        [--dry-run] [--limit N] [--batch-size 1000] [--chunk-size 250] [--force]
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

TRIGGER_API_BASE = os.environ.get("TRIGGER_API_URL", "https://api.trigger.dev").rstrip("/")
RESOLVE_TASK_ID = "enrichment-email-cascade-resolve"


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


def _build_contacts(limit: int | None) -> list[dict]:
    so = _storage_options()
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'; PRAGMA threads=4;")
    con.register("poc", lance.dataset(POC_PEOPLE_URI, storage_options=so).scanner(
        columns=["person_id", "uei", "poc_type", "has_mobile", "has_work_email",
                 "best_domain", "legal_business_name", "person_full_name",
                 "person_linkedin_url"]).to_reader())
    con.execute("CREATE TABLE pocp AS SELECT * FROM poc")

    rows = con.execute("""
        WITH ranked AS (
            SELECT
                person_id,
                nullif(trim(person_full_name), '')    AS full_name,
                lower(nullif(trim(best_domain), ''))  AS company_domain,
                nullif(trim(legal_business_name), '') AS company_name,
                nullif(trim(person_linkedin_url), '') AS person_linkedin_url,
                row_number() OVER (PARTITION BY person_id ORDER BY uei) AS rn
            FROM pocp
            WHERE poc_type = 'current_principal'
              AND NOT has_mobile
              AND NOT has_work_email
              AND nullif(trim(best_domain), '') IS NOT NULL
              AND trim(person_full_name) LIKE '% %'
              AND person_id IS NOT NULL
        )
        SELECT person_id, full_name, company_domain, company_name, person_linkedin_url
        FROM ranked
        WHERE rn = 1
        ORDER BY person_id
    """).fetchall()
    con.close()

    contacts: list[dict] = []
    for person_id, full_name, company_domain, company_name, li in rows:
        parts = (full_name or "").split()
        if len(parts) < 2 or not company_domain:
            continue  # defensive; the SQL already guarantees both
        first_name, last_name = parts[0], " ".join(parts[1:])
        c = {
            "contact_id": person_id,          # == person_id → closes the loop to active/people
            "first_name": first_name,
            "last_name": last_name,
            "company_domain": company_domain,
        }
        if company_name:
            c["company_name"] = company_name
        if li:
            c["person_linkedin_url"] = li
        contacts.append(c)

    if limit is not None:
        contacts = contacts[:limit]
    return contacts


def _trigger_batch(contacts: list[dict], batch_label: str, force: bool, chunk_size: int) -> str:
    import requests

    key = os.environ.get("TRIGGER_SECRET_KEY")
    if not key:
        raise SystemExit("TRIGGER_SECRET_KEY not set (expected from doppler core-x/prd).")
    url = f"{TRIGGER_API_BASE}/api/v1/tasks/{RESOLVE_TASK_ID}/trigger"
    body = {"payload": {"contacts": contacts, "batchLabel": batch_label,
                        "force": force, "chunkSize": chunk_size}}
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
    ap.add_argument("--batch-size", type=int, default=1000, help="contacts per resolve-task trigger")
    ap.add_argument("--chunk-size", type=int, default=250, help="contacts per Modal worker (resolve payload)")
    ap.add_argument("--force", action="store_true", help="re-resolve contacts already marked verified")
    args = ap.parse_args()

    contacts = _build_contacts(args.limit)
    n = len(contacts)
    stamp = dt.date.today().isoformat()

    _rule(f"DSBS current-principal → work-email cascade (Icypeas → LeadMagic → MillionVerifier)")
    print(f"    task            = {RESOLVE_TASK_ID}", flush=True)
    print(f"    contacts        = {n:,}"
          + (f"  (canary --limit {args.limit})" if args.limit is not None else ""), flush=True)
    print(f"    with domain     = {sum(1 for c in contacts if c.get('company_domain')):,} / {n:,}", flush=True)
    print(f"    with company    = {sum(1 for c in contacts if c.get('company_name')):,} / {n:,}", flush=True)
    print(f"    with linkedin   = {sum(1 for c in contacts if c.get('person_linkedin_url')):,} / {n:,}", flush=True)
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
        label = f"dsbs-principal-emails-{stamp}" + (f"#{idx}" if n_batches > 1 else "")
        run_id = _trigger_batch(batch, label, args.force, args.chunk_size)
        run_ids.append(run_id)
        print(f"    batch {idx + 1}/{n_batches}  ({len(batch):,} contacts)  →  run {run_id}  "
              f"[{label}]", flush=True)

    _rule("enrolled")
    print(f"    {n:,} contacts across {len(run_ids)} run(s). Results land async in "
          f"ops.email_resolutions (contact_id=person_id).", flush=True)
    print("    run ids: " + ", ".join(run_ids), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
