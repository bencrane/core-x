"""Enroller — dmu work emails → MillionVerifier (mv-verify-resolve rail).

Cohort: the 7,030 FOUND work emails in icypeas_dmu_work_emails (Icypeas Email
Finder over the identity-unmatched, finder-exhausted dmu people). The email is
already in hand, so this is verification-only — the standalone MV rail
(mv-verify-resolve → verify_mv_standalone → ops.email_verifications), never a
finder.

contact_id = sam_person_id (mart vocabulary; rows join straight back to
gtm_sam_people). Already-verified contacts are skipped by the worker unless
--force. Dry-run is the DEFAULT — nothing fires without --apply.

RUN:
    doppler run -- python3 pipelines/enrichment_mv/enroll_dmu_work_emails_verify.py           # preview
    doppler run -- python3 pipelines/enrichment_mv/enroll_dmu_work_emails_verify.py --apply    # fire
    [--limit N] [--batch-size 2000] [--chunk-size 250] [--force]
"""
from __future__ import annotations

import argparse
import os
import sys

import lance

SRC = "s3://data-sink/active/icypeas_dmu_work_emails/"
RESOLVE_TASK_ID = "mv-verify-resolve"
TRIGGER_API_BASE = os.environ.get("TRIGGER_API_URL", "https://api.trigger.dev").rstrip("/")
SOURCE = "icypeas_dmu_work_emails"


def _r2_storage_options() -> dict[str, str]:
    endpoint = os.environ.get("R2_ENDPOINT")
    account_id = os.environ.get("R2_ACCOUNT_ID")
    if not endpoint and account_id:
        endpoint = f"https://{account_id}.r2.cloudflarestorage.com"
    if not endpoint:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {
        "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
        "endpoint": endpoint,
        "region": "auto",
    }


def _build_contacts(limit: int | None) -> tuple[list[dict], dict]:
    import duckdb

    so = _r2_storage_options()
    ds = lance.dataset(SRC, storage_options=so)
    con = duckdb.connect()
    con.register("s", ds.scanner(
        columns=["sam_person_id", "email", "best_domain", "email_certainty"],
        filter="email IS NOT NULL").to_reader())
    rows = con.execute("""
        SELECT sam_person_id, lower(trim(email)) AS email, best_domain, email_certainty
        FROM s WHERE email IS NOT NULL AND email LIKE '%@%'
        ORDER BY sam_person_id""").fetchall()
    contacts = [{"contact_id": r[0], "email": r[1],
                 "company_domain": r[2], "source": SOURCE} for r in rows]
    if limit is not None:
        contacts = contacts[:limit]
    stats = {
        "ultra_sure": sum(1 for r in rows if r[3] == "ultra_sure"),
        "probable": sum(1 for r in rows if r[3] == "probable"),
        "with_domain": sum(1 for c in contacts if c["company_domain"]),
    }
    return contacts, stats


def _trigger_batch(contacts: list[dict], batch_label: str, force: bool, chunk_size: int) -> str:
    import requests

    key = os.environ.get("TRIGGER_SECRET_KEY")
    if not key:
        raise SystemExit("TRIGGER_SECRET_KEY not set (expected from doppler).")
    url = f"{TRIGGER_API_BASE}/api/v1/tasks/{RESOLVE_TASK_ID}/trigger"
    body = {"payload": {"contacts": contacts, "batchLabel": batch_label,
                        "force": force, "chunkSize": chunk_size}}
    resp = requests.post(url, headers={"Authorization": f"Bearer {key}",
                                       "Content-Type": "application/json"},
                         json=body, timeout=60)
    if resp.status_code // 100 != 2:
        raise SystemExit(f"Trigger POST {resp.status_code}: {resp.text[:400]}")
    run_id = (resp.json() or {}).get("id")
    if not run_id:
        raise SystemExit(f"Trigger accepted but no run id: {resp.text[:400]}")
    return run_id


def _rule(msg: str) -> None:
    print("=" * 78 + f"\n{msg}\n" + "=" * 78, flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="actually fire triggers; without it this is a dry-run")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--batch-size", type=int, default=2000)
    ap.add_argument("--chunk-size", type=int, default=250)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    contacts, stats = _build_contacts(args.limit)
    n = len(contacts)
    stamp = "2026-07-05"

    _rule("dmu work emails → MillionVerifier (mv-verify-resolve)")
    print(f"    task           = {RESOLVE_TASK_ID}", flush=True)
    print(f"    contacts       = {n:,}"
          + (f"  (--limit {args.limit})" if args.limit is not None else ""), flush=True)
    print(f"    certainty      = ultra_sure {stats['ultra_sure']:,} | probable {stats['probable']:,}", flush=True)
    print(f"    with domain    = {stats['with_domain']:,} / {n:,}", flush=True)
    print(f"    force={args.force}  chunk_size={args.chunk_size}  batch_size={args.batch_size}", flush=True)

    if n == 0:
        print("\nNothing to enroll — idempotent no-op.", flush=True)
        return 0
    if not args.apply:
        print("\n[dry-run] sample contacts:", flush=True)
        import json
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
        label = f"dmu-work-emails-mv-{stamp}" + (f"#{idx}" if n_batches > 1 else "")
        run_id = _trigger_batch(batch, label, args.force, args.chunk_size)
        run_ids.append(run_id)
        print(f"    batch {idx + 1}/{n_batches} ({len(batch):,}) → run {run_id} [{label}]", flush=True)

    _rule("enrolled")
    print(f"    {n:,} contacts across {len(run_ids)} run(s); results land in "
          f"ops.email_verifications (contact_id = sam_person_id).", flush=True)
    print("    run ids: " + ", ".join(run_ids), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
