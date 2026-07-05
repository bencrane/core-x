#!/usr/bin/env python3
"""Land a SUPPLIED work-email worklist (person_id + Work Email CSV) into the HQX email SoR arm
ops.email_verifications, so materialize_work_emails.py projects it into active/work_emails on its
next run — AND so the MillionVerifier pass (verify_mv_standalone.py::run_mv_verify) can pick the
cohort up by ``source`` and upsert real verdicts in place, keyed by contact_id.

WHY THIS ARM (not a direct Lance write). active/work_emails is a FULL-OVERWRITE projection of
ops.email_resolutions ∪ ops.email_verifications (most-recent-wins per contact_id). A direct append
to the Lance would be wiped on the next materialize run. ops.email_verifications is the
"supplied email, no finder" arm — the exact shape of a supplied worklist.

KEY ALIGNMENT. contact_id = CSV person_id = active/people.person_id (verified 100% overlap on this
cohort) → materialize_work_emails projects contact_id→person_id, so work_emails joins the spine
cleanly. person_id is the spine's native key (heterogeneous: uuid | hex64 | dex_target:…); it is
carried VERBATIM, never re-derived.

PRE-VERIFICATION STATE. These emails are SUPPLIED, not yet MV-validated. The arm's status CHECK is
IN ('verified','risky','unresolved'); there is no 'pending' slot. Per the fail-closed rubric a
supplied-but-unverified email lands 'unresolved' with mv_* NULL — an email on record with no
deliverability verdict YET. The MV pass re-processes it (its skip-set skips only 'verified') and
upserts the true verdict ON CONFLICT (contact_id) DO UPDATE.

REGRESSION GUARD. A contact already 'verified' in ops.email_resolutions is EXCLUDED: landing an
'unresolved' supplied row at resolved_at=now() would outrank the verified finder email in the
most-recent-wins master. We never regress a verified contact with an unverified supplied email.
(Contacts 'unresolved' in email_resolutions are NOT excluded — the supplied email is an improvement.)

IDEMPOTENT. Transactional: DELETE WHERE source=<cohort> (removes only this cohort's prior rows),
then INSERT ... ON CONFLICT (contact_id) DO NOTHING (never clobbers a fresher row owned by another
source). Re-runs converge.

RUN:
    doppler run -p core-x -c prd -- uv run --no-project --with 'psycopg[binary]' \
        python3 pipelines/work_emails/land_supplied_work_emails.py \
        --csv "/path/to/work-emails.csv" [--source supplied_worklist_2026-07-01] [--dry-run]
"""
from __future__ import annotations

import argparse
import csv as csvmod
import os
import re
from collections import Counter

import psycopg
from psycopg.types.json import Jsonb

DEFAULT_SOURCE = "supplied_worklist_2026-07-01"

_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")


def _normalize_domain(raw: str | None) -> str | None:
    """Canonical company-domain rule (== companies.normalized_domain / MV worker)."""
    d = (raw or "").strip().lower()
    d = _SCHEME.sub("", d)
    d = _WWW.sub("", d)
    d = d.split("/", 1)[0].rstrip(".")
    return d or None


def _dsn(key: str) -> str:
    dsn = os.environ[key]
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


INSERT_SQL = """
INSERT INTO ops.email_verifications
    (contact_id, email, verification_status, mv_resultcode, mv_result, mv_quality, mv_subresult,
     source, company_domain, mv_raw, attempts, batch_label, resolved_at, person_linkedin_url)
VALUES
    (%(contact_id)s, %(email)s, 'unresolved', NULL, NULL, NULL, NULL,
     %(source)s, %(company_domain)s, NULL, %(attempts)s, %(batch_label)s, now(), %(person_linkedin_url)s)
ON CONFLICT (contact_id) DO NOTHING
"""


_MAPPED_COLS = {"person_id", "Work Email", "domain", "person_linkedin_url"}


def _read_csv(path: str, source: str) -> list[dict]:
    """One row per contact_id (person_id→email is 1:1 on this feed). Verbatim email + linkedin;
    company_domain normalized, falling back to the email's own domain when the CSV domain is blank.
    Every non-mapped, non-empty CSV column rides verbatim in the attempts stage as ``payload`` —
    supplied feeds keep their full provider payload (attempts is carried losslessly to Lance)."""
    out: dict[str, dict] = {}
    with open(path, newline="", encoding="utf-8-sig") as fh:
        for r in csvmod.DictReader(fh):
            pid = (r.get("person_id") or "").strip()
            email = (r.get("Work Email") or "").strip()
            if not pid or not email:
                continue
            dom = _normalize_domain(r.get("domain")) or _normalize_domain(email.split("@", 1)[-1])
            li = (r.get("person_linkedin_url") or "").strip() or None
            stage: dict = {"stage": "supplied", "source": source}
            payload = {k: v for k, v in r.items() if k not in _MAPPED_COLS and (v or "").strip()}
            if payload:
                stage["payload"] = payload
            out.setdefault(pid, {  # first row wins; feed is 1:1 so this is deterministic
                "contact_id": pid,
                "email": email,
                "company_domain": dom,
                "person_linkedin_url": li,
                "source": source,
                "batch_label": source,
                "attempts": Jsonb([stage]),
            })
    return list(out.values())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--source", default=DEFAULT_SOURCE)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rows = _read_csv(args.csv, args.source)
    n_read = len(rows)
    ids = [r["contact_id"] for r in rows]
    print(f"supplied rows (1/contact_id) : {n_read:,}   source={args.source!r}")

    # Regression guard — drop contacts already 'verified' in the finder arm.
    with psycopg.connect(_dsn("HQX_DB_URL_POOLED")) as hqx:
        with hqx.cursor() as cur:
            cur.execute(
                "SELECT contact_id FROM ops.email_resolutions "
                "WHERE verification_status = 'verified' AND contact_id = ANY(%s)", (ids,))
            verified_excl = {r[0] for r in cur.fetchall()}
    rows = [r for r in rows if r["contact_id"] not in verified_excl]
    print(f"excluded (already verified in email_resolutions): {len(verified_excl):,}")
    if verified_excl:
        print(f"  {sorted(verified_excl)[:12]}{' …' if len(verified_excl) > 12 else ''}")
    print(f"landing set                  : {len(rows):,}")
    dom_known = sum(1 for r in rows if r["company_domain"])
    print(f"  company_domain populated   : {dom_known:,}/{len(rows):,}")
    print(f"  person_linkedin populated  : {sum(1 for r in rows if r['person_linkedin_url']):,}/{len(rows):,}")

    if args.dry_run:
        print("\n[dry-run] no HQX write. sample:")
        for r in rows[:6]:
            print(f"    {r['contact_id'][:22]:22} {r['email'][:34]:34} dom={r['company_domain']}")
        return 0

    with psycopg.connect(_dsn("HQX_DB_URL_POOLED")) as hqx:
        with hqx.cursor() as cur:
            cur.execute("SELECT count(*) FROM ops.email_verifications")
            before = cur.fetchone()[0]
            cur.execute("DELETE FROM ops.email_verifications WHERE source = %s", (args.source,))
            deleted = cur.rowcount
            cur.executemany(INSERT_SQL, rows)
            cur.execute("SELECT count(*) FROM ops.email_verifications WHERE source = %s", (args.source,))
            cohort_after = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM ops.email_verifications")
            after = cur.fetchone()[0]
            cur.execute(
                "SELECT verification_status, count(*) FROM ops.email_verifications "
                "WHERE source = %s GROUP BY 1", (args.source,))
            hist = dict(cur.fetchall())
        hqx.commit()

    skipped = len(rows) - cohort_after
    print(f"\nops.email_verifications: {before:,} → {after:,}")
    print(f"  cohort rows now : {cohort_after:,}   (deleted {deleted:,} prior; "
          f"skipped-on-conflict {skipped:,})")
    print(f"  cohort status   : {hist}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
