"""Standalone MillionVerifier Email Verifier — verify a KNOWN work email.

A single-purpose, dedicated work-email VERIFICATION pipeline: take an already-known
work email (e.g. gtm.contacts.work_email) and resolve its deliverability via
MillionVerifier — the sole arbiter. Endpoint-less Modal worker, spawned by the
Universal Dispatcher and woken via the Trigger waitpoint callback. The structural twin
of the Blitz Email Finder, MINUS the email-FINDING step: there is no Blitz/Icypeas/
LeadMagic call here — the email is supplied by the caller; MV is the only egress.

UNIVERSAL MILLIONVERIFIER (house rubric, mirror enrich_email_standalone.py). The MV
``resultcode`` is the single source of truth:

    1 ok          → verified
    2 catch_all   → risky (deliverability hold)
    3 unknown     → retry once at timeout=60; still unknown → risky
    4 error / 5 disposable / 6 invalid → unresolved
    (no terminal verdict / MV outage) → fail-closed: unresolved, never claim verified

MV is called DIRECTLY (its rate ceiling is generous); the key lives in the
``email-cascade`` Modal secret (``MILLIONVERIFIER_API_KEY``), exactly as the cascade and
the Blitz finder read it. There is NO Blitz gateway here — this worker holds no Blitz key
and makes no Blitz calls.

SINK. Latest-wins upsert per ``contact_id`` into ``ops.email_verifications`` — the
work-email verification system-of-record owned by this pipeline. Per-run terminal state
into ``ops.mv_verify_runs``. A downstream materializer mirrors the SoR → native Lance at
s3://data-sink/active/email_verifications/.

RAW PAYLOAD PRESERVATION. ``mv_raw`` holds EVERY MillionVerifier response (a list — 1, or
2 on an unknown re-check), VERBATIM. The derived columns (verification_status / mv_*) are a
convenience projection ON TOP of it, never a replacement.

    modal deploy pipelines/enrichment_mv/verify_mv_standalone.py
    modal run    pipelines/enrichment_mv/verify_mv_standalone.py::init_ops
    modal run    pipelines/enrichment_mv/verify_mv_standalone.py::run_manual \\
                 --contacts-json '[{"contact_id":"c1","email":"jane@acme.com"}]'
"""

from __future__ import annotations

import datetime as dt
import os
import re
import time
import uuid
from typing import Any

import modal

FEED = "mv_verify"

# MillionVerifier (sole arbiter) — direct HTTPS, key in the email-cascade secret.
MV_URL = os.environ.get("MILLIONVERIFIER_API_BASE", "https://api.millionverifier.com").rstrip("/") + "/api/v3/"

# MillionVerifier resultcode → action (house rubric; mirror enrich_email_standalone.py).
MV_OK = {1}            # ok                          → verified
MV_RISKY = {2, 3}      # catch_all, unknown          → risky
MV_BAD = {4, 5, 6}     # error, disposable, invalid  → unresolved

HTTP_TIMEOUT = 30.0

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "psycopg[binary]>=3.2",  # ops.email_verifications + ops.mv_verify_runs
    "requests>=2.32",        # MillionVerifier + Trigger callback
)

app = modal.App("mv-verify", image=image)

SECRETS = [
    modal.Secret.from_name("email-cascade"),  # MILLIONVERIFIER_API_KEY (shared with the cascade/finder)
    modal.Secret.from_name("hqx-postgres"),   # ops.email_verifications + ops.mv_verify_runs
]

# ── ops DDL — verbatim mirror of the .sql sibling, applied idempotently before each
# terminal write. ops.email_verifications is this pipeline's verification SoR;
# ops.mv_verify_runs is its dedicated run-state ledger. ─
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.email_verifications (
    contact_id          text        PRIMARY KEY,
    email               text        NOT NULL,
    verification_status text        NOT NULL,
    mv_resultcode       int,
    mv_result           text,
    mv_quality          text,
    mv_subresult        text,
    source              text,
    company_domain      text,
    mv_raw              jsonb,
    attempts            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    batch_label         text,
    resolved_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT email_verifications_status_chk
        CHECK (verification_status IN ('verified', 'risky', 'unresolved'))
);
ALTER TABLE ops.email_verifications ADD COLUMN IF NOT EXISTS mv_raw jsonb;
ALTER TABLE ops.email_verifications ADD COLUMN IF NOT EXISTS person_linkedin_url text;
CREATE INDEX IF NOT EXISTS email_verifications_status_idx ON ops.email_verifications (verification_status);
CREATE INDEX IF NOT EXISTS email_verifications_domain_idx ON ops.email_verifications (company_domain);
CREATE INDEX IF NOT EXISTS email_verifications_email_idx  ON ops.email_verifications (email);

CREATE TABLE IF NOT EXISTS ops.mv_verify_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    batch_label   text,
    run_root      text,
    requested     bigint      NOT NULL DEFAULT 0,
    skipped       bigint      NOT NULL DEFAULT 0,
    verified      bigint      NOT NULL DEFAULT 0,
    risky         bigint      NOT NULL DEFAULT 0,
    unresolved    bigint      NOT NULL DEFAULT 0,
    failed        bigint      NOT NULL DEFAULT 0,
    mv_calls      bigint      NOT NULL DEFAULT 0,
    status        text        NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT mv_verify_runs_status_chk CHECK (status IN ('success', 'error'))
);
CREATE INDEX IF NOT EXISTS mv_verify_runs_feed_idx        ON ops.mv_verify_runs (feed);
CREATE INDEX IF NOT EXISTS mv_verify_runs_recorded_at_idx ON ops.mv_verify_runs (recorded_at DESC);
"""

_SCHEME = re.compile(r"^https?://")
_WWW = re.compile(r"^www\.")


def _normalize_domain(raw: str | None) -> str | None:
    d = (raw or "").strip().lower()
    d = _SCHEME.sub("", d)
    d = _WWW.sub("", d)
    d = d.split("/", 1)[0].rstrip(".")
    return d or None


def _safe_json(resp) -> Any:
    try:
        return resp.json()
    except Exception:  # noqa: BLE001 — non-JSON body
        return {"raw": resp.text[:1000]}


def _hqx_dsn() -> str:
    """Transaction-mode (Supavisor :6543) hq-x DSN for this horizontally scaled worker.

    The worker holds one Postgres connection for the whole run while mostly idle (blocked
    on MV between sparse DB touches). On the SESSION pooler every such connection pins a
    backend for the run, exhausting the 15-slot ceiling under fan-out; the TRANSACTION
    pooler checks a backend out only per autocommit statement. Prefer an explicit
    HQX_DB_URL_TRANSACTION; else derive it from the session DSN (5432→6543)."""
    dsn = os.environ.get("HQX_DB_URL_TRANSACTION")
    if not dsn:
        pooled = os.environ.get("HQX_DB_URL_POOLED")
        if not pooled:
            raise RuntimeError(
                "Neither HQX_DB_URL_TRANSACTION nor HQX_DB_URL_POOLED set in the "
                "hqx-postgres Modal secret.")
        dsn = pooled.replace(".pooler.supabase.com:5432", ".pooler.supabase.com:6543")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _open_conn(dsn: str):
    import psycopg

    return psycopg.connect(dsn, autocommit=True, prepare_threshold=None)


# ── MillionVerifier — the sole deliverability arbiter (mirror finder/cascade) ─
def _mv_call(email: str, timeout: int) -> dict[str, Any]:
    """Return MillionVerifier's response EXACTLY as-is. On a no-key / network failure
    returns a synthetic ``{resultcode: None, error}`` so the caller still has a dict —
    that synthetic is what gets stored, honestly recording the absence of a verdict."""
    import requests

    key = os.environ.get("MILLIONVERIFIER_API_KEY")
    if not key:
        return {"resultcode": None, "error": "MILLIONVERIFIER_API_KEY absent in email-cascade secret"}
    try:
        resp = requests.get(MV_URL, params={"api": key, "email": email, "timeout": timeout},
                            timeout=timeout + 10)
    except Exception as exc:  # noqa: BLE001 — network / timeout
        return {"resultcode": None, "error": str(exc)}
    data = _safe_json(resp)
    return data if isinstance(data, dict) else {"resultcode": None, "raw": data}


def _millionverifier(email: str) -> tuple[dict[str, Any], list[dict]]:
    """The sole arbiter. Returns ``(verdict, responses)``: ``verdict`` is the FINAL raw MV
    payload; ``responses`` is the list of EVERY raw MV payload — 1, or 2 when an ``unknown``
    (3) is re-checked at timeout=60. Nothing is discarded; every response is kept for mv_raw."""
    responses: list[dict] = []
    mv = _mv_call(email, 20)
    responses.append(mv)
    if mv.get("resultcode") == 3:  # unknown is transient → one slow retry
        mv = _mv_call(email, 60)
        responses.append(mv)
    return mv, responses


def _make_result(c: dict, email: str | None, status: str, mv: dict | None,
                 attempts: list, mv_responses: list | None = None) -> dict:
    return {
        "contact_id": c.get("contact_id"),
        "email": email,
        "verification_status": status,
        "mv_resultcode": (mv or {}).get("resultcode"),
        "mv_result": (mv or {}).get("result"),
        "mv_quality": (mv or {}).get("quality"),
        "mv_subresult": (mv or {}).get("subresult"),
        "source": (c.get("source") or "").strip() or None,
        "company_domain": _normalize_domain(c.get("company_domain")),
        "mv_raw": mv_responses,                  # every MillionVerifier response, as-is (list)
        "attempts": attempts,
    }


def _resolve_contact(c: dict, counts: dict) -> dict:
    """One known email → verified/risky/unresolved via MillionVerifier. A contact with no
    email is unresolved (nothing to verify)."""
    email = (c.get("email") or "").strip() or None
    attempts: list[dict] = []

    if not email:
        attempts.append({"vendor": "millionverifier", "outcome": "skipped",
                         "reason": "no email supplied"})
        return _make_result(c, None, "unresolved", None, attempts)

    verdict, mv_responses = _millionverifier(email)
    counts["mv_calls"] += len(mv_responses)
    rc = verdict.get("resultcode")
    attempts.append({"vendor": "millionverifier", "email": email, "mv_resultcode": rc,
                     "mv_result": verdict.get("result"), "mv_quality": verdict.get("quality")})

    if rc in MV_OK:                                   # ok → verified
        return _make_result(c, email, "verified", verdict, attempts, mv_responses)
    if rc in MV_RISKY:                                # catch_all / unknown → risky
        return _make_result(c, email, "risky", verdict, attempts, mv_responses)
    # MV_BAD, or no terminal verdict (rc None / MV outage) → unresolved, fail-closed —
    # but the email + MV verdict are still persisted raw (only the status is unresolved).
    return _make_result(c, email, "unresolved", verdict, attempts, mv_responses)


# ── Sink writers (ops.email_verifications upsert) ────────────────────────────
def _upsert_verification(cur, r: dict, batch_label: str | None) -> None:
    from psycopg.types.json import Jsonb

    def _j(v):  # jsonb bind, or SQL NULL when there is no payload
        return Jsonb(v) if v is not None else None

    cur.execute(
        """
        INSERT INTO ops.email_verifications
            (contact_id, email, verification_status, mv_resultcode, mv_result, mv_quality,
             mv_subresult, source, company_domain, mv_raw, attempts, batch_label, resolved_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (contact_id) DO UPDATE SET
            email               = EXCLUDED.email,
            verification_status = EXCLUDED.verification_status,
            mv_resultcode       = EXCLUDED.mv_resultcode,
            mv_result           = EXCLUDED.mv_result,
            mv_quality          = EXCLUDED.mv_quality,
            mv_subresult        = EXCLUDED.mv_subresult,
            source              = EXCLUDED.source,
            company_domain      = EXCLUDED.company_domain,
            mv_raw              = EXCLUDED.mv_raw,
            -- attempts ACCUMULATE stages (supplied lander, prior verifies) — never replace:
            -- wholesale EXCLUDED.attempts clobbered supplied-payload custody (2026-07-05)
            attempts            = coalesce(ops.email_verifications.attempts, '[]'::jsonb)
                                  || EXCLUDED.attempts,
            batch_label         = EXCLUDED.batch_label,
            resolved_at         = now()
        """,
        (r["contact_id"], r["email"], r["verification_status"], r["mv_resultcode"],
         r["mv_result"], r["mv_quality"], r["mv_subresult"], r["source"],
         r["company_domain"], _j(r.get("mv_raw")), Jsonb(r["attempts"]), batch_label),
    )


def _record_run(cur, batch_label: str | None, run_root: str, counts: dict,
                status: str, error: str | None, started_at: dt.datetime,
                completed_at: dt.datetime) -> None:
    cur.execute(OPS_DDL)
    cur.execute(
        """
        INSERT INTO ops.mv_verify_runs
            (feed, batch_label, run_root, requested, skipped, verified, risky,
             unresolved, failed, mv_calls, status, error, started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (FEED, batch_label, run_root, counts["requested"], counts["skipped"],
         counts["verified"], counts["risky"], counts["unresolved"], counts["failed"],
         counts["mv_calls"], status, error, started_at, completed_at),
    )


def _post_callback(url: str | None, payload: dict, attempts: int = 3) -> None:
    """POST terminal counts to the Trigger waitpoint url — RAW body, no envelope."""
    if not url:
        print("No trigger_callback_url (manual run); skipping callback.")
        return
    import requests

    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code < 300:
                print(f"Callback delivered: {payload}")
                return
            print(f"Callback attempt {i + 1} non-2xx: {resp.status_code} {resp.text[:200]}")
        except Exception as exc:  # noqa: BLE001
            print(f"Callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    print(f"WARN: callback delivery failed after {attempts} attempts → {url}")


def _already_verified(cur, contact_ids: list[str]) -> set[str]:
    """Batched skip-set: contacts already resolved to a VERIFIED email (idempotency —
    never re-spend an MV call or clobber a settled contact unless force=True)."""
    if not contact_ids:
        return set()
    try:
        cur.execute(
            "SELECT contact_id FROM ops.email_verifications "
            "WHERE verification_status = 'verified' AND contact_id = ANY(%s)",
            (contact_ids,),
        )
        return {r[0] for r in cur.fetchall()}
    except Exception as exc:  # noqa: BLE001 — degrade skip, preserve correctness
        print(f"WARN: skip-set lookup failed ({exc}); proceeding without skip.")
        return set()


# ── Runner ────────────────────────────────────────────────────────────────────
def _run(contacts: list[dict], batch_label: str | None, run_id: str | None,
         force: bool, trigger_callback_url: str | None) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    run_root = run_id or uuid.uuid4().hex
    counts = {"requested": 0, "skipped": 0, "verified": 0, "risky": 0,
              "unresolved": 0, "failed": 0, "mv_calls": 0}
    status, error = "error", None
    dsn = _hqx_dsn()

    try:
        contacts = [c for c in (contacts or []) if isinstance(c, dict) and c.get("contact_id")]
        counts["requested"] = len(contacts)
        conn = _open_conn(dsn)
        try:
            cur = conn.cursor()
            cur.execute(OPS_DDL)  # ensure sinks exist before first write

            skip = set() if force else _already_verified(
                cur, [c["contact_id"] for c in contacts])

            for c in contacts:
                if c["contact_id"] in skip:
                    counts["skipped"] += 1
                    continue
                try:
                    r = _resolve_contact(c, counts)
                    _upsert_verification(cur, r, batch_label)
                    counts[r["verification_status"]] += 1   # verified | risky | unresolved
                except Exception as exc:  # noqa: BLE001 — one contact must not sink the batch
                    counts["failed"] += 1
                    print(f"WARN: contact {c.get('contact_id')!r} failed: {exc}")

            status = "success"
            _record_run(cur, batch_label, run_root, counts, status, None,
                        started_at, dt.datetime.now(dt.timezone.utc))
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — terminal handling + re-raise
        error = str(exc)
        status = "error"
        try:
            conn2 = _open_conn(dsn)
            try:
                _record_run(conn2.cursor(), batch_label, run_root, counts, status,
                            error, started_at, dt.datetime.now(dt.timezone.utc))
            finally:
                conn2.close()
        except Exception as exc2:  # noqa: BLE001 — audit must not mask the failure
            print(f"WARN: ops.mv_verify_runs write failed: {exc2}")
    finally:
        _post_callback(
            trigger_callback_url,
            {"status": status, "feed": FEED, "batch_label": batch_label, "error": error, **counts},
        )

    if status != "success":
        raise RuntimeError(f"mv_verify failed: {error}")
    return {"feed": FEED, "status": status, "batch_label": batch_label, **counts}


@app.function(secrets=SECRETS, timeout=60 * 60, memory=2048, cpu=1.0)
def run_mv_verify(contacts: list[dict], batch_label: str | None = None,
                  run_id: str | None = None, force: bool = False,
                  trigger_callback_url: str | None = None) -> dict:
    """Verify a chunk of KNOWN work emails via MillionVerifier. Each ``contact`` is a dict::

        {contact_id, email, company_domain?, source?}

    ``email`` is the work email to verify; a contact without one is unresolved. There is
    NO email-finding here — MV is the only egress."""
    return _run(contacts, batch_label, run_id, force, trigger_callback_url)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.email_verifications + ops.mv_verify_runs in HQX (idempotent)."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute("""
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name IN ('email_verifications','mv_verify_runs')
            ORDER BY table_name, ordinal_position
        """)
        cols: dict[str, list[str]] = {}
        for t, col in cur.fetchall():
            cols.setdefault(t, []).append(col)
    finally:
        conn.close()
    print(f"ops tables ready: { {k: len(v) for k, v in cols.items()} }")
    return {"tables": cols}


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def verify(limit: int = 8) -> dict:
    """Read-back: latest verifications + run-state + status histogram."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute(
            """SELECT contact_id, email, verification_status, mv_result, mv_raw, resolved_at
               FROM ops.email_verifications ORDER BY resolved_at DESC LIMIT %s""",
            (limit,),
        )
        rcols = [d.name for d in cur.description]
        verifications = [dict(zip(rcols, r)) for r in cur.fetchall()]
        cur.execute("SELECT verification_status, count(*) FROM ops.email_verifications GROUP BY 1")
        histogram = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute(
            """SELECT batch_label, requested, skipped, verified, risky, unresolved,
                      failed, mv_calls, status, recorded_at
               FROM ops.mv_verify_runs ORDER BY recorded_at DESC LIMIT 3""")
        runcols = [d.name for d in cur.description]
        runs = [dict(zip(runcols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    print(f"mv verify status histogram: {histogram}")
    return {"histogram": histogram, "recent_verifications": verifications, "recent_runs": runs}


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.email_verifications + ops.mv_verify_runs DDL (HQX)."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def verify_run(limit: int = 8) -> None:
    """Read-back assertion on the most-recent verifications."""
    import json

    print(json.dumps(verify.remote(limit), indent=2, default=str))


@app.local_entrypoint()
def run_manual(contacts_json: str) -> None:
    """Manual run. --contacts-json '[{"contact_id":"c1","email":"jane@acme.com"}]'"""
    import json

    contacts = json.loads(contacts_json)
    print(json.dumps(
        run_mv_verify.remote(contacts, batch_label="manual"),
        indent=2, default=str,
    ))
