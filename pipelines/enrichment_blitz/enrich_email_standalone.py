"""Standalone Blitz Email Finder — gateway-routed, MV-verified (Directive 28).

A single-purpose, dedicated work-email enrichment pipeline: take a known identity
(``person_linkedin_url``) and resolve a **deliverable** work email via BlitzAPI's
``/v2/enrichment/email`` endpoint, with **MillionVerifier as the sole arbiter** of
deliverability. Endpoint-less Modal worker, spawned by the Universal Dispatcher and
woken via the Trigger waitpoint callback.

This is the **Blitz tier** that was deliberately removed from the Directive-21
Icypeas→LeadMagic cascade — built standalone so a caller can resolve emails from a
LinkedIn identity through Blitz's unlimited Agency-Enterprise email plan, on its own
rate lane, writing into the SAME work-email system-of-record the cascade does.

DECOUPLED EGRESS (Directive 28 §1 — strict IPC). This worker holds **NO Blitz key**
and makes **NO direct ``api.blitz-api.ai`` calls**. Every Blitz email call is routed
through the single-container ``core/blitz_gateway.py`` (``blitz-gateway`` app, fn
``blitz_call``) — the authoritative global ≤5-RPS priority egress. These bulk
enrichment calls ride the **LOW** (or NORMAL) lane so interactive GTM tasks (HIGH)
are never starved. The worker implements NO Blitz rate logic — the gateway governs.

UNIVERSAL MILLIONVERIFIER (Directive 28 §2 — reused verbatim from the Directive-21
cascade). Blitz's own ``found`` flag detects hit-vs-miss ONLY; it is NEVER trusted
for deliverability. Every Blitz-returned email is passed to MillionVerifier, and the
house rubric on ``resultcode`` is the single source of truth:

    1 ok          → STOP. save, verification_status = verified
    2 catch_all   → save as risky candidate (deliverability hold)
    3 unknown     → retry once at timeout=60; still unknown → risky
    4 error / 5 disposable / 6 invalid → DISCARD (unresolved)
    (no terminal verdict / MV outage) → fail-closed: never save unverified

MV is called DIRECTLY (its rate ceiling is generous and Blitz-hit volume is bounded);
the key lives in the ``email-cascade`` Modal secret (``MILLIONVERIFIER_API_KEY``),
exactly as the cascade reads it.

SINK (Directive 28 §3 — shared work-email system-of-record). Latest-wins upsert per
``contact_id`` into ``ops.email_resolutions`` — the SAME table the cascade writes, so
a single downstream materializer rolls BOTH pipelines' verified emails into Lance with
no special-casing. Per-run terminal state into ``ops.blitz_email_finder_runs``.

RAW PAYLOAD PRESERVATION. Both upstream responses are persisted **verbatim, exactly
as-is, with no interpretation imposed**: ``blitz_email_raw`` holds Blitz's full
``/v2/enrichment/email`` payload (incl. ``all_emails``); ``mv_raw`` holds EVERY
MillionVerifier response (a list — 1, or 2 on an unknown re-check). These raw columns
are the source of truth; ``email`` / ``verification_status`` / ``mv_*`` are a convenience
projection ON TOP of them, never a replacement. A discarded-by-MV or missed address
still has its raw Blitz + MV payloads saved (only the derived ``email`` is nulled).

    modal deploy pipelines/enrichment_blitz/enrich_email_standalone.py
    modal run    pipelines/enrichment_blitz/enrich_email_standalone.py::init_ops
    modal run    pipelines/enrichment_blitz/enrich_email_standalone.py::run_manual \\
                 --contacts-json '[{"contact_id":"c1","person_linkedin_url":"https://www.linkedin.com/in/x"}]'
"""

from __future__ import annotations

import datetime as dt
import os
import re
import time
import uuid
from typing import Any

import modal

FEED = "blitz_email_finder"

# ── Gateway coordinates (Directive 23) — the ONLY Blitz egress. ──────────────
GATEWAY_APP, GATEWAY_FN = "blitz-gateway", "blitz_call"
EMAIL_PATH = "/v2/enrichment/email"           # raw path (gateway maps logical names OR raw /v2/…)
# Bulk enrichment lanes only — HIGH is reserved for interactive GTM (never starve it).
VALID_PRIORITIES = {"low", "normal"}

# MillionVerifier (sole arbiter) — direct HTTPS, key in the email-cascade secret.
MV_URL = os.environ.get("MILLIONVERIFIER_API_BASE", "https://api.millionverifier.com").rstrip("/") + "/api/v3/"

# MillionVerifier resultcode → action (house rubric; mirror enrich_email_cascade.py).
MV_OK = {1}            # ok                          → verified
MV_RISKY = {2, 3}      # catch_all, unknown          → risky
MV_BAD = {4, 5, 6}     # error, disposable, invalid  → discard (unresolved)

HTTP_TIMEOUT = 30.0

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "psycopg[binary]>=3.2",  # ops.email_resolutions + ops.blitz_email_finder_runs
    "requests>=2.32",        # MillionVerifier + Trigger callback
)

app = modal.App("blitz-email-finder", image=image)

SECRETS = [
    modal.Secret.from_name("email-cascade"),  # MILLIONVERIFIER_API_KEY (shared with the cascade)
    modal.Secret.from_name("hqx-postgres"),   # ops.email_resolutions + ops.blitz_email_finder_runs
]
# NOTE: the `blitz-api` secret is deliberately ABSENT — the worker never holds the
# Blitz key. All Blitz egress is the gateway's concern (Directive 28 §1).

# ── ops DDL — verbatim mirror of the .sql sibling, applied idempotently before each
# terminal write. ops.email_resolutions is the SHARED work-email system-of-record —
# its definition is byte-identical to enrich_email_cascade.py (the canonical owner);
# CREATE TABLE IF NOT EXISTS makes this a safe self-bootstrap whichever pipeline runs
# first. ops.blitz_email_finder_runs is this pipeline's dedicated run-state ledger. ─
OPS_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.email_resolutions (
    contact_id          text        PRIMARY KEY,
    email               text,
    verification_status text        NOT NULL,
    source_vendor       text,
    source_tier         int,
    mv_resultcode       int,
    mv_result           text,
    mv_quality          text,
    mv_subresult        text,
    certainty           text,
    company_domain      text,
    person_linkedin_url text,
    blitz_email_raw     jsonb,
    mv_raw              jsonb,
    attempts            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    batch_label         text,
    resolved_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT email_resolutions_status_chk
        CHECK (verification_status IN ('verified', 'risky', 'unresolved'))
);
-- Raw-payload preservation (Directive 28 follow-up). The shared table predates these
-- columns, so ADD COLUMN IF NOT EXISTS for the existing instance. The upstream
-- responses are stored VERBATIM — Blitz's /v2/enrichment/email payload and EVERY
-- MillionVerifier response — with no interpretation imposed; the derived columns above
-- (email / verification_status / mv_*) are a convenience projection ON TOP of these raw
-- payloads, never a replacement for them.
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS blitz_email_raw jsonb;
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS mv_raw          jsonb;
CREATE INDEX IF NOT EXISTS email_resolutions_status_idx ON ops.email_resolutions (verification_status);
CREATE INDEX IF NOT EXISTS email_resolutions_domain_idx ON ops.email_resolutions (company_domain);
CREATE INDEX IF NOT EXISTS email_resolutions_email_idx  ON ops.email_resolutions (email);

CREATE TABLE IF NOT EXISTS ops.blitz_email_finder_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    batch_label   text,
    run_root      text,
    priority      text        NOT NULL,
    requested     bigint      NOT NULL DEFAULT 0,
    skipped       bigint      NOT NULL DEFAULT 0,
    verified      bigint      NOT NULL DEFAULT 0,
    risky         bigint      NOT NULL DEFAULT 0,
    unresolved    bigint      NOT NULL DEFAULT 0,
    failed        bigint      NOT NULL DEFAULT 0,
    gateway_calls bigint      NOT NULL DEFAULT 0,
    mv_calls      bigint      NOT NULL DEFAULT 0,
    status        text        NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT blitz_email_finder_runs_status_chk   CHECK (status   IN ('success', 'error')),
    CONSTRAINT blitz_email_finder_runs_priority_chk CHECK (priority IN ('low', 'normal'))
);
CREATE INDEX IF NOT EXISTS blitz_email_finder_runs_feed_idx        ON ops.blitz_email_finder_runs (feed);
CREATE INDEX IF NOT EXISTS blitz_email_finder_runs_recorded_at_idx ON ops.blitz_email_finder_runs (recorded_at DESC);
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
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        raise RuntimeError("HQX_DB_URL_POOLED not set in the hqx-postgres Modal secret.")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _open_conn(dsn: str):
    import psycopg

    return psycopg.connect(dsn, autocommit=True)


def _gateway():
    return modal.Function.from_name(GATEWAY_APP, GATEWAY_FN)


# ── MillionVerifier — the sole deliverability arbiter (mirror cascade) ────────
def _mv_call(email: str, timeout: int) -> dict[str, Any]:
    """Return MillionVerifier's response **EXACTLY as-is** (the JSON payload, untouched).
    The rubric + the projected columns read ``resultcode``/``result``/``quality``/
    ``subresult`` straight off it; the full payload is persisted to ``mv_raw``. On a
    no-key / network failure (no real payload exists) returns a synthetic ``{resultcode:
    None, error}`` so the caller still has a dict — that synthetic is what gets stored,
    honestly recording the absence of a verdict."""
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
    # MillionVerifier's payload, verbatim. Non-dict bodies are wrapped so the caller
    # still gets a dict while the raw text is preserved under "raw".
    return data if isinstance(data, dict) else {"resultcode": None, "raw": data}


def _millionverifier(email: str) -> tuple[dict[str, Any], list[dict]]:
    """The sole arbiter. Returns ``(verdict, responses)``: ``verdict`` is the FINAL raw
    MV payload (the rubric + projections read it); ``responses`` is the list of EVERY raw
    MV payload for this email — 1, or 2 when an ``unknown`` (3) is re-checked at
    timeout=60. Nothing is discarded: every MV response is preserved for ``mv_raw``."""
    responses: list[dict] = []
    mv = _mv_call(email, 20)
    responses.append(mv)
    if mv.get("resultcode") == 3:  # unknown is transient → one slow retry
        mv = _mv_call(email, 60)
        responses.append(mv)
    return mv, responses


# ── Resolution (Blitz email via the gateway → MV gate) ───────────────────────
def _make_result(c: dict, email: str | None, status: str, vendor: str | None,
                 tier: int | None, mv: dict | None, attempts: list,
                 blitz_raw: Any = None, mv_responses: list | None = None) -> dict:
    return {
        "contact_id": c.get("contact_id"),
        "email": email,
        "verification_status": status,
        # Derived projections (convenience / indexed) — a view ON TOP of the raw payloads
        # below, never a replacement. mv = the FINAL MV verdict payload.
        "mv_resultcode": (mv or {}).get("resultcode"),
        "mv_result": (mv or {}).get("result"),
        "mv_quality": (mv or {}).get("quality"),
        "mv_subresult": (mv or {}).get("subresult"),
        "source_vendor": vendor,
        "source_tier": tier,
        "certainty": None,                       # Blitz email carries no certainty score
        "company_domain": _normalize_domain(c.get("company_domain")),
        "person_linkedin_url": (c.get("person_linkedin_url") or "").strip() or None,
        # Raw upstream responses, VERBATIM — the source of truth, no interpretation imposed.
        "blitz_email_raw": blitz_raw,            # Blitz /v2/enrichment/email payload, as-is
        "mv_raw": mv_responses,                  # every MillionVerifier response, as-is (list)
        "attempts": attempts,
    }


def _resolve_contact(c: dict, gw, priority: str, counts: dict) -> dict:
    """One identity → a verified/risky/unresolved work email. Blitz email is keyed on
    ``person_linkedin_url`` ONLY (it cannot resolve from name+domain — that is the
    Icypeas/LeadMagic cascade's job); a contact without one is unresolved here."""
    purl = (c.get("person_linkedin_url") or "").strip() or None
    attempts: list[dict] = []

    if not purl:
        attempts.append({"vendor": "blitz", "outcome": "skipped",
                         "reason": "Blitz email requires person_linkedin_url"})
        return _make_result(c, None, "unresolved", None, None, None, attempts)

    # ── Blitz email via the gateway (the ONLY egress), bulk LOW/NORMAL lane ──
    counts["gateway_calls"] += 1
    rb = gw.remote(endpoint=EMAIL_PATH, payload={"person_linkedin_url": purl}, priority=priority)
    blitz_raw = rb.get("data")                        # Blitz's response, VERBATIM (kept on hit AND miss)
    data = blitz_raw if isinstance(blitz_raw, dict) else {}
    found = bool(rb.get("ok")) and bool(data.get("found"))
    email = (data.get("email") if found else None) or None

    if not email:
        attempts.append({"vendor": "blitz", "outcome": "miss", "ok": rb.get("ok"),
                         "http_status": rb.get("http_status"), "error": rb.get("error")})
        return _make_result(c, None, "unresolved", None, None, None, attempts, blitz_raw=blitz_raw)

    # ── MillionVerifier gate — the sole arbiter. Every found email is verified, and
    #    every MV response is preserved raw, regardless of the verdict. ──
    verdict, mv_responses = _millionverifier(email)
    counts["mv_calls"] += len(mv_responses)
    rc = verdict.get("resultcode")
    attempts.append({"vendor": "blitz", "outcome": "hit", "email": email,
                     "mv_resultcode": rc, "mv_result": verdict.get("result"),
                     "mv_quality": verdict.get("quality")})

    if rc in MV_OK:                                   # OK → STOP & SAVE (verified)
        return _make_result(c, email, "verified", "blitz", 1, verdict, attempts,
                            blitz_raw=blitz_raw, mv_responses=mv_responses)
    if rc in MV_RISKY:                                # catch_all / unknown → save as risky
        return _make_result(c, email, "risky", "blitz", 1, verdict, attempts,
                            blitz_raw=blitz_raw, mv_responses=mv_responses)
    # MV_BAD, or no terminal verdict (rc None / MV outage) → DISCARD (email), fail-closed —
    # but the Blitz payload AND the MV verdict are still persisted raw.
    return _make_result(c, None, "unresolved", None, None, verdict, attempts,
                        blitz_raw=blitz_raw, mv_responses=mv_responses)


# ── Sink writers (ops.email_resolutions upsert — verbatim mirror of the cascade) ─
def _upsert_resolution(cur, r: dict, batch_label: str | None) -> None:
    from psycopg.types.json import Jsonb

    def _j(v):  # jsonb bind, or SQL NULL when there is no payload (call not made)
        return Jsonb(v) if v is not None else None

    cur.execute(
        """
        INSERT INTO ops.email_resolutions
            (contact_id, email, verification_status, source_vendor, source_tier,
             mv_resultcode, mv_result, mv_quality, mv_subresult, certainty,
             company_domain, person_linkedin_url, blitz_email_raw, mv_raw,
             attempts, batch_label, resolved_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (contact_id) DO UPDATE SET
            email               = EXCLUDED.email,
            verification_status = EXCLUDED.verification_status,
            source_vendor       = EXCLUDED.source_vendor,
            source_tier         = EXCLUDED.source_tier,
            mv_resultcode       = EXCLUDED.mv_resultcode,
            mv_result           = EXCLUDED.mv_result,
            mv_quality          = EXCLUDED.mv_quality,
            mv_subresult        = EXCLUDED.mv_subresult,
            certainty           = EXCLUDED.certainty,
            company_domain      = EXCLUDED.company_domain,
            person_linkedin_url = EXCLUDED.person_linkedin_url,
            blitz_email_raw     = EXCLUDED.blitz_email_raw,
            mv_raw              = EXCLUDED.mv_raw,
            attempts            = EXCLUDED.attempts,
            batch_label         = EXCLUDED.batch_label,
            resolved_at         = now()
        """,
        (r["contact_id"], r["email"], r["verification_status"], r["source_vendor"],
         r["source_tier"], r["mv_resultcode"], r["mv_result"], r["mv_quality"],
         r["mv_subresult"], r["certainty"], r["company_domain"], r["person_linkedin_url"],
         _j(r.get("blitz_email_raw")), _j(r.get("mv_raw")),
         Jsonb(r["attempts"]), batch_label),
    )


def _record_run(cur, batch_label: str | None, run_root: str, priority: str, counts: dict,
                status: str, error: str | None, started_at: dt.datetime,
                completed_at: dt.datetime) -> None:
    cur.execute(OPS_DDL)
    cur.execute(
        """
        INSERT INTO ops.blitz_email_finder_runs
            (feed, batch_label, run_root, priority, requested, skipped, verified, risky,
             unresolved, failed, gateway_calls, mv_calls, status, error, started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (FEED, batch_label, run_root, priority, counts["requested"], counts["skipped"],
         counts["verified"], counts["risky"], counts["unresolved"], counts["failed"],
         counts["gateway_calls"], counts["mv_calls"], status, error, started_at, completed_at),
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
    never re-spend a gateway call or clobber a settled contact unless force=True). The
    shared SoR means a contact verified by the Directive-21 cascade is also skipped."""
    if not contact_ids:
        return set()
    try:
        cur.execute(
            "SELECT contact_id FROM ops.email_resolutions "
            "WHERE verification_status = 'verified' AND contact_id = ANY(%s)",
            (contact_ids,),
        )
        return {r[0] for r in cur.fetchall()}
    except Exception as exc:  # noqa: BLE001 — degrade skip, preserve correctness
        print(f"WARN: skip-set lookup failed ({exc}); proceeding without skip.")
        return set()


# ── Runner ────────────────────────────────────────────────────────────────────
def _run(contacts: list[dict], batch_label: str | None, run_id: str | None, priority: str,
         force: bool, trigger_callback_url: str | None) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    run_root = run_id or uuid.uuid4().hex
    priority = priority if priority in VALID_PRIORITIES else "low"
    counts = {"requested": 0, "skipped": 0, "verified": 0, "risky": 0,
              "unresolved": 0, "failed": 0, "gateway_calls": 0, "mv_calls": 0}
    status, error = "error", None
    dsn = _hqx_dsn()

    try:
        contacts = [c for c in (contacts or []) if isinstance(c, dict) and c.get("contact_id")]
        counts["requested"] = len(contacts)
        gw = _gateway()
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
                    r = _resolve_contact(c, gw, priority, counts)
                    _upsert_resolution(cur, r, batch_label)
                    counts[r["verification_status"]] += 1   # verified | risky | unresolved
                except Exception as exc:  # noqa: BLE001 — one contact must not sink the batch
                    counts["failed"] += 1
                    print(f"WARN: contact {c.get('contact_id')!r} failed: {exc}")

            status = "success"
            _record_run(cur, batch_label, run_root, priority, counts, status, None,
                        started_at, dt.datetime.now(dt.timezone.utc))
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 — terminal handling + re-raise
        error = str(exc)
        status = "error"
        try:
            conn2 = _open_conn(dsn)
            try:
                _record_run(conn2.cursor(), batch_label, run_root, priority, counts, status,
                            error, started_at, dt.datetime.now(dt.timezone.utc))
            finally:
                conn2.close()
        except Exception as exc2:  # noqa: BLE001 — audit must not mask the failure
            print(f"WARN: ops.blitz_email_finder_runs write failed: {exc2}")
    finally:
        _post_callback(
            trigger_callback_url,
            {"status": status, "feed": FEED, "batch_label": batch_label, "error": error, **counts},
        )

    if status != "success":
        raise RuntimeError(f"blitz_email_finder failed: {error}")
    return {"feed": FEED, "status": status, "batch_label": batch_label, **counts}


@app.function(secrets=SECRETS, timeout=60 * 60, memory=2048, cpu=1.0)
def run_email_finder(contacts: list[dict], batch_label: str | None = None,
                     run_id: str | None = None, priority: str = "low", force: bool = False,
                     trigger_callback_url: str | None = None) -> dict:
    """Resolve a chunk of identities to verified work emails via Blitz (gateway-routed)
    → MillionVerifier. Each ``contact`` is a dict::

        {contact_id, person_linkedin_url, company_domain?, first_name?, last_name?, company_name?}

    Blitz email requires ``person_linkedin_url``; contacts without one are unresolved.
    ``priority`` is the gateway lane: ``"low"`` (default, bulk) or ``"normal"`` — never
    HIGH (reserved for interactive GTM)."""
    return _run(contacts, batch_label, run_id, priority, force, trigger_callback_url)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.email_resolutions + ops.blitz_email_finder_runs in HQX (idempotent)."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute("""
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name IN ('email_resolutions','blitz_email_finder_runs')
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
    """Read-back: latest Blitz-sourced resolutions + run-state + status histogram."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute(
            """SELECT contact_id, email, verification_status, source_vendor, mv_result,
                      blitz_email_raw, mv_raw, resolved_at
               FROM ops.email_resolutions WHERE source_vendor = 'blitz'
               ORDER BY resolved_at DESC LIMIT %s""",
            (limit,),
        )
        rcols = [d.name for d in cur.description]
        resolutions = [dict(zip(rcols, r)) for r in cur.fetchall()]
        cur.execute(
            "SELECT verification_status, count(*) FROM ops.email_resolutions "
            "WHERE source_vendor = 'blitz' GROUP BY 1")
        histogram = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute(
            """SELECT batch_label, priority, requested, skipped, verified, risky, unresolved,
                      failed, gateway_calls, mv_calls, status, recorded_at
               FROM ops.blitz_email_finder_runs ORDER BY recorded_at DESC LIMIT 3""")
        runcols = [d.name for d in cur.description]
        runs = [dict(zip(runcols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    print(f"blitz email status histogram: {histogram}")
    return {"histogram": histogram, "recent_resolutions": resolutions, "recent_runs": runs}


@app.function(secrets=SECRETS, timeout=60)
def verify_secrets() -> dict:
    """Evidence for the "ensure MV secrets are mounted" mandate + gateway egress —
    no MV/Blitz spend. Confirms the email-cascade secret mounts MILLIONVERIFIER_API_KEY
    (presence only; the value is never returned), the hq-x DSN is present, and the Blitz
    gateway IPC resolves with email enrichment unlocked (0-credit key-info)."""
    out = {
        "millionverifier_key_present": bool(os.environ.get("MILLIONVERIFIER_API_KEY")),
        "hqx_dsn_present": bool(os.environ.get("HQX_DB_URL_POOLED")),
        "gateway_keyinfo_ok": False,
        "email_enrichment_allowed": False,
    }
    try:
        ki = modal.Function.from_name(GATEWAY_APP, "key_info").remote()
        out["gateway_keyinfo_ok"] = bool(ki.get("valid"))
        out["email_enrichment_allowed"] = "/enrichment/email" in set(ki.get("allowed_apis") or [])
    except Exception as exc:  # noqa: BLE001
        out["gateway_error"] = str(exc)
    # Names-only audit of which vendor keys the email-cascade secret actually injects
    # (values NEVER returned) — pinpoints a missing/mis-named MillionVerifier key.
    out["vendor_keys_present"] = sorted(
        k for k in os.environ
        if re.search(r"MILLION|VERIF|LEADMAGIC|ICYPEAS|(^|_)MV(_|$)|EMAIL", k, re.IGNORECASE)
    )
    print(out)
    return out


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.email_resolutions + ops.blitz_email_finder_runs DDL (HQX)."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def verify_secrets_run() -> None:
    """MV-secret-mounted + gateway-egress evidence (no Blitz/MV spend)."""
    import json

    print(json.dumps(verify_secrets.remote(), indent=2, default=str))


@app.local_entrypoint()
def verify_run(limit: int = 8) -> None:
    """Read-back assertion on the most-recent Blitz email resolutions."""
    import json

    print(json.dumps(verify.remote(limit), indent=2, default=str))


@app.local_entrypoint()
def run_manual(contacts_json: str, priority: str = "low") -> None:
    """Manual run. --contacts-json '[{"contact_id":"c1",
    "person_linkedin_url":"https://www.linkedin.com/in/x"}]'"""
    import json

    contacts = json.loads(contacts_json)
    print(json.dumps(
        run_email_finder.remote(contacts, batch_label="manual", priority=priority),
        indent=2, default=str,
    ))
