"""Enrichment Email Cascade worker — Work-Email Waterfall (Directive 21-Final).

Decoupled, asynchronous work-email resolution. Blitz is REMOVED; the waterfall is
strictly **Icypeas → LeadMagic**, with **MillionVerifier as the sole arbiter** of
deliverability after every vendor hit. Spawned by the Universal Dispatcher
(``core/modal_dispatcher.py``) and woken via the Trigger waitpoint callback.

    run_cascade   contacts[] → resolved work emails (verified | risky | unresolved)

RATE GOVERNANCE. Icypeas is asynchronous and hard-capped (submit 10/sec, read
30/min). Every Icypeas call is delegated to the single-container
``core/icypeas_gateway.py`` (``icypeas-gateway`` app) — the authoritative global
egress. This worker holds NO Icypeas key and implements NO Icypeas rate logic; it
calls ``find_email`` and gets a final envelope. LeadMagic (300/min) and
MillionVerifier run ELASTICALLY inline — their volume is naturally bounded below
Icypeas's 30/min read ceiling (they only see Tier-1 misses), so no gateway needed.

VERIFICATION RUBRIC (MillionVerifier ``resultcode`` is the single source of truth;
vendor-native "valid" flags are NEVER trusted):

    1 ok          → STOP. save, verification_status = verified
    2 catch_all   → HOLD as risky candidate, keep cascading
    3 unknown     → retry once at timeout=60; still unknown → HOLD as risky
    4 error / 5 disposable / 6 invalid → DISCARD immediately, cascade
    (no terminal verdict / MV outage) → fail-closed: never save unverified, cascade

If the cascade exhausts with only risky candidates, the best-ranked
(catch_all ≻ unknown) is saved as ``verification_status = risky`` — never as verified.

SINK (ops-layer, no new Lance write pattern — Directive 23 §5 convention). Latest-
wins upsert per ``contact_id`` into ``ops.email_resolutions`` (the work-email
system-of-record at the control plane); per-run terminal state into
``ops.email_cascade_runs``. A downstream materializer can roll
``ops.email_resolutions`` into a Lance dataset on its own cadence, exactly as
``firmographics-blitz`` does for the Blitz firmo task_types.

RAW PAYLOAD PRESERVATION (Directive 28 doctrine). Every provider response is stored
**verbatim, exactly as-is, with no interpretation imposed**: ``icypeas_raw`` (the
Icypeas result item), ``leadmagic_raw`` (the LeadMagic response), and ``mv_raw``
(EVERY MillionVerifier response — an array, since an ``unknown`` re-check and a
multi-tier cascade produce several). These raw columns are the source of truth;
``email`` / ``verification_status`` / ``mv_*`` / ``certainty`` are a convenience
projection ON TOP of them, never a replacement. A discarded-by-MV or missed address
still has its raw provider + MV payloads saved (only the derived ``email`` is nulled).
The shared ``ops.email_resolutions`` table is co-written by the standalone Blitz email
finder (``blitz_email_raw``); this worker owns the canonical DDL.

    modal deploy pipelines/enrichment_email_cascade/enrich_email_cascade.py
    modal run    pipelines/enrichment_email_cascade/enrich_email_cascade.py::init_ops
    modal run    pipelines/enrichment_email_cascade/enrich_email_cascade.py::run_manual \\
                 --contacts-json '[{"contact_id":"c1","first_name":"Jean","last_name":"Dupont","company_domain":"icypeas.com"}]'
"""

from __future__ import annotations

import datetime as dt
import os
import re
import time
import uuid
from typing import Any

import modal

FEED = "email_cascade"
RESOLVE_TASK = "email_waterfall_resolve"

GATEWAY_APP, GATEWAY_FN = "icypeas-gateway", "find_email"

# Vendor coordinates (locked 2026-06-03 via official-docs recon).
LEADMAGIC_URL = os.environ.get("LEADMAGIC_API_BASE", "https://api.leadmagic.io").rstrip("/") + "/email-finder"
MV_URL = os.environ.get("MILLIONVERIFIER_API_BASE", "https://api.millionverifier.com").rstrip("/") + "/api/v3/"

# MillionVerifier resultcode → action. Single arbiter of deliverability.
MV_OK = {1}            # ok          → STOP, verified
MV_RISKY = {2, 3}      # catch_all, unknown → HOLD, keep cascading
MV_BAD = {4, 5, 6}     # error, disposable, invalid → DISCARD, cascade
_RISKY_RANK = {2: 0, 3: 1}   # catch_all outranks unknown

# LeadMagic statuses that count as a found address (still re-verified by MV).
_LEADMAGIC_HIT = {"valid", "valid_catch_all"}

HTTP_TIMEOUT = 30.0
MAX_RETRIES = 3

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "psycopg[binary]>=3.2",  # ops.email_resolutions + ops.email_cascade_runs
    "requests>=2.32",        # LeadMagic + MillionVerifier + Trigger callback
)

app = modal.App("enrichment-email-cascade", image=image)

SECRETS = [
    modal.Secret.from_name("email-cascade"),  # LEADMAGIC_API_KEY + MILLIONVERIFIER_API_KEY
    modal.Secret.from_name("hqx-postgres"),   # ops.email_resolutions + ops.email_cascade_runs
]

# ── ops DDL — verbatim mirror of the .sql sibling. Applied defensively before each
# terminal write (idempotent). Lives in the HQX control-plane DB. ──────────────
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
    icypeas_raw         jsonb,
    leadmagic_raw       jsonb,
    blitz_email_raw     jsonb,
    mv_raw              jsonb,
    attempts            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    batch_label         text,
    resolved_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT email_resolutions_status_chk
        CHECK (verification_status IN ('verified', 'risky', 'unresolved'))
);
-- Raw-payload preservation (Directive 28 doctrine, applied to the cascade). Every
-- upstream response is persisted VERBATIM, exactly as-is, with no interpretation
-- imposed: per-provider raw payloads (icypeas_raw / leadmagic_raw / blitz_email_raw)
-- and mv_raw (every MillionVerifier response, an array). These raw columns are the
-- source of truth; the derived columns above (email / verification_status / mv_* /
-- certainty) are a convenience projection ON TOP of them, never a replacement. The
-- shared table may predate any given column, so ADD COLUMN IF NOT EXISTS upgrades it.
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS icypeas_raw     jsonb;
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS leadmagic_raw   jsonb;
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS blitz_email_raw jsonb;
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS mv_raw          jsonb;
CREATE INDEX IF NOT EXISTS email_resolutions_status_idx ON ops.email_resolutions (verification_status);
CREATE INDEX IF NOT EXISTS email_resolutions_domain_idx ON ops.email_resolutions (company_domain);
CREATE INDEX IF NOT EXISTS email_resolutions_email_idx  ON ops.email_resolutions (email);

CREATE TABLE IF NOT EXISTS ops.email_cascade_runs (
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
    status        text        NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT email_cascade_runs_status_chk CHECK (status IN ('success', 'error'))
);
CREATE INDEX IF NOT EXISTS email_cascade_runs_feed_idx        ON ops.email_cascade_runs (feed);
CREATE INDEX IF NOT EXISTS email_cascade_runs_recorded_at_idx ON ops.email_cascade_runs (recorded_at DESC);
"""

# ── Domain anchor normalization — mirrors the rest of the fleet exactly. ───────
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
    """Transaction-mode (Supavisor :6543) hq-x DSN for this rate-gated, horizontally
    scaled worker.

    The worker holds one Postgres connection for the whole run while it is mostly idle —
    blocked on the rate-governed gateway between sparse DB touches. On the SESSION pooler
    (:5432, pool_size=15) every such connection pins a backend for the entire run, so N
    concurrent workers exhaust the 15-slot ceiling (``EMAXCONNSESSION: max clients reached
    in session mode``). The TRANSACTION pooler checks a backend out only for each autocommit
    statement, returning it between the batched read and the per-entity writes — a long-
    lived-but-idle worker costs ~0 backends. Prefer an explicit HQX_DB_URL_TRANSACTION; else
    derive it from the session DSN (Supavisor maps session→transaction by the pooler port
    5432→6543)."""
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

    # prepare_threshold=None disables psycopg3 server-side prepared statements: under
    # transaction-mode pooling consecutive statements may land on different backends, so a
    # prepared statement created on one is absent on the next. autocommit=True keeps each
    # statement its own short transaction so the pooler frees the backend between gateway calls.
    return psycopg.connect(dsn, autocommit=True, prepare_threshold=None)


def _gateway():
    return modal.Function.from_name(GATEWAY_APP, GATEWAY_FN)


# ── Vendor adapters (elastic, inline) ─────────────────────────────────────────
def _leadmagic_find(first: str, last: str, domain: str | None,
                    company_name: str | None) -> dict[str, Any]:
    """LeadMagic Email Finder — POST /email-finder, header X-API-Key.
    status enum: valid | valid_catch_all | not_found. Charged only on a found email."""
    import requests

    key = os.environ.get("LEADMAGIC_API_KEY")
    if not key:
        return {"ok": False, "email": None, "status": None, "http_status": 0,
                "error": "LEADMAGIC_API_KEY absent in email-cascade secret"}
    body: dict[str, Any] = {"first_name": first or "", "last_name": last or ""}
    if domain:
        body["domain"] = domain
    elif company_name:
        body["company_name"] = company_name

    last_err: str | None = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(LEADMAGIC_URL, json=body, timeout=HTTP_TIMEOUT,
                                 headers={"X-API-Key": key, "Content-Type": "application/json"})
        except Exception as exc:  # noqa: BLE001 — network / timeout
            last_err = str(exc)
            time.sleep(2 ** attempt)
            continue
        sc = resp.status_code
        if sc == 429:
            time.sleep(float(resp.headers.get("Retry-After") or 2 * (attempt + 1)))
            continue
        if sc // 100 == 5:
            last_err = f"HTTP {sc}"
            time.sleep(2 ** attempt)
            continue
        data = _safe_json(resp)
        if sc // 100 != 2:
            return {"ok": False, "email": None,
                    "status": (data.get("code") if isinstance(data, dict) else None),
                    "http_status": sc, "error": str(data)[:300], "raw": data}
        status = data.get("status") if isinstance(data, dict) else None
        email = data.get("email") if isinstance(data, dict) else None
        found = status in _LEADMAGIC_HIT and bool(email)
        # LeadMagic's response, VERBATIM (kept on hit AND miss); the derived
        # email/status are a projection on top of it, never a replacement.
        return {"ok": True, "email": email if found else None, "status": status,
                "http_status": sc, "error": None, "raw": data}
    return {"ok": False, "email": None, "status": None, "http_status": 0,
            "error": last_err or "leadmagic retries exhausted"}


def _mv_call(email: str, timeout: int) -> dict[str, Any]:
    """Return MillionVerifier's response **EXACTLY as-is** (the JSON payload, untouched).
    The rubric + derived columns read ``resultcode``/``result``/``quality``/``subresult``
    straight off it; the full payload is persisted to ``mv_raw``. On a no-key / network
    failure (no real payload exists) a synthetic ``{resultcode: None, error}`` is returned
    — that synthetic is what gets stored, honestly recording the absence of a verdict."""
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
    """The sole arbiter. Returns ``(verdict, responses)``: ``verdict`` is the FINAL raw MV
    payload (the rubric + derived columns read it); ``responses`` is the list of EVERY raw
    MV payload for this email — 1, or 2 when an ``unknown`` (3) is re-checked at
    timeout=60. Nothing is discarded: every MV response is preserved for ``mv_raw``."""
    responses: list[dict] = []
    mv = _mv_call(email, 20)
    responses.append(mv)
    if mv.get("resultcode") == 3:  # unknown is transient → one slow retry
        mv = _mv_call(email, 60)
        responses.append(mv)
    return mv, responses


# ── The cascade (Icypeas → LeadMagic, MV after every hit) ─────────────────────
def _make_result(c: dict, email: str | None, status: str, vendor: str | None,
                 tier: int | None, mv: dict | None, certainty: str | None,
                 attempts: list, icypeas_raw: Any = None, leadmagic_raw: Any = None,
                 mv_raw: list | None = None) -> dict:
    return {
        "contact_id": c.get("contact_id"),
        "email": email,
        "verification_status": status,
        # Derived projections (convenience / indexed) — a view ON TOP of the raw payloads
        # below, never a replacement. mv = the FINAL MV verdict payload of the winner.
        "source_vendor": vendor,
        "source_tier": tier,
        "mv_resultcode": (mv or {}).get("resultcode"),
        "mv_result": (mv or {}).get("result"),
        "mv_quality": (mv or {}).get("quality"),
        "mv_subresult": (mv or {}).get("subresult"),
        "certainty": certainty,
        "company_domain": _normalize_domain(c.get("company_domain")),
        "person_linkedin_url": (c.get("person_linkedin_url") or "").strip() or None,
        # Raw upstream responses, VERBATIM — the source of truth, no interpretation imposed.
        "icypeas_raw": icypeas_raw,          # Icypeas read item, as-is (None if not called)
        "leadmagic_raw": leadmagic_raw,      # LeadMagic response, as-is (None if not called)
        "mv_raw": mv_raw,                    # every MillionVerifier response, as-is (list)
        "attempts": attempts,
    }


def _resolve_contact(c: dict, gw) -> dict:
    """Run one contact through the Icypeas → LeadMagic cascade with MV governance.
    Every provider's raw payload is preserved VERBATIM (icypeas_raw / leadmagic_raw /
    mv_raw), independent of the cascade's stop/hold/discard control flow — a discarded
    or missed address still has its raw payloads saved; only the derived email is nulled."""
    first = (c.get("first_name") or "").strip()
    last = (c.get("last_name") or "").strip()
    domain = _normalize_domain(c.get("company_domain"))
    company = (c.get("company_name") or "").strip() or None
    cid = c.get("contact_id")

    attempts: list[dict] = []
    best_risky: dict | None = None
    icypeas_raw: Any = None       # Icypeas read item, VERBATIM (set when Icypeas is called)
    leadmagic_raw: Any = None     # LeadMagic response, VERBATIM (set when LeadMagic is called)
    mv_all: list[dict] = []       # EVERY MillionVerifier response across the whole cascade

    for tier, vendor in ((1, "icypeas"), (2, "leadmagic")):
        # ── find an address (vendor-native status only detects hit vs miss) ──
        if vendor == "icypeas":
            if not ((first or last) and (domain or company)):
                attempts.append({"vendor": vendor, "tier": tier, "outcome": "skipped"})
                continue
            env = gw.remote(firstname=first, lastname=last,
                            domain_or_company=(domain or company), external_id=cid)
            icypeas_raw = env.get("raw")     # provider payload, kept on hit AND miss
            email = env.get("email") if (env.get("ok") and env.get("found")) else None
            v_status, v_err, certainty = env.get("status"), env.get("error"), env.get("certainty")
        else:  # leadmagic — requires both names
            if not (first and last and (domain or company)):
                attempts.append({"vendor": vendor, "tier": tier, "outcome": "skipped"})
                continue
            env = _leadmagic_find(first, last, domain, company)
            leadmagic_raw = env.get("raw")   # provider payload, kept on hit AND miss
            email, v_status, v_err, certainty = env.get("email"), env.get("status"), env.get("error"), None

        if not email:
            attempts.append({"vendor": vendor, "tier": tier, "outcome": "miss",
                             "vendor_status": v_status, "error": v_err})
            continue

        # ── MillionVerifier gate — the sole arbiter. Every MV response is preserved raw. ──
        verdict, mv_responses = _millionverifier(email)
        mv_all.extend(mv_responses)
        rc = verdict.get("resultcode")
        attempts.append({"vendor": vendor, "tier": tier, "outcome": "hit", "email": email,
                         "vendor_status": v_status, "certainty": certainty,
                         "mv_resultcode": rc, "mv_result": verdict.get("result"),
                         "mv_quality": verdict.get("quality")})

        if rc in MV_OK:                                  # OK → STOP & SAVE (verified)
            return _make_result(c, email, "verified", vendor, tier, verdict, certainty,
                                attempts, icypeas_raw, leadmagic_raw, mv_all or None)
        if rc in MV_RISKY:                               # catch_all/unknown → HOLD & cascade
            cand = {"email": email, "vendor": vendor, "tier": tier, "mv": verdict,
                    "certainty": certainty, "rank": _RISKY_RANK[rc]}
            if best_risky is None or cand["rank"] < best_risky["rank"]:
                best_risky = cand
            continue
        # MV_BAD, or no terminal verdict (rc None / MV outage) → DISCARD, fail-closed, cascade
        continue

    if best_risky is not None:                           # exhausted → accept-degraded, flagged
        b = best_risky
        return _make_result(c, b["email"], "risky", b["vendor"], b["tier"], b["mv"],
                            b["certainty"], attempts, icypeas_raw, leadmagic_raw, mv_all or None)
    return _make_result(c, None, "unresolved", None, None, None, None, attempts,
                        icypeas_raw, leadmagic_raw, mv_all or None)


# ── Sink writers ──────────────────────────────────────────────────────────────
def _upsert_resolution(cur, r: dict, batch_label: str | None) -> None:
    from psycopg.types.json import Jsonb

    def _j(v):  # jsonb bind, or SQL NULL when there is no payload (provider not called)
        return Jsonb(v) if v is not None else None

    cur.execute(
        """
        INSERT INTO ops.email_resolutions
            (contact_id, email, verification_status, source_vendor, source_tier,
             mv_resultcode, mv_result, mv_quality, mv_subresult, certainty,
             company_domain, person_linkedin_url, icypeas_raw, leadmagic_raw, mv_raw,
             attempts, batch_label, resolved_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
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
            icypeas_raw         = EXCLUDED.icypeas_raw,
            leadmagic_raw       = EXCLUDED.leadmagic_raw,
            mv_raw              = EXCLUDED.mv_raw,
            attempts            = EXCLUDED.attempts,
            batch_label         = EXCLUDED.batch_label,
            resolved_at         = now()
        """,
        (r["contact_id"], r["email"], r["verification_status"], r["source_vendor"],
         r["source_tier"], r["mv_resultcode"], r["mv_result"], r["mv_quality"],
         r["mv_subresult"], r["certainty"], r["company_domain"], r["person_linkedin_url"],
         _j(r.get("icypeas_raw")), _j(r.get("leadmagic_raw")), _j(r.get("mv_raw")),
         Jsonb(r["attempts"]), batch_label),
    )


def _record_run(cur, batch_label: str | None, run_root: str, counts: dict, status: str,
                error: str | None, started_at: dt.datetime, completed_at: dt.datetime) -> None:
    cur.execute(OPS_DDL)
    cur.execute(
        """
        INSERT INTO ops.email_cascade_runs
            (feed, batch_label, run_root, requested, skipped, verified, risky,
             unresolved, failed, status, error, started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (FEED, batch_label, run_root, counts["requested"], counts["skipped"],
         counts["verified"], counts["risky"], counts["unresolved"], counts["failed"],
         status, error, started_at, completed_at),
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
    never re-spend vendor credits on a settled contact unless force=True)."""
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
def _run(contacts: list[dict], batch_label: str | None, run_id: str | None,
         force: bool, trigger_callback_url: str | None) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    run_root = run_id or uuid.uuid4().hex
    counts = {"requested": 0, "skipped": 0, "verified": 0, "risky": 0,
              "unresolved": 0, "failed": 0}
    status, error = "error", None
    dsn = _hqx_dsn()

    try:
        contacts = [c for c in (contacts or []) if isinstance(c, dict) and c.get("contact_id")]
        counts["requested"] = len(contacts)
        gw = _gateway()
        conn = _open_conn(dsn)
        try:
            cur = conn.cursor()
            cur.execute(OPS_DDL)  # ensure sink exists before first write

            skip = set() if force else _already_verified(
                cur, [c["contact_id"] for c in contacts])

            for c in contacts:
                if c["contact_id"] in skip:
                    counts["skipped"] += 1
                    continue
                try:
                    r = _resolve_contact(c, gw)
                    _upsert_resolution(cur, r, batch_label)
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
                _record_run(conn2.cursor(), batch_label, run_root, counts, status, error,
                            started_at, dt.datetime.now(dt.timezone.utc))
            finally:
                conn2.close()
        except Exception as exc2:  # noqa: BLE001 — audit must not mask the failure
            print(f"WARN: ops.email_cascade_runs write failed: {exc2}")
    finally:
        _post_callback(
            trigger_callback_url,
            {"status": status, "feed": FEED, "batch_label": batch_label, "error": error, **counts},
        )

    if status != "success":
        raise RuntimeError(f"email_cascade failed: {error}")
    return {"feed": FEED, "status": status, "batch_label": batch_label, **counts}


@app.function(secrets=SECRETS, timeout=60 * 60, memory=2048, cpu=1.0)
def run_cascade(contacts: list[dict], batch_label: str | None = None, run_id: str | None = None,
                force: bool = False, trigger_callback_url: str | None = None) -> dict:
    """Resolve a chunk of contacts through the Icypeas → LeadMagic → MillionVerifier
    waterfall. Each ``contact`` is a dict:
        {contact_id, first_name, last_name, company_domain?, company_name?, person_linkedin_url?}
    """
    return _run(contacts, batch_label, run_id, force, trigger_callback_url)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.email_resolutions + ops.email_cascade_runs in HQX (idempotent)."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute("""
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name IN ('email_resolutions','email_cascade_runs')
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
    """Read-back: latest resolutions + run-state + status histogram."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT contact_id, email, verification_status, source_vendor, source_tier,
                      mv_result, icypeas_raw, leadmagic_raw, mv_raw, resolved_at
               FROM ops.email_resolutions ORDER BY resolved_at DESC LIMIT %s""",
            (limit,),
        )
        rcols = [d.name for d in cur.description]
        resolutions = [dict(zip(rcols, r)) for r in cur.fetchall()]
        cur.execute(
            "SELECT verification_status, count(*) FROM ops.email_resolutions GROUP BY 1")
        histogram = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute(
            """SELECT batch_label, requested, skipped, verified, risky, unresolved, failed,
                      status, recorded_at
               FROM ops.email_cascade_runs ORDER BY recorded_at DESC LIMIT 3""")
        runcols = [d.name for d in cur.description]
        runs = [dict(zip(runcols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    print(f"status histogram: {histogram}")
    for r in resolutions:
        print(f"  {r['contact_id']:<20} {r['verification_status']:<11} "
              f"{(r['email'] or '-'):<36} via={r['source_vendor'] or '-'}")
    return {"histogram": histogram, "recent_resolutions": resolutions, "recent_runs": runs}


@app.function(secrets=SECRETS, timeout=120)
def health() -> dict:
    """Live key-wiring check for all three vendors before a real batch.
    Icypeas via the gateway (~1 credit); MillionVerifier /credits and the LeadMagic
    not_found probe are FREE. ``all_valid`` is the go/no-go signal."""
    import requests

    out: dict[str, Any] = {}

    # Icypeas — delegate to the gateway (owner of ICYPEAS_API_KEY).
    try:
        out["icypeas"] = modal.Function.from_name(GATEWAY_APP, "key_check").remote()
    except Exception as exc:  # noqa: BLE001
        out["icypeas"] = {"valid": False, "error": str(exc)}

    # MillionVerifier — free /credits endpoint confirms the key + remaining balance.
    mv_key = os.environ.get("MILLIONVERIFIER_API_KEY")
    if not mv_key:
        out["millionverifier"] = {"valid": False, "error": "MILLIONVERIFIER_API_KEY absent"}
    else:
        try:
            r = requests.get(MV_URL.rstrip("/") + "/credits", params={"api": mv_key}, timeout=20)
            data = _safe_json(r)
            out["millionverifier"] = {
                "valid": r.status_code == 200 and isinstance(data, dict) and "credits" in data,
                "http_status": r.status_code,
                "credits": (data.get("credits") if isinstance(data, dict) else None)}
        except Exception as exc:  # noqa: BLE001
            out["millionverifier"] = {"valid": False, "error": str(exc)}

    # LeadMagic — bogus name at a REAL (MX-bearing) domain ⇒ free not_found + HTTP 200,
    # proving the key authenticates AND a real lookup is processed end-to-end.
    lm_key = os.environ.get("LEADMAGIC_API_KEY")
    if not lm_key:
        out["leadmagic"] = {"valid": False, "error": "LEADMAGIC_API_KEY absent"}
    else:
        try:
            r = requests.post(
                LEADMAGIC_URL,
                json={"first_name": "Zznonexistent", "last_name": "Qqhealthcheck", "domain": "microsoft.com"},
                headers={"X-API-Key": lm_key, "Content-Type": "application/json"}, timeout=20)
            data = _safe_json(r)
            out["leadmagic"] = {
                "valid": r.status_code == 200,   # 200 ⇒ processed (found|not_found); 401/403 ⇒ bad key
                "http_status": r.status_code,
                "status": (data.get("status") if isinstance(data, dict) else None),
                "detail": (None if r.status_code == 200
                           else str(data)[:200] if isinstance(data, dict) else None)}
        except Exception as exc:  # noqa: BLE001
            out["leadmagic"] = {"valid": False, "error": str(exc)}

    out["all_valid"] = all(out[v].get("valid") for v in ("icypeas", "millionverifier", "leadmagic"))
    print(f"health: all_valid={out['all_valid']} · {out}")
    return out


@app.local_entrypoint()
def health_check() -> None:
    """Live vendor key-wiring check. --> all_valid is the go/no-go."""
    import json

    print(json.dumps(health.remote(), indent=2, default=str))


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.email_resolutions + ops.email_cascade_runs DDL (HQX)."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def verify_run(limit: int = 8) -> None:
    """Read-back assertion on the most-recent resolutions."""
    import json

    print(json.dumps(verify.remote(limit), indent=2, default=str))


@app.local_entrypoint()
def run_manual(contacts_json: str) -> None:
    """Manual run. --contacts-json '[{"contact_id":"c1","first_name":"Jean",
    "last_name":"Dupont","company_domain":"icypeas.com"}]'"""
    import json

    contacts = json.loads(contacts_json)
    print(json.dumps(run_cascade.remote(contacts, batch_label="manual"), indent=2, default=str))
