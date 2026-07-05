"""Enrichment Icypeas+MV worker — bulk Work-Email rail (icypeas + millionverifier only).

A two-stage batch pipeline: **Icypeas bulk email-search → MillionVerifier**. No LeadMagic,
no Blitz, no pattern permutation — a single finder gated by the sole deliverability arbiter.
Spawned by the Universal Dispatcher (``core/modal_dispatcher.py``) and woken via the Trigger
waitpoint callback.

    run_bulk   contacts[] (≤5000) → resolved work emails (verified | risky | unresolved)

WHY BULK (not the one-at-a-time cascade). Icypeas exposes a first-class ``/bulk-search``
primitive: up to 5000 rows in ONE launch (1/sec), a bulk-file progression poll
(``/search-files/read`` 15/min), and a paginated ``mode:"bulk"`` result drain
(``/bulk-single-searchs/read`` 30/min). This is ~100× the read-bound throughput of the
per-contact submit+poll path (``core/icypeas_gateway.py::find_email``): one launch covers
5000 contacts the single path would need 5000 submits + ≥5000 reads to resolve. This worker
holds NO Icypeas key and implements NO Icypeas rate logic — every Icypeas call is delegated
to the single-container ``icypeas-gateway`` bulk functions (``launch_bulk`` / ``file_status``
/ ``drain_results``), the authoritative global egress.

VERBATIM PAYLOAD PRESERVATION (Directive 28 doctrine, operator-mandated for this rail).
BOTH providers' payloads are persisted **exactly as returned, with no interpretation
imposed** — AND on every outcome, including no-response:

    icypeas_raw   the drained Icypeas result item, VERBATIM (found AND not-found). When a
                  contact was ineligible (never sent) or absent from the drain, an explicit
                  ``{"_synthetic": true, ...}`` marker records the absence honestly — never a
                  silent NULL for a processed contact.
    mv_raw        an ARRAY of EVERY MillionVerifier response for the address, VERBATIM —
                  including the synthetic ``{"resultcode": null, "error": ...}`` a network /
                  timeout / no-key failure yields (that synthetic IS the honest "no response"
                  record), and a tagged sentinel when MV was never called (Icypeas miss).

The derived columns (``email`` / ``verification_status`` / ``mv_*`` / ``certainty`` /
``email_domain_norm``) are a convenience projection ON TOP of the raw, never a replacement.

VERIFICATION RUBRIC (MillionVerifier ``resultcode`` is the single arbiter; Icypeas's own
``certainty`` is NEVER trusted as deliverability, only to detect hit-vs-miss):

    1 ok          → verified
    2 catch_all   → risky
    3 unknown     → retry once at timeout=60; still unknown → risky
    4/5/6 error/disposable/invalid, or NO verdict (MV outage) → fail-closed: unresolved,
                  derived email dropped (raw payloads still preserved verbatim)

With a single finder the cascade's cross-tier ``best_risky`` hold collapses: each contact is
Icypeas-hit? → MV-verify → {verified | risky | unresolved}.

SINK (ops-layer, shared work-email system-of-record). Latest-wins upsert per ``contact_id``
into ``ops.email_resolutions`` (co-written by the cascade + Blitz finders; this rail sets
``source_vendor='icypeas'`` + ``icypeas_raw`` + ``mv_raw``, leaving other finders' raw columns
untouched). Per-run terminal state into ``ops.icypeas_mv_runs``. A downstream materializer can
roll ``ops.email_resolutions`` into a Lance dataset on its own cadence.

    modal deploy pipelines/enrichment_icypeas_mv/enrich_icypeas_mv.py
    modal run    pipelines/enrichment_icypeas_mv/enrich_icypeas_mv.py::init_ops
    modal run    pipelines/enrichment_icypeas_mv/enrich_icypeas_mv.py::health_check
    modal run    pipelines/enrichment_icypeas_mv/enrich_icypeas_mv.py::run_manual \\
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

FEED = "icypeas_mv"

# icypeas-gateway bulk primitives (the sole Icypeas egress — this worker holds no key).
GATEWAY_APP = "icypeas-gateway"
GW_LAUNCH, GW_STATUS, GW_DRAIN = "launch_bulk", "file_status", "drain_results"

# MillionVerifier — single real-time API, called inline (elastic, no hard cap), concurrently.
MV_URL = os.environ.get("MILLIONVERIFIER_API_BASE", "https://api.millionverifier.com").rstrip("/") + "/api/v3/"
MV_OK = {1}            # ok          → verified
MV_RISKY = {2, 3}      # catch_all, unknown → risky
MV_BAD = {4, 5, 6}     # error, disposable, invalid → unresolved (fail-closed)

# Bulk drain page size + poll tuning (mirror the gateway's ≤100/page; overridable via env).
BULK_READ_LIMIT = int(os.environ.get("ICYPEAS_BULK_READ_LIMIT", "100"))
POLL_INTERVAL = float(os.environ.get("ICYPEAS_BULK_POLL_INTERVAL", "8.0"))
POLL_CEILING_SEC = float(os.environ.get("ICYPEAS_BULK_POLL_CEILING_SEC", "2400"))  # 40 min
MV_CONCURRENCY = int(os.environ.get("MV_CONCURRENCY", "32"))

HTTP_TIMEOUT = 30.0
MAX_RETRIES = 3

# Icypeas status lifecycle (mirror the gateway). Non-terminal ⇒ no usable email.
_PENDING = {"NONE", "SCHEDULED", "IN_PROGRESS", ""}
_TERMINAL_NOT_FOUND = {"NOT_FOUND", "DEBITED_NOT_FOUND"}

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "psycopg[binary]>=3.2",  # ops.email_resolutions + ops.icypeas_mv_runs
    "requests>=2.32",        # MillionVerifier + Trigger callback
)

app = modal.App("enrichment-icypeas-mv", image=image)

SECRETS = [
    # holds MILLIONVERIFIER_API_KEY (LEADMAGIC_API_KEY also present but unused here). The
    # Icypeas key is NOT here — it lives only in the icypeas-gateway app (blast radius).
    modal.Secret.from_name("email-cascade"),
    modal.Secret.from_name("hqx-postgres"),   # ops.email_resolutions + ops.icypeas_mv_runs
]

# ── ops DDL — canonical mirror of the .sql sibling, applied idempotently before each
# terminal write. ops.email_resolutions is the SHARED work-email SoR (co-written by other
# finders); this rail additionally introduces email_domain_norm (Directive 21 §8's key). ──
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
-- Additive, idempotent upgrades on the shared table (a prior writer may already own it).
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS icypeas_raw       jsonb;
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS leadmagic_raw     jsonb;
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS blitz_email_raw   jsonb;
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS mv_raw            jsonb;
-- email_domain_norm: normalized domain half of the resolved email — the BTREE dedupe/join
-- key the original Directive 21 §8 specified (the as-built cascade dropped it; revived here).
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS email_domain_norm text;
CREATE INDEX IF NOT EXISTS email_resolutions_status_idx      ON ops.email_resolutions (verification_status);
CREATE INDEX IF NOT EXISTS email_resolutions_domain_idx      ON ops.email_resolutions (company_domain);
CREATE INDEX IF NOT EXISTS email_resolutions_email_idx       ON ops.email_resolutions (email);
CREATE INDEX IF NOT EXISTS email_resolutions_email_dnorm_idx ON ops.email_resolutions (email_domain_norm);

CREATE TABLE IF NOT EXISTS ops.icypeas_mv_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    batch_label   text,
    run_root      text,
    requested     bigint      NOT NULL DEFAULT 0,
    skipped       bigint      NOT NULL DEFAULT 0,   -- already-verified, skipped (idempotency)
    ineligible    bigint      NOT NULL DEFAULT 0,   -- missing name+anchor, never sent to Icypeas
    verified      bigint      NOT NULL DEFAULT 0,
    risky         bigint      NOT NULL DEFAULT 0,
    unresolved    bigint      NOT NULL DEFAULT 0,
    failed        bigint      NOT NULL DEFAULT 0,
    status        text        NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT icypeas_mv_runs_status_chk CHECK (status IN ('success', 'error'))
);
CREATE INDEX IF NOT EXISTS icypeas_mv_runs_feed_idx        ON ops.icypeas_mv_runs (feed);
CREATE INDEX IF NOT EXISTS icypeas_mv_runs_recorded_at_idx ON ops.icypeas_mv_runs (recorded_at DESC);
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


def _synthetic(reason: str, called: bool = False, **extra) -> dict:
    """Explicit honest marker stored where a provider produced no real payload — a processed
    contact NEVER gets a silent NULL raw column. Tagged so it can't be mistaken for a payload."""
    return {"_synthetic": True, "called": called, "reason": reason, **extra}


def _hqx_dsn() -> str:
    """Transaction-mode (Supavisor :6543) hq-x DSN. This worker holds one connection for the
    whole run while mostly idle (blocked on the rate-governed bulk gateway between sparse DB
    touches); the SESSION pooler would pin a backend per run and exhaust the 15-slot ceiling.
    Prefer an explicit HQX_DB_URL_TRANSACTION; else derive it from the session DSN (5432→6543)."""
    dsn = os.environ.get("HQX_DB_URL_TRANSACTION")
    if not dsn:
        pooled = os.environ.get("HQX_DB_URL_POOLED")
        if not pooled:
            raise RuntimeError(
                "Neither HQX_DB_URL_TRANSACTION nor HQX_DB_URL_POOLED set in the hqx-postgres secret.")
        dsn = pooled.replace(".pooler.supabase.com:5432", ".pooler.supabase.com:6543")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    return dsn


def _open_conn(dsn: str):
    import psycopg

    # prepare_threshold=None: under transaction-mode pooling consecutive statements may land on
    # different backends, so server-side prepared statements are unsafe. autocommit=True frees the
    # backend between the batched read and the per-entity writes.
    return psycopg.connect(dsn, autocommit=True, prepare_threshold=None)


def _gw(fn: str):
    return modal.Function.from_name(GATEWAY_APP, fn)


# ── Icypeas result-item extraction (mirrors the gateway's projection) ─────────
def _extract(item: dict) -> tuple[str, str | None, str | None]:
    """(status, email|None, certainty|None) from a drained bulk result item."""
    status = (item.get("status") or "").upper()
    results = item.get("results") or {}
    emails = results.get("emails") or []
    if emails and isinstance(emails, list) and isinstance(emails[0], dict):
        return status, emails[0].get("email"), emails[0].get("certainty")
    return status, None, None


def _item_external_id(item: dict) -> str | None:
    """The echoed externalId that maps a drained item back to its contact_id. Exact location
    is VERIFY-AT-BUILD — probe custom.externalId then top-level externalId/external_id."""
    if not isinstance(item, dict):
        return None
    custom = item.get("custom")
    if isinstance(custom, dict):
        for k in ("externalId", "external_id"):
            v = custom.get(k)
            if isinstance(v, str) and v:
                return v
    for k in ("externalId", "external_id"):
        v = item.get(k)
        if isinstance(v, str) and v:
            return v
    return None


# ── MillionVerifier (the sole arbiter) — every response preserved verbatim ────
def _mv_call(email: str, timeout: int) -> dict[str, Any]:
    """Return MillionVerifier's response EXACTLY as-is. On a no-key / network / timeout failure
    (no real payload exists) a synthetic ``{resultcode: None, error}`` is returned — that
    synthetic is what gets stored, honestly recording the absence of a verdict."""
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
    """Returns ``(verdict, responses)``: ``verdict`` is the FINAL raw MV payload; ``responses``
    is the list of EVERY raw MV payload for this email — 1, or 2 when an ``unknown`` (3) is
    re-checked at timeout=60. Nothing is discarded; every response is preserved for ``mv_raw``."""
    responses: list[dict] = []
    mv = _mv_call(email, 20)
    responses.append(mv)
    if mv.get("resultcode") == 3:  # unknown is transient → one slow retry
        mv = _mv_call(email, 60)
        responses.append(mv)
    return mv, responses


def _mv_verify_batch(emails: list[str]) -> dict[str, tuple[dict, list]]:
    """Verify unique emails concurrently (MV is elastic, no hard cap). Deduped by address, so
    two contacts sharing an email spend one MV credit and receive the same verbatim responses."""
    out: dict[str, tuple[dict, list]] = {}
    if not emails:
        return out
    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=min(MV_CONCURRENCY, len(emails))) as ex:
        futs = {ex.submit(_millionverifier, e): e for e in emails}
        for fut in as_completed(futs):
            e = futs[fut]
            try:
                out[e] = fut.result()
            except Exception as exc:  # noqa: BLE001 — verifier thread must not sink the batch
                syn = {"resultcode": None, "error": str(exc)}
                out[e] = (syn, [syn])
    return out


# ── Result row assembly ────────────────────────────────────────────────────────
def _make_result(c: dict, email: str | None, status: str, vendor: str | None, tier: int | None,
                 mv: dict | None, certainty: str | None, attempts: list,
                 icypeas_raw: Any, mv_raw: list) -> dict:
    edn = _normalize_domain(email.split("@")[-1]) if (email and "@" in email) else None
    return {
        "contact_id": c.get("contact_id"),
        "email": email,
        "email_domain_norm": edn,
        "verification_status": status,
        # Derived projections (convenience / indexed) — a view ON TOP of the raw payloads below.
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
        "icypeas_raw": icypeas_raw,   # drained Icypeas item, or a tagged synthetic on absence
        "mv_raw": mv_raw,             # array of every MillionVerifier response (or sentinel)
        "attempts": attempts,
    }


def _eligible(c: dict) -> bool:
    """Icypeas email-search needs (firstname and/or lastname) + a domain/company anchor."""
    first = (c.get("first_name") or "").strip()
    last = (c.get("last_name") or "").strip()
    anchor = _normalize_domain(c.get("company_domain")) or (c.get("company_name") or "").strip() or None
    return bool((first or last) and anchor)


def _build_rows(eligible: list[dict]) -> tuple[list[list[str]], list[str]]:
    """Row-aligned (data, externalIds) for one bulk launch. email-search row =
    [firstname, lastname, domainOrCompany]; externalId = contact_id (the drain-to-sink key)."""
    rows: list[list[str]] = []
    ext_ids: list[str] = []
    for c in eligible:
        first = (c.get("first_name") or "").strip()
        last = (c.get("last_name") or "").strip()
        anchor = _normalize_domain(c.get("company_domain")) or (c.get("company_name") or "").strip() or ""
        rows.append([first, last, anchor])
        ext_ids.append(c["contact_id"])
    return rows, ext_ids


def _poll_until_done(status_fn, file_id: str) -> tuple[bool, dict | None]:
    deadline = time.monotonic() + POLL_CEILING_SEC
    last: dict | None = None
    while time.monotonic() < deadline:
        st = status_fn.remote(file_id)
        last = st
        if st.get("done"):
            return True, st
        time.sleep(POLL_INTERVAL)
    return False, last


def _drain_all(drain_fn, file_id: str, n_rows: int) -> tuple[dict[str, dict], int]:
    """Page the full bulk result set into {contact_id: verbatim_item}. Pagination advances on
    the ``sorts`` token echoed by each page (VERIFY-AT-BUILD field); stops on a short page."""
    items_by_cid: dict[str, dict] = {}
    sorts = None
    unmapped = 0
    max_pages = (n_rows // BULK_READ_LIMIT) + 5
    for _ in range(max_pages):
        page = drain_fn.remote(file_id, sorts)
        if not page.get("ok"):
            page = drain_fn.remote(file_id, sorts)  # one transient retry
            if not page.get("ok"):
                print(f"WARN: drain page failed for file {file_id}: {page.get('error')}")
                break
        for item in page.get("items", []):
            cid = _item_external_id(item)
            if cid:
                items_by_cid[cid] = item
            else:
                unmapped += 1
        sorts = page.get("sorts")
        if page.get("count", 0) < BULK_READ_LIMIT or not sorts:
            break
    return items_by_cid, unmapped


def _resolve_and_verify(eligible: list[dict], items_by_cid: dict[str, dict]) -> list[dict]:
    """Map drained items → contacts, MV-verify the hits concurrently, classify. Every contact
    gets verbatim icypeas_raw AND mv_raw regardless of the hit/miss/timeout control flow."""
    prepared: list[tuple] = []   # (c, icy_raw, email|None, certainty, icy_status)
    unique_emails: set[str] = set()
    for c in eligible:
        item = items_by_cid.get(c["contact_id"])
        if item is None:
            # Row was launched but absent from the drain (poll timeout / pagination gap).
            prepared.append((c, _synthetic("not_in_drain", called=True, status="POLL_TIMEOUT"),
                             None, None, "POLL_TIMEOUT"))
            continue
        icy_status, email, certainty = _extract(item)
        email = email if (email and icy_status not in _PENDING) else None
        prepared.append((c, item, email, certainty, icy_status))
        if email:
            unique_emails.add(email)

    verdicts = _mv_verify_batch(list(unique_emails))

    results: list[dict] = []
    for (c, icy_raw, email, certainty, icy_status) in prepared:
        if not email:
            # Icypeas miss / not-in-drain → unresolved. MV never called → tagged sentinel.
            outcome = "miss" if icy_status in _TERMINAL_NOT_FOUND else "no_result"
            attempts = [{"vendor": "icypeas", "tier": 1, "outcome": outcome, "icypeas_status": icy_status}]
            results.append(_make_result(c, None, "unresolved", "icypeas", 1, None, certainty,
                                        attempts, icy_raw, [_synthetic("no_candidate_email")]))
            continue

        verdict, responses = verdicts.get(email, (None, [_synthetic("mv_missing", called=True)]))
        rc = (verdict or {}).get("resultcode")
        if rc in MV_OK:
            out_status, out_email = "verified", email
        elif rc in MV_RISKY:
            out_status, out_email = "risky", email
        else:
            # MV bad (4/5/6) OR no verdict (None / outage) → fail-closed: drop the derived email;
            # raw payloads (icy_raw + responses) are still preserved verbatim below.
            out_status, out_email = "unresolved", None
        attempts = [{"vendor": "icypeas", "tier": 1, "outcome": "hit", "email": email,
                     "icypeas_status": icy_status, "certainty": certainty, "mv_resultcode": rc,
                     "mv_result": (verdict or {}).get("result"), "mv_quality": (verdict or {}).get("quality")}]
        results.append(_make_result(c, out_email, out_status, "icypeas", 1, verdict, certainty,
                                    attempts, icy_raw, responses))
    return results


# ── Sink writers ────────────────────────────────────────────────────────────────
def _upsert_resolution(cur, r: dict, batch_label: str | None) -> None:
    from psycopg.types.json import Jsonb

    def _j(v):  # jsonb bind, or SQL NULL when there is no payload
        return Jsonb(v) if v is not None else None

    # This rail owns only the columns below; leadmagic_raw / blitz_email_raw are NOT touched, so a
    # prior finder's raw payloads survive a latest-wins overwrite of the resolution.
    cur.execute(
        """
        INSERT INTO ops.email_resolutions
            (contact_id, email, email_domain_norm, verification_status, source_vendor, source_tier,
             mv_resultcode, mv_result, mv_quality, mv_subresult, certainty,
             company_domain, person_linkedin_url, icypeas_raw, mv_raw, attempts, batch_label, resolved_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
        ON CONFLICT (contact_id) DO UPDATE SET
            email               = EXCLUDED.email,
            email_domain_norm   = EXCLUDED.email_domain_norm,
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
            mv_raw              = EXCLUDED.mv_raw,
            attempts            = EXCLUDED.attempts,
            batch_label         = EXCLUDED.batch_label,
            resolved_at         = now()
        """,
        (r["contact_id"], r["email"], r["email_domain_norm"], r["verification_status"],
         r["source_vendor"], r["source_tier"], r["mv_resultcode"], r["mv_result"], r["mv_quality"],
         r["mv_subresult"], r["certainty"], r["company_domain"], r["person_linkedin_url"],
         _j(r.get("icypeas_raw")), _j(r.get("mv_raw")), Jsonb(r["attempts"]), batch_label),
    )


def _record_run(cur, batch_label: str | None, run_root: str, counts: dict, status: str,
                error: str | None, started_at: dt.datetime, completed_at: dt.datetime) -> None:
    cur.execute(OPS_DDL)
    cur.execute(
        """
        INSERT INTO ops.icypeas_mv_runs
            (feed, batch_label, run_root, requested, skipped, ineligible, verified, risky,
             unresolved, failed, status, error, started_at, completed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (FEED, batch_label, run_root, counts["requested"], counts["skipped"], counts["ineligible"],
         counts["verified"], counts["risky"], counts["unresolved"], counts["failed"],
         status, error, started_at, completed_at),
    )


def _already_verified(cur, contact_ids: list[str]) -> set[str]:
    """Batched skip-set: contacts already resolved to a VERIFIED email (idempotency — never
    re-spend vendor credits on a settled contact unless force=True)."""
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


# ── Runner ────────────────────────────────────────────────────────────────────
def _run(contacts: list[dict], batch_label: str | None, run_id: str | None,
         force: bool, trigger_callback_url: str | None) -> dict:
    started_at = dt.datetime.now(dt.timezone.utc)
    run_root = run_id or uuid.uuid4().hex
    counts = {"requested": 0, "skipped": 0, "ineligible": 0, "verified": 0, "risky": 0,
              "unresolved": 0, "failed": 0}
    status, error = "error", None
    dsn = _hqx_dsn()

    try:
        contacts = [c for c in (contacts or []) if isinstance(c, dict) and c.get("contact_id")]
        counts["requested"] = len(contacts)
        launch_fn, status_fn, drain_fn = _gw(GW_LAUNCH), _gw(GW_STATUS), _gw(GW_DRAIN)
        conn = _open_conn(dsn)
        try:
            cur = conn.cursor()
            cur.execute(OPS_DDL)  # ensure sink + runs table exist before first write

            skip = set() if force else _already_verified(cur, [c["contact_id"] for c in contacts])

            eligible, ineligible = [], []
            for c in contacts:
                if c["contact_id"] in skip:
                    counts["skipped"] += 1
                    continue
                (eligible if _eligible(c) else ineligible).append(c)

            # Ineligible → honest unresolved; neither provider was ever called.
            for c in ineligible:
                r = _make_result(
                    c, None, "unresolved", None, None, None, None,
                    [{"vendor": "icypeas", "tier": 1, "outcome": "skipped",
                      "reason": "ineligible_missing_inputs"}],
                    _synthetic("ineligible_missing_inputs"),
                    [_synthetic("ineligible_missing_inputs")])
                try:
                    _upsert_resolution(cur, r, batch_label)
                    counts["ineligible"] += 1
                except Exception as exc:  # noqa: BLE001
                    counts["failed"] += 1
                    print(f"WARN: ineligible upsert {c.get('contact_id')!r} failed: {exc}")

            if eligible:
                rows, ext_ids = _build_rows(eligible)
                name = (f"core-x {FEED} {batch_label or 'batch'} {run_root}")[:120]
                launch = launch_fn.remote(name, rows, ext_ids)
                if not launch.get("ok") or not launch.get("file_id"):
                    raise RuntimeError(
                        f"bulk launch failed: {launch.get('error')} raw={str(launch.get('raw'))[:200]}")
                file_id = launch["file_id"]
                logger_note = {"file_id": file_id, "rows": len(rows)}
                print(f"bulk launched: {logger_note}")

                done, last_status = _poll_until_done(status_fn, file_id)
                items_by_cid, unmapped = _drain_all(drain_fn, file_id, len(rows))
                if not done:
                    print(f"WARN: file {file_id} not 'done' by {POLL_CEILING_SEC}s ceiling; "
                          f"draining partial. last_status={last_status}")
                if unmapped:
                    print(f"WARN: {unmapped} drained items lacked an externalId — verify "
                          f"_item_external_id field mapping against the live drain payload.")

                results = _resolve_and_verify(eligible, items_by_cid)
                for r in results:
                    try:
                        _upsert_resolution(cur, r, batch_label)
                        counts[r["verification_status"]] += 1
                    except Exception as exc:  # noqa: BLE001 — one contact must not sink the batch
                        counts["failed"] += 1
                        print(f"WARN: upsert contact {r.get('contact_id')!r} failed: {exc}")

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
            print(f"WARN: ops.icypeas_mv_runs write failed: {exc2}")
    finally:
        _post_callback(
            trigger_callback_url,
            {"status": status, "feed": FEED, "batch_label": batch_label, "error": error, **counts},
        )

    if status != "success":
        raise RuntimeError(f"icypeas_mv failed: {error}")
    return {"feed": FEED, "status": status, "batch_label": batch_label, **counts}


@app.function(secrets=SECRETS, timeout=60 * 60 * 2, memory=4096, cpu=2.0)
def run_bulk(contacts: list[dict], batch_label: str | None = None, run_id: str | None = None,
             force: bool = False, trigger_callback_url: str | None = None) -> dict:
    """Resolve a chunk of contacts (≤5000) through the Icypeas bulk-search → MillionVerifier
    rail. Each ``contact`` is a dict:
        {contact_id, first_name, last_name, company_domain?, company_name?, person_linkedin_url?}
    """
    return _run(contacts, batch_label, run_id, force, trigger_callback_url)


@app.function(secrets=[modal.Secret.from_name("hqx-postgres")], timeout=60 * 5)
def apply_ops_ddl() -> dict:
    """Create ops.email_resolutions (+ email_domain_norm) + ops.icypeas_mv_runs (idempotent)."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(OPS_DDL)
        cur.execute("""
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema='ops' AND table_name IN ('email_resolutions','icypeas_mv_runs')
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
    """Read-back: latest icypeas-sourced resolutions + run-state + status histogram."""
    conn = _open_conn(_hqx_dsn())
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT contact_id, email, email_domain_norm, verification_status, source_vendor,
                      mv_result, certainty, icypeas_raw, mv_raw, resolved_at
               FROM ops.email_resolutions WHERE source_vendor = 'icypeas'
               ORDER BY resolved_at DESC LIMIT %s""",
            (limit,),
        )
        rcols = [d.name for d in cur.description]
        resolutions = [dict(zip(rcols, r)) for r in cur.fetchall()]
        cur.execute(
            "SELECT verification_status, count(*) FROM ops.email_resolutions "
            "WHERE source_vendor = 'icypeas' GROUP BY 1")
        histogram = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute(
            """SELECT batch_label, requested, skipped, ineligible, verified, risky, unresolved,
                      failed, status, recorded_at
               FROM ops.icypeas_mv_runs ORDER BY recorded_at DESC LIMIT 3""")
        runcols = [d.name for d in cur.description]
        runs = [dict(zip(runcols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    print(f"icypeas status histogram: {histogram}")
    for r in resolutions:
        print(f"  {r['contact_id']:<20} {r['verification_status']:<11} {(r['email'] or '-'):<36}")
    return {"histogram": histogram, "recent_resolutions": resolutions, "recent_runs": runs}


@app.function(secrets=SECRETS, timeout=120)
def health() -> dict:
    """Live key-wiring check before a real batch: Icypeas (via the gateway) + MillionVerifier.
    ``all_valid`` is the go/no-go signal."""
    import requests

    out: dict[str, Any] = {}
    try:
        out["icypeas"] = modal.Function.from_name(GATEWAY_APP, "key_check").remote()
    except Exception as exc:  # noqa: BLE001
        out["icypeas"] = {"valid": False, "error": str(exc)}

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

    out["all_valid"] = all(out[v].get("valid") for v in ("icypeas", "millionverifier"))
    print(f"health: all_valid={out['all_valid']} · {out}")
    return out


@app.local_entrypoint()
def health_check() -> None:
    """Live vendor key-wiring check. --> all_valid is the go/no-go."""
    import json

    print(json.dumps(health.remote(), indent=2, default=str))


@app.local_entrypoint()
def init_ops() -> None:
    """Apply the ops.email_resolutions (+ email_domain_norm) + ops.icypeas_mv_runs DDL (HQX)."""
    print(apply_ops_ddl.remote())


@app.local_entrypoint()
def verify_run(limit: int = 8) -> None:
    """Read-back assertion on the most-recent icypeas-sourced resolutions."""
    import json

    print(json.dumps(verify.remote(limit), indent=2, default=str))


@app.local_entrypoint()
def run_manual(contacts_json: str) -> None:
    """Manual run. --contacts-json '[{"contact_id":"c1","first_name":"Jean",
    "last_name":"Dupont","company_domain":"icypeas.com"}]'"""
    import json

    contacts = json.loads(contacts_json)
    print(json.dumps(run_bulk.remote(contacts, batch_label="manual"), indent=2, default=str))
