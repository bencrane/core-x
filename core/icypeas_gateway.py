"""Icypeas API gateway — the fleet's single rate-governed egress to Icypeas.

ONE authoritative pair of token buckets for the entire email-cascade plane
(Directive 21-Final). Every Icypeas HTTP call from the ``enrichment-email-cascade``
worker flows through this one function. This is the Icypeas-egress parallel to
``core/blitz_gateway.py``: a single cross-cutting rate concern localized in one
``core/`` Modal app.

WHY SINGLE-CONTAINER. Icypeas meters TWO independent per-route limits (verified
2026-06-03 against api-doc.icypeas.com/how-works/rate_limits):

    POST /api/email-search             submit  →  10 calls / SEC
    POST /api/bulk-single-searchs/read read    →  30 calls / MIN   ← binding constraint

Modal scales workers horizontally, so a per-worker limiter cannot bound a global
ceiling. ``max_containers=1`` puts both authoritative token buckets in exactly one
process — true global ≤10 RPS submit AND ≤30 RPM read, no distributed coordination.
``@modal.concurrent`` lets many in-flight submit+poll coroutines share the one
container so the read bucket (not the container count) paces egress.

ASYNC MODEL. Icypeas email discovery is asynchronous: submit returns a scan id
(``item._id``) with status ``NONE``; the result is fetched by polling the read
route until a terminal status. ``find_email`` encapsulates submit + bounded poll
behind ONE ``.remote()`` so the worker receives a final envelope. The read route's
30/min ceiling is the real production constraint; an initial settle delay plus
capped polling keeps poll amplification low. (Webhooks would remove the poll
ceiling entirely but require a public callback endpoint — deferred; see the spec.)

SECRET BLAST RADIUS. ``ICYPEAS_API_KEY`` lives ONLY here (the ``icypeas-api`` Modal
secret, mirror of Doppler ``core-x/prd``). The worker never holds it. Auth is a raw
key in the ``Authorization`` header — NOT an HMAC of outbound requests (the Icypeas
HMAC-SHA1 scheme is webhook-verification-only and unused here).

    modal deploy core/icypeas_gateway.py
    modal run    core/icypeas_gateway.py::smoke --domain icypeas.com   # smoke test
"""

from __future__ import annotations

import asyncio
import collections
import os
import time
from typing import Any

import modal

app = modal.App("icypeas-gateway")

image = modal.Image.debian_slim(python_version="3.12").pip_install("httpx>=0.27,<1.0")

# ── Icypeas coordinates ──────────────────────────────────────────────────────
BASE_URL = os.environ.get("ICYPEAS_API_BASE", "https://app.icypeas.com/api").rstrip("/")
SUBMIT_PATH = "/email-search"
READ_PATH = "/bulk-single-searchs/read"  # NOTE: docs spell it "searchs" (sic)

# Per-route rate caps (live docs 2026-06-03). Pinned as the authoritative buckets;
# overridable via env if Icypeas revises them ("always subject to change").
SUBMIT_MAX = int(os.environ.get("ICYPEAS_SUBMIT_PER_SEC", "10"))   # /email-search 10/sec
SUBMIT_WINDOW = 1.0
READ_MAX = int(os.environ.get("ICYPEAS_READ_PER_MIN", "30"))        # /…/read 30/min  ← binding
READ_WINDOW = 60.0

HTTP_TIMEOUT = 15.0
MAX_RETRIES = 3
RATE_429_COOLDOWN = 5.0          # short cool-down; buckets should prevent 429s

# Poll tuning — Icypeas resolves in a few seconds; settle before the first read to
# spend the fewest 30/min read tokens per contact.
POLL_INITIAL_DELAY = float(os.environ.get("ICYPEAS_POLL_INITIAL_DELAY", "4.0"))
POLL_INTERVAL = float(os.environ.get("ICYPEAS_POLL_INTERVAL", "3.0"))
MAX_POLLS = int(os.environ.get("ICYPEAS_MAX_POLLS", "8"))

# Status lifecycle (docs: how-works/search_statuses). Non-terminal → keep polling.
_PENDING = {"NONE", "SCHEDULED", "IN_PROGRESS"}
_TERMINAL_FOUND = {"FOUND", "DEBITED"}
_TERMINAL_NOT_FOUND = {"NOT_FOUND", "DEBITED_NOT_FOUND"}
_TERMINAL_ERROR = {"BAD_INPUT", "INSUFFICIENT_FUNDS", "ABORTED"}


# ── token buckets (one event loop in the single container) ────────────────────
class _Bucket:
    """Sliding-window token bucket. One emitter daemon per bucket releases up to
    ``max_per`` grants per ``window`` seconds to FIFO waiters."""

    def __init__(self, max_per: int, window: float):
        self.max_per = max_per
        self.window = window
        self._waiters: collections.deque[asyncio.Future] = collections.deque()
        self._grants: collections.deque[float] = collections.deque()
        self._started = False

    async def _emit(self) -> None:
        while True:
            now = time.monotonic()
            while self._grants and self._grants[0] <= now - self.window:
                self._grants.popleft()
            if self._waiters and len(self._grants) < self.max_per:
                fut = self._waiters.popleft()
                self._grants.append(time.monotonic())
                if not fut.done():
                    fut.set_result(None)
            else:
                await asyncio.sleep(0.01)

    async def acquire(self) -> None:
        if not self._started:  # safe: single-threaded loop, flag set before any await
            self._started = True
            asyncio.create_task(self._emit())
        fut = asyncio.get_running_loop().create_future()
        self._waiters.append(fut)
        await fut


_submit_bucket = _Bucket(SUBMIT_MAX, SUBMIT_WINDOW)
_read_bucket = _Bucket(READ_MAX, READ_WINDOW)


def _safe_json(resp) -> Any:
    try:
        return resp.json()
    except Exception:  # noqa: BLE001 — non-JSON body
        return {"raw": resp.text[:1000]}


def _headers() -> dict[str, str] | None:
    api_key = os.environ.get("ICYPEAS_API_KEY")
    if not api_key:
        return None
    # Raw API key in Authorization (no scheme prefix, no signature). Docs: getting-started.
    return {"Authorization": api_key, "Content-Type": "application/json",
            "Accept": "application/json"}


def _extract(item: dict) -> tuple[str, str | None, str | None]:
    """(status, email|None, certainty|None) from a read/webhook item object."""
    status = (item.get("status") or "").upper()
    results = item.get("results") or {}
    emails = results.get("emails") or []
    if emails and isinstance(emails, list) and isinstance(emails[0], dict):
        return status, emails[0].get("email"), emails[0].get("certainty")
    return status, None, None


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("icypeas-api")],
    max_containers=1,        # ← the entire design rests on exactly one bucket-owning process
    timeout=60 * 60,
)
@modal.concurrent(max_inputs=128)   # many concurrent submit+poll inputs share the one container
async def find_email(
    firstname: str,
    lastname: str,
    domain_or_company: str,
    external_id: str | None = None,
) -> dict[str, Any]:
    """Rate-governed Icypeas email discovery: submit + bounded poll → final envelope.

    Returns a structured envelope — NEVER raises across the Modal boundary for
    business/HTTP errors (the worker inspects ``ok`` / ``found``)::

        {"ok": bool, "found": bool, "email": str|None, "certainty": str|None,
         "status": str, "scan_id": str|None, "http_status": int, "error": str|None,
         "raw": <Icypeas result item, VERBATIM> | None}

    ``raw`` is the untouched Icypeas read item (``items[0]`` — results.emails[],
    mxRecords, certainty, status, …), present on every terminal outcome (found AND
    not-found) so the worker can persist the provider payload as-is. The extracted
    fields above are a convenience projection on top of it, never a replacement.
    """
    import httpx

    headers = _headers()
    if headers is None:
        return {"ok": False, "found": False, "email": None, "certainty": None,
                "status": "NO_KEY", "scan_id": None, "http_status": 0,
                "error": "ICYPEAS_API_KEY absent in icypeas-api secret"}

    body: dict[str, Any] = {
        "firstname": firstname or "",
        "lastname": lastname or "",
        "domainOrCompany": domain_or_company or "",
    }
    if external_id:
        body["custom"] = {"externalId": external_id}

    # ── 1) SUBMIT (10/sec bucket) ────────────────────────────────────────────
    scan_id: str | None = None
    last_err: str | None = None
    for attempt in range(MAX_RETRIES):
        await _submit_bucket.acquire()
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                resp = await client.post(f"{BASE_URL}{SUBMIT_PATH}", json=body, headers=headers)
            sc = resp.status_code
            if sc == 429:
                last_err = "429 submit rate limited"
                await asyncio.sleep(RATE_429_COOLDOWN)
                continue
            data = _safe_json(resp)
            # Icypeas returns validation errors as HTTP 200 + success:false.
            if isinstance(data, dict) and data.get("success") is False:
                return {"ok": False, "found": False, "email": None, "certainty": None,
                        "status": "VALIDATION_ERROR", "scan_id": None, "http_status": sc,
                        "error": str(data.get("validationErrors") or data)[:300]}
            if sc // 100 != 2:
                if sc // 100 == 5:
                    last_err = f"HTTP {sc} on submit"
                    await asyncio.sleep(2 ** attempt)
                    continue
                return {"ok": False, "found": False, "email": None, "certainty": None,
                        "status": "SUBMIT_ERROR", "scan_id": None, "http_status": sc,
                        "error": resp.text[:300]}
            scan_id = ((data.get("item") or {}).get("_id")) if isinstance(data, dict) else None
            if not scan_id:
                return {"ok": False, "found": False, "email": None, "certainty": None,
                        "status": "NO_SCAN_ID", "scan_id": None, "http_status": sc,
                        "error": f"submit ok but no item._id: {str(data)[:200]}"}
            break
        except Exception as exc:  # noqa: BLE001 — network / timeout
            last_err = str(exc)
            await asyncio.sleep(2 ** attempt)
    if not scan_id:
        return {"ok": False, "found": False, "email": None, "certainty": None,
                "status": "SUBMIT_FAILED", "scan_id": None, "http_status": 0, "error": last_err}

    # ── 2) POLL the read route until terminal (30/min bucket) ────────────────
    await asyncio.sleep(POLL_INITIAL_DELAY)  # settle before spending the first read token
    read_body = {"id": scan_id}
    for _ in range(MAX_POLLS):
        await _read_bucket.acquire()
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                resp = await client.post(f"{BASE_URL}{READ_PATH}", json=read_body, headers=headers)
            sc = resp.status_code
            if sc == 429:
                await asyncio.sleep(RATE_429_COOLDOWN)
                continue
            if sc // 100 != 2:
                await asyncio.sleep(POLL_INTERVAL)
                continue
            data = _safe_json(resp)
            items = data.get("items") if isinstance(data, dict) else None
            if not items:
                await asyncio.sleep(POLL_INTERVAL)
                continue
            raw_item = items[0]   # Icypeas result object, VERBATIM — surfaced for raw persistence
            status, email, certainty = _extract(raw_item)
            if status in _TERMINAL_FOUND or (email and status not in _PENDING):
                return {"ok": True, "found": bool(email), "email": email,
                        "certainty": certainty, "status": status or "FOUND",
                        "scan_id": scan_id, "http_status": sc, "error": None, "raw": raw_item}
            if status in _TERMINAL_NOT_FOUND:
                return {"ok": True, "found": False, "email": None, "certainty": None,
                        "status": status, "scan_id": scan_id, "http_status": sc,
                        "error": None, "raw": raw_item}
            if status in _TERMINAL_ERROR:
                return {"ok": False, "found": False, "email": None, "certainty": None,
                        "status": status, "scan_id": scan_id, "http_status": sc,
                        "error": f"terminal status {status}", "raw": raw_item}
            # still NONE / SCHEDULED / IN_PROGRESS → wait and poll again
            await asyncio.sleep(POLL_INTERVAL)
        except Exception as exc:  # noqa: BLE001 — network / timeout
            last_err = str(exc)
            await asyncio.sleep(POLL_INTERVAL)
    # Poll budget exhausted without a terminal status — soft miss (indeterminate).
    return {"ok": True, "found": False, "email": None, "certainty": None,
            "status": "POLL_TIMEOUT", "scan_id": scan_id, "http_status": 0,
            "error": last_err or "poll exhausted before terminal status"}


@app.function(image=image, secrets=[modal.Secret.from_name("icypeas-api")], timeout=60)
def key_check() -> dict[str, Any]:
    """Health gate — submit a throwaway discovery to confirm the key authenticates.
    Returns the submit envelope (success/item._id ⇒ key valid; 401 ⇒ invalid)."""
    import httpx

    headers = _headers()
    if headers is None:
        return {"valid": False, "error": "ICYPEAS_API_KEY absent in icypeas-api secret"}
    try:
        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            resp = client.post(f"{BASE_URL}{SUBMIT_PATH}",
                               json={"firstname": "John", "lastname": "Doe",
                                     "domainOrCompany": "icypeas.com"}, headers=headers)
        data = _safe_json(resp)
        valid = resp.status_code == 200 and isinstance(data, dict) and bool(
            (data.get("item") or {}).get("_id"))
        return {"valid": valid, "http_status": resp.status_code, "data": data}
    except Exception as exc:  # noqa: BLE001
        return {"valid": False, "error": str(exc)}


@app.local_entrypoint()
def smoke(firstname: str = "Jean", lastname: str = "Dupont", domain: str = "icypeas.com") -> None:
    """Smoke test: key-check + one rate-gated submit+poll discovery."""
    import json

    print("key_check:", json.dumps(key_check.remote(), indent=2, default=str))
    print("find_email:", json.dumps(
        find_email.remote(firstname, lastname, domain), indent=2, default=str))
