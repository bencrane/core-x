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

BULK EGRESS (added 2026-07-04). ``find_email`` above is the ONE-contact submit+poll
path (10/s submit, 30/min read). The ``enrichment-icypeas-mv`` rail uses the Icypeas
*bulk* primitive instead — ``launch_bulk`` (≤5000 rows/call, 1/s), ``file_status``
(bulk-file progression, 15/min), ``drain_results`` (paginated ``mode:"bulk"`` read,
30/min). Same single-container discipline: each is ``max_containers=1`` so its
module-level bucket is the authoritative global limiter. ONE launch enqueues 5000
rows the single path would need 5000 submits + ≥5000 reads to cover, so the bulk rail
is ~100× the read-bound throughput of ``find_email``. Both hold ``ICYPEAS_API_KEY``
here and nowhere else.

    GLOBAL-CEILING CAVEAT. ``drain_results`` and ``find_email``'s poll hit the SAME
    ``/bulk-single-searchs/read`` route (30/min global) but run in SEPARATE containers,
    so each owns an independent 30/min bucket — up to 60/min collectively if a large
    single-cascade backfill and a large bulk drain run at once. Bulk is the supplanting
    path; do not run both at scale simultaneously. Each alone is compliant.

    modal deploy core/icypeas_gateway.py
    modal run    core/icypeas_gateway.py::smoke --domain icypeas.com        # single path
    modal run    core/icypeas_gateway.py::bulk_smoke                        # bulk path
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

# Bulk egress routes (live docs 2026-07-04). See the BULK EGRESS docstring section.
BULK_SEARCH_PATH = "/bulk-search"        # launch a bulk search: 1 call/sec, ≤5000 rows/call
FILE_STATUS_PATH = "/search-files/read"  # bulk-file progression poll: 15 calls/min
BULK_READ_PATH = READ_PATH               # drain results (mode:"bulk"): 30 calls/min, paginated

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

# ── Bulk-route rate caps (live docs 2026-07-04) ───────────────────────────────
BULK_LAUNCH_MAX = int(os.environ.get("ICYPEAS_BULK_LAUNCH_PER_SEC", "1"))    # /bulk-search 1/sec
BULK_LAUNCH_WINDOW = 1.0
FILE_STATUS_MAX = int(os.environ.get("ICYPEAS_FILE_STATUS_PER_MIN", "15"))   # /search-files/read 15/min
FILE_STATUS_WINDOW = 60.0
BULK_ROWS_MAX = int(os.environ.get("ICYPEAS_BULK_ROWS_MAX", "5000"))         # hard per-launch row ceiling
BULK_READ_LIMIT = int(os.environ.get("ICYPEAS_BULK_READ_LIMIT", "100"))      # ≤100 results/page (docs max)

_launch_bucket = _Bucket(BULK_LAUNCH_MAX, BULK_LAUNCH_WINDOW)
_file_status_bucket = _Bucket(FILE_STATUS_MAX, FILE_STATUS_WINDOW)
# Independent 30/min bucket on the shared read route — see the GLOBAL-CEILING CAVEAT.
_bulk_read_bucket = _Bucket(READ_MAX, READ_WINDOW)

# ── Company-scrape coordinates (bulk /api/scrape → WEBHOOK delivery, added 2026-07-04) ─
# Company scraping is a SEPARATE bulk route from /bulk-search: POST /api/scrape with
# {type:"company", data:[≤50 urls], user:<ObjectId>}. Unlike /bulk-search it REQUIRES the
# account `user` id (ICYPEAS_USER_ID in the icypeas-api secret) — a bare key 401s
# "UserNotFoundError" (verified live 2026-07-04). Results are delivered by WEBHOOK
# (custom.webhookUrlItem → edge_api landing; custom.webhookUrlBulkDone → completion), so this
# path makes ZERO reads and NEVER touches the shared 30/min read route the email cascade +
# bulk drain already contend for (the GLOBAL-CEILING CAVEAT). Its ONLY Icypeas rate surface is
# the /api/scrape submit, governed by its OWN single-container bucket. The submit ceiling is
# undocumented → default deliberately gentle for account safety (the account was suspended once
# by ungoverned probing); raise ICYPEAS_SCRAPE_SUBMIT_PER_SEC once the true limit is confirmed.
SCRAPE_SUBMIT_PATH = "/scrape"          # POST /api/scrape (type=company)
SCRAPE_MAX_BATCH = int(os.environ.get("ICYPEAS_SCRAPE_MAX_BATCH", "50"))   # Icypeas hard cap
SCRAPE_SUBMIT_MAX = int(os.environ.get("ICYPEAS_SCRAPE_SUBMIT_PER_SEC", "2"))
SCRAPE_SUBMIT_WINDOW = 1.0

_scrape_submit_bucket = _Bucket(SCRAPE_SUBMIT_MAX, SCRAPE_SUBMIT_WINDOW)


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


# ════════════════════════════════════════════════════════════════════════════
# BULK EGRESS — Icypeas /bulk-search (≤5000 rows/launch). The go-forward batch
# path for the icypeas+MV rail (pipelines/enrichment_icypeas_mv). Three rate-
# governed primitives — launch, file-status poll, paginated drain — each a
# max_containers=1 owner of its global token bucket, exactly like find_email.
# ════════════════════════════════════════════════════════════════════════════

def _extract_file_id(data: Any) -> str | None:
    """The bulk-search launch echoes a file/bulk id. Docs don't print the exact field
    (VERIFY-AT-BUILD), so probe the known shapes in priority order: item._id (mirrors the
    single /email-search response), then file / fileId / _id / id at top level."""
    if not isinstance(data, dict):
        return None
    item = data.get("item")
    if isinstance(item, dict) and isinstance(item.get("_id"), str) and item["_id"]:
        return item["_id"]
    for k in ("file", "fileId", "_id", "id"):
        v = data.get(k)
        if isinstance(v, str) and v:
            return v
    return None


def _first_file_record(data: Any, file_id: str) -> dict:
    """Pull the record for ``file_id`` from a /search-files/read response. The array key is
    VERIFY-AT-BUILD: probe items/files/results/data, prefer the record whose _id|file matches,
    else the first. Some deployments may return the record at top level."""
    if not isinstance(data, dict):
        return {}
    for key in ("items", "files", "results", "data"):
        arr = data.get(key)
        if isinstance(arr, list) and arr:
            for rec in arr:
                if isinstance(rec, dict) and (rec.get("_id") == file_id or rec.get("file") == file_id):
                    return rec
            return arr[0] if isinstance(arr[0], dict) else {}
    return data


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("icypeas-api")],
    max_containers=1,        # global 1/sec launch bucket lives in exactly one process
    timeout=60 * 5,
)
@modal.concurrent(max_inputs=32)
async def launch_bulk(
    name: str,
    rows: list[list[str]],
    external_ids: list[str],
    task: str = "email-search",
) -> dict[str, Any]:
    """Launch ONE Icypeas bulk search (≤5000 rows). Rate-gated to 1/sec globally.

    ``rows`` — for ``email-search`` each row is ``[firstname, lastname, domainOrCompany]``
    (firstname and/or lastname may be empty; the anchor is required). ``external_ids`` MUST be
    row-aligned; each is echoed on the corresponding result item so the caller maps a drained
    item back to its ``contact_id``. NEVER raises across the Modal boundary — returns::

        {"ok": bool, "file_id": str|None, "http_status": int, "error": str|None,
         "raw": <launch response, VERBATIM> | None}
    """
    import httpx

    headers = _headers()
    if headers is None:
        return {"ok": False, "file_id": None, "http_status": 0,
                "error": "ICYPEAS_API_KEY absent in icypeas-api secret", "raw": None}
    if not rows:
        return {"ok": False, "file_id": None, "http_status": 0, "error": "empty rows", "raw": None}
    if len(rows) > BULK_ROWS_MAX:
        return {"ok": False, "file_id": None, "http_status": 0,
                "error": f"row count {len(rows)} exceeds Icypeas bulk max {BULK_ROWS_MAX}", "raw": None}
    if len(external_ids) != len(rows):
        return {"ok": False, "file_id": None, "http_status": 0,
                "error": f"external_ids len {len(external_ids)} != rows len {len(rows)}", "raw": None}

    body = {"name": name, "task": task, "data": rows, "custom": {"externalIds": external_ids}}
    last_err: str | None = None
    for attempt in range(MAX_RETRIES):
        await _launch_bucket.acquire()
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                resp = await client.post(f"{BASE_URL}{BULK_SEARCH_PATH}", json=body, headers=headers)
            sc = resp.status_code
            if sc == 429:
                last_err = "429 bulk-launch rate limited"
                await asyncio.sleep(RATE_429_COOLDOWN)
                continue
            data = _safe_json(resp)
            # Icypeas returns validation errors as HTTP 200 + success:false.
            if isinstance(data, dict) and data.get("success") is False:
                return {"ok": False, "file_id": None, "http_status": sc,
                        "error": str(data.get("validationErrors") or data)[:300], "raw": data}
            if sc // 100 != 2:
                if sc // 100 == 5:
                    last_err = f"HTTP {sc} on bulk-launch"
                    await asyncio.sleep(2 ** attempt)
                    continue
                return {"ok": False, "file_id": None, "http_status": sc,
                        "error": resp.text[:300], "raw": data}
            file_id = _extract_file_id(data)
            if not file_id:
                return {"ok": False, "file_id": None, "http_status": sc,
                        "error": f"launch ok but no file id in response: {str(data)[:200]}", "raw": data}
            return {"ok": True, "file_id": file_id, "http_status": sc, "error": None, "raw": data}
        except Exception as exc:  # noqa: BLE001 — network / timeout
            last_err = str(exc)
            await asyncio.sleep(2 ** attempt)
    return {"ok": False, "file_id": None, "http_status": 0,
            "error": last_err or "bulk-launch retries exhausted", "raw": None}


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("icypeas-api")],
    max_containers=1,        # global 15/min file-status bucket
    timeout=60 * 5,
)
@modal.concurrent(max_inputs=16)
async def file_status(file_id: str) -> dict[str, Any]:
    """Poll /search-files/read (15/min) for ONE bulk file's progression. Returns::

        {"ok": bool, "status": str|None, "done": bool, "http_status": int,
         "error": str|None, "raw": <response, VERBATIM> | None}

    ``done`` is True when the file's ``status == "done"``."""
    import httpx

    headers = _headers()
    if headers is None:
        return {"ok": False, "status": None, "done": False, "http_status": 0,
                "error": "ICYPEAS_API_KEY absent in icypeas-api secret", "raw": None}
    body = {"file": file_id, "limit": 1}
    await _file_status_bucket.acquire()
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.post(f"{BASE_URL}{FILE_STATUS_PATH}", json=body, headers=headers)
        sc = resp.status_code
        data = _safe_json(resp)
        if sc // 100 != 2:
            return {"ok": False, "status": None, "done": False, "http_status": sc,
                    "error": (resp.text[:300] if not isinstance(data, dict) else str(data)[:300]),
                    "raw": data}
        rec = _first_file_record(data, file_id)
        status = rec.get("status") if isinstance(rec, dict) else None
        return {"ok": True, "status": status, "done": status == "done",
                "http_status": sc, "error": None, "raw": data}
    except Exception as exc:  # noqa: BLE001 — network / timeout
        return {"ok": False, "status": None, "done": False, "http_status": 0,
                "error": str(exc), "raw": None}


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("icypeas-api")],
    max_containers=1,        # global 30/min drain bucket (shared route — see caveat)
    timeout=60 * 10,
)
@modal.concurrent(max_inputs=16)
async def drain_results(
    file_id: str,
    sorts: list | None = None,
    limit: int = BULK_READ_LIMIT,
) -> dict[str, Any]:
    """Drain ONE page of a bulk file's results from /bulk-single-searchs/read (30/min,
    ``mode:"bulk"``). Paginated via ``sorts`` (the opaque token echoed by the previous page —
    pass it back with ``next:true`` to advance). Returns::

        {"ok": bool, "items": [<result item, VERBATIM>, ...], "sorts": <next token|None>,
         "count": int, "http_status": int, "error": str|None, "raw_page": <response, VERBATIM>}

    Each item is the untouched Icypeas result object (results.emails[], certainty, status, and
    the echoed externalId). The caller pages until ``count < limit`` or ``sorts`` is None."""
    import httpx

    headers = _headers()
    if headers is None:
        return {"ok": False, "items": [], "sorts": None, "count": 0, "http_status": 0,
                "error": "ICYPEAS_API_KEY absent in icypeas-api secret", "raw_page": None}
    body: dict[str, Any] = {"mode": "bulk", "file": file_id, "limit": min(limit, BULK_READ_LIMIT)}
    if sorts:
        body["sorts"] = sorts
        body["next"] = True
    await _bulk_read_bucket.acquire()
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.post(f"{BASE_URL}{BULK_READ_PATH}", json=body, headers=headers)
        sc = resp.status_code
        data = _safe_json(resp)
        if sc // 100 != 2:
            return {"ok": False, "items": [], "sorts": None, "count": 0, "http_status": sc,
                    "error": (resp.text[:300] if not isinstance(data, dict) else str(data)[:300]),
                    "raw_page": data}
        items = data.get("items") if isinstance(data, dict) else None
        items = items if isinstance(items, list) else []
        next_sorts = data.get("sorts") if isinstance(data, dict) else None
        return {"ok": True, "items": items, "sorts": next_sorts, "count": len(items),
                "http_status": sc, "error": None, "raw_page": data}
    except Exception as exc:  # noqa: BLE001 — network / timeout
        return {"ok": False, "items": [], "sorts": None, "count": 0, "http_status": 0,
                "error": str(exc), "raw_page": None}


# ════════════════════════════════════════════════════════════════════════════
# COMPANY SCRAPE — Icypeas /api/scrape (≤50 company URLs/submit → webhook delivery).
# The go-forward path for the `enrichment-company-scrape` rail. Submit-ONLY and
# ZERO-read: results are pushed to edge_api (webhookUrlItem) and completion to a
# caller URL (webhookUrlBulkDone). One max_containers=1 owner of its submit bucket,
# exactly like launch_bulk — but on a route that never touches /bulk-single-searchs/read.
# ════════════════════════════════════════════════════════════════════════════

@app.function(
    image=image,
    secrets=[modal.Secret.from_name("icypeas-api")],
    max_containers=1,        # global /api/scrape submit governor lives in exactly one process
    timeout=60 * 5,
)
@modal.concurrent(max_inputs=32)
async def scrape_submit(
    urls: list[str],
    external_ids: list[str] | None = None,
    webhook_item_url: str | None = None,
    webhook_bulkdone_url: str | None = None,
) -> dict[str, Any]:
    """Submit ONE company-scrape batch (≤50 LinkedIn company URLs) to /api/scrape.

    Governed to a gentle global submit rate; makes ZERO reads — results arrive by webhook
    (``webhookUrlItem`` → edge_api landing; ``webhookUrlBulkDone`` → completion signal). Requires
    ``ICYPEAS_USER_ID`` (bulk /api/scrape 401s "UserNotFoundError" without it). ``external_ids`` MUST
    be row-aligned with ``urls`` — each is echoed on its result item so the landing correlates a
    scraped row back to the requested URL. NEVER raises across the Modal boundary — returns::

        {"ok": bool, "file_id": str|None, "count": int, "http_status": int,
         "error": str|None, "raw": <submit response, VERBATIM> | None}

    ``file_id`` is the Icypeas bulk file id — the correlation key for reconciling landed webhook
    rows against the submitted batch. Persist it in the run-ledger.
    """
    import httpx

    headers = _headers()
    if headers is None:
        return {"ok": False, "file_id": None, "count": 0, "http_status": 0,
                "error": "ICYPEAS_API_KEY absent in icypeas-api secret", "raw": None}
    user_id = os.environ.get("ICYPEAS_USER_ID")
    if not user_id:
        return {"ok": False, "file_id": None, "count": 0, "http_status": 0,
                "error": "ICYPEAS_USER_ID absent in icypeas-api secret — /api/scrape requires it", "raw": None}

    urls = [u for u in (urls or []) if isinstance(u, str) and u.strip()]
    if not urls:
        return {"ok": False, "file_id": None, "count": 0, "http_status": 0, "error": "empty urls", "raw": None}
    if len(urls) > SCRAPE_MAX_BATCH:
        return {"ok": False, "file_id": None, "count": len(urls), "http_status": 0,
                "error": f"batch {len(urls)} exceeds Icypeas scrape cap {SCRAPE_MAX_BATCH}; caller must chunk",
                "raw": None}
    if external_ids is not None and len(external_ids) != len(urls):
        return {"ok": False, "file_id": None, "count": len(urls), "http_status": 0,
                "error": f"external_ids len {len(external_ids)} != urls len {len(urls)}", "raw": None}

    body: dict[str, Any] = {"type": "company", "data": urls, "user": user_id}
    custom: dict[str, Any] = {}
    if external_ids:
        custom["externalIds"] = list(external_ids)          # row-aligned to data[]
    if webhook_item_url:
        custom["webhookUrlItem"] = webhook_item_url
    if webhook_bulkdone_url:
        custom["webhookUrlBulkDone"] = webhook_bulkdone_url
    if custom:
        body["custom"] = custom

    last_err: str | None = None
    for attempt in range(MAX_RETRIES):
        await _scrape_submit_bucket.acquire()
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                resp = await client.post(f"{BASE_URL}{SCRAPE_SUBMIT_PATH}", json=body, headers=headers)
            sc = resp.status_code
            if sc == 429:
                last_err = "429 scrape-submit rate limited"
                await asyncio.sleep(RATE_429_COOLDOWN)
                continue
            data = _safe_json(resp)
            # Icypeas returns validation errors as HTTP 200 + success:false.
            if isinstance(data, dict) and data.get("success") is False:
                return {"ok": False, "file_id": None, "count": len(urls), "http_status": sc,
                        "error": str(data.get("validationErrors") or data)[:300], "raw": data}
            if sc // 100 != 2:
                if sc // 100 == 5:
                    last_err = f"HTTP {sc} on scrape-submit"
                    await asyncio.sleep(2 ** attempt)
                    continue
                return {"ok": False, "file_id": None, "count": len(urls), "http_status": sc,
                        "error": resp.text[:300], "raw": data if isinstance(data, dict) else None}
            file_id = _extract_file_id(data)     # reuse the bulk id-probe (item._id / file / _id / id)
            if not file_id:
                return {"ok": False, "file_id": None, "count": len(urls), "http_status": sc,
                        "error": f"scrape-submit ok but no file id: {str(data)[:200]}", "raw": data}
            return {"ok": True, "file_id": file_id, "count": len(urls),
                    "http_status": sc, "error": None, "raw": data}
        except Exception as exc:  # noqa: BLE001 — network / timeout
            last_err = str(exc)
            await asyncio.sleep(2 ** attempt)
    return {"ok": False, "file_id": None, "count": len(urls), "http_status": 0,
            "error": last_err or "scrape-submit retries exhausted", "raw": None}


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


@app.local_entrypoint()
def bulk_smoke(firstname: str = "Jean", lastname: str = "Dupont", domain: str = "icypeas.com") -> None:
    """Bulk-path smoke test: launch a 1-row bulk search, poll to done, drain the page.
    Confirms the launch response id field + the file-status/drain shapes end-to-end
    (the three VERIFY-AT-BUILD unknowns) against the live key."""
    import json
    import time

    ext_id = "bulk-smoke-1"
    launch = launch_bulk.remote("core-x bulk_smoke", [[firstname, lastname, domain]], [ext_id])
    print("launch_bulk:", json.dumps(launch, indent=2, default=str))
    file_id = launch.get("file_id")
    if not file_id:
        print("no file_id — cannot poll/drain; inspect launch.raw for the id field.")
        return

    for _ in range(20):
        st = file_status.remote(file_id)
        print("file_status:", json.dumps(st, indent=2, default=str))
        if st.get("done"):
            break
        time.sleep(5)

    page = drain_results.remote(file_id)
    print("drain_results:", json.dumps(page, indent=2, default=str))


@app.local_entrypoint()
def scrape_smoke(url: str = "https://www.linkedin.com/company/nec-technologies",
                 webhook_item_url: str = "", webhook_bulkdone_url: str = "") -> None:
    """Governed ONE-URL company scrape submit — confirms ICYPEAS_USER_ID clears the 401
    UserNotFoundError and /api/scrape returns a bulk file id. Pass --webhook-item-url to also
    register edge_api delivery and watch a row land end-to-end.

        modal run core/icypeas_gateway.py::scrape_smoke \\
            --webhook-item-url https://api.edgeapi.run/webhooks/icypeas/item
    """
    import json

    env = scrape_submit.remote(
        [url], external_ids=[url],
        webhook_item_url=(webhook_item_url or None),
        webhook_bulkdone_url=(webhook_bulkdone_url or None),
    )
    print("scrape_submit:", json.dumps(env, indent=2, default=str))
