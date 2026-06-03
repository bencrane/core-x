"""Blitz API gateway — the fleet's single rate-governed egress to BlitzAPI.

ONE authoritative 5-RPS token bucket for the entire firmographic enrichment plane
(Directive 23 §3). Every Blitz HTTP call from the ``enrichment-blitz`` workers
(Workflows A/B/C) flows through this one function. This is the Blitz-egress
parallel to the Universal Dispatcher (``core/modal_dispatcher.py``): a single
cross-cutting concern localized in one ``core/`` Modal app.

WHY SINGLE-CONTAINER. BlitzAPI caps all plans at 5 requests/second across the
workspace key. Modal scales workers horizontally, so a per-worker limiter cannot
bound a global ceiling. ``max_containers=1`` puts the authoritative token bucket
in exactly one process — true global ≤5 RPS, no distributed coordination. The
predecessor (data-engine-x ``gtm_hydration_cascade_app``, ``max_containers=1``)
proved single-container serialization but achieved only ~2 RPS (serial,
~500 ms/call). This gateway adds an explicit token bucket + ``@modal.concurrent``
so many concurrent in-flight requests are gated to ≤5 RPS rather than serialized
— recovering the lost throughput while never breaching the cap.

PRIORITY. Callers tag ``priority``: ``high`` (interactive / standalone Workflow
B), ``normal`` (Workflow C cascades), ``low`` (bulk Workflow A). HIGH preempts at
the next token; LOW is reserved a floor (≥1 grant of every ``LOW_FLOOR_GAP``+1)
so a continuous HIGH/NORMAL stream never fully starves a bulk-A batch.

SECRET BLAST RADIUS. ``BLITZAPI_API_KEY`` lives ONLY here (the ``blitz-api`` Modal
secret, mirror of Doppler ``hq-all/prd``). Workers never hold it.

    modal deploy core/blitz_gateway.py
    modal run    core/blitz_gateway.py::smoke --domain openai.com   # smoke test
"""

from __future__ import annotations

import asyncio
import collections
import os
import time
from typing import Any

import modal

app = modal.App("blitz-gateway")

image = modal.Image.debian_slim(python_version="3.12").pip_install("httpx>=0.27,<1.0")

# ── BlitzAPI coordinates ─────────────────────────────────────────────────────
BASE_URL = os.environ.get("BLITZAPI_API_BASE", "https://api.blitz-api.ai").rstrip("/")

# Logical endpoint name → Blitz path. A caller may also pass a raw "/v2/..." path.
ENDPOINTS = {
    "resolve": "/v2/enrichment/domain-to-linkedin",  # A: {domain} → {found, company_linkedin_url}
    "company": "/v2/enrichment/company",             # B: {company_linkedin_url} → {found, company{}}
    "key_info": "/v2/account/key-info",              # GET; health gate (max_requests_per_seconds)
}

# Workspace rate cap — `max_requests_per_seconds` from /v2/account/key-info (5 on
# all plans). Read live via key_info(); pinned here as the authoritative bucket size.
MAX_RPS = int(os.environ.get("BLITZ_MAX_RPS", "5"))
HTTP_TIMEOUT = 10.0          # Blitz docs: 10 s for enrichment endpoints
RATE_429_COOLDOWN = 60.0     # docs: wait ≥60 s after a server-side 429
MAX_RETRIES = 3

# Priority lanes. LOW is reserved a floor so bulk A is throttled, never starved:
# after LOW_FLOOR_GAP consecutive non-LOW grants, a backlogged LOW is served next
# (⇒ LOW gets ≥1 of every LOW_FLOOR_GAP+1 grants).
LOW_FLOOR_GAP = 4

# ── token-bucket scheduler (one event loop in the single container) ──────────
_lanes: dict[str, "collections.deque[asyncio.Future]"] = {
    "high": collections.deque(),
    "normal": collections.deque(),
    "low": collections.deque(),
}
_grants: "collections.deque[float]" = collections.deque()  # monotonic ts of recent grants
_consecutive_non_low = 0
_emitter_started = False


def _pick_lane() -> str | None:
    if _lanes["low"] and _consecutive_non_low >= LOW_FLOOR_GAP:
        return "low"
    for lane in ("high", "normal", "low"):
        if _lanes[lane]:
            return lane
    return None


async def _emitter() -> None:
    """Release ≤MAX_RPS grants per rolling 1 s to waiting calls, honoring priority
    + the LOW floor. One daemon coroutine per container (started lazily)."""
    global _consecutive_non_low
    while True:
        now = time.monotonic()
        while _grants and _grants[0] <= now - 1.0:
            _grants.popleft()
        lane = _pick_lane() if len(_grants) < MAX_RPS else None
        if lane is None:
            await asyncio.sleep(0.01)
            continue
        fut = _lanes[lane].popleft()
        _grants.append(time.monotonic())
        _consecutive_non_low = 0 if lane == "low" else _consecutive_non_low + 1
        if not fut.done():
            fut.set_result(None)


async def _acquire(priority: str) -> None:
    """Block until the global bucket grants this call a token."""
    global _emitter_started
    if not _emitter_started:  # safe: single-threaded loop, flag set before any await
        _emitter_started = True
        asyncio.create_task(_emitter())
    lane = priority if priority in _lanes else "normal"
    fut = asyncio.get_running_loop().create_future()
    _lanes[lane].append(fut)
    await fut


def _safe_json(resp) -> Any:
    try:
        return resp.json()
    except Exception:  # noqa: BLE001 — non-JSON body
        return {"raw": resp.text[:1000]}


@app.function(
    image=image,
    secrets=[modal.Secret.from_name("blitz-api")],
    max_containers=1,        # ← the entire design rests on exactly one bucket-owning process
    timeout=60 * 60,
)
@modal.concurrent(max_inputs=256)   # many concurrent .remote() inputs share the one container
async def blitz_call(
    endpoint: str,
    payload: dict | None = None,
    priority: str = "normal",
    method: str = "POST",
) -> dict[str, Any]:
    """Rate-governed single Blitz call. Blocks on the global token bucket, then
    issues the HTTP request holding the only ``x-api-key``.

    Returns a structured envelope — NEVER raises across the Modal boundary for
    business/HTTP errors (the caller inspects ``ok`` / ``found``)::

        {"ok": bool, "found": bool|None, "data": <json>|None,
         "http_status": int, "error": str|None, "endpoint": str}
    """
    import httpx

    path = ENDPOINTS.get(endpoint, endpoint)  # logical name OR raw path
    api_key = os.environ.get("BLITZAPI_API_KEY")
    if not api_key:
        return {"ok": False, "found": None, "data": None, "http_status": 0,
                "error": "BLITZAPI_API_KEY absent in blitz-api secret", "endpoint": path}
    headers = {"x-api-key": api_key, "Content-Type": "application/json",
               "Accept": "application/json"}
    url = f"{BASE_URL}{path}"

    last_err: str | None = None
    for attempt in range(MAX_RETRIES):
        await _acquire(priority)  # ← the global ≤5-RPS gate (one token per HTTP attempt)
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
                if method == "GET":
                    resp = await client.get(url, headers=headers)
                else:
                    resp = await client.post(url, json=payload or {}, headers=headers)
            sc = resp.status_code
            if sc == 429:  # bucket should prevent this; cool down per docs and retry
                last_err = "429 rate limited"
                await asyncio.sleep(RATE_429_COOLDOWN)
                continue
            if sc // 100 == 5:  # transient — exponential backoff
                last_err = f"HTTP {sc}: {resp.text[:300]}"
                await asyncio.sleep(2 ** attempt)
                continue
            if sc // 100 != 2:  # 401/402/404/422 — terminal, surface to caller
                return {"ok": False, "found": None, "data": _safe_json(resp),
                        "http_status": sc, "error": resp.text[:300], "endpoint": path}
            data = _safe_json(resp)
            found = data.get("found") if isinstance(data, dict) else None
            return {"ok": True, "found": found, "data": data, "http_status": sc,
                    "error": None, "endpoint": path}
        except Exception as exc:  # noqa: BLE001 — network / timeout
            last_err = str(exc)
            await asyncio.sleep(2 ** attempt)
    return {"ok": False, "found": None, "data": None, "http_status": 0,
            "error": last_err, "endpoint": path}


@app.function(image=image, secrets=[modal.Secret.from_name("blitz-api")], timeout=60)
def key_info() -> dict[str, Any]:
    """Health gate — GET /v2/account/key-info (0 credits). Confirms the key is
    valid and reports the live ``max_requests_per_seconds`` to size the bucket."""
    import httpx

    api_key = os.environ.get("BLITZAPI_API_KEY")
    if not api_key:
        return {"valid": False, "error": "BLITZAPI_API_KEY absent in blitz-api secret"}
    try:
        with httpx.Client(timeout=HTTP_TIMEOUT) as client:
            resp = client.get(f"{BASE_URL}{ENDPOINTS['key_info']}",
                              headers={"x-api-key": api_key, "Accept": "application/json"})
        return {"http_status": resp.status_code, **_safe_json(resp)}
    except Exception as exc:  # noqa: BLE001
        return {"valid": False, "error": str(exc)}


@app.local_entrypoint()
def smoke(domain: str = "openai.com") -> None:
    """Smoke test: key-info health check + one rate-gated domain→linkedin call."""
    import json

    print("key_info:", json.dumps(key_info.remote(), indent=2, default=str))
    print("resolve:", json.dumps(
        blitz_call.remote("resolve", {"domain": domain}, "high"), indent=2, default=str))
