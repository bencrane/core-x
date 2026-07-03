"""OpenWeb Ninja JSearch API gateway — the fleet's single credit-metered egress to JSearch.

TRANSPORT-ONLY. Given a jobs query, returns a normalized envelope carrying one page of raw job
objects plus the next-page ``cursor``; it does NOT interpret them. Title-variant fan-out,
employer/publisher classification, domain normalization, and the PG/Lance projection live in the
caller (``pipelines/jsearch/harvest_capture_roles.py``) — precision logic is a data concern, not a
transport concern. This mirrors ``core/serper_gateway.py`` envelope discipline: this function NEVER
raises for HTTP / business errors, so the caller drives the pagination loop off ``ok``/``cursor``
and sums ``credits`` against the account budget.

CREDIT MODEL. JSearch ``/search-v2`` bills exactly 1 request credit per PAGE returned (a page is
≤10 jobs). This gateway fetches a SINGLE page per call (``num_pages=1``) and hands back the
``cursor`` so the caller paginates deterministically and the credit count is exact — 1 credit per
successful call. 402 (out of quota), 401/403 (bad key), and every network/5xx failure report
``credits=0`` so a retry or a dead key never decrements the caller's budget.

SECRET BLAST RADIUS. ``OPENWEBNINJA_API_KEY`` is read from the process env (doppler ``core-x/prd``
injects it locally; the ``openwebninja-api`` Modal secret injects it in the worker). This module
never persists, echoes, or logs the key.

    GET https://api.openwebninja.com/jsearch/search-v2
    header  x-api-key: <key>
    params  query, country, date_posted, cursor, num_pages, ...
"""
from __future__ import annotations

import os
import time
from typing import Any

JSEARCH_BASE = os.environ.get("OPENWEBNINJA_BASE", "https://api.openwebninja.com/jsearch").rstrip("/")
SEARCH_URL = JSEARCH_BASE + "/search-v2"
HTTP_TIMEOUT = float(os.environ.get("JSEARCH_TIMEOUT", "45"))
MAX_RETRIES = int(os.environ.get("JSEARCH_MAX_RETRIES", "4"))
RETRY_BACKOFF = float(os.environ.get("JSEARCH_RETRY_BACKOFF", "1.5"))


def _key() -> str | None:
    k = os.environ.get("OPENWEBNINJA_API_KEY")
    return k.strip() if k and k.strip() else None


def key_present() -> bool:
    return _key() is not None


def _envelope(ok: bool, *, credits: int, http_status: int, jobs: list, cursor: str | None,
              error, raw) -> dict[str, Any]:
    return {"ok": ok, "credits": credits, "http_status": http_status, "jobs": jobs,
            "cursor": cursor, "error": error, "raw": raw}


def _extract(data: dict) -> tuple[list, str | None]:
    """search-v2 returns ``data`` as an object ``{jobs:[...], cursor:"..."}``. Tolerate the older
    shape where ``data`` is a bare list of jobs (cursor then sits at the top level)."""
    d = data.get("data")
    if isinstance(d, dict):
        jobs = d.get("jobs") or []
        cursor = d.get("cursor") or None
    elif isinstance(d, list):
        jobs = d
        cursor = data.get("cursor") or None
    else:
        jobs, cursor = [], None
    jobs = [j for j in jobs if isinstance(j, dict)]
    return jobs, (cursor if isinstance(cursor, str) and cursor.strip() else None)


def search(query: str, *, country: str = "us", date_posted: str = "all", cursor: str | None = None,
           num_pages: int = 1, employment_types: str | None = None, language: str | None = None,
           session: Any = None) -> dict[str, Any]:
    """One credit-metered JSearch page. Returns::

        {"ok": bool, "credits": 0|N, "http_status": int,
         "jobs": [<raw job dict>, ...], "cursor": str|None,
         "error": str|None, "raw": <full jsearch json>|None}

    ``cursor`` is the value to pass on the next call to fetch the following page; ``None`` means the
    caller has reached the end of the result set. Retries only retryable failures (429 / 5xx /
    network) with exponential backoff; terminal auth (401/403) and quota (402) short-circuit with
    ``credits=0``.
    """
    import requests

    key = _key()
    if not key:
        return _envelope(False, credits=0, http_status=0, jobs=[], cursor=None,
                         error="OPENWEBNINJA_API_KEY absent in env", raw=None)

    params: dict[str, Any] = {"query": query, "country": country, "date_posted": date_posted,
                              "num_pages": num_pages}
    if cursor:
        params["cursor"] = cursor
    if employment_types:
        params["employment_types"] = employment_types
    if language:
        params["language"] = language
    headers = {"x-api-key": key}
    http = session or requests
    last_err: str | None = None

    for attempt in range(MAX_RETRIES):
        try:
            resp = http.get(SEARCH_URL, params=params, headers=headers, timeout=HTTP_TIMEOUT)
            sc = resp.status_code
            if sc == 429 or sc // 100 == 5:                 # retryable — no credit charged
                last_err = f"HTTP {sc}"
                time.sleep(RETRY_BACKOFF * (2 ** attempt))
                continue
            if sc in (401, 403):
                return _envelope(False, credits=0, http_status=sc, jobs=[], cursor=None,
                                 error="auth/key rejected", raw=None)
            if sc == 402:
                return _envelope(False, credits=0, http_status=sc, jobs=[], cursor=None,
                                 error="out of quota (402)", raw=None)
            if sc // 100 != 2:
                return _envelope(False, credits=0, http_status=sc, jobs=[], cursor=None,
                                 error=(getattr(resp, "text", "") or "")[:300], raw=None)
            data = resp.json()
            if str(data.get("status") or "").upper() != "OK":
                return _envelope(False, credits=0, http_status=sc, jobs=[], cursor=None,
                                 error=f"status={data.get('status')!r}", raw=data)
            jobs, next_cursor = _extract(data)
            return _envelope(True, credits=int(num_pages), http_status=sc, jobs=jobs,
                             cursor=next_cursor, error=None, raw=data)
        except Exception as exc:  # noqa: BLE001 — network / timeout / json decode
            last_err = str(exc)
            time.sleep(RETRY_BACKOFF * (2 ** attempt))

    return _envelope(False, credits=0, http_status=0, jobs=[], cursor=None,
                     error=last_err or "exhausted retries", raw=None)


if __name__ == "__main__":
    import json
    import sys

    q = " ".join(sys.argv[1:]) or "capture manager"
    print(f"key_present={key_present()}")
    env = search(q)
    preview = {**env, "raw": "<omitted>" if env.get("raw") else None,
               "jobs": [{"employer_name": j.get("employer_name"), "job_title": j.get("job_title"),
                         "employer_website": j.get("employer_website"),
                         "job_publisher": j.get("job_publisher")} for j in env["jobs"][:5]]}
    print(json.dumps(preview, indent=2, default=str))
