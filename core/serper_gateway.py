"""Serper.dev (Google Search) API gateway — the fleet's single credit-metered egress to serper.

TRANSPORT-ONLY. Given a query string, returns a normalized envelope carrying the organic
results; it does NOT interpret them. LinkedIn slug-parse and name/company validation live in
the caller (``pipelines/sba_dsbs/resolve_dsbs_poc_linkedin.py``) — the precision logic is a
data concern, not a transport concern. This mirrors ``core/icypeas_gateway.py`` envelope
discipline: this function NEVER raises for HTTP / business errors, so the caller drives the
loop off ``ok`` and sums ``credits`` against a hard account-budget ceiling.

CREDIT MODEL. serper ``/search`` bills exactly 1 credit per 200 response regardless of ``num``
(≤10 organic). We report ``credits=1`` only on ``ok`` (HTTP 200); 402 (out of credits),
401/403 (bad key), and every network/5xx failure report ``credits=0`` so a retry or a dead key
never decrements the caller's budget. The caller's budget ceiling is therefore exact.

SECRET BLAST RADIUS. ``SERPER_API_KEY`` is read from the process env (doppler ``core-x/prd``
injects it at run time). This module never persists, echoes, or logs the key.

    POST https://google.serper.dev/search
    header  X-API-KEY: <key>
    body    {"q": <query>, "num": 10, "gl": "us", "hl": "en"}
"""
from __future__ import annotations

import os
import time
from typing import Any

SERPER_URL = os.environ.get("SERPER_API_BASE", "https://google.serper.dev").rstrip("/") + "/search"
HTTP_TIMEOUT = float(os.environ.get("SERPER_TIMEOUT", "20"))
MAX_RETRIES = int(os.environ.get("SERPER_MAX_RETRIES", "4"))
RETRY_BACKOFF = float(os.environ.get("SERPER_RETRY_BACKOFF", "1.5"))


def _key() -> str | None:
    k = os.environ.get("SERPER_API_KEY")
    return k.strip() if k and k.strip() else None


def key_present() -> bool:
    return _key() is not None


def _envelope(ok: bool, *, credits: int, http_status: int, organic: list, error, raw) -> dict[str, Any]:
    return {"ok": ok, "credits": credits, "http_status": http_status,
            "organic": organic, "error": error, "raw": raw}


def search(query: str, *, num: int = 10, gl: str = "us", hl: str = "en",
           session: Any = None) -> dict[str, Any]:
    """One credit-metered Google search. Returns::

        {"ok": bool, "credits": 0|1, "http_status": int,
         "organic": [{"title","link","snippet","position"}, ...],
         "error": str|None, "raw": <full serper json>|None}

    Retries only the retryable failures (429 / 5xx / network) with exponential backoff;
    terminal auth (401/403) and budget (402) short-circuit with ``credits=0``.
    """
    import requests

    key = _key()
    if not key:
        return _envelope(False, credits=0, http_status=0, organic=[],
                         error="SERPER_API_KEY absent in env", raw=None)

    headers = {"X-API-KEY": key, "Content-Type": "application/json"}
    body = {"q": query, "num": num, "gl": gl, "hl": hl}
    http = session or requests
    last_err: str | None = None

    for attempt in range(MAX_RETRIES):
        try:
            resp = http.post(SERPER_URL, json=body, headers=headers, timeout=HTTP_TIMEOUT)
            sc = resp.status_code
            if sc == 429 or sc // 100 == 5:                 # retryable — no credit charged
                last_err = f"HTTP {sc}"
                time.sleep(RETRY_BACKOFF * (2 ** attempt))
                continue
            if sc in (401, 403):
                return _envelope(False, credits=0, http_status=sc, organic=[],
                                 error="auth/key rejected", raw=None)
            if sc == 402:
                return _envelope(False, credits=0, http_status=sc, organic=[],
                                 error="out of credits (402)", raw=None)
            if sc // 100 != 2:
                return _envelope(False, credits=0, http_status=sc, organic=[],
                                 error=(getattr(resp, "text", "") or "")[:300], raw=None)
            data = resp.json()
            organic_raw = data.get("organic") or []
            organic = [{"title": o.get("title"), "link": o.get("link"),
                        "snippet": o.get("snippet"), "position": o.get("position")}
                       for o in organic_raw if isinstance(o, dict)]
            return _envelope(True, credits=1, http_status=sc, organic=organic,
                             error=None, raw=data)
        except Exception as exc:  # noqa: BLE001 — network / timeout / json decode
            last_err = str(exc)
            time.sleep(RETRY_BACKOFF * (2 ** attempt))

    return _envelope(False, credits=0, http_status=0, organic=[],
                     error=last_err or "exhausted retries", raw=None)


if __name__ == "__main__":
    import json
    import sys

    q = " ".join(sys.argv[1:]) or '"Aaron Early" site:linkedin.com/in'
    print(f"key_present={key_present()}")
    env = search(q)
    env_print = {**env, "raw": "<omitted>" if env.get("raw") else None}
    print(json.dumps(env_print, indent=2, default=str))
