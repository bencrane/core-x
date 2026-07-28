"""Shared, host-scoped rate governor for polite crawls of un-headered public feeds.

WHY THIS EXISTS (read the directive's ⚠ RATE DISCIPLINE section):
    The federal publishers this program fetches (whitehouse.gov, api.usaspending.gov,
    bls.gov, cbo.gov) return NO rate-limit headers — no ``X-RateLimit-*``, no
    ``Retry-After`` in the normal case. There is no warning shot: a host either tolerates
    you or goes straight to a block page, and a block is IP-scoped and can persist for
    hours. A block does not merely fail a run — it can cost access to the source for the
    rest of the day. This governor makes over-fetching structurally impossible, per the
    binding limits in the directive. It is NET-NEW: no token bucket / warm-up / breaker /
    checkpoint existed anywhere in the ingest fleet before it.

CONTRACT (all binding; do not relax without an operator ruling):
    1. Sustained aggregate rate ≤ ``max_rate`` req/s (default 2.0), enforced by a token
       bucket — never a naive ``sleep()`` between calls. Concurrency ceiling ``max_workers``
       (default 3) is exposed but this helper is safe single-threaded; callers in this
       program drive it single-threaded.
    2. Warm-up ramp: the first ``warmup_requests`` (default 100) run at ``warmup_rate``
       (default 1.0 req/s). The ceiling is unlocked only after ``warmup_requests``
       CONSECUTIVE clean 200s; ANY non-200 resets that streak back into warm-up.
    3. Circuit breaker — halt, never grind. ``breaker_consecutive`` (default 3) consecutive
       non-200s, OR any single 403/429, trips it: the FIRST trip sleeps ``breaker_sleep_s``
       (default 300 s) and resumes at warm-up settings; a SECOND trip in the same governor
       halts (``Outcome.halt`` / ``ThrottleHalt``) with ``disposition='throttled'``.
    4. ``Retry-After`` is honored over every other setting when present.
    5. Path checkpoint: ``mark_done`` / ``is_done`` persist completed work keys to disk and
       round-trip across a process restart, so a block costs only the in-flight batch.
    6. One honest descriptive User-Agent by default; per-host override is a baseline for
       hosts that 403 a bare client (whitehouse.gov), NOT UA cycling to defeat a live 403.

The clock and sleep are injectable so the mandated unit tests exercise pacing and the
300 s breaker deterministically, with no real wall-clock waits.
"""
from __future__ import annotations

import email.utils
import json
import os
import threading
import time
from dataclasses import dataclass

# One honest, descriptive UA (directive ⚠ RATE DISCIPLINE §6). Per-host overrides below.
OPERATOR_CONTACT = "benjamin.crane@engineereddemand.com"
DEFAULT_UA = (
    f"core-x-data-factory/1.0 (federal reference-data ingest; contact: {OPERATOR_CONTACT})"
)
# Some hosts (whitehouse.gov) 403 a bare/library client and require a browser UA merely to
# serve a static file — a baseline, not evasion. NEVER cycle UAs to defeat an active 403.
BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126 Safari/537.36"
)
# Per-host UA policy (directive §2.1 / ⚠ RATE DISCIPLINE §6).
HOST_UA = {
    "www.whitehouse.gov": BROWSER_UA,
    "whitehouse.gov": BROWSER_UA,
    "api.usaspending.gov": DEFAULT_UA,
    "files.usaspending.gov": DEFAULT_UA,
}


class ThrottleHalt(RuntimeError):
    """The circuit breaker tripped a second time — the run must halt (not retry into a wall)."""

    def __init__(self, host: str, message: str, disposition: str = "throttled"):
        super().__init__(message)
        self.host = host
        self.disposition = disposition


@dataclass
class Outcome:
    """Result of one governed request."""

    status_code: int | None
    response: object | None
    disposition: str            # 'ok' | 'blocked' | 'throttled'
    tripped: bool = False       # breaker tripped on this call
    halt: bool = False          # second trip → run must halt
    error: str | None = None
    breaker_sleep_s: float = 0.0
    retry_after: float | None = None   # server Retry-After (s), surfaced on every non-200


class RateGovernor:
    """Host-scoped token-bucket pacer + warm-up ramp + circuit breaker + path checkpoint."""

    def __init__(
        self,
        *,
        host: str,
        user_agent: str | None = None,
        max_rate: float = 2.0,
        warmup_rate: float = 1.0,
        warmup_requests: int = 100,
        max_workers: int = 3,
        breaker_sleep_s: float = 300.0,
        breaker_consecutive: int = 3,
        checkpoint_path: str | None = None,
        checkpoint_every: int = 200,
        clock=time.monotonic,
        sleep=time.sleep,
    ):
        if max_rate <= 0 or warmup_rate <= 0:
            raise ValueError("rates must be positive")
        self.host = host
        self.user_agent = user_agent or HOST_UA.get(host, DEFAULT_UA)
        self.max_rate = float(max_rate)
        self.warmup_rate = float(warmup_rate)
        self.warmup_requests = int(warmup_requests)
        self.max_workers = int(max_workers)
        self.breaker_sleep_s = float(breaker_sleep_s)
        self.breaker_consecutive = int(breaker_consecutive)
        self._clock = clock
        self._sleep = sleep

        # token bucket — capacity 1.0 (no burst beyond the single in-hand token) so that
        # over any half-open window of length W the count is ≤ ceil(rate*W); at rate 2.0 a
        # 10 s window holds ≤ 20 = ≤2 req/s.
        self._capacity = 1.0
        self._tokens = 1.0                     # first request is immediate
        self._last_refill = self._clock()

        # warm-up + breaker state
        self._clean_200_streak = 0
        self._consec_non200 = 0
        self._breaker_trips = 0

        # path checkpoint
        self._checkpoint_path = checkpoint_path
        self._checkpoint_every = int(checkpoint_every)
        self._since_flush = 0
        self._done: set[str] = set()
        if checkpoint_path and os.path.exists(checkpoint_path):
            try:
                with open(checkpoint_path, encoding="utf-8") as fh:
                    self._done = set(json.load(fh))
            except (OSError, ValueError):
                self._done = set()

        self._lock = threading.RLock()

    # ── pacing ────────────────────────────────────────────────────────────────────
    def _current_rate(self) -> float:
        return self.max_rate if self._clean_200_streak >= self.warmup_requests else self.warmup_rate

    def current_concurrency(self) -> int:
        """1 during warm-up (single worker), ``max_workers`` once the ceiling is unlocked."""
        return self.max_workers if self._clean_200_streak >= self.warmup_requests else 1

    def _pace(self) -> None:
        """Block (via injected sleep) until a token is available, then consume one."""
        rate = self._current_rate()
        now = self._clock()
        elapsed = max(0.0, now - self._last_refill)
        self._tokens = min(self._capacity, self._tokens + elapsed * rate)
        self._last_refill = now
        if self._tokens < 1.0:
            need = (1.0 - self._tokens) / rate
            self._sleep(need)
            now = self._clock()
            elapsed = max(0.0, now - self._last_refill)
            self._tokens = min(self._capacity, self._tokens + elapsed * rate)
            self._last_refill = now
        self._tokens -= 1.0

    # ── breaker / warm-up bookkeeping ───────────────────────────────────────────────
    @staticmethod
    def _parse_retry_after(resp) -> float | None:
        try:
            headers = getattr(resp, "headers", None) or {}
            raw = headers.get("Retry-After")
        except Exception:  # noqa: BLE001
            return None
        if raw is None:
            return None
        raw = str(raw).strip()
        if raw.isdigit():
            return float(raw)
        try:  # HTTP-date form
            when = email.utils.parsedate_to_datetime(raw)
            if when is not None:
                return max(0.0, when.timestamp() - time.time())
        except (TypeError, ValueError):
            return None
        return None

    def _classify(self, status: int | None, resp) -> Outcome:
        """Update streak/breaker counters under lock and return the outcome (+ any sleep hint)."""
        if status == 200:
            self._clean_200_streak += 1
            self._consec_non200 = 0
            return Outcome(200, resp, disposition="ok")

        # any non-200 (incl. transport error status=None) resets the warm-up streak
        self._clean_200_streak = 0
        self._consec_non200 += 1
        retry_after = self._parse_retry_after(resp)
        hard = status in (403, 429)
        trip = hard or (self._consec_non200 >= self.breaker_consecutive)
        if not trip:
            # transient non-200 (e.g. a lone 5xx) — caller may back off & retry. Surface
            # Retry-After so the caller honors it over its own exponential backoff (§4);
            # dropping it here is the over-fetch-into-a-block the governor exists to prevent.
            return Outcome(status, resp, disposition="ok", retry_after=retry_after)

        self._breaker_trips += 1
        if self._breaker_trips >= 2:
            # SECOND trip in this governor → halt the run; do NOT retry into a wall.
            return Outcome(status, resp, disposition="throttled", tripped=True, halt=True,
                           retry_after=retry_after)

        # FIRST trip → cool down, then resume at warm-up. A POSITIVE Retry-After wins over the
        # 300 s default (§4); a 0 / negative / stale-date value must NOT be able to shrink the
        # mandated breaker floor (a hard 403/429 with `Retry-After: 0` would otherwise resume
        # instantly straight into the wall).
        sleep_s = retry_after if (retry_after is not None and retry_after > 0) else self.breaker_sleep_s
        self._consec_non200 = 0
        self._clean_200_streak = 0
        return Outcome(status, resp, disposition="blocked", tripped=True, halt=False,
                       breaker_sleep_s=sleep_s, retry_after=retry_after)

    # ── the one governed call ───────────────────────────────────────────────────────
    def request(self, send, *, extra_headers: dict | None = None) -> Outcome:
        """Pace, execute ``send(headers)->response``, observe status, drive the breaker.

        ``send`` receives the full header dict (this governor's UA + ``extra_headers``) and
        must return a response object exposing ``.status_code`` and ``.headers``. Never
        raises for HTTP status; a transport exception is folded into ``Outcome(error=...)``
        and counts as a non-200 for the breaker. Returns an :class:`Outcome`; on the second
        breaker trip ``Outcome.halt`` is True — callers must stop and record
        ``disposition='throttled'``.
        """
        headers = {"User-Agent": self.user_agent}
        if extra_headers:
            headers.update(extra_headers)

        with self._lock:
            self._pace()

        try:
            resp = send(headers)
            status = getattr(resp, "status_code", None)
        except Exception as exc:  # noqa: BLE001 — transport failure = non-200 for the breaker
            with self._lock:
                out = self._classify(None, None)
            out.error = str(exc)
            if out.tripped and not out.halt:
                self._sleep(out.breaker_sleep_s)
            return out

        with self._lock:
            out = self._classify(status, resp)
        if out.tripped and not out.halt:
            # breaker first-trip cool-down happens outside the lock
            self._sleep(out.breaker_sleep_s)
        return out

    # ── convenience HTTP wrappers (lazy-import requests) ────────────────────────────
    def get(self, url: str, *, timeout: int = 600, extra_headers: dict | None = None,
            retries_5xx: int = 5):
        """GET ``url`` through the governor. Returns the response object (caller inspects
        ``.status_code``). Retries transport errors / 5xx with capped backoff. Raises
        :class:`ThrottleHalt` on a second breaker trip."""
        import requests

        last = None
        for attempt in range(retries_5xx + 1):
            def send(headers, _u=url, _t=timeout):
                return requests.get(_u, headers=headers, timeout=_t)

            out = self.request(send, extra_headers=extra_headers)
            if out.halt:
                raise ThrottleHalt(self.host, f"circuit breaker halt on GET {url}")
            if out.status_code == 200:
                return out.response
            last = out
            transient = out.status_code in (500, 502, 503, 504) or out.status_code is None
            if transient and attempt < retries_5xx:
                backoff = min(60.0, 2.0 ** attempt)
                wait = out.retry_after if (out.retry_after and out.retry_after > 0) else backoff
                self._sleep(wait)  # Retry-After wins over exponential backoff (§4)
                continue
            return out.response  # non-retryable non-200 (404, first-trip 403, …) → caller handles
        return last.response if last else None

    def post(self, url: str, *, json_body: dict | None = None, timeout: int = 600,
             extra_headers: dict | None = None, retries_5xx: int = 5):
        """POST ``url`` (JSON body) through the governor. Same semantics as :meth:`get`."""
        import requests

        last = None
        for attempt in range(retries_5xx + 1):
            def send(headers, _u=url, _t=timeout, _b=json_body):
                return requests.post(_u, headers=headers, json=_b, timeout=_t)

            out = self.request(send, extra_headers=extra_headers)
            if out.halt:
                raise ThrottleHalt(self.host, f"circuit breaker halt on POST {url}")
            if out.status_code == 200:
                return out.response
            last = out
            transient = out.status_code in (500, 502, 503, 504) or out.status_code is None
            if transient and attempt < retries_5xx:
                backoff = min(60.0, 2.0 ** attempt)
                wait = out.retry_after if (out.retry_after and out.retry_after > 0) else backoff
                self._sleep(wait)  # Retry-After wins over exponential backoff (§4)
                continue
            return out.response
        return last.response if last else None

    # ── path checkpoint ─────────────────────────────────────────────────────────────
    def is_done(self, key: str) -> bool:
        with self._lock:
            return key in self._done

    def mark_done(self, key: str) -> None:
        with self._lock:
            self._done.add(key)
            self._since_flush += 1
            if self._since_flush >= self._checkpoint_every:
                self._flush_locked()

    def flush_checkpoint(self) -> None:
        with self._lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        if not self._checkpoint_path:
            self._since_flush = 0
            return
        tmp = f"{self._checkpoint_path}.tmp"
        os.makedirs(os.path.dirname(self._checkpoint_path) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(sorted(self._done), fh)
        os.replace(tmp, self._checkpoint_path)
        self._since_flush = 0
