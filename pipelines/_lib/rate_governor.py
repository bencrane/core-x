"""Shared rate governor for polite public-data crawls — token bucket + warm-up ramp +
circuit breaker + resumable path-checkpoint.

WHY THIS EXISTS
    The federal reference-data ingests (OMB apportionment, federal appropriations, CBO
    scoring) crawl publisher hosts that return **no** rate-limit headers — no
    ``X-RateLimit-*``, no ``Retry-After``. There is no warning shot: a host either
    tolerates you or goes straight to a block page, and the block is IP-scoped and can
    persist for hours. ``bls.gov`` and ``cbo.gov`` have already hard-403'd this program's
    egress. So every network call routes through ONE chokepoint that enforces the binding
    discipline mechanically rather than trusting a ``sleep()`` between calls.

    All three sibling directives import this module; it is deliberately transport-agnostic
    (it drives a caller-supplied ``do_get`` callable) and clock-injectable (so the
    discipline is unit-tested deterministically with a fake clock, no wall-clock waits).

THE CONTRACT (do not relax without an operator ruling)
    1. Sustained rate ≤ ``max_rate`` req/s (default 2.0), enforced by a spacing token
       bucket shared across all workers — not by per-call sleeps.
    2. Concurrency ≤ ``max_workers`` (default 3), and exactly **1** during warm-up.
    3. Warm-up ramp: the first ``warmup_requests`` (default 100) requests run at
       ``warmup_rate`` req/s (default 1.0), single worker. Ramp to the ceiling only after
       that many CONSECUTIVE clean 200s. Any non-200 resets the streak.
    4. Circuit breaker — halt, never grind. 3 consecutive non-200s, OR any single 403/429,
       stops all workers, sleeps ``breaker_sleep_s`` (default 300), and resumes at warm-up
       settings. A **second** trip in the same run raises ``ThrottledError`` (disposition
       ``'throttled'``) so the caller halts the run and flushes its checkpoint.
    5. ``Retry-After`` (if the response ever carries one) wins over ``breaker_sleep_s``.
    6. The path-checkpoint persists the completed-path set to a pluggable store and
       round-trips across a process restart, so a block costs only the in-flight batch.

    The governor NEVER spawns threads. The caller owns its pool; the governor makes the
    shared pool safe (rate + concurrency + breaker are all enforced inside ``request``).
"""
from __future__ import annotations

import json
import threading
from typing import Any, Callable, Optional, Protocol


class ThrottledError(RuntimeError):
    """Raised on the SECOND circuit-breaker trip in a run — the host is walling us and
    grinding further risks a persistent IP block. The caller catches this, writes the
    ledger row (``status='failed'``, ``disposition='throttled'``), flushes the checkpoint,
    and surfaces to the operator. Carries ``disposition='throttled'``."""

    disposition = "throttled"


# ── spacing token bucket ────────────────────────────────────────────────────────────
class TokenBucket:
    """Thread-safe minimum-inter-request-spacing limiter. A pure spacing model (not an
    accumulating bucket) so no burst can ever exceed the sustained rate: with rate ``r``
    the grants are spaced ``1/r`` apart, hence at most ``floor(W*r)`` grants land in any
    half-open window of length ``W`` (e.g. r=2 → ≤20 per any 10 s window). The first
    request is granted immediately; the rate can be changed live (warm-up ↔ steady)."""

    def __init__(self, rate: float, *, clock: Callable[[], float], sleep: Callable[[float], None]):
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self._interval = 1.0 / rate
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next_allowed = clock()

    def set_rate(self, rate: float) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        with self._lock:
            self._interval = 1.0 / rate

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = self._clock()
                if now >= self._next_allowed:
                    # grant now; schedule the next slot one interval out
                    self._next_allowed = now + self._interval
                    return
                wait = self._next_allowed - now
            # sleep OUTSIDE the lock so other workers can compute their own waits;
            # re-check after waking (another worker may have taken this slot).
            self._sleep(wait)


# ── dynamic concurrency limiter ──────────────────────────────────────────────────────
class _DynamicConcurrency:
    """A semaphore whose ceiling can change at runtime (1 during warm-up, ``max_workers``
    steady). Cheaper than tearing the pool down and rebuilding it on every ramp."""

    def __init__(self, limit: int):
        self._limit = max(1, limit)
        self._active = 0
        self._cv = threading.Condition()

    def set_limit(self, limit: int) -> None:
        with self._cv:
            self._limit = max(1, limit)
            self._cv.notify_all()

    def acquire(self) -> None:
        with self._cv:
            while self._active >= self._limit:
                self._cv.wait()
            self._active += 1

    def release(self) -> None:
        with self._cv:
            self._active -= 1
            self._cv.notify_all()


# ── resumable path checkpoint ─────────────────────────────────────────────────────────
class CheckpointStore(Protocol):
    """Byte blob persistence for the completed-path set. Backends: R2 object (prod),
    local file (tests / offline), in-memory (unit tests)."""

    def read(self) -> Optional[bytes]: ...
    def write(self, data: bytes) -> None: ...


class InMemoryCheckpointStore:
    """A store that survives only in-process. Useful in tests to prove serialization."""

    def __init__(self, data: Optional[bytes] = None):
        self._data = data

    def read(self) -> Optional[bytes]:
        return self._data

    def write(self, data: bytes) -> None:
        self._data = data


class FileCheckpointStore:
    """A store backed by a local file — round-trips across a real process restart."""

    def __init__(self, path: str):
        self._path = path

    def read(self) -> Optional[bytes]:
        try:
            with open(self._path, "rb") as fh:
                return fh.read()
        except FileNotFoundError:
            return None

    def write(self, data: bytes) -> None:
        tmp = f"{self._path}.tmp"
        with open(tmp, "wb") as fh:
            fh.write(data)
        import os

        os.replace(tmp, self._path)


class PathCheckpoint:
    """The resumable completed-path ledger. Loads its set from the store at construction
    (so a fresh process picks up exactly where a killed one left off), is safe to mutate
    from many workers, and flushes back to the store on demand (the caller flushes every
    N completions). A block costs only the un-flushed tail, never the whole crawl."""

    def __init__(self, store: CheckpointStore):
        self._store = store
        self._lock = threading.Lock()
        raw = store.read()
        self._done: set[str] = set(json.loads(raw.decode("utf-8"))) if raw else set()

    def __contains__(self, path: str) -> bool:
        with self._lock:
            return path in self._done

    def __len__(self) -> int:
        with self._lock:
            return len(self._done)

    def add(self, path: str) -> None:
        with self._lock:
            self._done.add(path)

    def snapshot(self) -> set[str]:
        with self._lock:
            return set(self._done)

    def flush(self) -> None:
        with self._lock:
            data = json.dumps(sorted(self._done)).encode("utf-8")
        self._store.write(data)


# ── the governor ──────────────────────────────────────────────────────────────────────
_WARMUP = "warmup"
_STEADY = "steady"


class RateGovernor:
    """The single chokepoint every network call routes through. See module docstring for
    the binding contract. ``request(do_get, url)`` acquires a concurrency slot and a rate
    token, calls ``do_get(url)`` (which must return an object with ``.status_code`` and a
    dict-like ``.headers``), classifies the response, and:

      * 200            → resets the failure counter, advances the warm-up streak, returns.
      * 403 / 429      → trips the breaker (sleep + resume-at-warmup) and RETRIES; a second
                         trip raises ``ThrottledError``.
      * other non-200  → resets the warm-up streak, increments the consecutive-failure
                         counter (trips the breaker at 3), and returns the response so the
                         CALLER applies its own bounded 5xx/timeout retry or skip.

    Timeouts / connection errors are not responses — the caller catches those, and should
    call ``note_transport_error()`` so they count toward the 3-consecutive-failure breaker
    exactly like a non-200 would.
    """

    def __init__(
        self,
        *,
        max_rate: float = 2.0,
        max_workers: int = 3,
        warmup_requests: int = 100,
        warmup_rate: float = 1.0,
        breaker_sleep_s: float = 300.0,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
    ):
        import time

        self._clock = clock or time.monotonic
        self._sleep = sleep or time.sleep
        self._max_rate = max_rate
        self._max_workers = max_workers
        self._warmup_requests = warmup_requests
        self._warmup_rate = warmup_rate
        self._breaker_sleep_s = breaker_sleep_s

        self._bucket = TokenBucket(warmup_rate, clock=self._clock, sleep=self._sleep)
        self._slots = _DynamicConcurrency(1)  # single worker during warm-up

        self._state_lock = threading.Lock()
        self._gate = threading.Event()
        self._gate.set()  # open
        self._phase = _WARMUP
        self._success_streak = 0
        self._consec_fail = 0
        self._episode = 0     # monotonic breaker-episode id (absorbs concurrent failures)
        self._trips = 0
        self.disposition = "ok"

        # observability counters
        self.total_requests = 0
        self.total_200 = 0
        self.total_non200 = 0
        self.total_trips = 0

    # -- public state (read-only-ish) --------------------------------------------------
    @property
    def phase(self) -> str:
        return self._phase

    @property
    def in_warmup(self) -> bool:
        return self._phase == _WARMUP

    @property
    def max_workers(self) -> int:
        return self._max_workers

    def stats(self) -> dict:
        return {
            "phase": self._phase,
            "requests": self.total_requests,
            "ok": self.total_200,
            "non200": self.total_non200,
            "trips": self.total_trips,
            "disposition": self.disposition,
        }

    # -- the chokepoint ----------------------------------------------------------------
    def request(self, do_get: Callable[[str], Any], url: str) -> Any:
        while True:
            self._gate.wait()
            with self._state_lock:
                ep = self._episode
            self._slots.acquire()
            try:
                self._bucket.acquire()
                resp = do_get(url)
            finally:
                self._slots.release()

            code = getattr(resp, "status_code", None)
            with self._state_lock:
                self.total_requests += 1
            if code == 200:
                self._on_success()
                return resp
            with self._state_lock:
                self.total_non200 += 1
            if code in (403, 429):
                self._trip(ep, resp)   # sleeps + resumes, or raises ThrottledError
                continue               # retry the same fetch after the breaker cools
            # other non-200 (404 / 5xx / ...): a soft failure the caller owns retry for
            self._on_soft_fail(ep, resp)
            return resp

    def note_transport_error(self) -> None:
        """A timeout / connection error is not an HTTP response but IS a consecutive
        failure for breaker purposes. The caller calls this before its own retry so a
        wall of timeouts trips the breaker instead of grinding."""
        self._on_soft_fail(self._episode, None)

    # -- internals ---------------------------------------------------------------------
    def _on_success(self) -> None:
        with self._state_lock:
            self.total_200 += 1
            self._consec_fail = 0
            if self._phase == _WARMUP:
                self._success_streak += 1
                if self._success_streak >= self._warmup_requests:
                    self._enter_steady_locked()

    def _on_soft_fail(self, ep: int, resp: Any) -> None:
        should_trip = False
        with self._state_lock:
            if self._phase == _WARMUP:
                self._success_streak = 0
            self._consec_fail += 1
            if self._consec_fail >= 3:
                should_trip = True
        if should_trip:
            self._trip(ep, resp)

    def _trip(self, ep: int, resp: Any) -> None:
        perform = False
        delay = 0.0
        with self._state_lock:
            # Only the worker that observes the failure FIRST (matching, live episode with
            # the gate still open) owns the trip; concurrent failures from the same episode
            # fall through and simply wait out the cool-down — they do not double-count.
            if ep == self._episode and self._gate.is_set():
                self._episode += 1
                self._trips += 1
                self.total_trips += 1
                if self._trips >= 2:
                    self.disposition = "throttled"
                    raise ThrottledError(
                        "second circuit-breaker trip in this run — halting to avoid a "
                        "persistent IP block"
                    )
                self._gate.clear()  # freeze all workers
                self._enter_warmup_locked()
                delay = self._retry_after(resp)
                if delay <= 0:
                    delay = self._breaker_sleep_s
                perform = True
        if perform:
            self._sleep(delay)      # every other worker is blocked on the closed gate
            self._gate.set()        # thaw at warm-up settings
        else:
            self._gate.wait()       # someone else is cooling this episode down

    def _retry_after(self, resp: Any) -> float:
        if resp is None:
            return 0.0
        headers = getattr(resp, "headers", None) or {}
        val = None
        for k in ("Retry-After", "retry-after"):
            if k in headers:
                val = headers[k]
                break
        if val is None:
            return 0.0
        try:
            return max(0.0, float(int(str(val).strip())))
        except (TypeError, ValueError):
            return 0.0  # HTTP-date form: fall back to the fixed breaker sleep

    def _enter_warmup_locked(self) -> None:
        self._phase = _WARMUP
        self._success_streak = 0
        self._consec_fail = 0
        self._bucket.set_rate(self._warmup_rate)
        self._slots.set_limit(1)

    def _enter_steady_locked(self) -> None:
        self._phase = _STEADY
        self._consec_fail = 0
        self._bucket.set_rate(self._max_rate)
        self._slots.set_limit(self._max_workers)
