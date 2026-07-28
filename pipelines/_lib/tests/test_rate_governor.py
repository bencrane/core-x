"""Unit tests for the shared rate governor (pipelines/_lib/rate_governor.py).

The three properties the directives make binding are proven here with an injected fake
clock — no wall-clock waits, fully deterministic:

  1. sustained ≤ 2 req/s over ANY 10 s window (the token bucket's spacing invariant);
  2. a synthetic 403 trips the breaker (300 s cool-down) and a SECOND trip surfaces
     disposition='throttled';
  3. the path-checkpoint round-trips across a process restart.

Plus: warm-up ramp only after N consecutive 200s (any non-200 resets), the 3-consecutive-
soft-failure breaker, and Retry-After winning over the fixed cool-down.

    python -m pytest pipelines/_lib/tests/test_rate_governor.py -q
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

# repo root = .../pipelines/_lib/tests/this_file → parents[3]
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pipelines._lib.rate_governor import (  # noqa: E402
    FileCheckpointStore,
    InMemoryCheckpointStore,
    PathCheckpoint,
    RateGovernor,
    ThrottledError,
    TokenBucket,
)


class FakeClock:
    """Deterministic clock: sleep advances virtual time. Single-threaded use only."""

    def __init__(self) -> None:
        self.t = 0.0

    def now(self) -> float:
        return self.t

    def sleep(self, dt: float) -> None:
        self.t += max(0.0, dt)


class Resp:
    def __init__(self, status_code: int, headers: dict | None = None) -> None:
        self.status_code = status_code
        self.headers = headers or {}


# ── 1. sustained ≤ 2 req/s over any 10 s window ──────────────────────────────────────
def test_token_bucket_sustained_rate_over_any_10s_window():
    fc = FakeClock()
    bucket = TokenBucket(2.0, clock=fc.now, sleep=fc.sleep)
    grants = []
    for _ in range(80):  # 40 s of virtual traffic
        bucket.acquire()
        grants.append(fc.now())

    # exact 0.5 s spacing → never faster than 2/s
    deltas = [b - a for a, b in zip(grants, grants[1:])]
    assert min(deltas) >= 0.5 - 1e-9, f"spacing dipped below 0.5s: min={min(deltas)}"

    # no half-open 10 s window holds more than 20 grants (= 2 req/s)
    worst = 0
    for start in grants:
        n = sum(1 for g in grants if start <= g < start + 10.0)
        worst = max(worst, n)
    assert worst <= 20, f"a 10s window held {worst} grants (> 2 req/s)"


def test_token_bucket_first_request_immediate():
    fc = FakeClock()
    bucket = TokenBucket(1.0, clock=fc.now, sleep=fc.sleep)
    bucket.acquire()
    assert fc.now() == 0.0  # first grant costs nothing


# ── 2. 403 trips the breaker; second trip → throttled ────────────────────────────────
def test_single_403_trips_breaker_and_second_trip_throttles():
    fc = FakeClock()
    gov = RateGovernor(warmup_requests=100, breaker_sleep_s=300.0,
                       clock=fc.now, sleep=fc.sleep)
    calls = []

    def do_get(url):
        calls.append(fc.now())
        return Resp(403)

    with pytest.raises(ThrottledError):
        gov.request(do_get, "https://host/x")

    assert gov.disposition == "throttled"
    assert fc.t >= 300.0, "first 403 must have forced a 300s cool-down"
    assert len(calls) == 2, "expected: 403 → cool-down → retry → 403 → throttled"
    assert gov.total_trips == 2


def test_429_also_trips_breaker():
    fc = FakeClock()
    gov = RateGovernor(clock=fc.now, sleep=fc.sleep)
    with pytest.raises(ThrottledError):
        gov.request(lambda u: Resp(429), "https://host/x")
    assert gov.disposition == "throttled"


def test_retry_after_wins_over_fixed_cooldown():
    fc = FakeClock()
    gov = RateGovernor(breaker_sleep_s=300.0, clock=fc.now, sleep=fc.sleep)
    seq = iter([Resp(429, {"Retry-After": "42"}), Resp(200)])

    resp = gov.request(lambda u: next(seq), "https://host/x")
    assert resp.status_code == 200
    # honored the 42s header, not the 300s default
    assert 42.0 <= fc.t < 300.0, f"expected ~42s cool-down, got {fc.t}"
    assert gov.total_trips == 1


# ── 3-consecutive soft failures trip the breaker (no single 403 needed) ───────────────
def test_three_consecutive_soft_failures_trip_breaker():
    fc = FakeClock()
    gov = RateGovernor(clock=fc.now, sleep=fc.sleep)
    # 500s are soft-failures: returned to caller, but 3 in a row trip the breaker.
    # (the ~1s/req warm-up spacing advances the clock a little; the 300s breaker
    # cool-down is the discriminator, not t==0.)
    for _ in range(2):
        r = gov.request(lambda u: Resp(500), "https://host/x")
        assert r.status_code == 500
    assert fc.t < 300.0, "no 300s cool-down before the 3rd consecutive failure"
    r = gov.request(lambda u: Resp(500), "https://host/x")
    assert r.status_code == 500
    assert fc.t >= 300.0, "3rd consecutive non-200 must trip the breaker"
    assert gov.total_trips == 1


def test_success_resets_consecutive_failure_counter():
    fc = FakeClock()
    gov = RateGovernor(clock=fc.now, sleep=fc.sleep)
    gov.request(lambda u: Resp(500), "https://host/x")
    gov.request(lambda u: Resp(500), "https://host/x")
    gov.request(lambda u: Resp(200), "https://host/x")  # resets
    gov.request(lambda u: Resp(500), "https://host/x")
    gov.request(lambda u: Resp(500), "https://host/x")
    assert fc.t < 300.0, "counter must have reset on the 200 — no breaker cool-down"
    assert gov.total_trips == 0


# ── warm-up ramp: only after N consecutive 200s; any non-200 resets ──────────────────
def test_warmup_ramps_only_after_streak_and_resets_on_non200():
    fc = FakeClock()
    gov = RateGovernor(warmup_requests=5, warmup_rate=1.0, max_rate=2.0,
                       clock=fc.now, sleep=fc.sleep)
    assert gov.in_warmup
    for _ in range(4):
        gov.request(lambda u: Resp(200), "https://host/x")
    assert gov.in_warmup, "still warming up at 4/5 clean 200s"
    gov.request(lambda u: Resp(404), "https://host/x")  # resets the streak
    for _ in range(4):
        gov.request(lambda u: Resp(200), "https://host/x")
    assert gov.in_warmup, "the 404 must have reset the streak back to 0"
    gov.request(lambda u: Resp(200), "https://host/x")  # 5th clean 200 → ramp
    assert not gov.in_warmup, "should have ramped to steady after 5 consecutive 200s"


# ── 3. path-checkpoint round-trips across a process restart ──────────────────────────
def test_path_checkpoint_round_trips_in_memory():
    store = InMemoryCheckpointStore()
    ck = PathCheckpoint(store)
    ck.add("/Fiscal Year 2026/a.json")
    ck.add("/Fiscal Year 2025/b.json")
    ck.flush()

    # a fresh process would construct a NEW PathCheckpoint over the SAME store
    ck2 = PathCheckpoint(store)
    assert "/Fiscal Year 2026/a.json" in ck2
    assert "/Fiscal Year 2025/b.json" in ck2
    assert len(ck2) == 2


def test_path_checkpoint_round_trips_across_file_restart():
    with tempfile.TemporaryDirectory() as td:
        path = str(Path(td) / "ckpt.json")
        store1 = FileCheckpointStore(path)
        ck1 = PathCheckpoint(store1)
        for i in range(250):
            ck1.add(f"/p/{i}.json")
        ck1.flush()

        # simulate a hard restart: brand-new store object + checkpoint over the same file
        ck2 = PathCheckpoint(FileCheckpointStore(path))
        assert len(ck2) == 250
        assert "/p/0.json" in ck2 and "/p/249.json" in ck2
        assert "/p/250.json" not in ck2


def test_checkpoint_unflushed_tail_is_the_only_loss():
    # what's added-but-not-flushed is exactly the tail a crash would cost
    store = InMemoryCheckpointStore()
    ck = PathCheckpoint(store)
    ck.add("a"); ck.add("b")
    ck.flush()
    ck.add("c")  # not flushed
    recovered = PathCheckpoint(store)
    assert "a" in recovered and "b" in recovered
    assert "c" not in recovered  # the un-flushed tail is the only loss
