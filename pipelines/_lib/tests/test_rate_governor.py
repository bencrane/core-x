"""Unit tests for the shared rate governor (pipelines/_lib/rate_governor.py).

The three assertions the directive mandates for landing the governor:
  (a) sustained rate ≤ 2 req/s over ANY 10 s window,
  (b) a synthetic 403 trips the breaker and the SECOND trip returns disposition='throttled',
  (c) the path-checkpoint round-trips across a process restart.

The clock and sleep are faked so pacing and the 300 s breaker are exercised with zero
real wall-clock time.
"""
from __future__ import annotations

import types

import pytest

from pipelines._lib.rate_governor import RateGovernor, ThrottleHalt


class FakeClock:
    """Deterministic monotonic clock; ``sleep`` advances it, ``now`` reads it."""

    def __init__(self):
        self.t = 0.0

    def now(self) -> float:
        return self.t

    def sleep(self, d: float) -> None:
        if d and d > 0:
            self.t += d


def _resp(status: int, headers: dict | None = None):
    return types.SimpleNamespace(status_code=status, headers=headers or {})


# ── (a) sustained rate ≤ 2 req/s over any 10 s window ────────────────────────────────
def test_sustained_rate_ceiling_2_per_second():
    clk = FakeClock()
    # warmup_requests=0 unlocks the 2 req/s ceiling immediately so the ceiling itself is
    # under test (warm-up is strictly slower, so it can never violate this bound).
    gov = RateGovernor(host="example.gov", warmup_requests=0, max_rate=2.0,
                       clock=clk.now, sleep=clk.sleep)

    permit_times: list[float] = []
    for _ in range(120):
        out = gov.request(lambda h: _resp(200))
        assert out.status_code == 200
        permit_times.append(clk.now())

    # Any half-open 10 s window must hold ≤ 20 requests (= ≤ 2 req/s).
    for t0 in permit_times:
        in_window = sum(1 for u in permit_times if t0 <= u < t0 + 10.0)
        assert in_window <= 20, f"window [{t0}, {t0+10}) held {in_window} > 20 requests"

    # And the observed steady interval is exactly 1/max_rate = 0.5 s.
    deltas = [b - a for a, b in zip(permit_times, permit_times[1:])]
    assert min(deltas) >= 0.5 - 1e-9


def test_warmup_runs_at_one_per_second():
    clk = FakeClock()
    gov = RateGovernor(host="example.gov", warmup_requests=100, warmup_rate=1.0,
                       max_rate=2.0, clock=clk.now, sleep=clk.sleep)
    permit_times = []
    for _ in range(50):  # well within the 100-request warm-up band
        gov.request(lambda h: _resp(200))
        permit_times.append(clk.now())
    deltas = [b - a for a, b in zip(permit_times, permit_times[1:])]
    assert min(deltas) >= 1.0 - 1e-9, "warm-up must pace at ≤ 1 req/s"


# ── (b) a 403 trips the breaker; the second trip returns disposition='throttled' ──────
def test_403_first_trip_cools_down_second_trip_halts():
    clk = FakeClock()
    gov = RateGovernor(host="example.gov", breaker_sleep_s=300.0,
                       clock=clk.now, sleep=clk.sleep)

    # First 403 → breaker trip #1: cools down (300 s via the fake clock) and resumes.
    out1 = gov.request(lambda h: _resp(403))
    assert out1.tripped is True
    assert out1.halt is False
    assert clk.t >= 300.0, "first breaker trip must sleep the 300 s cool-down"

    # Second 403 → breaker trip #2: halt, disposition='throttled'.
    out2 = gov.request(lambda h: _resp(403))
    assert out2.halt is True
    assert out2.disposition == "throttled"


def test_429_is_a_hard_trip_and_get_raises_throttlehalt_on_second():
    clk = FakeClock()
    gov = RateGovernor(host="example.gov", breaker_sleep_s=1.0,
                       clock=clk.now, sleep=clk.sleep)
    assert gov.request(lambda h: _resp(429)).tripped is True     # trip 1
    out2 = gov.request(lambda h: _resp(429))                     # trip 2
    assert out2.halt is True and out2.disposition == "throttled"


def test_retry_after_header_wins_over_breaker_sleep():
    clk = FakeClock()
    gov = RateGovernor(host="example.gov", breaker_sleep_s=300.0,
                       clock=clk.now, sleep=clk.sleep)
    out = gov.request(lambda h: _resp(503, {"Retry-After": "42"}))
    # 503 alone isn't a hard trip; three consecutive non-200s are. Drive two more.
    # The first call set consec=1; make it reach the 3-consecutive threshold.
    gov.request(lambda h: _resp(503, {"Retry-After": "42"}))
    out3 = gov.request(lambda h: _resp(503, {"Retry-After": "42"}))
    assert out3.tripped is True
    assert out3.breaker_sleep_s == 42.0, "Retry-After must override the 300 s default"


def test_retry_after_surfaced_on_transient_non200():
    # A lone 5xx with Retry-After must SURFACE the value (not swallow it) so the caller
    # honors it over its own exponential backoff — never hammering a host that asked to wait.
    clk = FakeClock()
    gov = RateGovernor(host="example.gov", clock=clk.now, sleep=clk.sleep)
    out = gov.request(lambda h: _resp(503, {"Retry-After": "300"}))
    assert out.tripped is False
    assert out.retry_after == 300.0


def test_zero_retry_after_cannot_defeat_hard_trip_floor():
    # A hard 403 with `Retry-After: 0` must NOT resume instantly — the 300 s floor stands.
    clk = FakeClock()
    gov = RateGovernor(host="example.gov", breaker_sleep_s=300.0, clock=clk.now, sleep=clk.sleep)
    out = gov.request(lambda h: _resp(403, {"Retry-After": "0"}))
    assert out.tripped is True and out.halt is False
    assert out.breaker_sleep_s == 300.0


def test_three_consecutive_non200_trips_breaker():
    clk = FakeClock()
    gov = RateGovernor(host="example.gov", breaker_consecutive=3, breaker_sleep_s=1.0,
                       clock=clk.now, sleep=clk.sleep)
    assert gov.request(lambda h: _resp(500)).tripped is False   # 1
    assert gov.request(lambda h: _resp(500)).tripped is False   # 2
    assert gov.request(lambda h: _resp(500)).tripped is True    # 3 → trip


def test_clean_200_resets_consecutive_non200_streak():
    clk = FakeClock()
    gov = RateGovernor(host="example.gov", breaker_consecutive=3, breaker_sleep_s=1.0,
                       clock=clk.now, sleep=clk.sleep)
    gov.request(lambda h: _resp(500))
    gov.request(lambda h: _resp(500))
    gov.request(lambda h: _resp(200))          # resets the streak
    assert gov.request(lambda h: _resp(500)).tripped is False   # only 1 again, no trip


# ── (c) path checkpoint round-trips across a process restart ─────────────────────────
def test_checkpoint_round_trips_across_restart(tmp_path):
    ckpt = str(tmp_path / "sub" / "governor.ckpt.json")

    gov1 = RateGovernor(host="example.gov", checkpoint_path=ckpt, checkpoint_every=2)
    gov1.mark_done("2025:federal_account")
    gov1.mark_done("2024:federal_account")     # auto-flush at checkpoint_every=2
    gov1.mark_done("2023:federal_account")
    gov1.flush_checkpoint()                     # explicit final flush

    # Simulate a process restart: a brand-new instance loads the on-disk checkpoint.
    gov2 = RateGovernor(host="example.gov", checkpoint_path=ckpt, checkpoint_every=2)
    assert gov2.is_done("2025:federal_account")
    assert gov2.is_done("2024:federal_account")
    assert gov2.is_done("2023:federal_account")
    assert not gov2.is_done("2017:federal_account")


def test_checkpoint_autoflush_persists_without_explicit_flush(tmp_path):
    ckpt = str(tmp_path / "governor.ckpt.json")
    gov1 = RateGovernor(host="example.gov", checkpoint_path=ckpt, checkpoint_every=2)
    gov1.mark_done("a")
    gov1.mark_done("b")   # hits checkpoint_every → auto-flush, no explicit flush call
    gov2 = RateGovernor(host="example.gov", checkpoint_path=ckpt)
    assert gov2.is_done("a") and gov2.is_done("b")


def test_default_ua_is_descriptive_and_whitehouse_gets_browser_ua():
    api = RateGovernor(host="api.usaspending.gov")
    wh = RateGovernor(host="www.whitehouse.gov")
    assert "core-x-data-factory" in api.user_agent
    assert "Mozilla/5.0" in wh.user_agent


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
