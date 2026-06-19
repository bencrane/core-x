"""Acceptance tests (frozen Scorer) for the hot-lookup result cache
(apps/gtm_mcp/src/tools/lookup_cache.py — to be implemented by the executor).

WHY. The retrieval cost is native Lance scan + R2 I/O per call. Within an agent session the
SAME point-lookup recurs (the agent re-fetches a company/entity it already saw). A small
TTL+LRU RESULT cache (distinct from the existing warm-handle cache, which caches the dataset
HANDLE, not the result) serves a repeat identical lookup with ZERO scan. Correctness rails:
TTL-bounded staleness (tiered to dataset cadence), LRU-bounded memory, thread-safe (the SSE
gateway is multi-threaded), keyed by NORMALIZED args, transparent (cached == uncached result),
and an env kill-switch.

CONTRACT (executor implements to GREEN):
  TTLCache(ttl_s, maxsize, clock=time.monotonic): .get(key)->(found,value), .set(key,value),
      .clear(), .hits, .misses, .size(); LRU eviction at maxsize; TTL via the injected clock.
  memoize(ttl_s, maxsize=…, key_fn=…, clock=…, enabled=…): decorator (functools.wraps) that
      caches a function's return keyed by key_fn(*a,**k); registers the cache for clear_all().
  clear_all(): clears every memoized cache (test isolation + an ops lever).
  cache_enabled(): reads env GTM_LOOKUP_CACHE_DISABLED (default ON).
  APPLICATION: the hot read-only point-lookups are wrapped — pinned here for
      audience.search_company_by_domain (a repeat identical call issues ONE scan).

Determinism: the TTL/expiry tests inject a fake clock (no sleeps). The integration test uses
the real clock with two rapid calls (well within any sane TTL).
"""

from __future__ import annotations

import threading

import lance
import pyarrow as pa
import pytest

from apps.gtm_mcp.src import database

lookup_cache = pytest.importorskip(
    "apps.gtm_mcp.src.tools.lookup_cache",
    reason="lookup_cache module not implemented yet (executor builds it to GREEN)",
)


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


# ── TTLCache primitive ────────────────────────────────────────────────────────────────────────
def test_ttlcache_hit_miss_and_counts():
    clk = _Clock()
    c = lookup_cache.TTLCache(ttl_s=10, maxsize=8, clock=clk)
    found, _ = c.get("k")
    assert found is False and c.misses == 1
    c.set("k", 42)
    found, val = c.get("k")
    assert found is True and val == 42 and c.hits == 1
    assert c.size() == 1


def test_ttlcache_ttl_expiry():
    clk = _Clock()
    c = lookup_cache.TTLCache(ttl_s=10, maxsize=8, clock=clk)
    c.set("k", "v")
    clk.advance(5)
    assert c.get("k") == (True, "v")        # within TTL
    clk.advance(6)                          # now 11s > 10s ttl
    found, _ = c.get("k")
    assert found is False                    # expired → miss


def test_ttlcache_lru_eviction():
    clk = _Clock()
    c = lookup_cache.TTLCache(ttl_s=1000, maxsize=2, clock=clk)
    c.set("a", 1)
    c.set("b", 2)
    assert c.get("a")[0] is True            # touch a → b is now LRU
    c.set("c", 3)                           # evicts b
    assert c.get("b")[0] is False           # b evicted
    assert c.get("a") == (True, 1) and c.get("c") == (True, 3)
    assert c.size() == 2                     # bounded


# ── memoize decorator ─────────────────────────────────────────────────────────────────────────
def test_memoize_hit_within_ttl_then_expire():
    clk = _Clock()
    calls = {"n": 0}

    @lookup_cache.memoize(ttl_s=10, maxsize=16, clock=clk)
    def f(x):
        calls["n"] += 1
        return x * 2

    assert f(3) == 6 and f(3) == 6 and calls["n"] == 1   # 2nd served from cache
    clk.advance(11)
    assert f(3) == 6 and calls["n"] == 2                  # expired → recompute
    assert f.__name__ == "f"                              # functools.wraps preserved


def test_memoize_distinct_args_and_key_fn():
    clk = _Clock()
    calls = {"n": 0}

    @lookup_cache.memoize(ttl_s=10, maxsize=16, key_fn=lambda s: s.strip().lower(), clock=clk)
    def f(s):
        calls["n"] += 1
        return s.upper()

    f("ACME"); f("acme"); f("  Acme ")     # all normalize to one key
    assert calls["n"] == 1
    f("beta")
    assert calls["n"] == 2                  # distinct key → recompute


def test_memoize_disabled_bypasses():
    clk = _Clock()
    calls = {"n": 0}

    @lookup_cache.memoize(ttl_s=10, maxsize=16, clock=clk, enabled=lambda: False)
    def f(x):
        calls["n"] += 1
        return x

    f(1); f(1); f(1)
    assert calls["n"] == 3                  # disabled → never cached


def test_cache_enabled_env_kill_switch(monkeypatch):
    # Default ON (var absent) → caching active.
    monkeypatch.delenv("GTM_LOOKUP_CACHE_DISABLED", raising=False)
    assert lookup_cache.cache_enabled() is True
    # Each documented disabling token flips it OFF.
    for tok in ("1", "true", "yes"):
        monkeypatch.setenv("GTM_LOOKUP_CACHE_DISABLED", tok)
        assert lookup_cache.cache_enabled() is False, tok
    # An unrelated value leaves caching ON (only the documented tokens disable).
    monkeypatch.setenv("GTM_LOOKUP_CACHE_DISABLED", "0")
    assert lookup_cache.cache_enabled() is True

    # And the decorator honors the real cache_enabled() default-ON path end-to-end.
    monkeypatch.delenv("GTM_LOOKUP_CACHE_DISABLED", raising=False)
    clk = _Clock()
    calls = {"n": 0}

    @lookup_cache.memoize(ttl_s=1000, maxsize=16, clock=clk)
    def f(x):
        calls["n"] += 1
        return x

    f(5); f(5)
    assert calls["n"] == 1                   # default-ON → 2nd call served from cache


def test_clear_all_resets():
    clk = _Clock()
    calls = {"n": 0}

    @lookup_cache.memoize(ttl_s=1000, maxsize=16, clock=clk)
    def f(x):
        calls["n"] += 1
        return x

    f(7); f(7)
    assert calls["n"] == 1
    lookup_cache.clear_all()
    f(7)
    assert calls["n"] == 2                  # cleared → recompute


def test_thread_safe_no_corruption():
    clk = _Clock()
    calls = {"n": 0}
    lock = threading.Lock()

    @lookup_cache.memoize(ttl_s=1000, maxsize=64, clock=clk)
    def f(x):
        with lock:
            calls["n"] += 1
        return x * 10

    results = []
    rlock = threading.Lock()

    def worker(v):
        r = f(v)
        with rlock:
            results.append((v, r))

    threads = [threading.Thread(target=worker, args=(i % 5,)) for i in range(40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert all(r == v * 10 for v, r in results)     # every result correct under concurrency
    assert len(results) == 40
    assert 1 <= calls["n"] <= 5                       # at most one compute per distinct key family


# ── application to a real point-lookup (integration; real clock, rapid calls) ──────────────────
class _CountingDataset:
    def __init__(self, ds, counter):
        self._ds, self._counter = ds, counter

    def scanner(self, **kw):
        self._counter[0] += 1
        return self._ds.scanner(**kw)

    @property
    def schema(self):
        return self._ds.schema


def test_cache_applied_to_search_company_by_domain(tmp_path, monkeypatch):
    from apps.gtm_mcp.src.tools import audience
    rows = [{"company_id": "co1", "company_name": "Acme", "normalized_domain": "acme.com",
             "company_linkedin_url": "li/1", "source_platform": "fixture"}]
    p = str(tmp_path / "companies.lance")
    lance.write_dataset(pa.Table.from_pylist(rows), p, mode="create")
    lance.dataset(p).create_scalar_index("normalized_domain", index_type="BTREE")
    counter = [0]
    monkeypatch.setattr(database, "open_dataset",
                        lambda name: _CountingDataset(lance.dataset(p), counter))
    lookup_cache.clear_all()                          # isolate from any prior cache state
    a = audience.search_company_by_domain("acme.com")
    b = audience.search_company_by_domain("acme.com")  # identical → served from cache
    assert a == b and a["match_count"] == 1
    assert counter[0] == 1                            # ONE scan despite two identical lookups
