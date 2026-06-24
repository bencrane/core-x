"""Hermetic tests for the warm dataset-handle cache (stale-while-revalidate) — no R2.

Pins the latency contract behind the sub-second dossier: ONE open per URI within the
TTL (handle + resident index reuse), stale handles KEEP SERVING past the TTL while a
single-flight background refresh warms a replacement before swapping it in, refresh
failure never evicts the stale handle, and concurrent first-callers converge on the
cache. ``_open_uri`` / ``_now`` / the refresh pool are monkeypatched so everything is
deterministic and offline.
"""

from __future__ import annotations

import threading

import pytest

from apps.catalyst_api.src import lance_store


class FakeDataset:
    """Stands in for lance.LanceDataset — identity is what the cache tests assert."""

    _seq = 0

    def __init__(self, uri: str):
        FakeDataset._seq += 1
        self.uri = uri
        self.version = FakeDataset._seq
        self.warmed = False

    def scanner(self, **kwargs):  # the warm probe path
        ds = self

        class _S:
            def to_table(self):
                ds.warmed = True
                return None

        return _S()


class InlinePool:
    """Executor double: runs submissions synchronously so refreshes are deterministic."""

    def __init__(self):
        self.submitted = 0

    def submit(self, fn, *args):
        self.submitted += 1
        fn(*args)


@pytest.fixture()
def cache(monkeypatch):
    """Fresh cache state + controllable clock/opens; yields a mutable test context."""
    ctx = {"t": 1000.0, "opens": []}

    def fake_now():
        return ctx["t"]

    def fake_open(uri):
        ctx["opens"].append(uri)
        return FakeDataset(uri)

    monkeypatch.setattr(lance_store, "_now", fake_now)
    monkeypatch.setattr(lance_store, "_open_uri", fake_open)
    monkeypatch.setattr(lance_store, "_HANDLE_TTL_S", 300.0)
    monkeypatch.setattr(lance_store, "_handle_cache", {})
    monkeypatch.setattr(lance_store, "_refreshing", set())
    pool = InlinePool()
    monkeypatch.setattr(lance_store, "_REFRESH_POOL", pool)
    ctx["pool"] = pool
    return ctx


URI = "s3://data-sink/active/entity_profile_gold/"


def test_one_open_per_uri_within_ttl(cache):
    a = lance_store._dataset(URI)
    b = lance_store._dataset(URI)
    assert a is b
    assert cache["opens"] == [URI]


def test_distinct_uris_get_distinct_handles(cache):
    a = lance_store._dataset(URI)
    b = lance_store._dataset("s3://data-sink/active/contractor_award_summary/")
    assert a is not b and len(cache["opens"]) == 2


def test_stale_handle_keeps_serving_while_refresh_swaps_warm_replacement(cache):
    a = lance_store._dataset(URI)
    cache["t"] += 301  # past TTL
    # The expired call returns the STALE handle (no blocking)…
    b = lance_store._dataset(URI)
    assert b is a
    # …and the (inline) single-flight refresh swapped a NEW, WARMED handle in.
    assert cache["pool"].submitted == 1
    c = lance_store._dataset(URI)
    assert c is not a and c.version > a.version
    assert c.warmed is True  # _warm_key_columns covers entity_profile_gold (uei)


def test_refresh_is_single_flight(cache, monkeypatch):
    lance_store._dataset(URI)
    cache["t"] += 301
    # A refresh that never completes: occupy the in-flight marker manually.
    recorded = []
    monkeypatch.setattr(lance_store, "_REFRESH_POOL",
                        type("P", (), {"submit": lambda self, fn, *a: recorded.append(fn)})())
    lance_store._dataset(URI)   # schedules (records) one refresh
    lance_store._dataset(URI)   # in-flight marker set → no second submission
    assert len(recorded) == 1


def test_refresh_failure_keeps_stale_handle(cache, monkeypatch):
    a = lance_store._dataset(URI)

    def boom(uri):
        raise RuntimeError("r2 blip")

    monkeypatch.setattr(lance_store, "_open_uri", boom)
    cache["t"] += 301
    b = lance_store._dataset(URI)        # stale served; inline refresh fails
    assert b is a
    c = lance_store._dataset(URI)        # still serving the stale handle, not crashed
    assert c is a
    # the in-flight marker was released, so a later attempt can retry
    assert URI not in lance_store._refreshing


def test_concurrent_first_callers_converge(cache):
    results = []

    def hit():
        results.append(lance_store._dataset(URI))

    threads = [threading.Thread(target=hit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # Multiple first-opens are allowed (cost-only) but every caller got a handle and
    # the cache converged to ONE — subsequent calls reuse it with no new opens.
    assert len(results) == 8 and all(r is not None for r in results)
    settled = lance_store._dataset(URI)
    opens_before = len(cache["opens"])
    assert lance_store._dataset(URI) is settled
    assert len(cache["opens"]) == opens_before


def test_prewarm_covers_the_dossier_surfaces(cache):
    timings = lance_store.prewarm_dossier_surfaces()
    assert set(timings) == {"entity_profile_gold", "contractor_award_summary",
                            "usaspending_awards_map_serving", "people"}
    # every prewarmed handle had its key BTREE probed
    for uri in lance_store._warm_key_columns():
        assert lance_store._handle_cache[uri][1].warmed is True
