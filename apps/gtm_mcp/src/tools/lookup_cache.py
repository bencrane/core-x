"""Hot-lookup RESULT cache — a small TTL + LRU layer over the read-only point-lookups.

WHY. The retrieval cost of a point-lookup is a native Lance scan + R2 round-trip per call.
Within an agent session the SAME identical lookup recurs (the agent re-fetches a company /
entity it already resolved). This caches the lookup's RESULT keyed by its NORMALIZED args, so
a repeat identical call within the TTL is served with ZERO scan / ZERO R2 I/O.

This is DISTINCT from the warm-HANDLE cache (``database._cached_dataset``): that keeps the
dataset's BTREE index resident but still SCANS on every call; this short-circuits the scan
entirely for a repeat hit. The two compose — handle warm, result memoized.

Correctness rails (the whole point):
  • TTL-bounded staleness — an entry older than ``ttl_s`` is a miss (never served).
  • LRU-bounded memory — at most ``maxsize`` entries; the least-recently-used is evicted.
  • Thread-safe — the SSE gateway is multi-threaded; all cache mutations are under an RLock,
    and the wrapped function is computed OUTSIDE the lock (no serialized I/O, no deadlock).
  • Keyed by NORMALIZED args — casing / whitespace variants share one entry (via ``key_fn``).
  • Transparent — a cached result is the byte-identical object the uncached call returned.
  • Env kill-switch — ``GTM_LOOKUP_CACHE_DISABLED`` disables caching without a deploy.

stdlib only (``collections.OrderedDict``, ``threading``, ``functools``, ``time``, ``os``).
"""

from __future__ import annotations

import functools
import os
import threading
import time
from collections import OrderedDict
from typing import Any, Callable

__all__ = [
    "TTLCache",
    "memoize",
    "cache_enabled",
    "clear_all",
    "COMPANY_TTL_S",
    "PEOPLE_TTL_S",
    "AWARDS_TTL_S",
    "SAM_TTL_S",
]


# ── env kill-switch + cadence-tiered TTLs (env-overridable) ──────────────────────────────────
def cache_enabled() -> bool:
    """Caching is ON by default; the env var ``GTM_LOOKUP_CACHE_DISABLED`` set to one of the
    documented disabling tokens (``"1"``, ``"true"``, ``"yes"``) turns it OFF without a deploy.
    Any other value (including ``"0"`` or an empty string) leaves caching ON — only the
    documented tokens disable. Re-evaluated on every call so ops can flip it live."""
    return os.environ.get("GTM_LOOKUP_CACHE_DISABLED") not in ("1", "true", "yes")


def _ttl_from_env(var: str, default: float) -> float:
    """Read a TTL override from the environment, falling back to ``default`` when the var is
    absent or not a positive number — so a fat-fingered override can never silently disable
    expiry."""
    raw = os.environ.get(var)
    if raw is None:
        return default
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return default
    return val if val > 0 else default


# Daily-cadence datasets (companies, people) — short TTL. Monthly-cadence (awards, sam_master_*)
# — longer TTL. All env-overridable so ops can tune per deployment without a code change.
COMPANY_TTL_S = _ttl_from_env("GTM_LOOKUP_CACHE_COMPANY_TTL_S", 120.0)
PEOPLE_TTL_S = _ttl_from_env("GTM_LOOKUP_CACHE_PEOPLE_TTL_S", 120.0)
AWARDS_TTL_S = _ttl_from_env("GTM_LOOKUP_CACHE_AWARDS_TTL_S", 300.0)
SAM_TTL_S = _ttl_from_env("GTM_LOOKUP_CACHE_SAM_TTL_S", 300.0)


# ── TTLCache primitive (thread-safe, TTL + LRU) ──────────────────────────────────────────────
class TTLCache:
    """A bounded TTL + LRU cache.

    ``.get(key) -> (found, value)``; a present-fresh entry is a HIT (and is touched to MRU),
    a present-EXPIRED entry is a MISS (and is removed), an absent entry is a MISS. ``.set(
    key, value)`` inserts/updates, touches to MRU, and evicts the least-recently-used while
    over ``maxsize``. ``.clear()`` empties it. ``.hits`` / ``.misses`` are running counters;
    ``.size()`` is the live entry count.

    Per-entry expiry is stamped with ``clock()`` at ``set`` time (deadline = ``clock() +
    ttl_s``); ``get`` compares ``clock()`` to that deadline with a strict ``>`` (so the entry
    is live through exactly ``ttl_s``). ``clock`` is injectable for deterministic tests.

    All mutating operations are guarded by a ``threading.RLock`` so concurrent gateway threads
    cannot corrupt the underlying ``OrderedDict`` (no mutation-during-iteration, no torn LRU
    order). The lock guards only the in-memory bookkeeping — callers compute values outside it.
    """

    def __init__(self, ttl_s: float, maxsize: int, clock: Callable[[], float] = time.monotonic):
        self._ttl_s = float(ttl_s)
        self._maxsize = int(maxsize)
        self._clock = clock
        self._store: "OrderedDict[Any, tuple[Any, float]]" = OrderedDict()
        self._lock = threading.RLock()
        self.hits = 0
        self.misses = 0

    def get(self, key: Any) -> tuple[bool, Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self.misses += 1
                return (False, None)
            value, deadline = entry
            if self._clock() > deadline:
                # Expired → evict and count as a miss.
                del self._store[key]
                self.misses += 1
                return (False, None)
            # Live → touch to MRU and count as a hit.
            self._store.move_to_end(key)
            self.hits += 1
            return (True, value)

    def set(self, key: Any, value: Any) -> None:
        with self._lock:
            deadline = self._clock() + self._ttl_s
            self._store[key] = (value, deadline)
            self._store.move_to_end(key)
            while len(self._store) > self._maxsize:
                self._store.popitem(last=False)  # evict least-recently-used

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def size(self) -> int:
        with self._lock:
            return len(self._store)


# ── module-level registry of live caches (for clear_all / ops) ───────────────────────────────
_REGISTRY: list[TTLCache] = []
_REGISTRY_LOCK = threading.Lock()


def _register(cache: TTLCache) -> None:
    with _REGISTRY_LOCK:
        _REGISTRY.append(cache)


def clear_all() -> None:
    """Clear every memoized cache. Test-isolation primitive and a live ops lever (flush all
    cached lookups without a deploy)."""
    with _REGISTRY_LOCK:
        caches = list(_REGISTRY)
    for cache in caches:
        cache.clear()


# ── memoize decorator ────────────────────────────────────────────────────────────────────────
def _default_key(*args: Any, **kwargs: Any) -> Any:
    """Default cache key: the positional args tuple plus sorted keyword items. Hashable for the
    point-lookups (a single normalized str arg), and stable across call shapes."""
    return (args, tuple(sorted(kwargs.items())))


def memoize(
    ttl_s: float,
    maxsize: int = 512,
    key_fn: Callable[..., Any] | None = None,
    clock: Callable[[], float] = time.monotonic,
    enabled: Callable[[], bool] = cache_enabled,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Wrap a read-only lookup with a TTL + LRU result cache.

    The wrapper preserves the wrapped function's ``__name__`` / ``__doc__`` / signature via
    ``functools.wraps`` — so an MCP-registered tool keeps an identical name + schema. On each
    call: if ``enabled()`` is False the call goes straight through (no caching, no key build);
    otherwise the key is ``key_fn(*args, **kwargs)`` (default: the args tuple) and a cache hit
    returns the stored result, a miss computes → stores → returns. ``enabled`` is a CALLABLE
    re-evaluated per call (so the env kill-switch takes effect live, not at decoration time).
    The computation runs OUTSIDE the cache lock (the lock guards only the in-memory store).
    """
    cache = TTLCache(ttl_s=ttl_s, maxsize=maxsize, clock=clock)
    _register(cache)
    keyer = key_fn if key_fn is not None else _default_key

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not enabled():
                return fn(*args, **kwargs)
            key = keyer(*args, **kwargs)
            found, value = cache.get(key)
            if found:
                return value
            result = fn(*args, **kwargs)
            cache.set(key, result)
            return result

        # Expose the underlying cache for diagnostics / targeted flush; not part of the schema.
        wrapper.cache = cache  # type: ignore[attr-defined]
        return wrapper

    return decorator
