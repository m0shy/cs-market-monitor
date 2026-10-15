"""Bounded TTL cache for read-only Web aggregates.

Concurrent requests for the same missing key share one build. While a key is
being refreshed, existing callers can continue to read its previous value.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from typing import Any, Callable


MAX_ENTRIES = 768
_CACHE: OrderedDict[str, tuple[float, Any]] = OrderedDict()
_CACHE_INFLIGHT: dict[str, threading.Event] = {}
_CACHE_LOCK = threading.Lock()


def _prune_cache(now: float) -> None:
    expired = [key for key, entry in _CACHE.items() if entry[0] <= now]
    for key in expired:
        _CACHE.pop(key, None)
    while len(_CACHE) > MAX_ENTRIES:
        _CACHE.popitem(last=False)


def cached(key: str, ttl: float, fn: Callable[[], Any], *, refresh: bool = False) -> Any:
    now = time.monotonic()
    stale = None
    has_stale = False
    with _CACHE_LOCK:
        hit = _CACHE.get(key)
        if hit is not None:
            stale = hit[1]
            has_stale = True
        if hit is not None and hit[0] > now and not refresh:
            _CACHE.move_to_end(key)
            return hit[1]
        pending = _CACHE_INFLIGHT.get(key)
        if pending is None:
            pending = threading.Event()
            _CACHE_INFLIGHT[key] = pending
            owner = True
        else:
            owner = False

    if not owner:
        if has_stale:
            return stale
        pending.wait()
        with _CACHE_LOCK:
            hit = _CACHE.get(key)
            if hit is not None:
                _CACHE.move_to_end(key)
                return hit[1]
        return cached(key, ttl, fn, refresh=refresh)

    try:
        value = fn()
    except Exception:
        with _CACHE_LOCK:
            _CACHE_INFLIGHT.pop(key, None)
            pending.set()
        if has_stale:
            return stale
        raise

    with _CACHE_LOCK:
        _CACHE[key] = (time.monotonic() + ttl, value)
        _CACHE.move_to_end(key)
        _prune_cache(time.monotonic())
        _CACHE_INFLIGHT.pop(key, None)
        pending.set()
    return value
