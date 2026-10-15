# -*- coding: utf-8 -*-
"""web.app 的模块级共享状态：路径常量与进程内 TTL 缓存（纯搬运自 app.py）."""
from __future__ import annotations

import threading
import time as _time
from collections import OrderedDict
from copy import copy
from functools import wraps
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parent
DATA_ROOT = PROJECT.parent / "csgo-monitor-data" / "crawler"
EXPORTS_DIR = DATA_ROOT / "exports"

_TTL_CACHE: OrderedDict[tuple, tuple[float, Any, tuple | None]] = OrderedDict()
_TTL_CACHE_LOCK = threading.Lock()
_MAX_TTL_ENTRIES = 256


def _stable_token(value: Any) -> Any:
    """把端点参数折叠成稳定的缓存键成分.

    Starlette/FastAPI 的 Request/Response 等对象按请求唯一：直接进键会让
    缓存永不命中，且每个条目都钉住一份请求对象——长期运行的服务会无界
    泄漏。这类值折叠为类型标记；不可哈希参数退化为 repr 保证键仍可计算。
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    module = type(value).__module__.split(".", 1)[0]
    if module in ("starlette", "fastapi"):
        return f"<{type(value).__name__}>"
    try:
        hash(value)
    except TypeError:
        return repr(value)
    return value


def _cache_response_headers(payload: Any) -> tuple | None:
    """缓存 Response 时冻结一份原始响应头.

    Starlette 的 Response.__call__ 传出的是 raw_headers 列表引用，GZip 等
    中间件会就地改写（追加 Content-Encoding/改 Content-Length）。直接复用
    被缓存的 Response 会让第二个请求带着 gzip 头发出未压缩正文。
    """
    raw = getattr(payload, "raw_headers", None)
    return tuple(raw) if raw is not None else None


def _revive_response(payload: Any, pristine: tuple | None) -> Any:
    """命中缓存返回 Response 时给浅拷贝装回干净的头列表."""
    if pristine is None:
        return payload
    revived = copy(payload)
    revived.raw_headers = list(pristine)
    return revived


def _ttl_cached(ttl_seconds: float):
    """重查询端点的进程内 TTL 缓存（supply-trend 实测 10s/次，
    仪表盘每分钟轮询会持续压库——缓存后 4/5 请求零成本）。"""
    def deco(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            tokens = (
                tuple(_stable_token(arg) for arg in args),
                tuple(sorted(
                    (name, _stable_token(value))
                    for name, value in kwargs.items()
                )),
            )
            key = (fn.__name__, tokens)
            now = _time.monotonic()
            with _TTL_CACHE_LOCK:
                hit = _TTL_CACHE.get(key)
                if hit is not None and hit[0] > now:
                    return _revive_response(hit[1], hit[2])
            payload = fn(*args, **kwargs)
            with _TTL_CACHE_LOCK:
                _TTL_CACHE[key] = (
                    now + ttl_seconds, payload, _cache_response_headers(payload),
                )
                expired = [k for k, entry in _TTL_CACHE.items() if entry[0] <= now]
                for k in expired:
                    del _TTL_CACHE[k]
                while len(_TTL_CACHE) > _MAX_TTL_ENTRIES:
                    _TTL_CACHE.popitem(last=False)
            return payload
        return wrapper
    return deco
