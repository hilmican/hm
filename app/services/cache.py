import os
import json
from typing import Any, Callable, Optional, TypeVar, Dict

# Reuse existing Redis client helper
from .monitoring import _get_redis

T = TypeVar("T")


def _cache_enabled() -> bool:
    return os.getenv("CACHE_ENABLED", "1") != "0"


def _key_with_namespace(key: str) -> str:
    try:
        r = _get_redis()
        ns = r.get("cache:ns") or "1"
    except Exception:
        ns = "1"
    return f"c{ns}:{key}"


def bump_namespace() -> Optional[int]:
    try:
        r = _get_redis()
        return int(r.incr("cache:ns"))
    except Exception:
        return None


def get_json(key: str) -> Optional[Any]:
    try:
        r = _get_redis()
        val = r.get(_key_with_namespace(key))
        if val is None:
            return None
        return json.loads(val)
    except Exception:
        return None


def set_json(key: str, value: Any, ttl_seconds: int = 60) -> None:
    try:
        r = _get_redis()
        r.setex(_key_with_namespace(key), ttl_seconds, json.dumps(value))
    except Exception:
        pass


def cached_json(key: str, ttl_seconds: int, compute_fn: Callable[[], T]) -> T:
    if _cache_enabled():
        cached = get_json(key)
        if cached is not None:
            return cached  # type: ignore
    value = compute_fn()
    if _cache_enabled():
        try:
            set_json(key, value, ttl_seconds)
        except Exception:
            pass
    return value


