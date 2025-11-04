import os
import json
import socket
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

from redis import Redis


_redis_client: Optional[Redis] = None


def _get_redis() -> Redis:
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    # Fail fast if Redis is unreachable to avoid UI stalls
    socket_timeout = float(os.getenv("REDIS_SOCKET_TIMEOUT", "0.25"))
    connect_timeout = float(os.getenv("REDIS_CONNECT_TIMEOUT", "0.25"))
    _redis_client = Redis.from_url(
        url,
        decode_responses=True,
        socket_timeout=socket_timeout,
        socket_connect_timeout=connect_timeout,
    )
    return _redis_client


def _now_ts() -> float:
    return dt.datetime.utcnow().timestamp()


def _version_str() -> Optional[str]:
    for key in ("APP_VERSION", "HM_VERSION", "RELEASE_VERSION"):
        val = os.getenv(key)
        if val:
            return str(val)
    return None


def record_heartbeat(kind: str, pid: int, host: Optional[str] = None, extra: Optional[Dict[str, Any]] = None, ttl_seconds: int = 15) -> None:
    r = _get_redis()
    host_val = host or socket.gethostname()
    key = f"hb:worker:{kind}:{host_val}:{pid}"
    payload = {
        "pid": pid,
        "host": host_val,
        "kind": kind,
        "ts": _now_ts(),
        "version": _version_str(),
    }
    if extra:
        payload.update(extra)
    r.set(key, json.dumps(payload), ex=ttl_seconds)


def get_worker_statuses() -> List[Dict[str, Any]]:
    r = _get_redis()
    out: List[Dict[str, Any]] = []
    now = _now_ts()
    for key in r.scan_iter("hb:worker:*"):
        try:
            data_raw = r.get(key)
            if not data_raw:
                continue
            data = json.loads(data_raw)
            last_ts = float(data.get("ts") or 0.0)
            data["last_seen_sec"] = max(0, int(now - last_ts)) if last_ts else None
            data["key"] = key
            out.append(data)
        except Exception:
            continue
    # Sort by kind, then host, then pid
    out.sort(key=lambda d: (str(d.get("kind") or ""), str(d.get("host") or ""), int(d.get("pid") or 0)))
    return out


def increment_counter(name: str, delta: int = 1) -> None:
    r = _get_redis()
    now = dt.datetime.utcnow().replace(second=0, microsecond=0)
    bucket = now.strftime("%Y%m%d%H%M")
    key = f"metrics:count:{name}:{bucket}"
    with r.pipeline() as p:
        p.incrby(key, int(delta))
        p.expire(key, 7 * 24 * 60 * 60)
        p.execute()


def sum_counters(name: str, minutes: int) -> int:
    r = _get_redis()
    now = dt.datetime.utcnow().replace(second=0, microsecond=0)
    keys: List[str] = []
    for i in range(minutes):
        ts = now - dt.timedelta(minutes=i)
        bucket = ts.strftime("%Y%m%d%H%M")
        keys.append(f"metrics:count:{name}:{bucket}")
    total = 0
    if not keys:
        return 0
    vals = r.mget(keys)
    for val in vals:
        try:
            total += int(val or 0)
        except Exception:
            continue
    return total


def queue_enqueue_time_add(kind: str, job_id: int, ts: Optional[float] = None) -> None:
    r = _get_redis()
    score = float(ts or _now_ts())
    r.zadd(f"qtime:{kind}", {str(job_id): score})
    # track seen kinds for discovery
    r.sadd("qkinds", kind)


def queue_enqueue_time_remove(kind: str, job_id: int) -> None:
    r = _get_redis()
    r.zrem(f"qtime:{kind}", str(job_id))


def discover_queue_kinds() -> List[str]:
    r = _get_redis()
    kinds = set(["ingest", "enrich_user", "enrich_page", "fetch_media"])  # known defaults
    try:
        for k in r.smembers("qkinds") or []:
            kinds.add(str(k))
    except Exception:
        pass
    return sorted(kinds)


def get_queue_stats(kinds: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    r = _get_redis()
    ks = kinds or discover_queue_kinds()
    out: List[Dict[str, Any]] = []
    now = _now_ts()
    for kind in ks:
        try:
            depth = int(r.llen(f"jobs:{kind}"))
        except Exception:
            depth = 0
        oldest_age_seconds: Optional[int] = None
        try:
            z = r.zrange(f"qtime:{kind}", 0, 0, withscores=True)
            if z:
                _, score = z[0]
                oldest_age_seconds = max(0, int(now - float(score)))
        except Exception:
            oldest_age_seconds = None
        out.append({"kind": kind, "depth": depth, "oldest_age_seconds": oldest_age_seconds})
    return out


