import os
import json
import datetime as dt
from typing import Any, Optional, Tuple
import time
import sqlite3

from redis import Redis
from sqlalchemy import text

from ..db import get_session
from .monitoring import queue_enqueue_time_add, queue_enqueue_time_remove


_redis_client: Optional[Redis] = None


def _get_redis() -> Redis:
	global _redis_client
	if _redis_client is not None:
		return _redis_client
	url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
	# Redis-py supports URL in from_url
	_redis_client = Redis.from_url(url, decode_responses=True)
	return _redis_client


def _ensure_job(kind: str, key: str, payload: Optional[dict] = None, max_attempts: int = 8) -> int:
	"""Insert into jobs table idempotently (UNIQUE(kind,key)) and return job id."""
	with get_session() as session:
		# Try insert-or-ignore, then select id
		session.exec(
			text(
				"""
				INSERT OR IGNORE INTO jobs(kind, key, run_after, attempts, max_attempts, payload)
				VALUES (:kind, :key, CURRENT_TIMESTAMP, 0, :max_attempts, :payload)
				"""
			).params(kind=kind, key=key, max_attempts=max_attempts, payload=json.dumps(payload or {}))
		)
		row = session.exec(
			text("SELECT id FROM jobs WHERE kind=:kind AND key=:key").params(kind=kind, key=key)
		).first()
		if not row:
			# As a fallback, create a new unique key by appending timestamp
			sfx = dt.datetime.utcnow().timestamp()
			session.exec(
				text(
					"""
					INSERT INTO jobs(kind, key, run_after, attempts, max_attempts, payload)
					VALUES (:kind, :key, CURRENT_TIMESTAMP, 0, :max_attempts, :payload)
					"""
				).params(kind=kind, key=f"{key}:{sfx}", max_attempts=max_attempts, payload=json.dumps(payload or {}))
			)
			row = session.exec(text("SELECT last_insert_rowid() as id")).first()
		return int(row.id if hasattr(row, "id") else row[0])  # type: ignore


def enqueue(kind: str, key: str, payload: Optional[dict] = None, max_attempts: int = 8) -> int:
    # Robust against transient SQLite writer locks
    attempts = 0
    backoff = 0.2
    last_err: Exception | None = None
    while attempts < 6:
        try:
            job_id = _ensure_job(kind=kind, key=key, payload=payload, max_attempts=max_attempts)
            msg = json.dumps({"id": job_id, "kind": kind, "key": key})
            _get_redis().lpush(f"jobs:{kind}", msg)
            try:
                queue_enqueue_time_add(kind, job_id)
            except Exception:
                pass
            return job_id
        except Exception as e:
            last_err = e
            msg = str(e).lower()
            is_locked = isinstance(e, sqlite3.OperationalError) or ("database is locked" in msg)
            if not is_locked:
                raise
            attempts += 1
            time.sleep(backoff)
            backoff = min(backoff * 1.7, 2.0)
    # If we exhaust retries, re-raise the last error
    if last_err:
        raise last_err
    raise RuntimeError("enqueue failed: unknown error")


def delete_job(job_id: int) -> None:
	with get_session() as session:
		session.exec(text("DELETE FROM jobs WHERE id = :id").params(id=job_id))


def increment_attempts(job_id: int) -> None:
	with get_session() as session:
		session.exec(text("UPDATE jobs SET attempts = attempts + 1 WHERE id = :id").params(id=job_id))


def mark_failed(job_id: int, error: str) -> None:
	with get_session() as session:
		session.exec(text("UPDATE jobs SET payload = :payload WHERE id = :id").params(id=job_id, payload=json.dumps({"error": error})))


def get_job(job_id: int) -> Optional[dict]:
	with get_session() as session:
		row = session.exec(text("SELECT id, kind, key, run_after, attempts, max_attempts, payload FROM jobs WHERE id=:id").params(id=job_id)).first()
		if not row:
			return None
		# row can be RowMapping or tuple depending on driver; normalize
		def _get(name: str, idx: int) -> Any:
			return getattr(row, name) if hasattr(row, name) else row[idx]
		payload = _get("payload", 6)
		try:
			payload_obj = json.loads(payload) if isinstance(payload, str) and payload else {}
		except Exception:
			payload_obj = {}
		return {
			"id": _get("id", 0),
			"kind": _get("kind", 1),
			"key": _get("key", 2),
			"run_after": _get("run_after", 3),
			"attempts": _get("attempts", 4),
			"max_attempts": _get("max_attempts", 5),
			"payload": payload_obj,
		}


def dequeue(kind: str, timeout: int = 5) -> Optional[dict]:
	"""Blocking pop a job message and hydrate from DB."""
	res: Optional[Tuple[str, str]] = _get_redis().brpop([f"jobs:{kind}"], timeout=timeout)  # type: ignore
	if not res:
		return None
	_, msg = res
	try:
		data = json.loads(msg)
		job_id = int(data.get("id"))
	except Exception:
		return None
	# remove from enqueue-time tracking upon dequeue
	try:
		queue_enqueue_time_remove(kind, job_id)
	except Exception:
		pass
	# Robustly hydrate job; if DB lookup fails for any reason, skip gracefully
	try:
		return get_job(job_id)
	except Exception:
		return None


