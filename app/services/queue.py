import os
import json
import datetime as dt
from typing import Any, Optional, Tuple
import time

from redis import Redis
from redis.exceptions import TimeoutError as RedisTimeoutError, ConnectionError as RedisConnectionError
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from ..db import get_session
from .monitoring import queue_enqueue_time_add, queue_enqueue_time_remove


_redis_client: Optional[Redis] = None


def _get_redis() -> Redis:
	global _redis_client
	if _redis_client is not None:
		return _redis_client
	url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
	# Redis-py supports URL in from_url; allow blocking ops (BRPOP) without premature read timeout
	sto_s = os.getenv("REDIS_SOCKET_TIMEOUT")
	socket_timeout = float(sto_s) if (sto_s is not None and sto_s != "") else None
	socket_connect_timeout = float(os.getenv("REDIS_CONNECT_TIMEOUT", "0.5"))
	_redis_client = Redis.from_url(
		url,
		decode_responses=True,
		socket_timeout=socket_timeout,  # None -> no read timeout; BRPOP can block up to its own timeout
		socket_connect_timeout=socket_connect_timeout,
		retry_on_timeout=True,
	)
	return _redis_client


def _ensure_job(kind: str, key: str, payload: Optional[dict] = None, max_attempts: int = 8) -> int:
	"""Insert into jobs table idempotently (UNIQUE(kind,key)) and return job id."""
	with get_session() as session:
		stmt = text(
			"""
			INSERT INTO `jobs`(`kind`, `key`, `run_after`, `attempts`, `max_attempts`, `payload`)
			VALUES (:kind, :key, CURRENT_TIMESTAMP, 0, :max_attempts, :payload)
			ON DUPLICATE KEY UPDATE id = id
			"""
		)
		# Execute insert, with MySQL-specific self-heal for missing AUTO_INCREMENT on id
		try:
			session.exec(stmt.params(kind=kind, key=key, max_attempts=max_attempts, payload=json.dumps(payload or {})))
		except OperationalError as e:
			msg = str(e).lower()
			needs_auto_inc = (
				"doesn't have a default value" in msg or "does not have a default value" in msg
			)
			if needs_auto_inc:
				# Best-effort MySQL fixups to ensure jobs.id is AUTO_INCREMENT primary key
				for sql in (
					"ALTER TABLE `jobs` MODIFY COLUMN `id` INT NOT NULL",
					"ALTER TABLE `jobs` DROP PRIMARY KEY",
					"ALTER TABLE `jobs` CHANGE `id` `id` INT NOT NULL AUTO_INCREMENT",
					"ALTER TABLE `jobs` ADD PRIMARY KEY (`id`)",
				):
					try:
						session.exec(text(sql))
					except Exception:
						pass
				# Retry the insert once after attempting to fix schema
				session.exec(stmt.params(kind=kind, key=key, max_attempts=max_attempts, payload=json.dumps(payload or {})))
			else:
				raise
		# Lookup inserted row by unique key
		sel = text("SELECT `id` FROM `jobs` WHERE `kind`=:kind AND `key`=:key")
		row = session.exec(sel.params(kind=kind, key=key)).first()
		if not row:
			# As a fallback, create a new unique key by appending timestamp
			sfx = dt.datetime.utcnow().timestamp()
			ins2 = text(
				"""
				INSERT INTO `jobs`(`kind`, `key`, `run_after`, `attempts`, `max_attempts`, `payload`)
				VALUES (:kind, :key, CURRENT_TIMESTAMP, 0, :max_attempts, :payload)
				"""
			)
			session.exec(ins2.params(kind=kind, key=f"{key}:{sfx}", max_attempts=max_attempts, payload=json.dumps(payload or {})))
			# Try get id in a backend-agnostic way
			row = session.exec(text("SELECT `id` FROM `jobs` WHERE `kind`=:kind AND `key`=:key").params(kind=kind, key=f"{key}:{sfx}")).first()
		# Normalize id
		return int(row.id if hasattr(row, "id") else row[0])  # type: ignore


def enqueue(kind: str, key: str, payload: Optional[dict] = None, max_attempts: int = 8) -> int:
	# Robust against transient MySQL lock waits / deadlocks
	attempts = 0
	backoff = 0.2
	last_err: Exception | None = None
	while attempts < 6:
		try:
			job_id = _ensure_job(kind=kind, key=key, payload=payload, max_attempts=max_attempts)
			msg = json.dumps({"id": job_id, "kind": kind, "key": key})
			try:
				_get_redis().lpush(f"jobs:{kind}", msg)
			except (RedisTimeoutError, RedisConnectionError) as re:
				raise RuntimeError(f"queue unavailable: {re}")
			try:
				queue_enqueue_time_add(kind, job_id)
			except Exception:
				pass
			return job_id
		except Exception as e:
			last_err = e
			msg = str(e).lower()
			is_locked = ("lock wait timeout" in msg) or ("deadlock" in msg) or ("try restarting transaction" in msg)
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
		# MySQL reserves the word `key`; quote identifiers accordingly
		try:
			dialect = str(session.get_bind().dialect.name)
		except Exception:
			dialect = ""
		if dialect == "mysql":
			qry = "SELECT `id`, `kind`, `key`, `run_after`, `attempts`, `max_attempts`, `payload` FROM `jobs` WHERE `id`=:id"
		else:
			qry = "SELECT id, kind, key, run_after, attempts, max_attempts, payload FROM jobs WHERE id=:id"
		row = session.exec(text(qry).params(id=job_id)).first()
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
	try:
		res: Optional[Tuple[str, str]] = _get_redis().brpop([f"jobs:{kind}"], timeout=timeout)  # type: ignore
	except (RedisTimeoutError, RedisConnectionError):
		return None
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


