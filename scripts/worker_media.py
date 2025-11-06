#!/usr/bin/env python3
import time
import logging

from app.services.queue import dequeue, delete_job, increment_attempts
from app.services.media import fetch_and_store
from app.services.monitoring import record_heartbeat, increment_counter
import os
import socket


log = logging.getLogger("worker.media")
logging.basicConfig(level=logging.INFO)


def main() -> None:
	log.info("worker_media starting pid=%s host=%s", os.getpid(), socket.gethostname())
	# Redis diag at startup
	try:
		from app.services.queue import _get_redis
		r = _get_redis()
		pong = r.ping()
		llen = int(r.llen("jobs:fetch_media"))
		log.info("redis ok=%s url=%s qdepth fetch_media=%s", bool(pong), os.getenv("REDIS_URL"), llen)
	except Exception as e:
		log.warning("redis diag failed: %s", e)
	while True:
		# heartbeat when idle
		try:
			record_heartbeat("media", os.getpid(), socket.gethostname())
		except Exception:
			pass
		log.debug("waiting for jobs: fetch_media")
		job = dequeue("fetch_media", timeout=5)
		if not job:
			time.sleep(0.25)
			continue
		jid = int(job["id"])  # type: ignore
		payload = job.get("payload") or {}
		att_id = int(payload.get("attachment_id") or 0)
		try:
			log.info("dequeued jid=%s kind=%s key=%s", jid, job.get("kind"), job.get("key"))
		except Exception:
			pass
		# Backward compatibility: allow key format message_id:position
		if not att_id:
			key = job.get("key") or ""
			if ":" in key:
				# Find attachment row by message_id and position
				try:
					msg_id_s, pos_s = key.split(":", 1)
					done = _fetch_by_message_and_pos(int(msg_id_s), int(pos_s))
					if done:
						delete_job(jid)
						continue
				except Exception:
					pass
		try:
			if not att_id:
				delete_job(jid)
				continue
			done = fetch_and_store.__wrapped__(att_id) if hasattr(fetch_and_store, "__wrapped__") else None  # type: ignore
			if done is None:
				# async function; run via event loop
				import asyncio
				asyncio.run(fetch_and_store(att_id))
			log.info("media ok jid=%s att=%s", jid, att_id)
			# increment generic media fetch counter; fine-grained per kind will be handled after DB update by separate read if needed
			try:
				increment_counter("media_fetch", 1)
			except Exception:
				pass
			delete_job(jid)
		except Exception as e:
			log.warning("media fail jid=%s att=%s err=%s", jid, att_id, e)
			increment_attempts(jid)
			time.sleep(1)


def _fetch_by_message_and_pos(message_id: int, position: int) -> bool:
	from sqlalchemy import text
	from app.db import get_session
	from app.services.media import fetch_and_store
	with get_session() as session:
		row = session.exec(text("SELECT id FROM attachments WHERE message_id=:m AND position=:p"))\
			.params(m=message_id, p=position).first()
		if not row:
			return False
		att_id = row.id if hasattr(row, "id") else row[0]
		import asyncio
		asyncio.run(fetch_and_store(int(att_id)))
		return True


if __name__ == "__main__":
	main()


