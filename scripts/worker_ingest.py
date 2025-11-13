#!/usr/bin/env python3
import time
import logging
from typing import Optional

from app.services.queue import dequeue, delete_job, increment_attempts
from app.services.ingest import handle as handle_ingest
from app.services.instagram_api import fetch_thread_messages
from app.services.ingest import upsert_message_from_ig_event
from app.services.monitoring import increment_counter
from app.db import get_session
from sqlalchemy import text
from app.services.monitoring import record_heartbeat, increment_counter
import os
import socket


log = logging.getLogger("worker.ingest")
logging.basicConfig(level=logging.INFO)


def main() -> None:
	log.info("worker_ingest starting pid=%s host=%s", os.getpid(), socket.gethostname())
	# Redis diag at startup
	try:
		from app.services.queue import _get_redis
		r = _get_redis()
		pong = r.ping()
		llen_ing = int(r.llen("jobs:ingest"))
		llen_hyd = int(r.llen("jobs:hydrate_conversation"))
		log.info("redis ok=%s url=%s qdepth ingest=%s hydrate=%s", bool(pong), os.getenv("REDIS_URL"), llen_ing, llen_hyd)
	except Exception as e:
		log.warning("redis diag failed: %s", e)
	while True:
		# heartbeat even when idle
		try:
			record_heartbeat("ingest", os.getpid(), socket.gethostname())
		except Exception:
			pass
		# Prefer ingest jobs; if none, try hydrate jobs (conversation/ad)
		log.debug("waiting for jobs: ingest|hydrate_conversation|hydrate_ad")
		job = dequeue("ingest", timeout=1) or dequeue("hydrate_conversation", timeout=1) or dequeue("hydrate_ad", timeout=1)
		if not job:
			time.sleep(0.25)
			continue
		jid = int(job["id"])  # type: ignore
		payload = job.get("payload") or {}
		kind = job.get("kind")
		try:
			log.info("dequeued jid=%s kind=%s key=%s", jid, kind, job.get("key"))
		except Exception:
			pass
		raw_id = int(payload.get("raw_event_id") or 0)
		try:
			if kind == "ingest":
				if not raw_id:
					delete_job(jid)
					continue
				inserted = handle_ingest(raw_id)
				log.info("ingest ok jid=%s raw=%s inserted=%s", jid, raw_id, inserted)
				# counters: messages ingested
				try:
					if inserted and int(inserted) > 0:
						increment_counter("messages", int(inserted))
				except Exception:
					pass
				delete_job(jid)
			elif kind == "hydrate_conversation":
				igba_id = str(payload.get("igba_id") or "")
				ig_user_id = str(payload.get("ig_user_id") or "")
				limit = int(payload.get("max_messages") or 200)
				if not (igba_id and ig_user_id):
					delete_job(jid)
					continue
				# fetch thread messages and upsert
				msgs = []
				try:
					msgs = __import__("asyncio").get_event_loop().run_until_complete(fetch_thread_messages(igba_id, ig_user_id, limit))
				except Exception as e:
					log.warning("hydrate fetch fail jid=%s convo=%s:%s err=%s", jid, igba_id, ig_user_id, e)
					increment_attempts(jid)
					time.sleep(1)
					continue
				inserted = 0
				with get_session() as session:
					for ev in msgs:
						try:
							mid = upsert_message_from_ig_event(session, ev, igba_id)
							if mid:
								inserted += 1
						except Exception as e:
							log.warning("hydrate upsert err convo=%s:%s ev=%s err=%s", igba_id, ig_user_id, ev.get("id"), e)
					# mark conversation hydrated (ai_conversations)
					try:
						cid_ai = f"dm:{ig_user_id}"
						# ensure ai_conversations row exists
						try:
							session.exec(text("INSERT OR IGNORE INTO ai_conversations(convo_id) VALUES (:cid)")).params(cid=cid_ai)
						except Exception:
							try:
								session.exec(text("INSERT IGNORE INTO ai_conversations(convo_id) VALUES (:cid)")).params(cid=cid_ai)
							except Exception:
								pass
						session.exec(text("UPDATE ai_conversations SET hydrated_at=CURRENT_TIMESTAMP WHERE convo_id=:cid")).params(cid=cid_ai)
					except Exception:
						pass
				log.info("hydrate ok jid=%s convo=%s:%s msgs=%s", jid, igba_id, ig_user_id, inserted)
				try:
					increment_counter("hydrate_conversation", int(inserted))
				except Exception:
					pass
				delete_job(jid)
			elif kind == "hydrate_ad":
				ad_id = str(payload.get("ad_id") or "")
				if not ad_id:
					delete_job(jid)
					continue
				with get_session() as session:
					# Placeholder: if name is empty, set to ad_id; in real flow, populate from your catalog/cache
					try:
						row = session.exec(text("SELECT name FROM ads WHERE ad_id=:id")).params(id=ad_id).first()
						name = (row.name if hasattr(row, "name") else (row[0] if row else None)) if row else None
						if not (name and str(name).strip()):
							session.exec(text("UPDATE ads SET name=:n, updated_at=CURRENT_TIMESTAMP WHERE ad_id=:id")).params(id=ad_id, n=ad_id)
					except Exception:
						pass
				delete_job(jid)
			else:
				delete_job(jid)
		except Exception as e:
			log.warning("ingest fail jid=%s raw=%s err=%s", jid, raw_id, e)
			increment_attempts(jid)
			time.sleep(1)


if __name__ == "__main__":
	main()


