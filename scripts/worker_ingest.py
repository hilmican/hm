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
	while True:
		# heartbeat even when idle
		try:
			record_heartbeat("ingest", os.getpid(), socket.gethostname())
		except Exception:
			pass
		# Prefer ingest jobs; if none, try hydrate jobs
		job = dequeue("ingest", timeout=1) or dequeue("hydrate_conversation", timeout=1)
		if not job:
			time.sleep(0.25)
			continue
		jid = int(job["id"])  # type: ignore
		payload = job.get("payload") or {}
		kind = job.get("kind")
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
					# mark conversation hydrated
					try:
						session.exec(text("UPDATE conversations SET hydrated_at=CURRENT_TIMESTAMP WHERE convo_id=:cid OR (igba_id=:g AND ig_user_id=:u)"))\
							.params(cid=f"{igba_id}:{ig_user_id}", g=igba_id, u=ig_user_id)
					except Exception:
						pass
				log.info("hydrate ok jid=%s convo=%s:%s msgs=%s", jid, igba_id, ig_user_id, inserted)
				try:
					increment_counter("hydrate_conversation", int(inserted))
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


