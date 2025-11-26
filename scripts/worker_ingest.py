#!/usr/bin/env python3
import time
import logging
import asyncio

from app.services.queue import dequeue, delete_job, increment_attempts
from app.services.ingest import handle as handle_ingest
from app.services.instagram_api import fetch_thread_messages, fetch_message_details, fetch_messages
from app.services.ingest import upsert_message_from_ig_event
from app.services.monitoring import increment_counter
from app.db import get_session
from sqlalchemy import text
from app.services.monitoring import record_heartbeat, increment_counter
import os
import socket
from pymysql.err import OperationalError


log = logging.getLogger("worker.ingest")
logging.basicConfig(level=logging.INFO)


def _get_or_create_loop() -> asyncio.AbstractEventLoop:
	try:
		loop = asyncio.get_event_loop()
		if loop.is_closed():
			raise RuntimeError("event loop is closed")
	except RuntimeError:
		loop = asyncio.new_event_loop()
		asyncio.set_event_loop(loop)
	return loop


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
		# Prefer ingest jobs; if none, try hydrate jobs (conversation/ad/by_cid)
		log.debug("waiting for jobs: ingest|hydrate_conversation|hydrate_by_conversation_id|hydrate_ad")
		job = (
			dequeue("ingest", timeout=1)
			or dequeue("hydrate_conversation", timeout=1)
			or dequeue("hydrate_by_conversation_id", timeout=1)
			or dequeue("hydrate_ad", timeout=1)
		)
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
				graph_conversation_id = str(payload.get("graph_conversation_id") or "") or None
				limit = int(payload.get("max_messages") or 200)
				if not (igba_id and ig_user_id):
					delete_job(jid)
					continue
				# fetch thread messages and upsert
				# Use graph_conversation_id if provided (saves API calls - only 1 request instead of discovery)
				msgs = []
				try:
					loop = _get_or_create_loop()
					msgs = loop.run_until_complete(
						fetch_thread_messages(igba_id, ig_user_id, limit=limit, graph_conversation_id=graph_conversation_id)
					)
				except Exception as e:
					log.warning("hydrate fetch fail jid=%s convo=%s:%s graph_cid=%s err=%s", jid, igba_id, ig_user_id, graph_conversation_id[:50] if graph_conversation_id else "none", e)
					increment_attempts(jid)
					time.sleep(1)
					continue
				# Step log: raw fetch summary
				try:
					raw_count = (len(msgs) if isinstance(msgs, list) else None)
					raw_types = list({type(m).__name__ for m in (msgs or [])}) if isinstance(msgs, list) else []
					first_sample = None
					if isinstance(msgs, list) and msgs:
						s0 = msgs[0]
						first_sample = (s0 if isinstance(s0, dict) else str(s0))[:160]
					log.info("hydrate: raw fetched count=%s types=%s first_sample=%s", raw_count, raw_types, first_sample)
				except Exception:
					pass
				# If Graph sometimes returns only message IDs (strings), fetch details
				try:
					if isinstance(msgs, list) and any(not isinstance(m, dict) for m in msgs):
						cnt = len([m for m in msgs if not isinstance(m, dict)])
						log.info("hydrate: normalizing %s non-dict messages via detail fetch", cnt)
						loop = _get_or_create_loop()
						norm_msgs = []
						for m in msgs:
							if isinstance(m, dict):
								norm_msgs.append(m)
							else:
								try:
									det = loop.run_until_complete(fetch_message_details(str(m)))
									if isinstance(det, dict):
										norm_msgs.append(det)
									else:
										log.warning("hydrate: detail not dict mid=%s got=%s", str(m), type(det).__name__)
								except Exception as de:
									log.warning("hydrate: detail fetch failed mid=%s err=%s", str(m), de)
						msgs = norm_msgs
						try:
							first_keys = list(msgs[0].keys())[:8] if msgs and isinstance(msgs[0], dict) else []
							log.info("hydrate: normalized messages count=%s first_keys=%s", len(msgs), first_keys)
						except Exception:
							pass
				except Exception:
					pass
				inserted = 0
				with get_session() as session:
					for ev in msgs:
						try:
							mid = upsert_message_from_ig_event(session, ev, igba_id)
							if mid:
								inserted += 1
						except Exception as e:
							# Safe logging when ev may be a string
							try:
								ev_id = ev.get("id") if hasattr(ev, "get") else (ev if isinstance(ev, str) else None)
							except Exception:
								ev_id = None
							log.warning("hydrate upsert err convo=%s:%s ev=%s ev_type=%s err=%s", igba_id, ig_user_id, ev_id, type(ev).__name__, e)
					# mark conversation hydrated (update conversations table)
					try:
						from sqlalchemy import text as _t
						# Use provided graph_conversation_id or extract from messages
						cid_to_use = graph_conversation_id
						if not cid_to_use and isinstance(msgs, list):
							for _m in msgs:
								if isinstance(_m, dict) and _m.get("__graph_conversation_id"):
									cid_to_use = str(_m.get("__graph_conversation_id"))
									break
						
						# Update conversations table with hydrated_at timestamp
						if cid_to_use:
							# Find conversation by graph_conversation_id or (igba_id, ig_user_id)
							session.exec(
								_t(
									"""
									UPDATE conversations
									SET hydrated_at=CURRENT_TIMESTAMP, graph_conversation_id=COALESCE(graph_conversation_id, :gc)
									WHERE (graph_conversation_id=:gc OR (igba_id=:g AND ig_user_id=:u))
									LIMIT 1
									"""
								).params(gc=str(cid_to_use), g=str(igba_id), u=str(ig_user_id))
							)
						else:
							# Fallback: update by (igba_id, ig_user_id) if no graph_cid available
							session.exec(
								_t(
									"UPDATE conversations SET hydrated_at=CURRENT_TIMESTAMP WHERE igba_id=:g AND ig_user_id=:u LIMIT 1"
								).params(g=str(igba_id), u=str(ig_user_id))
							)
					except Exception as e:
						log.debug("hydrate: failed to update hydrated_at: %s", str(e)[:200])
				log.info("hydrate ok jid=%s convo=%s:%s msgs=%s", jid, igba_id, ig_user_id, inserted)
				try:
					increment_counter("hydrate_conversation", int(inserted))
				except Exception:
					pass
				delete_job(jid)
			elif kind == "hydrate_by_conversation_id":
				conversation_id = str(payload.get("conversation_id") or "")
				limit = int(payload.get("max_messages") or 200)
				if not conversation_id:
					delete_job(jid)
					continue
				msgs = []
				try:
					loop = _get_or_create_loop()
					msgs = loop.run_until_complete(fetch_messages(conversation_id, limit=limit))
				except Exception as e:
					log.warning("hydrate by cid fetch fail jid=%s cid=%s err=%s", jid, conversation_id, e)
					increment_attempts(jid)
					time.sleep(1)
					continue
				try:
					raw_count = (len(msgs) if isinstance(msgs, list) else None)
					first_keys = list(msgs[0].keys())[:8] if msgs and isinstance(msgs[0], dict) else []
					log.info("hydrate by cid: fetched count=%s first_keys=%s", raw_count, first_keys)
				except Exception:
					pass
				inserted = 0
				with get_session() as session:
					for ev in (msgs or []):
						try:
							if isinstance(ev, dict):
								ev["__graph_conversation_id"] = str(conversation_id)
							mid = upsert_message_from_ig_event(session, ev, "")
							if mid:
								inserted += 1
						except Exception as e:
							try:
								ev_id = ev.get("id") if hasattr(ev, "get") else (ev if isinstance(ev, str) else None)
							except Exception:
								ev_id = None
							log.warning("hydrate by cid upsert err cid=%s ev=%s ev_type=%s err=%s", conversation_id, ev_id, type(ev).__name__, e)
					try:
						from sqlalchemy import text as _t
						session.exec(_t("INSERT OR IGNORE INTO ai_conversations(convo_id) VALUES (:cid)")).params(cid=str(conversation_id))
					except Exception:
						try:
							from sqlalchemy import text as _t
							session.exec(_t("INSERT IGNORE INTO ai_conversations(convo_id) VALUES (:cid)")).params(cid=str(conversation_id))
						except Exception:
							pass
					try:
						from sqlalchemy import text as _t
						session.exec(_t("UPDATE ai_conversations SET hydrated_at=CURRENT_TIMESTAMP WHERE convo_id=:cid")).params(cid=str(conversation_id))
					except Exception:
						pass
				log.info("hydrate by cid ok jid=%s cid=%s msgs=%s", jid, conversation_id, inserted)
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
			# Special-case MySQL lock wait timeout (1205) as a transient error and retry the job.
			code = None
			try:
				if isinstance(e, OperationalError) and len(e.args) > 0:
					# e.args[0] is the MySQL error code for pymysql OperationalError
					code = e.args[0]
			except Exception:
				code = None

			if code == 1205:
				log.warning("ingest lock wait timeout jid=%s raw=%s err=%s (will retry)", jid, raw_id, e)
				increment_attempts(jid)
				# Small backoff to reduce contention; keep job in queue for retry
				time.sleep(1.5)
				continue

			log.warning("ingest fail jid=%s raw=%s err=%s", jid, raw_id, e)
			increment_attempts(jid)
			time.sleep(1)


if __name__ == "__main__":
	main()


