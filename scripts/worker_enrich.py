#!/usr/bin/env python3
import time
import logging
import asyncio

from app.services.queue import dequeue, delete_job, increment_attempts
from app.services.enrichers import enrich_user, enrich_page
from app.services.ai_ig import process_run as ig_ai_process_run
from app.services.monitoring import record_heartbeat, increment_counter, ai_run_log
import os
import socket


log = logging.getLogger("worker.enrich")
logging.basicConfig(level=logging.INFO)


def main() -> None:
	while True:
		# heartbeat when idle too
		try:
			record_heartbeat("enrich", os.getpid(), socket.gethostname())
		except Exception:
			pass
		job = dequeue("enrich_user", timeout=1) or dequeue("enrich_page", timeout=1) or dequeue("ig_ai_process_run", timeout=1)
		if not job:
			time.sleep(0.25)
			continue
		jid = int(job["id"])  # type: ignore
		kind = job.get("kind")
		payload = job.get("payload") or {}
		try:
			if kind == "enrich_user":
				uid = str(payload.get("ig_user_id") or job.get("key"))
				asyncio.run(enrich_user(uid))
				try:
					increment_counter("enrich_user", 1)
					increment_counter("enrich_success", 1)
				except Exception:
					pass
			elif kind == "enrich_page":
				gid = str(payload.get("igba_id") or job.get("key"))
				asyncio.run(enrich_page(gid))
				try:
					increment_counter("enrich_page", 1)
					increment_counter("enrich_success", 1)
				except Exception:
					pass
			elif kind == "ig_ai_process_run":
				payload = payload or {}
				rid = int(payload.get("run_id") or 0) or int(job.get("key") or 0)
				df = payload.get("date_from")
				dtv = payload.get("date_to")
				age = int(payload.get("min_age_minutes") or 60)
				lim = int(payload.get("limit") or 200)
				try:
					log.info("ig_ai start rid=%s date_from=%s date_to=%s min_age=%s limit=%s", rid, df, dtv, age, lim)
				except Exception:
					pass
				ai_run_log(rid, "info", "worker_start", {"date_from": df, "date_to": dtv, "min_age_minutes": age, "limit": lim})
				dfp = None
				dtp = None
				try:
					if df:
						import datetime as _dt
						dfp = _dt.date.fromisoformat(str(df))
				except Exception:
					dfp = None
				try:
					if dtv:
						import datetime as _dt
						dtp = _dt.date.fromisoformat(str(dtv))
				except Exception:
					dtp = None
				res = ig_ai_process_run(run_id=rid, date_from=dfp, date_to=dtp, min_age_minutes=age, limit=lim)
				try:
					log.info(
						"ig_ai done rid=%s considered=%s processed=%s linked=%s purchases=%s unlinked=%s errors=%s",
						rid,
						int(res.get("considered", 0)),
						int(res.get("processed", 0)),
						int(res.get("linked", 0)),
						int(res.get("purchases", 0)),
						int(res.get("purchases_unlinked", 0)),
						len(res.get("errors", []) if isinstance(res.get("errors"), list) else []),
					)
				except Exception:
					pass
				ai_run_log(rid, "info", "worker_done", {
					"considered": int(res.get("considered", 0)),
					"processed": int(res.get("processed", 0)),
					"linked": int(res.get("linked", 0)),
					"purchases": int(res.get("purchases", 0)),
					"unlinked": int(res.get("purchases_unlinked", 0)),
					"errors": len(res.get("errors", []) if isinstance(res.get("errors"), list) else []),
				})
				try:
					increment_counter("ig_ai_process_run", 1)
				except Exception:
					pass
			else:
				delete_job(jid); continue
			log.info("enrich ok jid=%s kind=%s", jid, kind)
			delete_job(jid)
		except Exception as e:
			log.warning("enrich fail jid=%s kind=%s err=%s", jid, kind, e)
			increment_attempts(jid)
			time.sleep(1)


if __name__ == "__main__":
	main()


