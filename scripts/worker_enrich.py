#!/usr/bin/env python3
import time
import logging
import asyncio

from app.services.queue import dequeue, delete_job, increment_attempts
from app.services.enrichers import enrich_user, enrich_page


log = logging.getLogger("worker.enrich")
logging.basicConfig(level=logging.INFO)


def main() -> None:
	while True:
		job = dequeue("enrich_user", timeout=1) or dequeue("enrich_page", timeout=1)
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
			elif kind == "enrich_page":
				gid = str(payload.get("igba_id") or job.get("key"))
				asyncio.run(enrich_page(gid))
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


