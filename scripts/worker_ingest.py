#!/usr/bin/env python3
import time
import logging
from typing import Optional

from app.services.queue import dequeue, delete_job, increment_attempts
from app.services.ingest import handle as handle_ingest
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
		job = dequeue("ingest", timeout=5)
		if not job:
			time.sleep(0.25)
			continue
		jid = int(job["id"])  # type: ignore
		payload = job.get("payload") or {}
		raw_id = int(payload.get("raw_event_id") or 0)
		try:
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
		except Exception as e:
			log.warning("ingest fail jid=%s raw=%s err=%s", jid, raw_id, e)
			increment_attempts(jid)
			time.sleep(1)


if __name__ == "__main__":
	main()


