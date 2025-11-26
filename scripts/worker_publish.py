#!/usr/bin/env python3
"""
Background worker that scans scheduled Instagram posts and publishes them.
"""

from __future__ import annotations

import logging
import os
import time

from app.services import content_publish

log = logging.getLogger("worker.publish")
logging.basicConfig(level=logging.INFO)

POLL_SECONDS = float(os.getenv("IG_PUBLISHER_POLL_SECONDS", "5"))


def main() -> None:
	log.info("worker_publish starting poll=%ss", POLL_SECONDS)
	while True:
		try:
			due = content_publish.list_due_posts()
		except Exception as exc:
			log.exception("Failed to list due posts: %s", exc)
			time.sleep(POLL_SECONDS)
			continue
		if not due:
			time.sleep(POLL_SECONDS)
			continue
		log.info("Found %s scheduled post(s) ready", len(due))
		for post in due:
			try:
				# optimistic claim to avoid duplicate workers picking same row
				content_publish.update_post_status(post.id, status="publishing")
				content_publish.publish_post(post)
			except Exception as exc:
				log.exception("publish failed post_id=%s err=%s", post.id, exc)
		time.sleep(1)


if __name__ == "__main__":
	main()

