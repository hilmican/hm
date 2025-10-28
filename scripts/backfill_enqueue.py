#!/usr/bin/env python3
import os
from typing import Tuple

from sqlalchemy import text

from app.db import get_session
from app.services.queue import enqueue


def enqueue_from_recent_conversations(limit: int = 50) -> Tuple[int, int, int]:
	"""Enqueue enrich_page, enrich_user, hydrate_conversation for recent conversations."""
	enrich_page_q = 0
	enrich_user_q = 0
	hydrate_q = 0
	with get_session() as session:
		rows = session.exec(
			text("SELECT igba_id, ig_user_id FROM conversations ORDER BY last_message_at DESC LIMIT :n").params(n=int(limit))
		).all()
		for r in rows:
			igba_id = r.igba_id if hasattr(r, "igba_id") else r[0]
			ig_user_id = r.ig_user_id if hasattr(r, "ig_user_id") else r[1]
			try:
				enqueue("enrich_page", key=str(igba_id), payload={"igba_id": str(igba_id)})
				enrich_page_q += 1
			except Exception:
				pass
			try:
				enqueue("enrich_user", key=str(ig_user_id), payload={"ig_user_id": str(ig_user_id)})
				enrich_user_q += 1
			except Exception:
				pass
			try:
				cid = f"{igba_id}:{ig_user_id}"
				enqueue(
					"hydrate_conversation",
					key=cid,
					payload={"igba_id": str(igba_id), "ig_user_id": str(ig_user_id), "max_messages": 200},
				)
				hydrate_q += 1
			except Exception:
				pass
	return enrich_page_q, enrich_user_q, hydrate_q


def enqueue_pending_media(limit: int = 100) -> int:
	"""Enqueue media fetch for attachments pending or error."""
	count = 0
	with get_session() as session:
		rows = session.exec(
			text(
				"""
				SELECT id FROM attachments
				WHERE fetch_status IS NULL OR fetch_status IN ('pending','error')
				LIMIT :n
				"""
			).params(n=int(limit))
		).all()
		for r in rows:
			att_id = r.id if hasattr(r, "id") else r[0]
			try:
				enqueue("fetch_media", key=str(att_id), payload={"attachment_id": int(att_id)})
				count += 1
			except Exception:
				pass
	return count


def main() -> None:
	ep, eu, hy = enqueue_from_recent_conversations(limit=int(os.getenv("BF_CONV_LIMIT", "50")))
	media = enqueue_pending_media(limit=int(os.getenv("BF_MEDIA_LIMIT", "100")))
	print(f"enqueued enrich_page={ep} enrich_user={eu} hydrate={hy} media={media}")


if __name__ == "__main__":
	main()


