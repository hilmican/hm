import datetime as dt
from sqlalchemy import text

from ..db import get_session
from .queue import enqueue


def enqueue_stale_users(hours: int = 48) -> int:
	cut = dt.datetime.utcnow() - dt.timedelta(hours=hours)
	count = 0
	with get_session() as session:
		rows = session.exec(
			text(
				"""
				SELECT ig_user_id FROM ig_users
				WHERE fetched_at IS NULL OR fetched_at < :cut
				"""
			)
		).params(cut=cut).all()
		for r in rows:
			uid = r.ig_user_id if hasattr(r, "ig_user_id") else r[0]
			enqueue("enrich_user", key=str(uid), payload={"ig_user_id": str(uid)})
			count += 1
	return count


def enqueue_missing_attachments(max_attempts: int = 8) -> int:
	count = 0
	with get_session() as session:
		rows = session.exec(
			text(
				"""
				SELECT id, COALESCE(attempts,0) FROM attachments a
				LEFT JOIN (
				  SELECT key, attempts FROM jobs WHERE kind='fetch_media'
				) j ON j.key = CAST(a.message_id AS TEXT) || ':' || CAST(a.position AS TEXT)
				WHERE COALESCE(fetch_status, 'pending') IN ('pending','error')
				"""
			)
		).all()
		for r in rows:
			att_id = r.id if hasattr(r, "id") else r[0]
			enqueue("fetch_media", key=str(att_id), payload={"attachment_id": int(att_id)})
			count += 1
	return count


