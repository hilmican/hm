#!/usr/bin/env python3
import time
import logging
import datetime as dt
from typing import Any, Optional

from app.db import get_session
from sqlalchemy import text as _text

from app.services.ai_reply import draft_reply

log = logging.getLogger("worker.reply")
logging.basicConfig(level=logging.INFO)


DEBOUNCE_SECONDS = 30
POSTPONE_WINDOW_SECONDS = 180  # 3 minutes
POSTPONE_MAX = 3


def _utcnow() -> dt.datetime:
	return dt.datetime.utcnow()


def _now_ms() -> int:
	return int(_utcnow().timestamp() * 1000)


def _postpone(convo_id: str, *, increment: bool = True) -> None:
	with get_session() as session:
		next_at = _utcnow() + dt.timedelta(seconds=POSTPONE_WINDOW_SECONDS)
		if increment:
			session.exec(_text("UPDATE ai_shadow_state SET postpone_count=postpone_count+1, status='paused', next_attempt_at=:na, updated_at=CURRENT_TIMESTAMP WHERE convo_id=:cid").params(na=next_at.isoformat(" "), cid=convo_id))
		else:
			session.exec(_text("UPDATE ai_shadow_state SET status='paused', next_attempt_at=:na, updated_at=CURRENT_TIMESTAMP WHERE convo_id=:cid").params(na=next_at.isoformat(" "), cid=convo_id))


def _set_status(convo_id: str, status: str) -> None:
	with get_session() as session:
		session.exec(_text("UPDATE ai_shadow_state SET status=:s, updated_at=CURRENT_TIMESTAMP WHERE convo_id=:cid").params(s=status, cid=convo_id))


def main() -> None:
	log.info("worker_reply starting")
	while True:
		# Pull due states
		due: list[dict[str, Any]] = []
		try:
			with get_session() as session:
				rows = session.exec(
					_text(
						"""
						SELECT convo_id, last_inbound_ms, postpone_count, COALESCE(status,'pending') AS status
						FROM ai_shadow_state
						WHERE (status IN ('pending','paused') OR status IS NULL)
						  AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
						ORDER BY (next_attempt_at IS NULL) DESC, next_attempt_at ASC
						LIMIT 20
						"""
					)
				).all()
				for r in rows:
					item = {
						"convo_id": r.convo_id if hasattr(r, "convo_id") else r[0],
						"last_inbound_ms": int((r.last_inbound_ms if hasattr(r, "last_inbound_ms") else r[1]) or 0),
						"postpone_count": int((r.postpone_count if hasattr(r, "postpone_count") else r[2]) or 0),
						"status": (r.status if hasattr(r, "status") else r[3]) or "pending",
					}
					due.append(item)
		except Exception as e:
			try:
				log.warning("scan error: %s", e)
			except Exception:
				pass
			time.sleep(0.5)
			continue

		if not due:
			time.sleep(0.5)
			continue

		for st in due:
			cid = str(st.get("convo_id") or "")
			if not cid:
				continue
			last_ms = int(st.get("last_inbound_ms") or 0)
			postpones = int(st.get("postpone_count") or 0)
			# If user likely still typing, postpone
			if last_ms > 0 and (_now_ms() - last_ms) < (DEBOUNCE_SECONDS * 1000):
				if postpones >= POSTPONE_MAX:
					_set_status(cid, "exhausted")
					continue
				_postpone(cid, increment=True)
				continue
			# Transition to running
			try:
				with get_session() as session:
					session.exec(
						_text("UPDATE ai_shadow_state SET status='running', next_attempt_at=NULL, updated_at=CURRENT_TIMESTAMP WHERE convo_id=:cid")
						.params(cid=cid)
					)
			except Exception:
				continue
			# Generate draft
			try:
				data = draft_reply(cid, limit=40, include_meta=False)
				reply_text = (data.get("reply_text") or "").strip()
				if not reply_text:
					_set_status(cid, "error")
					continue
				# Persist draft
				try:
					with get_session() as session:
						session.exec(
							_text(
								"""
								INSERT INTO ai_shadow_reply(convo_id, reply_text, model, confidence, reason, json_meta, attempt_no, status, created_at)
								VALUES(:cid, :txt, :model, :conf, :reason, NULL, :att, 'suggested', CURRENT_TIMESTAMP)
								"""
							).params(
								cid=cid,
								txt=reply_text,
								model=str(data.get("model") or ""),
								conf=(float(data.get("confidence") or 0.6)),
								reason=(data.get("reason") or "auto"),
								att=int(postpones or 0),
							)
						)
						session.exec(_text("UPDATE ai_shadow_state SET status='suggested', updated_at=CURRENT_TIMESTAMP WHERE convo_id=:cid").params(cid=cid))
				except Exception as pe:
					try:
						log.warning("persist draft error cid=%s err=%s", cid, pe)
					except Exception:
						pass
					_set_status(cid, "error")
			except Exception as ge:
				try:
					log.warning("generate error cid=%s err=%s", cid, ge)
				except Exception:
					pass
				_set_status(cid, "error")


if __name__ == "__main__":
	main()


