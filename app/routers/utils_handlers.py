from fastapi import APIRouter
import os

from ..db import get_session
from ..services.queue import _get_redis
from ..services.instagram_api import _get_base_token_and_id
from sqlalchemy import text as _text

router = APIRouter()


@router.get("/queue/status")
def queue_status():
	"""Return approximate queue sizes for related workers."""
	out = {"ingest": None, "hydrate_conversation": None, "enrich_user": None, "enrich_page": None}
	try:
		r = _get_redis()
		out["ingest"] = int(r.llen("jobs:ingest"))
		out["hydrate_conversation"] = int(r.llen("jobs:hydrate_conversation"))
		out["enrich_user"] = int(r.llen("jobs:enrich_user"))
		out["enrich_page"] = int(r.llen("jobs:enrich_page"))
	except Exception:
		# keep None to indicate unavailable
		pass
	return {"status": "ok", "queues": out}


@router.get("/ai_reply/queue_stats")
def ai_reply_queue_stats():
	"""AI cevap kuyruğunda kaç konuşma bekliyor ve hangi worker işliyor (worker tarafı DB'den okur)."""
	due_where = """
		(
			(status = 'pending' OR status IS NULL)
			AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
		)
		OR (
			status = 'paused'
			AND postpone_count > 0 AND postpone_count <= 8
			AND next_attempt_at IS NOT NULL AND next_attempt_at <= CURRENT_TIMESTAMP
		)
	"""
	total_pending = 0
	try:
		with get_session() as session:
			row = session.exec(_text(f"SELECT COUNT(*) FROM ai_shadow_state WHERE {due_where}")).first()
			if row is not None:
				try:
					total_pending = int(row[0])
				except (TypeError, ValueError, IndexError, KeyError):
					pass
	except Exception:
		pass
	return {
		"status": "ok",
		"total_pending": total_pending,
		"worker_deployment": "hm-worker-reply",
		"namespace": "hm",
		"note": "Kuyruk ai_shadow_state tablosundan; worker her döngüde en fazla 20 konuşma alır. Pod: kubectl get pods -n hm -l app=hm-worker-reply",
	}


@router.get("/debug/env")
def debug_env():
    """Lightweight diagnostics: show which token path is active (page vs user) and env presence.

    Does NOT return secrets; only booleans and token length/suffix for verification.
    """
    data: dict[str, object] = {
        "has_page_id": bool(os.getenv("IG_PAGE_ID")),
        "has_page_token": bool(os.getenv("IG_PAGE_ACCESS_TOKEN")),
        "has_user_id": bool(os.getenv("IG_USER_ID")),
        "has_user_token": bool(os.getenv("IG_ACCESS_TOKEN")),
        "graph_version": os.getenv("IG_GRAPH_API_VERSION", "v21.0"),
    }
    try:
        token, ident, is_page = _get_base_token_and_id()
        data["active_path"] = "page" if is_page else "user"
        data["id_in_use"] = str(ident)
        data["token_len"] = len(token or "")
        data["token_suffix"] = (token[-6:] if token else None)
    except Exception as e:
        data["resolve_error"] = str(e)
    return data
