from fastapi import APIRouter
import os

from ..services.queue import _get_redis
from ..services.instagram_api import _get_base_token_and_id

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
