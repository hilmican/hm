from fastapi import APIRouter, Request
from sqlmodel import select
import datetime as dt

from ..db import get_session
from ..models import Message
from ..services.monitoring import get_worker_statuses, get_queue_stats, sum_counters


router = APIRouter(prefix="/noc", tags=["noc"])


@router.get("")
def noc_page(request: Request):
    uid = request.session.get("uid")
    if not uid:
        templates = request.app.state.templates
        return templates.TemplateResponse("login.html", {"request": request, "error": None})
    templates = request.app.state.templates
    return templates.TemplateResponse("noc.html", {"request": request})


@router.get("/data")
def noc_data(request: Request, window: int = 60):
    # No heavy DB ops; mostly Redis-backed
    window_minutes = max(1, min(int(window or 60), 24 * 60))
    now = dt.datetime.utcnow().isoformat()
    workers = get_worker_statuses()
    queues = get_queue_stats()
    rates = {
        "messages": sum_counters("messages", window_minutes),
        "enrich_success": sum_counters("enrich_success", window_minutes),
        "enrich_user": sum_counters("enrich_user", window_minutes),
        "enrich_page": sum_counters("enrich_page", window_minutes),
        "media_fetch": sum_counters("media_fetch", window_minutes),
        "media_image": sum_counters("media_image", window_minutes),
        "media_video": sum_counters("media_video", window_minutes),
        "media_audio": sum_counters("media_audio", window_minutes),
    }
    return {
        "now": now,
        "window_minutes": window_minutes,
        "workers": workers,
        "queues": queues,
        "rates": rates,
    }


