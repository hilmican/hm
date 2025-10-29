import os
import json
from pathlib import Path
from typing import Optional, Dict, Any

from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import text

from ..db import get_session
from ..services.queue import _get_redis
from ..services.queue import enqueue


router = APIRouter(prefix="/admin", tags=["admin"])


def _read_git_version(repo_root: Path = Path(".")) -> Optional[str]:
	# Prefer explicit env override
	for key in ("APP_VERSION", "HM_VERSION", "RELEASE_VERSION"):
		val = os.getenv(key)
		if val:
			return str(val)
	git_dir = repo_root / ".git"
	try:
		head = (git_dir / "HEAD").read_text().strip()
		if head.startswith("ref:"):
			ref = head.split(" ", 1)[1].strip()
			ref_path = git_dir / ref
			if ref_path.exists():
				sha = ref_path.read_text().strip()
				return sha[:12]
			# packed-refs fallback
			packed = (git_dir / "packed-refs").read_text().splitlines()
			for line in packed:
				if line.startswith("#") or not line.strip():
					continue
				if line.endswith(" " + ref):
					sha = line.split(" ", 1)[0].strip()
					return sha[:12]
		else:
			# Detached HEAD contains SHA directly
			return head[:12]
	except Exception:
		return None


def _check_db() -> Dict[str, Any]:
	try:
		with get_session() as session:
			session.exec(text("SELECT 1")).first()
			# quick counts (cheap) with robust row extraction
			counts: Dict[str, Any] = {}
			for tbl in ("message", "attachments", "raw_events", "jobs"):
				try:
					row = session.exec(text(f"SELECT COUNT(1) AS c FROM {tbl}")).first()
					val = None
					if row is None:
						val = 0
					elif isinstance(row, (list, tuple)):
						val = int(row[0])
					else:
						# RowMapping-like
						try:
							val = int(getattr(row, "c", 0))
						except Exception:
							# attempt index access
							try:
								val = int(row[0])  # type: ignore
							except Exception:
								val = None
					counts[tbl] = val
				except Exception:
					counts[tbl] = None
			return {"ok": True, "counts": counts}
	except Exception as e:
		return {"ok": False, "error": str(e)}


def _check_redis() -> Dict[str, Any]:
	try:
		from ..services.queue import _get_redis  # lazy import
		r = _get_redis()
		pong = r.ping()
		return {"ok": bool(pong)}
	except Exception as e:
		return {"ok": False, "error": str(e)}


@router.get("/version")
def version() -> Dict[str, Any]:
	return {"version": _read_git_version() or "unknown"}


@router.get("/health")
def health() -> Dict[str, Any]:
	media_root = Path(os.getenv("MEDIA_ROOT", "data/media"))
	thumbs_root = Path(os.getenv("THUMBS_ROOT", "data/thumbs"))
	return {
		"status": "ok",
		"version": _read_git_version() or "unknown",
		"db": _check_db(),
		"redis": _check_redis(),
		"media_root_exists": media_root.exists(),
		"thumbs_root_exists": thumbs_root.exists(),
	}


@router.get("/status")
def status_page(request: Request):
	templates = request.app.state.templates
	ctx = {
		"version": _read_git_version() or "unknown",
		"db": _check_db(),
		"redis": _check_redis(),
	}
	return templates.TemplateResponse("admin_status.html", {"request": request, **ctx})


@router.get("/debug/jobs")
def debug_jobs(kind: str, limit: int = 50):
    try:
        r = _get_redis()
        ll = int(r.llen(f"jobs:{kind}"))
        items = [r.lindex(f"jobs:{kind}", i) for i in range(0, min(max(limit, 1), ll))]
        return {"kind": kind, "length": ll, "items": items}
    except Exception as e:
        return {"error": str(e)}


@router.get("/debug/attachments")
def debug_attachments(limit: int = 50):
    with get_session() as session:
        safe_n = max(1, min(int(limit or 50), 1000))
        # SQLite has quirks with bound params in LIMIT; embed sanitized value
        rows = session.exec(text(f"SELECT id, message_id, kind, fetch_status, fetched_at FROM attachments ORDER BY id DESC LIMIT {safe_n}")) .all()
        out = []
        for r in rows:
            try:
                out.append({
                    "id": int(r.id if hasattr(r, 'id') else r[0]),
                    "message_id": int(r.message_id if hasattr(r, 'message_id') else r[1]),
                    "kind": (r.kind if hasattr(r, 'kind') else r[2]),
                    "fetch_status": (r.fetch_status if hasattr(r, 'fetch_status') else r[3]),
                    "fetched_at": (r.fetched_at if hasattr(r, 'fetched_at') else r[4]),
                })
            except Exception:
                continue
        return {"attachments": out}


@router.get("/debug/conversations")
def debug_conversations(limit: int = 20):
    with get_session() as session:
        safe_n = max(1, min(int(limit or 20), 200))
        rows = session.exec(text(f"SELECT convo_id, igba_id, ig_user_id, hydrated_at, last_message_at FROM conversations ORDER BY last_message_at DESC LIMIT {safe_n}")).all()
        out = []
        for r in rows:
            try:
                out.append({
                    "convo_id": (r.convo_id if hasattr(r, 'convo_id') else r[0]),
                    "igba_id": (r.igba_id if hasattr(r, 'igba_id') else r[1]),
                    "ig_user_id": (r.ig_user_id if hasattr(r, 'ig_user_id') else r[2]),
                    "hydrated_at": (r.hydrated_at if hasattr(r, 'hydrated_at') else r[3]),
                    "last_message_at": (r.last_message_at if hasattr(r, 'last_message_at') else r[4]),
                })
            except Exception:
                continue
        return {"conversations": out}


@router.post("/debug/hydrate")
def debug_hydrate(igba_id: str, ig_user_id: str):
    try:
        from ..services.queue import enqueue
        enqueue("hydrate_conversation", key=f"{igba_id}:{ig_user_id}", payload={"igba_id": str(igba_id), "ig_user_id": str(ig_user_id), "max_messages": 200})
        return {"status": "queued"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.post("/debug/backfill")
def debug_backfill(conv_limit: int = 50, media_limit: int = 100):
    """Enqueue enrich_page, enrich_user, hydrate_conversation and pending media fetch.

    This mirrors scripts/backfill_enqueue.py but is callable via HTTP for ops.
    """
    ep = 0
    eu = 0
    hy = 0
    md = 0
    # conversations backfill
    try:
        with get_session() as session:
            rows = session.exec(text("SELECT igba_id, ig_user_id FROM conversations ORDER BY last_message_at DESC LIMIT :n").params(n=int(conv_limit))).all()
            for r in rows:
                igba_id = r.igba_id if hasattr(r, "igba_id") else r[0]
                ig_user_id = r.ig_user_id if hasattr(r, "ig_user_id") else r[1]
                try:
                    enqueue("enrich_page", key=str(igba_id), payload={"igba_id": str(igba_id)})
                    ep += 1
                except Exception:
                    pass
                try:
                    enqueue("enrich_user", key=str(ig_user_id), payload={"ig_user_id": str(ig_user_id)})
                    eu += 1
                except Exception:
                    pass
                try:
                    cid = f"{igba_id}:{ig_user_id}"
                    enqueue("hydrate_conversation", key=cid, payload={"igba_id": str(igba_id), "ig_user_id": str(ig_user_id), "max_messages": 200})
                    hy += 1
                except Exception:
                    pass
    except Exception as e:
        return {"status": "error", "error": f"conversations backfill: {e}"}
    # media backfill
    try:
        with get_session() as session:
            rows = session.exec(text(
                """
                SELECT id FROM attachments
                WHERE fetch_status IS NULL OR fetch_status IN ('pending','error')
                ORDER BY id DESC
                LIMIT :n
                """
            ).params(n=int(media_limit))).all()
            for r in rows:
                att_id = r.id if hasattr(r, "id") else r[0]
                try:
                    enqueue("fetch_media", key=str(att_id), payload={"attachment_id": int(att_id)})
                    md += 1
                except Exception:
                    pass
    except Exception as e:
        return {"status": "partial", "enrich_page": ep, "enrich_user": eu, "hydrate": hy, "media": md, "error": f"media backfill: {e}"}
    return {"status": "ok", "enrich_page": ep, "enrich_user": eu, "hydrate": hy, "media": md}
