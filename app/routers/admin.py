import os
import json
from pathlib import Path
from typing import Optional, Dict, Any

from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import text

from ..db import get_session


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
			v = session.exec(text("SELECT 1")).first()
			# quick counts (cheap)
			counts = {}
			for tbl in ("message", "attachments", "raw_events", "jobs"):
				try:
					row = session.exec(text(f"SELECT COUNT(1) FROM {tbl}")).first()
					counts[tbl] = int(row[0] if isinstance(row, (list, tuple)) else (row or 0))
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


