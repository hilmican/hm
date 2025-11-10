from fastapi import APIRouter, Request, Form
from fastapi.responses import JSONResponse, RedirectResponse
from typing import Optional

from ..db import get_session
from ..models import User


router = APIRouter(prefix="/i18n", tags=["i18n"])


@router.get("/select")
def select_language(request: Request, next: Optional[str] = None):
	templates = request.app.state.templates
	return templates.TemplateResponse("i18n_select.html", {"request": request, "next": next})


@router.post("/set")
async def set_language(request: Request, lang: Optional[str] = Form(None), next: Optional[str] = Form(None)):
	if lang is None:
		try:
			data = await request.json()
			lang = (data or {}).get("lang")
			if next is None:
				next = (data or {}).get("next")
		except Exception:
			pass
	lang = (lang or "").strip().lower()
	# validate against loaded catalogs if available
	available = getattr(getattr(request.app.state, "i18n", None), "catalogs", {}) or {}
	if lang not in available:
		# fallback: keep existing or default
		return JSONResponse({"status": "error", "detail": "unsupported_language"}, status_code=400)
	# set session preference
	request.session["lang"] = lang
	# persist on user if logged in
	try:
		uid = request.session.get("uid")
		if uid:
			with get_session() as session:
				u = session.get(User, int(uid))
				if u:
					u.preferred_language = lang
					session.add(u)
	except Exception:
		# best effort only
		pass
	if next:
		return JSONResponse({"status": "ok", "redirect": next})
	return JSONResponse({"status": "ok"})


@router.get("/switch")
def switch_language(request: Request, lang: Optional[str] = None, next: Optional[str] = None):
	"""Convenience GET endpoint for public pages to switch language via link."""
	lang = (lang or "").strip().lower()
	available = getattr(getattr(request.app.state, "i18n", None), "catalogs", {}) or {}
	if lang in available:
		request.session["lang"] = lang
		# persist to user if logged in (best-effort)
		try:
			uid = request.session.get("uid")
			if uid:
				with get_session() as session:
					u = session.get(User, int(uid))
					if u:
						u.preferred_language = lang
						session.add(u)
		except Exception:
			pass
	dest = next or request.headers.get("referer") or "/dashboard"
	return RedirectResponse(dest, status_code=303)


