from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .db import init_db
from .db import engine as _db_engine
from .services.ai import AIClient
from .routers import dashboard, importer, clients, items, orders, payments, reconcile, auth
from .routers import reports
from .routers import inventory, mappings, products
from .routers import instagram
from .routers import legal
from .routers import ig
from .routers import ig_ai
from .routers import ads
from .routers import stories
from .routers import noc
from .routers import costs
from collections import deque
import time as _time
from .routers import admin
from . import i18n as _i18n
from .routers import i18n as i18n_router


def create_app() -> FastAPI:
	app = FastAPI(title="Kargo Importer & Management", docs_url=None, redoc_url=None, openapi_url=None)

	app.mount("/static", StaticFiles(directory="static"), name="static")

	templates = Jinja2Templates(directory="templates")
	app.state.templates = templates
	# Register Jinja helpers for translations
	try:
		templates.env.globals["t"] = _i18n.t
		templates.env.globals["current_lang"] = _i18n.current_lang
	except Exception:
		pass
	# Eager i18n init (fallback in case startup hook is skipped/errored)
	try:
		import os as _os_init
		from pathlib import Path as _Path_init
		_default_lang = _os_init.getenv("DEFAULT_LANG", "tr")
		_candidates = [
			_Path_init(__file__).resolve().parent / "locales",
			_Path_init.cwd() / "app" / "locales",
			_Path_init.cwd() / "locales",
			_Path_init("app/locales"),
		]
		_catalog_dir = None
		for c in _candidates:
			try:
				if c.exists() and any(c.glob("*.json")):
					_catalog_dir = c
					break
			except Exception:
				continue
		if _catalog_dir is None:
			_catalog_dir = _Path_init(__file__).resolve().parent / "locales"
		app.state.i18n = _i18n.I18n.load_from_dir(str(_catalog_dir), default_lang=_default_lang)
	except Exception:
		# keep a minimal manager to avoid template errors
		app.state.i18n = _i18n.I18n()

	# simple session middleware for cookie-based auth (HTTPOnly cookies)
	import os as _os
	app.add_middleware(SessionMiddleware, secret_key=_os.getenv("SESSION_SECRET", "dev-secret-change"))

	@app.on_event("startup")
	def _startup() -> None:
		# Ensure application loggers output to stdout (uvicorn doesn't auto-configure custom loggers)
		try:
			import logging as _lg
			_fmt = _lg.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
			_h = _lg.StreamHandler()
			_h.setFormatter(_fmt)
			for lname in ("instagram.webhook", "instagram.inbox", "graph.api", "ingest.upsert"):
				lg = _lg.getLogger(lname)
				lg.setLevel(_lg.INFO)
				# avoid duplicate handlers
				if not any(isinstance(h, _lg.StreamHandler) for h in lg.handlers):
					lg.addHandler(_h)
				lg.propagate = False
		except Exception:
			pass
		init_db()
		# Load i18n catalogs
		try:
			from pathlib import Path as _Path
			_default_lang = _os.getenv("DEFAULT_LANG", "tr")
			_candidates = [
				_Path(__file__).resolve().parent / "locales",
				_Path.cwd() / "app" / "locales",
				_Path.cwd() / "locales",
				_Path("app/locales"),
			]
			_catalog_dir = None
			for c in _candidates:
				try:
					if c.exists() and any(c.glob("*.json")):
						_catalog_dir = c
						break
				except Exception:
					continue
			if _catalog_dir is None:
				_catalog_dir = _Path(__file__).resolve().parent / "locales"
			app.state.i18n = _i18n.I18n.load_from_dir(str(_catalog_dir), default_lang=_default_lang)
			print(f"[i18n] loaded languages: {', '.join(app.state.i18n.available_languages())}")
		except Exception:
			app.state.i18n = _i18n.I18n()
		# Ensure Jinja globals are registered (idempotent)
		try:
			tmpl = getattr(app.state, "templates", None)
			if tmpl and getattr(tmpl, "env", None):
				tmpl.env.globals["t"] = _i18n.t
				tmpl.env.globals["current_lang"] = _i18n.current_lang
		except Exception:
			pass
		# Log DB backend once for sanity
		try:
			_backend = getattr(_db_engine.url, "get_backend_name", lambda: "")()
			print(f"[DB] backend: {_backend}")
		except Exception:
			pass
		# Init AI client (optional)
		try:
			app.state.ai = AIClient()
		except Exception:
			app.state.ai = None
		# Ensure media directories exist when app boots
		try:
			import os as _os
			from pathlib import Path as _Path
			_Path(_os.getenv("MEDIA_ROOT", "data/media")).mkdir(parents=True, exist_ok=True)
			_Path(_os.getenv("THUMBS_ROOT", "data/thumbs")).mkdir(parents=True, exist_ok=True)
		except Exception:
			pass
		# Initialize in-memory slowlog buffer
		try:
			app.state.slowlog = deque(maxlen=int(_os.getenv("SLOWLOG_SIZE", "500")))
		except Exception:
			app.state.slowlog = deque(maxlen=500)

	# Language resolution middleware
	@app.middleware("http")
	async def _lang_mw(request: Request, call_next):
		try:
			lang = request.session.get("lang")
			if not lang:
				i18n_mgr = getattr(request.app.state, "i18n", None)
				if i18n_mgr:
					lang = i18n_mgr.default_lang
			request.state.lang = lang or "tr"
		except Exception:
			request.state.lang = "tr"
		return await call_next(request)

	# Lightweight timing middleware for slow-request diagnostics
	@app.middleware("http")
	async def _timing_mw(request: Request, call_next):
		start = _time.perf_counter()
		response = await call_next(request)
		dt_ms = int(((_time.perf_counter() - start) * 1000.0))
		try:
			import os as _os
			thr = int(_os.getenv("APP_SLOW_MS", "800"))
		except Exception:
			thr = 800
		if dt_ms >= thr:
			try:
				entry = {
					"ts": int(_time.time()),
					"ms": dt_ms,
					"method": request.method,
					"path": str(request.url.path),
					"status": getattr(response, "status_code", None),
				}
				buf = getattr(request.app.state, "slowlog", None)
				if buf is not None:
					buf.append(entry)
				# Also print to stdout for live tailing
			except Exception:
				pass
		return response

	app.include_router(dashboard.router)
	app.include_router(auth.router)
	app.include_router(admin.router)
	app.include_router(importer.router, prefix="/import")
	app.include_router(reconcile.router, prefix="/reconcile")
	app.include_router(clients.router, prefix="/clients", tags=["clients"]) 
	app.include_router(items.router, prefix="/items", tags=["items"]) 
	app.include_router(orders.router, prefix="/orders", tags=["orders"]) 
	app.include_router(payments.router, prefix="/payments", tags=["payments"]) 
	app.include_router(inventory.router)
	app.include_router(mappings.router)
	app.include_router(products.router)
	app.include_router(instagram.router)
	app.include_router(legal.router)
	app.include_router(ig.router)
	app.include_router(ig_ai.router)
	app.include_router(ads.router)
	app.include_router(stories.router)
	app.include_router(reports.router, prefix="/reports", tags=["reports"]) 
	app.include_router(noc.router)
	app.include_router(costs.router, prefix="/costs", tags=["costs"])
	# i18n endpoints
	app.include_router(i18n_router.router)

	# Diag router for slowlog endpoints
	from fastapi import APIRouter as _AR
	diag = _AR(prefix="/admin", tags=["admin"])

	@diag.get("/slowlog")
	def _slowlog_list(limit: int = 100):
		buf = getattr(app.state, "slowlog", None) or []
		rows = list(buf)[-int(max(1, min(limit, 1000))):]
		return {"slow": rows}

	@diag.post("/slowlog/clear")
	def _slowlog_clear():
		buf = getattr(app.state, "slowlog", None)
		if hasattr(buf, "clear"):
			buf.clear()
		return {"status": "ok"}

	app.include_router(diag)

	# ultra-light health endpoint (no DB touches)
	@app.get("/health")
	def _health() -> dict:
		return {"status": "ok"}

	return app


app = create_app()
