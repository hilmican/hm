from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .db import init_db
from .routers import dashboard, importer, clients, items, orders, payments, reconcile, auth


def create_app() -> FastAPI:
	app = FastAPI(title="Kargo Importer & Management", docs_url=None, redoc_url=None, openapi_url=None)

	app.mount("/static", StaticFiles(directory="static"), name="static")

	templates = Jinja2Templates(directory="templates")
	app.state.templates = templates

	# simple session middleware for cookie-based auth (HTTPOnly cookies)
	import os as _os
	app.add_middleware(SessionMiddleware, secret_key=_os.getenv("SESSION_SECRET", "dev-secret-change"))

	@app.on_event("startup")
	def _startup() -> None:
		init_db()

	app.include_router(dashboard.router)
	app.include_router(auth.router)
	app.include_router(importer.router, prefix="/import")
	app.include_router(reconcile.router, prefix="/reconcile")
	app.include_router(clients.router, prefix="/clients", tags=["clients"]) 
	app.include_router(items.router, prefix="/items", tags=["items"]) 
	app.include_router(orders.router, prefix="/orders", tags=["orders"]) 
	app.include_router(payments.router, prefix="/payments", tags=["payments"]) 

	return app


app = create_app()
