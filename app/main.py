from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .db import init_db
from .routers import dashboard, importer, clients, items, orders, payments, reconcile


def create_app() -> FastAPI:
	app = FastAPI(title="Kargo Importer & Management", docs_url=None, redoc_url=None, openapi_url=None)

	app.mount("/static", StaticFiles(directory="static"), name="static")

	templates = Jinja2Templates(directory="templates")
	app.state.templates = templates

	@app.on_event("startup")
	def _startup() -> None:
		init_db()

	app.include_router(dashboard.router)
	app.include_router(importer.router, prefix="/import")
	app.include_router(reconcile.router, prefix="/reconcile")
	app.include_router(clients.router, prefix="/clients", tags=["clients"]) 
	app.include_router(items.router, prefix="/items", tags=["items"]) 
	app.include_router(orders.router, prefix="/orders", tags=["orders"]) 
	app.include_router(payments.router, prefix="/payments", tags=["payments"]) 

	return app


app = create_app()
