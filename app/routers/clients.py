from fastapi import APIRouter, Query, Request
from sqlmodel import select

from ..db import get_session
from ..models import Client

router = APIRouter()


@router.get("")
@router.get("/")
def list_clients(limit: int = Query(default=100, ge=1, le=1000)):
	with get_session() as session:
		rows = session.exec(select(Client).order_by(Client.id.desc()).limit(limit)).all()
		return {
			"clients": [
				{
					"id": c.id or 0,
					"name": c.name,
					"phone": c.phone,
					"address": c.address,
					"city": c.city,
					"created_at": c.created_at.isoformat(),
				}
				for c in rows
			]
		}


@router.get("/table")
def list_clients_table(request: Request, limit: int = Query(default=100, ge=1, le=2000)):
	from ..main import create_app  # circular-safe import of templates via app state
	with get_session() as session:
		rows = session.exec(select(Client).order_by(Client.id.desc()).limit(limit)).all()
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"clients_table.html",
			{"request": request, "rows": rows, "limit": limit},
		)
