from fastapi import APIRouter, Query, Request
from sqlmodel import select
from sqlalchemy import func

from ..db import get_session
from ..models import Client, Order, Item, Payment, ImportRun, ImportRow
from ..services.finance import get_effective_total

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
def list_clients_table(request: Request):
	with get_session() as session:
		rows = session.exec(select(Client).order_by(Client.id.desc())).all()
		# order counts per client
		counts = session.exec(select(Order.client_id, func.count().label("cnt")).group_by(Order.client_id)).all()
		order_counts = {cid: cnt for cid, cnt in counts}
		templates = request.app.state.templates
		return templates.TemplateResponse(
			"clients_table.html",
			{"request": request, "rows": rows, "order_counts": order_counts},
		)


@router.get("/{client_id}")
def client_detail(client_id: int, request: Request):
	with get_session() as session:
		client = session.get(Client, client_id)
		if not client:
			from fastapi import HTTPException
			raise HTTPException(status_code=404, detail="Client not found")
		orders = session.exec(select(Order).where(Order.client_id == client_id).order_by(Order.id.desc())).all()
		item_ids = [o.item_id for o in orders if o.item_id]
		items = session.exec(select(Item).where(Item.id.in_(item_ids))).all() if item_ids else []
		item_map = {it.id: it for it in items if it.id is not None}

		# Status / payment context (align with orders table rendering)
		order_ids = [o.id for o in orders if o.id]
		pays = session.exec(select(Payment).where(Payment.order_id.in_(order_ids))).all() if order_ids else []
		paid_map: dict[int, float] = {}
		for p in pays:
			if p.order_id is None:
				continue
			paid_map[p.order_id] = paid_map.get(p.order_id, 0.0) + float(p.amount or 0.0)

		# Partial payment grouping
		from collections import defaultdict

		partial_groups: dict[int, list[int]] = defaultdict(list)
		for o in orders:
			if o.partial_payment_group_id and o.id:
				partial_groups[o.partial_payment_group_id].append(o.id)

		for group_id, group_order_ids in partial_groups.items():
			if group_id in group_order_ids:
				group_paid = sum(paid_map.get(oid, 0.0) for oid in group_order_ids)
				paid_map[group_id] = group_paid

		status_map: dict[int, str] = {}

		for o in orders:
			if not o.id:
				continue
			oid = int(o.id)
			total = float(get_effective_total(o))

			# primary status string
			primary_status = (o.status or "").lower()

			# For partial payment groups, use group total payments for the primary order
			if o.partial_payment_group_id and o.partial_payment_group_id == oid:
				paid = paid_map.get(oid, 0.0)
			else:
				paid = paid_map.get(oid, 0.0)

			if primary_status in ("refunded", "switched", "stitched", "cancelled"):
				status_map[oid] = primary_status
			elif primary_status == "iade_bekliyor":
				status_map[oid] = "iade_bekliyor"
			elif primary_status == "paid":
				status_map[oid] = "paid"
			elif primary_status == "partial_paid":
				status_map[oid] = "partial_paid"
			elif primary_status in ("tanzim_bekliyor", "tanzim_basari", "tanzim_basarisiz"):
				status_map[oid] = primary_status
			else:
				if bool(o.paid_by_bank_transfer):
					status_map[oid] = "paid"
				else:
					status_map[oid] = "paid" if (paid > 0 and paid >= total) else "unpaid"

		# Import provenance per order (which Excel/run created or touched it)
		order_imports: dict[int, list[dict]] = {}
		if order_ids:
			rows_with_runs = session.exec(
				select(ImportRow, ImportRun)
				.join(ImportRun, ImportRun.id == ImportRow.import_run_id)
				.where(ImportRow.matched_order_id.in_(order_ids))
				.order_by(ImportRun.id.desc(), ImportRow.row_index)
			).all()
			for ir, run in rows_with_runs:
				if ir.matched_order_id is None:
					continue
				oid = int(ir.matched_order_id)
				order_imports.setdefault(oid, []).append(
					{
						"run_id": run.id,
						"source": run.source,
						"filename": run.filename,
						"status": ir.status,
						"row_index": ir.row_index,
					}
				)

		templates = request.app.state.templates
		return templates.TemplateResponse(
			"client_detail.html",
			{
				"request": request,
				"client": client,
				"orders": orders,
				"item_map": item_map,
				"status_map": status_map,
				"order_imports": order_imports,
			},
		)
