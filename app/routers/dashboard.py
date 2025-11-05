from fastapi import APIRouter, Request, HTTPException
from sqlmodel import select
from sqlalchemy import text
import os

from ..db import get_session
from ..models import Client, Item, Order, Payment, ImportRow, ImportRun, StockMovement
from ..services.shipping import compute_shipping_fee
from ..services.cache import cached_json

router = APIRouter()


@router.get("/dashboard")
def dashboard(request: Request):
	# require login
	uid = request.session.get("uid")
	if not uid:
		templates = request.app.state.templates
		return templates.TemplateResponse("login.html", {"request": request, "error": None})
	# pull small samples for quick display
	
	with get_session() as session:
		# Fast aggregates with short-lived cache
		ttl = int(os.getenv("CACHE_TTL_DASHBOARD", "60"))
		agg = cached_json(
			"dash:totals",
			ttl,
			lambda: {
				"total_sales": float((session.exec(text('SELECT SUM(total_amount) AS s FROM "order" WHERE COALESCE(status, "") NOT IN ("refunded","switched","stitched")')).first() or [0])[0] or 0),
				"net_collected": float((session.exec(text('SELECT SUM(net_amount) AS s FROM payment')).first() or [0])[0] or 0),
				"fee_kom": float((session.exec(text('SELECT SUM(COALESCE(fee_komisyon,0)) FROM payment')).first() or [0])[0] or 0),
				"fee_hiz": float((session.exec(text('SELECT SUM(COALESCE(fee_hizmet,0)) FROM payment')).first() or [0])[0] or 0),
				"fee_iad": float((session.exec(text('SELECT SUM(COALESCE(fee_iade,0)) FROM payment')).first() or [0])[0] or 0),
				"fee_eok": float((session.exec(text('SELECT SUM(COALESCE(fee_erken_odeme,0)) FROM payment')).first() or [0])[0] or 0),
				"fee_kar": float((session.exec(text('SELECT SUM(COALESCE(shipping_fee,0)) FROM "order" WHERE COALESCE(status, "") NOT IN ("refunded","switched","stitched")')).first() or [0])[0] or 0),
				"linked_gross_paid": float((session.exec(text('SELECT SUM(amount) FROM payment WHERE order_id IS NOT NULL')).first() or [0])[0] or 0),
			},
		)
		total_sales = float(agg.get("total_sales", 0.0))
		net_collected = float(agg.get("net_collected", 0.0))
		fee_kom = float(agg.get("fee_kom", 0.0))
		fee_hiz = float(agg.get("fee_hiz", 0.0))
		fee_iad = float(agg.get("fee_iad", 0.0))
		fee_eok = float(agg.get("fee_eok", 0.0))
		fee_kar = float(agg.get("fee_kar", 0.0))
		total_fees = fee_kom + fee_hiz + fee_kar + fee_iad + fee_eok
		linked_gross_paid = float(agg.get("linked_gross_paid", 0.0))
		total_to_collect = max(0.0, float(total_sales) - linked_gross_paid)

		# Order status counts (cached)
		status_counts = cached_json(
			"dash:status_counts",
			ttl,
			lambda: [
				{"status": row[0] or "unknown", "count": int(row[1] or 0)}
				for row in session.exec(text('SELECT COALESCE(status, "unknown") AS s, COUNT(*) FROM "order" GROUP BY s')).all()
			],
		)

		# Best-selling low stock: all-time best sellers with on-hand <= 5, top 10
		from ..services.inventory import compute_all_time_sold_map, get_stock_map
		sold_map = compute_all_time_sold_map(session)
		stock_map_all = get_stock_map(session)
		candidates = [
			(iid, sold_map.get(iid, 0), int(stock_map_all.get(iid, 0)))
			for iid in sold_map.keys()
			if int(stock_map_all.get(iid, 0)) <= 5 and int(sold_map.get(iid, 0)) > 0
		]
		candidates.sort(key=lambda t: t[1], reverse=True)
		top_ids = [iid for iid, _sold, _onhand in candidates[:50]]  # fetch up to 50 to map names, then slice to 10
		items_top = session.exec(select(Item).where(Item.id.in_(top_ids))).all() if top_ids else []
		item_map = {it.id: it for it in items_top if it.id is not None}
		low_stock_best = []
		for iid, sold, onhand in candidates:
			if iid not in item_map:
				continue
			low_stock_best.append((item_map[iid], onhand, sold))
			if len(low_stock_best) >= 10:
				break

		templates = request.app.state.templates
		return templates.TemplateResponse(
			"dashboard.html",
			{
				"request": request,
				"total_sales": total_sales,
				"total_collected": net_collected,
				"total_to_collect": total_to_collect,
				"total_fees": total_fees,
				"order_status_counts": status_counts,
				"low_stock_best": low_stock_best,
			},
		)
