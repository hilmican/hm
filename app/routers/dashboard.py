from fastapi import APIRouter, Request, HTTPException
from sqlmodel import select
from sqlalchemy import text, func, or_, not_
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
		# detect dialect for cross-db SQL where needed
		backend = session.get_bind().dialect.name if session.get_bind() is not None else "sqlite"
		agg = cached_json(
			"dash:totals",
			ttl,
			lambda: {
				# Use ORM for DB-agnostic quoting
				"total_sales": float(
					(
						session.exec(
							select(func.coalesce(func.sum(Order.total_amount), 0)).where(
								or_(Order.status.is_(None), not_(Order.status.in_(["refunded", "switched", "stitched"]))
							)
						)
						.first()
						or [0]
					)[0]
					or 0
				),
				"net_collected": float((session.exec(select(func.coalesce(func.sum(Payment.net_amount), 0))).first() or [0])[0] or 0),
				"fee_kom": float((session.exec(select(func.coalesce(func.sum(Payment.fee_komisyon), 0))).first() or [0])[0] or 0),
				"fee_hiz": float((session.exec(select(func.coalesce(func.sum(Payment.fee_hizmet), 0))).first() or [0])[0] or 0),
				"fee_iad": float((session.exec(select(func.coalesce(func.sum(Payment.fee_iade), 0))).first() or [0])[0] or 0),
				"fee_eok": float((session.exec(select(func.coalesce(func.sum(Payment.fee_erken_odeme), 0))).first() or [0])[0] or 0),
				"fee_kar": float(
					(
						session.exec(
							select(func.coalesce(func.sum(Order.shipping_fee), 0)).where(
								or_(Order.status.is_(None), not_(Order.status.in_(["refunded", "switched", "stitched"]))
							)
						)
						.first()
						or [0]
					)[0]
					or 0
				),
				"linked_gross_paid": float((session.exec(select(func.coalesce(func.sum(Payment.amount), 0)).where(Payment.order_id.is_not(None))).first() or [0])[0] or 0),
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



		# Order status counts (cached) — buckets in Turkish UI terms:
		# - tamamlandi: paid (payments sum >= total)
		# - dagitimda: unpaid and base_date within last 7 days or missing
		# - gecikmede: unpaid and 8..17 days old
		# - sorunlu: unpaid and >17 days old
		# Plus explicit statuses: refunded, switched, stitched
		def _compute_status_counts():
			base = {"tamamlandi": 0, "dagitimda": 0, "gecikmede": 0, "sorunlu": 0, "refunded": 0, "switched": 0, "stitched": 0}
			# explicit statuses
			if backend == "mysql":
				rows_explicit = session.exec(text("SELECT status, COUNT(*) FROM `order` WHERE status IN ('refunded','switched','stitched') GROUP BY status")).all()
			else:
				rows_explicit = session.exec(text('SELECT status, COUNT(*) FROM "order" WHERE status IN ("refunded","switched","stitched") GROUP BY status')).all()
			for st, cnt in rows_explicit:
				if st in ("refunded", "switched", "stitched"):
					base[str(st)] = int(cnt or 0)
			# derived buckets for others
			if backend == "mysql":
				row_buckets = session.exec(text(
					"SELECT\n"
					"  SUM(CASE WHEN COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0 THEN 1 ELSE 0 END) AS tamamlandi,\n"
					"  SUM(CASE WHEN NOT (COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0)\n"
					"           AND (COALESCE(DATEDIFF(CURDATE(), COALESCE(o.shipment_date, o.data_date)), 0) <= 7) THEN 1 ELSE 0 END) AS dagitimda,\n"
					"  SUM(CASE WHEN NOT (COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0)\n"
					"           AND (COALESCE(DATEDIFF(CURDATE(), COALESCE(o.shipment_date, o.data_date)), 0) > 7)\n"
					"           AND (COALESCE(DATEDIFF(CURDATE(), COALESCE(o.shipment_date, o.data_date)), 0) <= 17) THEN 1 ELSE 0 END) AS gecikmede,\n"
					"  SUM(CASE WHEN NOT (COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0)\n"
					"           AND (COALESCE(DATEDIFF(CURDATE(), COALESCE(o.shipment_date, o.data_date)), 0) > 17) THEN 1 ELSE 0 END) AS sorunlu\n"
					"FROM `order` o\n"
					"LEFT JOIN (SELECT order_id, SUM(amount) AS paid FROM payment GROUP BY order_id) p ON p.order_id = o.id\n"
					"WHERE COALESCE(o.status, '') NOT IN ('refunded','switched','stitched')"
				)).first() or [0, 0, 0, 0]
			else:
				row_buckets = session.exec(text(
					'SELECT\n'
					'  SUM(CASE\n'
					'        WHEN COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0 THEN 1\n'
					'        ELSE 0 END) AS tamamlandi,\n'
					'  SUM(CASE\n'
					'        WHEN NOT (COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0)\n'
					'         AND (COALESCE(julianday(date("now")) - julianday(COALESCE(o.shipment_date, o.data_date)), 0) <= 7) THEN 1\n'
					'        ELSE 0 END) AS dagitimda,\n'
					'  SUM(CASE\n'
					'        WHEN NOT (COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0)\n'
					'         AND (COALESCE(julianday(date("now")) - julianday(COALESCE(o.shipment_date, o.data_date)), 0) > 7)\n'
					'         AND (COALESCE(julianday(date("now")) - julianday(COALESCE(o.shipment_date, o.data_date)), 0) <= 17) THEN 1\n'
					'        ELSE 0 END) AS gecikmede,\n'
					'  SUM(CASE\n'
					'        WHEN NOT (COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0)\n'
					'         AND (COALESCE(julianday(date("now")) - julianday(COALESCE(o.shipment_date, o.data_date)), 0) > 17) THEN 1\n'
					'        ELSE 0 END) AS sorunlu\n'
					'FROM "order" o\n'
					'LEFT JOIN (SELECT order_id, SUM(amount) AS paid FROM payment GROUP BY order_id) p ON p.order_id = o.id\n'
					'WHERE COALESCE(o.status, "") NOT IN ("refunded","switched","stitched")'
				)).first() or [0, 0, 0, 0]
			base["tamamlandi"] = int(row_buckets[0] or 0)
			base["dagitimda"] = int(row_buckets[1] or 0)
			base["gecikmede"] = int(row_buckets[2] or 0)
			base["sorunlu"] = int(row_buckets[3] or 0)
			# return as list in consistent order used by UI
			order = ["tamamlandi", "dagitimda", "gecikmede", "sorunlu", "refunded", "switched", "stitched"]
			return [{"status": k, "count": base.get(k, 0)} for k in order]

		status_counts = cached_json("dash:status_counts", ttl, _compute_status_counts)

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
