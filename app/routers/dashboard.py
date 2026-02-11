from fastapi import APIRouter, Request, HTTPException
from sqlmodel import select
from sqlalchemy import text, func, or_, not_
import os
import datetime as dt
from collections import defaultdict

from ..db import get_session
from ..models import Client, Item, Order, Payment, ImportRow, ImportRun, StockMovement
from ..services.shipping import compute_shipping_fee
from ..services.cache import cached_json
from ..services.finance import get_effective_total

router = APIRouter()


@router.get("/dashboard")
def dashboard(request: Request, days: int = 30):
	# require login
	uid = request.session.get("uid")
	if not uid:
		templates = request.app.state.templates
		return templates.TemplateResponse("login.html", {"request": request, "error": None})
	# pull small samples for quick display
	try:
		days = int(days)
	except Exception:
		days = 30
	days = max(1, min(days, 365))
	since_date = dt.date.today() - dt.timedelta(days=days)
	
	with get_session() as session:
		# Fast aggregates with short-lived cache
		ttl = int(os.getenv("CACHE_TTL_DASHBOARD", "60"))
		# detect dialect for cross-db SQL where needed
		backend = session.get_bind().dialect.name if session.get_bind() is not None else "mysql"
		order_base_date = func.coalesce(Order.shipment_date, Order.data_date)
		payment_base_date = func.coalesce(Payment.payment_date, Payment.date)
		def _scalar(sel):
			res = session.exec(sel).one_or_none()
			if res is None:
				return 0.0
			try:
				return float(res[0])
			except Exception:
				try:
					return float(res)
				except Exception:
					return 0.0
		def _agg():
			# Only count primary orders (exclude merged orders)
			# For partial payment groups, use primary order's total_amount
			# Window: last N days by order base date (shipment_date or data_date). Include NULL base dates.
			_recent_orders = or_(order_base_date.is_(None), order_base_date >= since_date)
			_recent_payments = payment_base_date >= since_date
			return {
				"total_sales": _scalar(select(func.coalesce(func.sum(Order.total_amount), 0)).where(
					Order.merged_into_order_id.is_(None),
					_recent_orders,
					or_(Order.status.is_(None), not_(Order.status.in_(["refunded", "switched", "stitched"])) )
				)),
				"net_collected": _scalar(select(func.coalesce(func.sum(Payment.net_amount), 0)).where(_recent_payments)),
				"fee_kom": _scalar(select(func.coalesce(func.sum(Payment.fee_komisyon), 0)).where(_recent_payments)),
				"fee_hiz": _scalar(select(func.coalesce(func.sum(Payment.fee_hizmet), 0)).where(_recent_payments)),
				"fee_iad": _scalar(select(func.coalesce(func.sum(Payment.fee_iade), 0)).where(_recent_payments)),
				"fee_eok": _scalar(select(func.coalesce(func.sum(Payment.fee_erken_odeme), 0)).where(_recent_payments)),
				"fee_kar": _scalar(select(func.coalesce(func.sum(Order.shipping_fee), 0)).where(
					Order.merged_into_order_id.is_(None),
					_recent_orders,
					or_(Order.status.is_(None), not_(Order.status.in_(["refunded", "switched", "stitched"])) )
				)),
				"linked_gross_paid": _scalar(select(func.coalesce(func.sum(Payment.amount), 0)).where(
					_recent_payments,
					Payment.order_id.is_not(None),
				)),
			}
		agg = cached_json(f"dash:totals:{days}", ttl, _agg)
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
			if backend.startswith("sqlite"):
				# SQLite fallback: compute in Python (sufficient for local/dev DB sizes)
				orders = session.exec(select(Order).where(
					Order.merged_into_order_id.is_(None),
					or_(order_base_date.is_(None), order_base_date >= since_date),
				)).all()
				paid_by_order = dict(session.exec(text("SELECT order_id, COALESCE(SUM(amount),0) FROM payment WHERE order_id IS NOT NULL GROUP BY order_id")).all())
				today = dt.date.today()
				for o in orders:
					st = (o.status or "").strip()
					if st in ("refunded", "switched", "stitched"):
						base[st] += 1
						continue
					total_amt = float(o.total_amount or 0.0)
					paid = float(paid_by_order.get(o.id, 0.0)) if o.id is not None else 0.0
					if total_amt > 0 and paid >= total_amt:
						base["tamamlandi"] += 1
						continue
					bd = o.shipment_date or o.data_date
					age = (today - bd).days if bd else 0
					if age <= 7:
						base["dagitimda"] += 1
					elif age <= 17:
						base["gecikmede"] += 1
					else:
						base["sorunlu"] += 1
				order = ["tamamlandi", "dagitimda", "gecikmede", "sorunlu", "refunded", "switched", "stitched"]
				return [{"status": k, "count": int(base.get(k, 0))} for k in order]

			# MySQL/MariaDB: explicit statuses
			rows_explicit = session.exec(text(
				"SELECT status, COUNT(*)\n"
				"FROM `order`\n"
				"WHERE status IN ('refunded','switched','stitched')\n"
				"  AND merged_into_order_id IS NULL\n"
				"  AND (COALESCE(shipment_date, data_date) IS NULL OR COALESCE(shipment_date, data_date) >= :since)\n"
				"GROUP BY status"
			).params(since=since_date)).all()
			for st, cnt in rows_explicit:
				if st in ("refunded", "switched", "stitched"):
					base[str(st)] = int(cnt or 0)

			# derived buckets for others (exclude merged orders)
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
				"WHERE COALESCE(o.status, '') NOT IN ('refunded','switched','stitched')\n"
				"  AND o.merged_into_order_id IS NULL\n"
				"  AND (COALESCE(o.shipment_date, o.data_date) IS NULL OR COALESCE(o.shipment_date, o.data_date) >= :since)"
			).params(since=since_date)).first() or [0, 0, 0, 0]
			base["tamamlandi"] = int(row_buckets[0] or 0)
			base["dagitimda"] = int(row_buckets[1] or 0)
			base["gecikmede"] = int(row_buckets[2] or 0)
			base["sorunlu"] = int(row_buckets[3] or 0)
			# return as list in consistent order used by UI
			order = ["tamamlandi", "dagitimda", "gecikmede", "sorunlu", "refunded", "switched", "stitched"]
			return [{"status": k, "count": base.get(k, 0)} for k in order]

		status_counts = cached_json(f"dash:status_counts:{days}", ttl, _compute_status_counts)

		# Ongoing status counts (excluding tamamlandi, refunded, switched)
		def _compute_ongoing_status_counts():
			base = {"dagitimda": 0, "gecikmede": 0, "sorunlu": 0, "stitched": 0}
			if backend.startswith("sqlite"):
				orders = session.exec(select(Order).where(
					Order.merged_into_order_id.is_(None),
					or_(order_base_date.is_(None), order_base_date >= since_date),
				)).all()
				paid_by_order = dict(session.exec(text("SELECT order_id, COALESCE(SUM(amount),0) FROM payment WHERE order_id IS NOT NULL GROUP BY order_id")).all())
				today = dt.date.today()
				for o in orders:
					if (o.status or "").strip() == "stitched":
						base["stitched"] += 1
						continue
					st = (o.status or "").strip()
					if st in ("refunded", "switched"):
						continue
					total_amt = float(o.total_amount or 0.0)
					paid = float(paid_by_order.get(o.id, 0.0)) if o.id is not None else 0.0
					if total_amt > 0 and paid >= total_amt:
						continue
					bd = o.shipment_date or o.data_date
					age = (today - bd).days if bd else 0
					if age <= 7:
						base["dagitimda"] += 1
					elif age <= 17:
						base["gecikmede"] += 1
					else:
						base["sorunlu"] += 1
				order = ["dagitimda", "gecikmede", "sorunlu", "stitched"]
				return [{"status": k, "count": int(base.get(k, 0))} for k in order]

			# explicit stitched status
			rows_stitched = session.exec(text(
				"SELECT COUNT(*)\n"
				"FROM `order`\n"
				"WHERE status = 'stitched'\n"
				"  AND merged_into_order_id IS NULL\n"
				"  AND (COALESCE(shipment_date, data_date) IS NULL OR COALESCE(shipment_date, data_date) >= :since)"
			).params(since=since_date)).first()
			base["stitched"] = int(rows_stitched[0] or 0) if rows_stitched else 0
			# derived buckets for ongoing (excluding tamamlandi, refunded, switched, and merged orders)
			row_buckets = session.exec(text(
				"SELECT\n"
				"  SUM(CASE WHEN NOT (COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0)\n"
				"           AND (COALESCE(DATEDIFF(CURDATE(), COALESCE(o.shipment_date, o.data_date)), 0) <= 7) THEN 1 ELSE 0 END) AS dagitimda,\n"
				"  SUM(CASE WHEN NOT (COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0)\n"
				"           AND (COALESCE(DATEDIFF(CURDATE(), COALESCE(o.shipment_date, o.data_date)), 0) > 7)\n"
				"           AND (COALESCE(DATEDIFF(CURDATE(), COALESCE(o.shipment_date, o.data_date)), 0) <= 17) THEN 1 ELSE 0 END) AS gecikmede,\n"
				"  SUM(CASE WHEN NOT (COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0)\n"
				"           AND (COALESCE(DATEDIFF(CURDATE(), COALESCE(o.shipment_date, o.data_date)), 0) > 17) THEN 1 ELSE 0 END) AS sorunlu\n"
				"FROM `order` o\n"
				"LEFT JOIN (SELECT order_id, SUM(amount) AS paid FROM payment GROUP BY order_id) p ON p.order_id = o.id\n"
				"WHERE COALESCE(o.status, '') NOT IN ('refunded','switched','stitched')\n"
				"  AND o.merged_into_order_id IS NULL\n"
				"  AND (COALESCE(o.shipment_date, o.data_date) IS NULL OR COALESCE(o.shipment_date, o.data_date) >= :since)"
			).params(since=since_date)).first() or [0, 0, 0]
			base["dagitimda"] = int(row_buckets[0] or 0)
			base["gecikmede"] = int(row_buckets[1] or 0)
			base["sorunlu"] = int(row_buckets[2] or 0)
			# return as list in consistent order used by UI
			order = ["dagitimda", "gecikmede", "sorunlu", "stitched"]
			return [{"status": k, "count": base.get(k, 0)} for k in order]

		ongoing_status_counts = cached_json(f"dash:ongoing_status_counts:{days}", ttl, _compute_ongoing_status_counts)

		# Order lifecycle distribution (bizim orders: bizim excel date to payment date)
		def _compute_lifecycle_distribution():
			# Only completed orders from bizim source
			# Use o.data_date as the bizim order creation date (from bizim Excel filename or shipment_date)
			# Use COALESCE(MAX(payment_date), MAX(date)) to get the actual payment date (from kargo Excel filename)
			# Buckets: 0-3, 4-6, 7-9, 10-12, 13-15, 16+ days
			if backend.startswith("sqlite"):
				# Lightweight SQLite fallback: compute distribution in Python
				orders = session.exec(select(Order).where(
					Order.merged_into_order_id.is_(None),
					Order.source == "bizim",
					Order.data_date.is_not(None),
					Order.data_date >= since_date,
					or_(Order.status.is_(None), not_(Order.status.in_(["refunded", "switched", "stitched"]))),
				)).all()
				paid_and_max = dict(session.exec(text(
					"SELECT order_id, COALESCE(SUM(amount),0) AS paid, COALESCE(MAX(payment_date), MAX(date)) AS completion_payment_date "
					"FROM payment WHERE order_id IS NOT NULL GROUP BY order_id"
				)).all())
				buckets = {"0-3": 0, "4-6": 0, "7-9": 0, "10-12": 0, "13-15": 0, "16+": 0}
				for o in orders:
					if o.id is None or o.data_date is None:
						continue
					row = paid_and_max.get(o.id)
					if not row:
						continue
					paid, comp_dt = row[0], row[1]
					try:
						paid = float(paid or 0.0)
					except Exception:
						paid = 0.0
					total_amt = float(o.total_amount or 0.0)
					if not (total_amt > 0 and paid >= total_amt):
						continue
					if not comp_dt:
						continue
					# sqlite returns str for dates sometimes
					if isinstance(comp_dt, str):
						try:
							comp_dt = dt.date.fromisoformat(comp_dt)
						except Exception:
							continue
					if isinstance(comp_dt, dt.datetime):
						comp_dt = comp_dt.date()
					if not isinstance(comp_dt, dt.date):
						continue
					delta = (comp_dt - o.data_date).days
					if delta < 0:
						continue
					if delta <= 3:
						buckets["0-3"] += 1
					elif delta <= 6:
						buckets["4-6"] += 1
					elif delta <= 9:
						buckets["7-9"] += 1
					elif delta <= 12:
						buckets["10-12"] += 1
					elif delta <= 15:
						buckets["13-15"] += 1
					else:
						buckets["16+"] += 1
				return [
					{"bucket": "0-3", "count": buckets["0-3"]},
					{"bucket": "4-6", "count": buckets["4-6"]},
					{"bucket": "7-9", "count": buckets["7-9"]},
					{"bucket": "10-12", "count": buckets["10-12"]},
					{"bucket": "13-15", "count": buckets["13-15"]},
					{"bucket": "16+", "count": buckets["16+"]},
				]

			row_buckets = session.exec(text(
				"SELECT\n"
				"  SUM(CASE WHEN COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0\n"
				"           AND o.source = 'bizim'\n"
				"           AND o.data_date IS NOT NULL\n"
				"           AND o.data_date >= :since\n"
				"           AND p.completion_payment_date IS NOT NULL\n"
				"           AND o.merged_into_order_id IS NULL\n"
				"           AND DATEDIFF(p.completion_payment_date, o.data_date) >= 0\n"
				"           AND DATEDIFF(p.completion_payment_date, o.data_date) <= 3 THEN 1 ELSE 0 END) AS days_0_3,\n"
				"  SUM(CASE WHEN COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0\n"
				"           AND o.source = 'bizim'\n"
				"           AND o.data_date IS NOT NULL\n"
				"           AND o.data_date >= :since\n"
				"           AND p.completion_payment_date IS NOT NULL\n"
				"           AND o.merged_into_order_id IS NULL\n"
				"           AND DATEDIFF(p.completion_payment_date, o.data_date) >= 4\n"
				"           AND DATEDIFF(p.completion_payment_date, o.data_date) <= 6 THEN 1 ELSE 0 END) AS days_4_6,\n"
				"  SUM(CASE WHEN COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0\n"
				"           AND o.source = 'bizim'\n"
				"           AND o.data_date IS NOT NULL\n"
				"           AND o.data_date >= :since\n"
				"           AND p.completion_payment_date IS NOT NULL\n"
				"           AND o.merged_into_order_id IS NULL\n"
				"           AND DATEDIFF(p.completion_payment_date, o.data_date) >= 7\n"
				"           AND DATEDIFF(p.completion_payment_date, o.data_date) <= 9 THEN 1 ELSE 0 END) AS days_7_9,\n"
				"  SUM(CASE WHEN COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0\n"
				"           AND o.source = 'bizim'\n"
				"           AND o.data_date IS NOT NULL\n"
				"           AND o.data_date >= :since\n"
				"           AND p.completion_payment_date IS NOT NULL\n"
				"           AND o.merged_into_order_id IS NULL\n"
				"           AND DATEDIFF(p.completion_payment_date, o.data_date) >= 10\n"
				"           AND DATEDIFF(p.completion_payment_date, o.data_date) <= 12 THEN 1 ELSE 0 END) AS days_10_12,\n"
				"  SUM(CASE WHEN COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0\n"
				"           AND o.source = 'bizim'\n"
				"           AND o.data_date IS NOT NULL\n"
				"           AND o.data_date >= :since\n"
				"           AND p.completion_payment_date IS NOT NULL\n"
				"           AND o.merged_into_order_id IS NULL\n"
				"           AND DATEDIFF(p.completion_payment_date, o.data_date) >= 13\n"
				"           AND DATEDIFF(p.completion_payment_date, o.data_date) <= 15 THEN 1 ELSE 0 END) AS days_13_15,\n"
				"  SUM(CASE WHEN COALESCE(p.paid,0) >= COALESCE(o.total_amount,0) AND COALESCE(o.total_amount,0) > 0\n"
				"           AND o.source = 'bizim'\n"
				"           AND o.data_date IS NOT NULL\n"
				"           AND o.data_date >= :since\n"
				"           AND p.completion_payment_date IS NOT NULL\n"
				"           AND o.merged_into_order_id IS NULL\n"
				"           AND DATEDIFF(p.completion_payment_date, o.data_date) >= 16 THEN 1 ELSE 0 END) AS days_16_plus\n"
				"FROM `order` o\n"
				"LEFT JOIN (\n"
				"  SELECT order_id, SUM(amount) AS paid, COALESCE(MAX(payment_date), MAX(date)) AS completion_payment_date\n"
				"  FROM payment\n"
				"  WHERE order_id IS NOT NULL\n"
				"  GROUP BY order_id\n"
				") p ON p.order_id = o.id\n"
				"WHERE COALESCE(o.status, '') NOT IN ('refunded','switched','stitched')\n"
				"  AND o.data_date >= :since"
			).params(since=since_date)).first() or [0, 0, 0, 0, 0, 0]
			return [
				{"bucket": "0-3", "count": int(row_buckets[0] or 0)},
				{"bucket": "4-6", "count": int(row_buckets[1] or 0)},
				{"bucket": "7-9", "count": int(row_buckets[2] or 0)},
				{"bucket": "10-12", "count": int(row_buckets[3] or 0)},
				{"bucket": "13-15", "count": int(row_buckets[4] or 0)},
				{"bucket": "16+", "count": int(row_buckets[5] or 0)},
			]

		lifecycle_distribution = cached_json(f"dash:lifecycle_distribution:{days}", ttl, _compute_lifecycle_distribution)

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

		# Daily series for charts (orders & finance)
		def _compute_daily_series():
			orders = session.exec(select(Order).where(
				Order.merged_into_order_id.is_(None),
				order_base_date.is_not(None),
				order_base_date >= since_date,
			)).all()
			count_map: dict[dt.date, dict[str, float]] = defaultdict(lambda: {"sales": 0, "cancelled": 0, "refunded": 0})
			fin_map: dict[dt.date, dict[str, float]] = defaultdict(lambda: {"revenue": 0.0, "cost": 0.0})
			for o in orders:
				day = o.shipment_date or o.data_date
				if not day:
					continue
				status = (o.status or "").strip().lower()
				is_cancelled = status == "cancelled"
				is_refund = status in ("refunded", "iade_bekliyor")
				is_skip = status in ("switched", "stitched")

				if is_cancelled:
					count_map[day]["cancelled"] += 1
				elif is_refund:
					count_map[day]["refunded"] += 1
				elif not is_skip:
					count_map[day]["sales"] += 1

				# Financials: include only active sales (exclude cancelled/refunded/degisim)
				if is_cancelled or is_refund or is_skip:
					continue
				try:
					revenue = float(get_effective_total(o) or 0.0)
				except Exception:
					revenue = float(o.total_amount or 0.0) if o.total_amount is not None else 0.0
				try:
					cost = float(o.total_cost or 0.0)
				except Exception:
					cost = 0.0
				try:
					shipping_cost = float(o.shipping_fee or 0.0)
				except Exception:
					shipping_cost = 0.0
				fin_map[day]["revenue"] += revenue
				fin_map[day]["cost"] += (cost + shipping_cost)

			daily_counts = []
			daily_financials = []
			today = dt.date.today()
			span_days = (today - since_date).days
			for i in range(span_days + 1):
				day = since_date + dt.timedelta(days=i)
				c = count_map.get(day, {"sales": 0, "cancelled": 0, "refunded": 0})
				f = fin_map.get(day, {"revenue": 0.0, "cost": 0.0})
				revenue = float(f.get("revenue", 0.0) or 0.0)
				cost = float(f.get("cost", 0.0) or 0.0)
				daily_counts.append({
					"date": day.isoformat(),
					"sales": int(c.get("sales", 0) or 0),
					"cancelled": int(c.get("cancelled", 0) or 0),
					"refunded": int(c.get("refunded", 0) or 0),
				})
				daily_financials.append({
					"date": day.isoformat(),
					"revenue": revenue,
					"cost": cost,
					"profit": revenue - cost,
				})
			return {"counts": daily_counts, "financials": daily_financials}

		daily_series = cached_json(f"dash:daily_series:{since_date.isoformat()}:{days}", ttl, _compute_daily_series)
		daily_counts = daily_series.get("counts", []) if isinstance(daily_series, dict) else []
		daily_financials = daily_series.get("financials", []) if isinstance(daily_series, dict) else []

		templates = request.app.state.templates
		return templates.TemplateResponse(
			"dashboard.html",
			{
				"request": request,
				"days": days,
				"since_date": since_date,
				"total_sales": total_sales,
				"total_collected": net_collected,
				"total_to_collect": total_to_collect,
				"total_fees": total_fees,
				"order_status_counts": status_counts,
				"ongoing_status_counts": ongoing_status_counts,
				"lifecycle_distribution": lifecycle_distribution,
				"low_stock_best": low_stock_best,
				"daily_counts": daily_counts,
				"daily_financials": daily_financials,
			},
		)
