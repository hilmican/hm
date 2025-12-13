from typing import Dict, List, Optional, Tuple
import datetime as dt
from collections import defaultdict

from sqlmodel import Session, select, func
from sqlalchemy import case, text

from ..models import Account, Income, Cost, OrderPayment, Order, Payment


def calculate_account_balance(session: Session, account_id: int) -> float:
	"""Calculate current balance for an account.
	
	Balance = initial_balance + sum(income) - sum(expenses linked to account)
	"""
	account = session.exec(select(Account).where(Account.id == account_id)).first()
	if not account:
		return 0.0
	
	balance = float(account.initial_balance or 0.0)
	
	# Add all income entries
	income_total = session.exec(
		select(func.sum(Income.amount))
		.where(Income.account_id == account_id)
	).first()
	if income_total:
		balance += float(income_total or 0.0)
	
	# Subtract all expenses linked to this account
	expense_total = session.exec(
		select(func.sum(Cost.amount))
		.where(Cost.account_id == account_id)
	).first()
	if expense_total:
		balance -= float(expense_total or 0.0)
	
	return balance


def get_account_balances(session: Session) -> Dict[int, float]:
	"""Get current balances for all active accounts."""
	accounts = session.exec(select(Account).where(Account.is_active == True)).all()
	return {acc.id: calculate_account_balance(session, acc.id) for acc in accounts if acc.id is not None}


def get_unpaid_orders(session: Session, start_date: Optional[dt.date] = None, end_date: Optional[dt.date] = None) -> List[Order]:
	"""Get orders that haven't been marked as collected yet."""
	# Get all order IDs that have been collected
	collected_order_ids = session.exec(
		select(OrderPayment.order_id).distinct()
	).all()
	collected_set = set(collected_order_ids) if collected_order_ids else set()
	
	# Build query for unpaid orders
	q = select(Order)
	if collected_set:
		q = q.where(~Order.id.in_(collected_set))
	
	# Optional date filtering
	if start_date:
		q = q.where(
			(Order.shipment_date >= start_date) | (Order.data_date >= start_date)
		)
	if end_date:
		q = q.where(
			(Order.shipment_date <= end_date) | (Order.data_date <= end_date)
		)
	
	return session.exec(q.order_by(Order.shipment_date.desc(), Order.id.desc())).all()


def calculate_expected_payment(session: Session, order_id: int) -> float:
	"""Calculate expected payment amount for an order.
	
	Expected = total_amount - shipping_fee - platform fees
	For now, we'll use total_amount - shipping_fee as base.
	Platform fees are tracked separately in Payment model.
	"""
	order = session.exec(select(Order).where(Order.id == order_id)).first()
	if not order:
		return 0.0
	
	total = float(order.total_amount or 0.0)
	shipping = float(order.shipping_fee or 0.0)
	
	# Get platform fees from Payment records
	payments = session.exec(select(Payment).where(Payment.order_id == order_id)).all()
	total_fees = 0.0
	for p in payments:
		total_fees += float(p.fee_komisyon or 0.0)
		total_fees += float(p.fee_hizmet or 0.0)
		total_fees += float(p.fee_kargo or 0.0)
		total_fees += float(p.fee_iade or 0.0)
		total_fees += float(p.fee_erken_odeme or 0.0)
	
	# Expected payment = total - shipping - fees
	expected = total - shipping - total_fees
	return max(0.0, expected)


def mark_orders_collected(
	session: Session,
	order_ids: List[int],
	income_id: int,
	collected_amounts: Optional[Dict[int, float]] = None
) -> int:
	"""Mark multiple orders as collected and link them to an income entry.
	
	Args:
		session: Database session
		order_ids: List of order IDs to mark as collected
		income_id: Income entry ID that represents the bulk payment
		collected_amounts: Optional dict mapping order_id -> actual collected amount
		
	Returns:
		Number of OrderPayment records created
	"""
	count = 0
	now = dt.datetime.utcnow()
	
	for order_id in order_ids:
		# Calculate expected amount
		expected = calculate_expected_payment(session, order_id)
		
		# Get actual collected amount (use provided or expected)
		collected = expected
		if collected_amounts and order_id in collected_amounts:
			collected = collected_amounts[order_id]
		
		# Create OrderPayment record
		op = OrderPayment(
			income_id=income_id,
			order_id=order_id,
			expected_amount=expected,
			collected_amount=collected,
			collected_at=now
		)
		session.add(op)
		count += 1
	
	session.flush()
	return count


def detect_payment_leaks(session: Session, min_days_old: int = 7) -> List[Dict]:
	"""Detect orders that should have been paid but aren't marked as collected.
	
	Uses the same logic as the orders table:
	- Checks Payment table (not OrderPayment) for actual payments
	- Considers paid_by_bank_transfer flag
	- Handles partial payment groups
	- Excludes refunded/switched/stitched/cancelled orders
	
	Returns list of dicts with order info and expected payment amounts.
	"""
	cutoff_date = dt.date.today() - dt.timedelta(days=min_days_old)
	
	# Use efficient SQL query to find potentially unpaid orders
	# Similar logic to dashboard status counts query
	query = text("""
		SELECT 
			o.id,
			o.tracking_no,
			o.total_amount,
			o.shipment_date,
			o.data_date,
			o.paid_by_bank_transfer,
			o.status,
			o.partial_payment_group_id
		FROM `order` o
		WHERE o.merged_into_order_id IS NULL
		AND COALESCE(o.status, '') NOT IN ('refunded', 'switched', 'stitched', 'cancelled')
		AND (
			COALESCE(DATEDIFF(CURDATE(), COALESCE(o.shipment_date, o.data_date)), 0) > :min_days
			OR (o.shipment_date IS NULL AND o.data_date IS NULL)
		)
		AND (
			COALESCE(o.shipment_date, o.data_date) IS NULL
			OR COALESCE(o.shipment_date, o.data_date) <= :cutoff_date
		)
		AND NOT COALESCE(o.paid_by_bank_transfer, FALSE)
		AND COALESCE(o.total_amount, 0) > 0
		ORDER BY COALESCE(o.shipment_date, o.data_date) ASC, o.id ASC
		LIMIT 2000
	""").bindparams(min_days=min_days_old, cutoff_date=cutoff_date)
	
	rows = session.exec(query).all()
	
	if not rows:
		return []
	
	# Get all order IDs and group info
	order_ids = [row[0] for row in rows]
	partial_groups: Dict[int, List[int]] = defaultdict(list)
	order_map: Dict[int, Dict] = {}
	
	for row in rows:
		order_id = row[0]
		group_id = row[7]  # partial_payment_group_id
		
		order_map[order_id] = {
			"order_id": order_id,
			"tracking_no": row[1],
			"total_amount": float(row[2] or 0.0),
			"shipment_date": row[3],
			"data_date": row[4],
			"paid_by_bank_transfer": False,  # Already filtered
			"status": row[6],
			"partial_payment_group_id": group_id,
		}
		
		if group_id:
			partial_groups[group_id].append(order_id)
	
	# Get all payments for these orders in one query
	payments_query = session.exec(
		select(Payment.order_id, func.sum(Payment.amount).label("paid"))
		.where(Payment.order_id.in_(order_ids))
		.where(Payment.order_id.is_not(None))
		.group_by(Payment.order_id)
	).all()
	
	paid_map = {row.order_id: float(row.paid or 0.0) for row in payments_query if row.order_id}
	
	# For partial payment groups, sum payments across all orders in the group
	# Track which orders should be excluded (non-primary orders in groups)
	excluded_order_ids = set()
	
	for group_id, group_order_ids in partial_groups.items():
		if group_id in group_order_ids:  # Primary order exists in group
			total_group_paid = sum(paid_map.get(oid, 0.0) for oid in group_order_ids)
			order_map[group_id]["paid_amount"] = total_group_paid
			# Mark non-primary orders as excluded (they're part of the group)
			for oid in group_order_ids:
				if oid != group_id:
					excluded_order_ids.add(oid)
		else:
			# If primary order not in group, treat individually
			for oid in group_order_ids:
				order_map[oid]["paid_amount"] = paid_map.get(oid, 0.0)
	
	# Set paid amounts for non-group orders
	for order_id in order_ids:
		if order_id not in order_map:
			continue
		if "paid_amount" not in order_map[order_id]:
			order_map[order_id]["paid_amount"] = paid_map.get(order_id, 0.0)
	
	leaks = []
	for order_id, order_data in order_map.items():
		# Skip excluded orders (non-primary orders in partial payment groups)
		if order_id in excluded_order_ids:
			continue
		
		total = order_data["total_amount"]
		paid = order_data["paid_amount"]
		
		# Skip if paid by bank transfer (already filtered, but double-check)
		if order_data["paid_by_bank_transfer"]:
			continue
		
		# Skip if fully paid
		if paid >= total and total > 0:
			continue
		
		# Calculate expected payment (total - shipping - fees)
		expected = calculate_expected_payment(session, order_id)
		
		if expected > 0:
			leaks.append({
				"order_id": order_id,
				"tracking_no": order_data["tracking_no"],
				"total_amount": total,
				"expected_payment": expected,
				"shipment_date": order_data["shipment_date"],
				"data_date": order_data["data_date"],
			})
	
	return leaks

