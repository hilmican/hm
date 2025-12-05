from typing import Dict, List, Optional, Tuple
import datetime as dt

from sqlmodel import Session, select, func
from sqlalchemy import case

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
	
	Returns list of dicts with order info and expected payment amounts.
	"""
	cutoff_date = dt.date.today() - dt.timedelta(days=min_days_old)
	
	unpaid = get_unpaid_orders(session, start_date=None, end_date=cutoff_date)
	
	leaks = []
	for order in unpaid:
		if order.id is None:
			continue
		
		expected = calculate_expected_payment(session, order.id)
		if expected > 0:
			leaks.append({
				"order_id": order.id,
				"tracking_no": order.tracking_no,
				"total_amount": order.total_amount,
				"expected_payment": expected,
				"shipment_date": order.shipment_date,
				"data_date": order.data_date,
			})
	
	return leaks

