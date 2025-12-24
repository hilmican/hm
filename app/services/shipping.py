from __future__ import annotations
from typing import Optional
import json

from sqlmodel import Session, select

from ..models import ShippingCompanyRate


def _compute_shipping_fee_mng(amount: float) -> float:
	"""Compute MNG shipping fee (default/legacy rates).
	
	Rules:
	- Base cost always applies per shipment: 89.00
	- Fractional by collected amount (TahsilatTutari) in TL:
	  <= 500: 17.81
	  <= 1000: 31.46
	  <= 2000: 58.76
	  <= 3000: 86.06
	  <= 4000: 113.36
	  <= 5000: 140.66
	  > 5000: 1.50% of amount (rounded to 2 decimals)

	Returns total rounded to 2 decimals.
	"""
	base = 89.0
	a = float(amount or 0.0)
	# Zero or negative totals still incur base fee only
	if a <= 0:
		return round(base, 2)
	if a <= 500:
		frac = 17.81
	elif a <= 1000:
		frac = 31.46
	elif a <= 2000:
		frac = 58.76
	elif a <= 3000:
		frac = 86.06
	elif a <= 4000:
		frac = 113.36
	elif a <= 5000:
		frac = 140.66
	else:
		frac = round(a * 0.015, 2)
	return round(base + frac, 2)


def compute_shipping_fee(
	amount: float,
	company_code: Optional[str] = None,
	paid_by_bank_transfer: bool = False,
	session: Optional[Session] = None
) -> float:
	"""Compute shipping fee based on company rates.
	
	Args:
		amount: Order total amount
		company_code: Shipping company code (surat|mng|dhl|ptt). If None, defaults to Sürat Kargo.
		paid_by_bank_transfer: If True, only base fee applies (IBAN ödeme)
		session: Optional database session for fetching rates. If None, uses default Sürat Kargo rates.
	
	Returns:
		Total shipping fee rounded to 2 decimals.
	"""
	a = float(amount or 0.0)
	
	# IBAN ödemelerinde sadece base fee (kargo firmasına göre değişebilir)
	if paid_by_bank_transfer:
		if company_code and session:
			# Kargo firmasının base fee'sini kullan
			rate = session.exec(
				select(ShippingCompanyRate)
				.where(ShippingCompanyRate.company_code == company_code)
				.where(ShippingCompanyRate.is_active == True)
			).first()
			if rate:
				return round(float(rate.base_fee or 89.0), 2)
		# Default Sürat Kargo base
		return 89.0
	
	# Company-specific rates
	if company_code and session:
		rate = session.exec(
			select(ShippingCompanyRate)
			.where(ShippingCompanyRate.company_code == company_code)
			.where(ShippingCompanyRate.is_active == True)
		).first()
		
		if rate and rate.rates_json:
			try:
				rates = json.loads(rate.rates_json)
				base = float(rate.base_fee or 89.0)
				
				# Zero or negative totals still incur base fee only
				if a <= 0:
					return round(base, 2)
				
				# Find matching rate tier
				for tier in rates:
					max_val = tier.get("max")
					fee = tier.get("fee")
					fee_percent = tier.get("fee_percent")
					
					if max_val is None or a <= max_val:
						if fee_percent is not None:
							# Percentage-based fee
							frac = round(a * (fee_percent / 100.0), 2)
						elif fee is not None:
							# Fixed fee
							frac = float(fee)
						else:
							frac = 0.0
						return round(base + frac, 2)
				
				# Fallback: if no tier matches, use base only
				return round(base, 2)
			except Exception:
				# JSON parse error or invalid structure, fall back to Sürat
				pass
	
	# Default Sürat Kargo rates (backward compatibility - same rates as old MNG)
	return _compute_shipping_fee_mng(a)


