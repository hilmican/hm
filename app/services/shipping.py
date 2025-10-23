from __future__ import annotations


def compute_shipping_fee(amount: float) -> float:
	"""Compute shipping fee as base + fractional by TahsilatTutari.

	Rules (confirmed):
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


