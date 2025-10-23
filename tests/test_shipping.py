from app.services.shipping import compute_shipping_fee


def test_shipping_fee_boundaries():
	# exact boundaries
	assert compute_shipping_fee(0.01) == 89.0 + 17.81
	assert compute_shipping_fee(500) == 89.0 + 17.81
	assert compute_shipping_fee(501) == 89.0 + 31.46
	assert compute_shipping_fee(1000) == 89.0 + 31.46
	assert compute_shipping_fee(1001) == 89.0 + 58.76
	assert compute_shipping_fee(2000) == 89.0 + 58.76
	assert compute_shipping_fee(2001) == 89.0 + 86.06
	assert compute_shipping_fee(3000) == 89.0 + 86.06
	assert compute_shipping_fee(3001) == 89.0 + 113.36
	assert compute_shipping_fee(4000) == 89.0 + 113.36
	assert compute_shipping_fee(4001) == 89.0 + 140.66
	assert compute_shipping_fee(5000) == 89.0 + 140.66


def test_shipping_fee_over_5000():
	# 1.50% of amount + base, rounded to 2 decimals
	amt = 6666.66
	expected = round(89.0 + round(amt * 0.015, 2), 2)
	assert compute_shipping_fee(amt) == expected


