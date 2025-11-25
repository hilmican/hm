from app.services.ai_utils import parse_height_weight


def test_parse_height_weight_accepts_decimal_height():
	result = parse_height_weight("boyum 1.60 kilom 78")
	assert result["height_cm"] == 160
	assert result["weight_kg"] == 78


def test_parse_height_weight_accepts_space_separated_height():
	result = parse_height_weight("boyum 1 75 kilo 82")
	assert result["height_cm"] == 175
	assert result["weight_kg"] == 82


def test_parse_height_weight_accepts_spelled_height():
	result = parse_height_weight("boyum bir atmış kilom 78")
	assert result["height_cm"] == 160
	assert result["weight_kg"] == 78

