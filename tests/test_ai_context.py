from app.services.ai_context import parse_variant_exclusions, variant_is_excluded


def test_parse_variant_exclusions_supports_json_and_tokens():
	payload = '{"sizes":["xs","s"],"colors":["beyaz"],"variants":[{"color":"siyah","size":"36"}]}'
	exclusions = parse_variant_exclusions(payload)
	assert variant_is_excluded(exclusions, size="XS")
	assert variant_is_excluded(exclusions, color="beyaz")
	assert variant_is_excluded(exclusions, color="Siyah", size="36")
	assert not variant_is_excluded(exclusions, color="siyah", size="38")


def test_parse_variant_exclusions_fallback_tokens():
	exclusions = parse_variant_exclusions("color:red,s, beden:xl")
	assert variant_is_excluded(exclusions, color="RED")
	assert variant_is_excluded(exclusions, size="s")
	assert variant_is_excluded(exclusions, size="XL")
	assert not variant_is_excluded(exclusions, color="blue")

