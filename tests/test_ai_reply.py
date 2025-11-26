from app.services.ai_reply import _sanitize_reply_text


def test_sanitize_removes_control_sequences():
	dirty = "S\x1fs\x1fnf vinleks"
	clean = _sanitize_reply_text(dirty)
	assert clean == "Ssnf vinleks"


def test_sanitize_transliterates_turkish_chars():
	text = "Ürün tek renk siyah geliyor 1599₺"
	clean = _sanitize_reply_text(text)
	assert "Urun tek renk siyah geliyor 1599" == clean

