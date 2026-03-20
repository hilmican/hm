"""FOCUS × Sürat şablon OCR ve QR sinyali."""
from app.services.kargo_label_text_parse import parse_kargo_label_ocr_text
from app.services.kargo_templates.focus_surat import is_focus_surat_qr, maybe_parse_focus_surat


_SAMPLE_OCR = """
FOCUS express
TARİH: 2026-03-19 11:00:04
89070731395831
Gönderen: FOCUS 543 / UMUTCAN KANSUZ
Alıcı: ZAKARIA OTHMAN
+90 5394963258
Adres: CEDİT ALİ PAŞA MAH. ÇAYIR SOK. NO:16 İ.K.NO:5 Marmaraereğlisi / Tekirdağ
İçerik: M-SİYAH DUBLE KUMAŞ TAKIM (170,70)
Tahsilat: 1530.00 ₺
Sürat Kargo
"""


def test_is_focus_surat_qr_barkod_param():
	assert is_focus_surat_qr("https://etiket.example/?barkod=89070731395831")


def test_is_focus_surat_qr_generic_json_no_false_positive():
	assert not is_focus_surat_qr('{"takip_no":"89070731395831"}')


def test_maybe_parse_plain_qr_and_plain_ocr_returns_none():
	"""Ne QR sinyali ne de etiket parmak izi yoksa şablon devreye girmez."""
	assert maybe_parse_focus_surat("rastgele metin 8907", qr_content="plain_digits_only_8907") is None


def test_parse_via_api_helpers_with_barkod_qr():
	out = parse_kargo_label_ocr_text(
		_SAMPLE_OCR,
		tracking_hint="89070731395831",
		qr_content="?barkod=89070731395831",
	)
	assert out.get("shipping_company") == "surat"
	assert out.get("name") == "ZAKARIA OTHMAN"
	assert out.get("phone")
	assert "CEDİT" in (out.get("address") or "")
	assert "Tekirdağ" in (out.get("city") or "")
	assert "DUBLE" in (out.get("notes") or "")
	assert out.get("total_amount") == 1530.0


def test_parse_via_ocr_fingerprint_without_qr():
	out = parse_kargo_label_ocr_text(_SAMPLE_OCR, tracking_hint="89070731395831", qr_content=None)
	assert out.get("name") == "ZAKARIA OTHMAN"
	assert out.get("total_amount") == 1530.0
