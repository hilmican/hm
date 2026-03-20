"""
FOCUS express × Sürat Kargo etiketi — sabit kutu düzeni için OCR ayrıştırma.

Tespit: QR (surat/focus URL veya barkod=…) veya OCR parmak izi (FOCUS + Sürat + alan başlıkları).
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from ..kargo_ocr_common import (
    empty_label_dict,
    low_tr,
    normalize_phone,
    parse_money,
    reject_bad_name,
    split_address_city,
    strip_trailing_ic_from_blob,
)


def is_focus_surat_qr(qr: str) -> bool:
	"""Okunan kodun Focus / Sürat kaynaklı olduğuna dair güçlü sinyal."""
	q = (qr or "").strip()
	if not q:
		return False
	ql = q.lower()
	if ql.startswith("{") and ql.endswith("}"):
		try:
			obj = json.loads(q)
			if isinstance(obj, dict):
				blob = json.dumps(obj, ensure_ascii=False).lower()
				if "surat" in blob or "focus" in blob:
					return True
		except Exception:
			pass
	if "barkod=" in ql:
		return True
	if "suratkargo" in ql.replace(" ", "") or "webservices.surat" in ql:
		return True
	if "focus" in ql and ("express" in ql or "focusexpress" in ql.replace(" ", "")):
		return True
	if "://" in q or q.startswith("http"):
		try:
			u = urlparse(q if "://" in q else f"https://{q}")
			host = (u.netloc or "").lower()
			qs = f"{u.path}?{u.query}".lower()
			if "surat" in host or "focus" in host:
				return True
			if "barkod=" in qs or "surat" in qs:
				return True
		except Exception:
			pass
	return False


def is_focus_surat_ocr_fingerprint(ocr: str) -> bool:
	"""Saf barkod QR sonrası etiket fotoğrafından gelen metin (logo + başlıklar)."""
	t = low_tr(ocr)
	if len(t) < 40:
		return False
	has_focus = "focus" in t and "express" in t
	has_surat_brand = ("sürat" in (ocr or "")) or ("surat" in t and "kargo" in t)
	keys = sum(
		1
		for k in ("alıcı", "adres", "içerik", "tahsilat", "gönderen")
		if k in t
	)
	if has_focus and (has_surat_brand or keys >= 3):
		return True
	if has_surat_brand and keys >= 3:
		return True
	if has_focus and keys >= 4:
		return True
	return False


def should_use_focus_surat_parser(qr_content: Optional[str], ocr_text: str) -> bool:
	o = (ocr_text or "").strip()
	if not o:
		return False
	if qr_content and is_focus_surat_qr(str(qr_content)):
		return True
	if is_focus_surat_ocr_fingerprint(o):
		return True
	return False


def _normalize_raw(ocr: str) -> str:
	return re.sub(r"\r\n?", "\n", str(ocr))


def _soft_fix_focus_ocr_typos(s: str) -> str:
	"""Sık OCR karışıklıkları — yapısal parse öncesi hafif düzeltme."""
	t = s
	# Alıcı etiketi düşük kalite
	t = re.sub(r"(?i)\bA1ici\s*:", "Alıcı:", t)
	t = re.sub(r"(?i)\bAlici\s*:", "Alıcı:", t)
	return t


def _split_gonderen_alici_from_line(s: str) -> Optional[str]:
	"""
	Tek satırda 'Gönderen: ... Alıcı: İSİM' veya sadece 'Alıcı: ...' sağ parça.
	"""
	m = re.search(
		r"(?is)\b(?:gönderen|gonderen)\s*:\s*.+?\b(?:alıcı|alici)\s*[:\.]?\s*(.+?)"
		r"(?=\s*\+?\s*90\s*\d|\s*90\s*5\d{2}\s|\s*0\s*5\d{2}\s|\s*adres\s*[:\.]|\s*tarih\s*:|\Z)",
		s,
	)
	if m:
		cand = m.group(1).strip()
		if cand:
			return cand
	m2 = re.search(
		r"(?is)\b(?:alıcı|alici)\s*[:\.]?\s*(.+?)"
		r"(?=\s*\+?\s*90\s*\d|\s*90\s*5\d{2}\s|\s*0\s*5\d{2}\s|\s*adres\s*[:\.]|\s*tarih\s*:|\Z)",
		s,
	)
	if m2:
		cand = m2.group(1).strip()
		if cand and "gönderen" not in low_tr(cand) and "gonderen" not in low_tr(cand):
			return cand
	return None


def _looks_like_recipient_name_line(line: str, tracking_hint: Optional[str]) -> bool:
	s = (line or "").strip()
	if len(s) < 4 or len(s) > 140:
		return False
	low = low_tr(s)
	if any(
		x in low
		for x in (
			"gönderen",
			"gonderen",
			"adres:",
			"adres ",
			"içerik",
			"icerik",
			"tahsilat",
			"focus",
			"express",
			"mah.",
			"mah ",
			"sok.",
			"sürat",
			"surat",
			"tarih:",
		)
	):
		return False
	if tracking_hint and tracking_hint.replace(" ", "") in s.replace(" ", ""):
		return False
	if re.search(r"\d{12,16}", s.replace(" ", "")):
		return False
	parts = re.findall(r"[A-Za-zğüşıöçĞÜŞİÖÇ]{2,}", s)
	if len(parts) >= 2:
		return True
	if len(parts) == 1 and 8 <= len(parts[0]) <= 40:
		return True
	return False


def _name_line_before_phone(lines: list[str], tracking_hint: Optional[str]) -> Optional[str]:
	"""Telefon satırının hemen üstünde genelde alıcı adı (Alıcı etiketi OCR'da yoksa)."""
	for i, ln in enumerate(lines):
		if not re.search(
			r"(?:\+?\s*90\s*5\d|\+90\s*\d{10}|\+90\d{10}|905\d{9,10}|0\s*5\d{2}\s*\d{3})",
			ln,
		):
			continue
		if i == 0:
			continue
		prev = lines[i - 1].strip()
		if _looks_like_recipient_name_line(prev, tracking_hint):
			return prev
	return None


def _extract_phone_loose(big: str) -> Optional[str]:
	"""Boşluklu/birleşik cep telefonu."""
	for pat in (
		r"\+?\s*90\s*5\d{2}\s*\d{3}\s*\d{2}\s*\d{2}",
		r"90\s*5\d{2}\s*\d{3}\s*\d{2}\s*\d{2}",
		r"0\s*5\d{2}\s*\d{3}\s*\d{2}\s*\d{2}",
	):
		m = re.search(pat, big)
		if m:
			return normalize_phone(re.sub(r"\s+", "", m.group(0)))
	return None


def parse_focus_surat_label(ocr: str, *, tracking_hint: Optional[str] = None) -> Dict[str, Any]:
	"""
	Etiket satır düzeni: Gönderen | Alıcı (+tel), Adres, İçerik, Tahsilat, barkod.
	Önce şablona özel desenler; eksikler generic ile tamamlanmaz — burada tam doldurulur.
	"""
	out = empty_label_dict()
	out["shipping_company"] = "surat"
	raw = _normalize_raw(ocr)
	raw = _soft_fix_focus_ocr_typos(raw)
	lines = [re.sub(r"\s+", " ", ln.strip()) for ln in raw.split("\n") if ln.strip()]
	big = " ".join(lines)

	if tracking_hint:
		out["tracking_no"] = tracking_hint.strip()

	# Barkod (hint yoksa)
	if not out.get("tracking_no"):
		tm = re.search(r"\b(\d{12,16})\b", big)
		if tm:
			out["tracking_no"] = tm.group(1)

	# Tahsilat (büyük punto)
	for m in re.finditer(
		r"Tahsilat\s*:?\s*([\d]+(?:[.,][\d]+)?)\s*(?:₺|TL)?", big, re.IGNORECASE
	):
		amt = parse_money(m.group(1))
		if amt is not None:
			out["total_amount"] = amt
			out["payment_amount"] = amt
			break

	# İçerik
	ic_m = re.search(
		r"(?is)(?:i̇çerik|içerik|icerik)\s*[:\.]?\s*(.+?)(?=\s*tahsilat\s*[:\.]?|tahsilat\s*\d|₺\s*\d|\Z)",
		raw,
	)
	if ic_m:
		ic_raw = ic_m.group(1).strip()
		ic_raw = re.split(r"(?is)\s*tahsilat\s*", ic_raw, maxsplit=1)[0].strip()
		if ic_raw:
			out["notes"] = ic_raw

	# Tek satırda: … Gönderen: … Alıcı: İSİM …
	ga = re.search(
		r"(?is)(?:alıcı|alici)\s*[:\.]?\s*(.+?)(?=\s*\+?\s*90\s*\d|\s*0\s*5\d{2}\s*\d{3}|\badres\s*[:\.])",
		raw,
	)
	if ga:
		out["name"] = reject_bad_name(ga.group(1).strip(), tracking_hint)

	# Aynı satırda Gönderen … Alıcı … (isim bazen dar)
	if not out.get("name"):
		ga2 = re.search(
			r"(?is)(?:alıcı|alici)\s*[:\.]?\s*([A-Za-zğüşıöçĞÜŞİÖÇ][A-Za-zğüşıöçĞÜŞİÖÇ\s\.\-]{1,80}?)"
			r"(?=\s*\+?\s*90\s|\s*0\s*5\d{2}\s|\s*adres\s)",
			raw,
		)
		if ga2:
			out["name"] = reject_bad_name(ga2.group(1).strip(), tracking_hint)

	pm = re.search(r"(\+90\s*[\d\s]{10,14}|0\s*5\d{2}\s*\d{3}\s*\d{2}\s*\d{2})", big)
	if pm:
		out["phone"] = normalize_phone(re.sub(r"\s+", "", pm.group(1)))

	# Adres (İçerik / Tahsilat öncesi)
	if not out.get("address"):
		adr_m = re.search(
			r"(?is)\badres\s*[:\.]?\s*(.+?)(?=\s*(?:i̇çerik|içerik|icerik)\s*[:\.]?|\s*tahsilat\s*[:\.]?\s*\d|tahsilat\s*\d|\Z)",
			raw,
		)
		if adr_m:
			addr_blob = adr_m.group(1).strip()
			addr_clean, ic_from_addr = strip_trailing_ic_from_blob(addr_blob)
			if ic_from_addr and not out.get("notes"):
				out["notes"] = ic_from_addr
			if addr_clean:
				street, city = split_address_city(addr_clean)
				if city:
					city2, ic_from_city = strip_trailing_ic_from_blob(city)
					if ic_from_city and not out.get("notes"):
						out["notes"] = ic_from_city
					city = city2
				out["address"] = street or addr_clean
				out["city"] = city

	# Satır tabanlı düşüm (OCR satırları net ise)
	if not out.get("name"):
		for i, ln in enumerate(lines):
			low = low_tr(ln)
			if "alıcı" in low or low.startswith("alici"):
				if ":" in ln:
					rest = ln.split(":", 1)[1].strip()
					out["name"] = reject_bad_name(rest.split("+")[0].strip(), tracking_hint)
				if not out.get("name") and i + 1 < len(lines):
					cand = lines[i + 1].strip()
					cl = low_tr(cand)
					if (
						cand
						and "adres" not in cl
						and "gönderen" not in cl
						and "alıcı" not in cl
					):
						out["name"] = reject_bad_name(cand, tracking_hint)
				break

	if not out.get("address"):
		for i, ln in enumerate(lines):
			low = low_tr(ln)
			if "adres:" in low or low.startswith("adres"):
				first = ln.split(":", 1)[1].strip() if ":" in ln else ""
				chunks = [first] if first else []
				for j in range(i + 1, len(lines)):
					l2 = lines[j]
					l2l = low_tr(l2)
					if any(
						l2l.startswith(x)
						for x in ("içerik:", "icerik:", "tahsilat", "gönderen:", "gonderen:")
					):
						break
					if "tahsilat" in l2l and "adres" not in l2l:
						break
					chunks.append(l2)
				addr_full = " ".join(chunks).strip()
				addr_clean, ic_from_addr = strip_trailing_ic_from_blob(addr_full)
				if ic_from_addr and not out.get("notes"):
					out["notes"] = ic_from_addr
				if addr_clean:
					street, city = split_address_city(addr_clean)
					if city:
						city2, ic_from_city = strip_trailing_ic_from_blob(city)
						if ic_from_city and not out.get("notes"):
							out["notes"] = ic_from_city
						city = city2
					out["address"] = street or addr_clean
					out["city"] = city
				break

	if not out.get("notes"):
		for i, ln in enumerate(lines):
			low = low_tr(ln)
			if "içerik:" in low or "icerik:" in low:
				rest = ln.split(":", 1)[1].strip()
				parts = [rest] if rest else []
				for j in range(i + 1, len(lines)):
					l2 = lines[j]
					l2l = low_tr(l2)
					if "tahsilat" in l2l or "sürat" in l2l or "surat" in l2l:
						break
					parts.append(l2)
				ic = " ".join(parts).strip()
				if ic:
					out["notes"] = ic
				break

	# OCR bazen gönderen satırını alıcı sanır veya alıcı/telefon bloğunu düşürür.
	if out.get("name"):
		lnm = low_tr(out["name"])
		if "focus" in lnm or "kansuz" in lnm or "umutcan" in lnm:
			out["name"] = None

	if not out.get("name"):
		for block in (big,):
			cand = _split_gonderen_alici_from_line(block)
			if cand:
				out["name"] = reject_bad_name(cand, tracking_hint)
				break
	if not out.get("name"):
		for ln in lines:
			cand = _split_gonderen_alici_from_line(ln)
			if cand:
				out["name"] = reject_bad_name(cand, tracking_hint)
				break
	if not out.get("name"):
		fb = _name_line_before_phone(lines, tracking_hint)
		if fb:
			out["name"] = reject_bad_name(fb, tracking_hint)

	if not out.get("phone"):
		ph = _extract_phone_loose(big)
		if ph:
			out["phone"] = ph

	return out


def maybe_parse_focus_surat(
	ocr_text: str,
	*,
	qr_content: Optional[str] = None,
	tracking_hint: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
	"""Şablon uygulanacaksa dict, değilse None."""
	if not should_use_focus_surat_parser(qr_content, ocr_text):
		return None
	return parse_focus_surat_label(ocr_text, tracking_hint=tracking_hint)
