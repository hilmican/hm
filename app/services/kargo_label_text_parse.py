"""Extract carrier label fields from raw OCR text (Sürat / Focus-style Turkish layouts)."""
from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple

from .kargo_templates.focus_surat import maybe_parse_focus_surat


def _low_tr(s: str) -> str:
    """Lowercase with Turkish İ/I → i/ı so keyword checks work after OCR."""
    return (s or "").replace("İ", "i").replace("I", "ı").lower()


def _parse_money(s: str) -> Optional[float]:
    s = s.strip().replace("₺", "").replace("TL", "").replace("tl", "").strip()
    s = s.replace(".", "").replace(",", ".") if "," in s and "." in s else s.replace(",", ".")
    try:
        v = float(s)
        return v if v == v and v >= 0 else None
    except ValueError:
        return None


def _normalize_phone(raw: str) -> Optional[str]:
    r = re.sub(r"[\s\-()]", "", (raw or "").strip())
    if not r:
        return None
    if r.startswith("+90"):
        r = r[3:]
    if r.startswith("90") and len(r) >= 12:
        r = r[2:]
    if r.startswith("0") and len(r) >= 10:
        return r
    if len(r) == 10 and r.isdigit():
        return "0" + r
    return raw.strip() or None


def _split_address_city(address: str) -> Tuple[str, Optional[str]]:
    """Trailing 'ilçe / il' → city, rest = street address."""
    a = (address or "").strip()
    if not a:
        return "", None
    parts = [p.strip() for p in a.split("/") if p.strip()]
    if len(parts) >= 2:
        city = parts[-1].strip()
        street = " / ".join(parts[:-1]).strip()
        return street, city or None
    return a, None


def _strip_trailing_ic_from_blob(blob: str) -> Tuple[str, Optional[str]]:
    """
    OCR tek satırda '... Tekirdağ İçerik: ...' birleştirirse adres'ten içerik ayır.
    Returns (clean_blob, ic_text_or_none).
    """
    b = (blob or "").strip()
    if not b:
        return "", None
    m = re.split(r"(?is)\s+(?:i̇çerik|içerik|icerik)\s*[:\.]?\s*", b, maxsplit=1)
    if len(m) == 2 and m[1].strip():
        left, right = m[0].strip(), m[1].strip()
        right = re.split(r"(?is)\s*tahsilat\s*[:\.]?", right, maxsplit=1)[0].strip()
        return left, right or None
    return b, None


def _reject_bad_name(name: Optional[str], tracking_hint: Optional[str]) -> Optional[str]:
    if not name:
        return None
    n = name.strip()
    if len(n) < 2:
        return None
    low = _low_tr(n)
    if tracking_hint and tracking_hint in n.replace(" ", ""):
        return None
    if re.fullmatch(r"[\d\s\-]+", n):
        return None
    if re.match(r"(?i)kargo\s*\d+", n):
        return None
    if "adres" in low and ":" in n:
        return None
    return n


def parse_kargo_label_ocr_text(
    text: str,
    *,
    tracking_hint: Optional[str] = None,
    qr_content: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Return keys compatible with merge_kargo_fields:
    name, phone, address, city, notes (içerik), total_amount, payment_amount,
    tracking_no (if visible under barcode).

    tracking_hint: QR'dan bilinen takip no; OCR'da yanlış isme düşmesini engellemek için.
    """
    out: Dict[str, Any] = {
        "tracking_no": None,
        "name": None,
        "phone": None,
        "address": None,
        "city": None,
        "total_amount": None,
        "payment_amount": None,
        "quantity": None,
        "notes": None,
        "shipping_company": None,
    }
    if not text or not str(text).strip():
        return out

    specialized = maybe_parse_focus_surat(
        str(text).strip(),
        qr_content=(str(qr_content).strip() if qr_content else None),
        tracking_hint=tracking_hint,
    )
    if specialized is not None:
        return specialized

    raw = re.sub(r"\r\n?", "\n", str(text))
    lines = [ln.strip() for ln in raw.split("\n") if ln.strip()]
    big = " ".join(lines)

    # —— Tahsilat
    for m in re.finditer(
        r"Tahsilat\s*:?\s*([\d]+(?:[.,][\d]+)?)\s*(?:₺|TL)?", big, re.IGNORECASE
    ):
        amt = _parse_money(m.group(1))
        if amt is not None:
            out["total_amount"] = amt
            out["payment_amount"] = amt
            break

    # —— İçerik (tüm metin üzerinde; tek satırlık OCR için)
    ic_m = re.search(
        r"(?is)(?:i̇çerik|içerik|icerik)\s*[:\.]?\s*(.+?)(?=\s*tahsilat\s*[:\.]?|tahsilat\s*\d|₺\s*\d|$)",
        raw,
    )
    if ic_m:
        ic_raw = ic_m.group(1).strip()
        ic_raw = re.split(r"(?is)\s*tahsilat\s*", ic_raw, maxsplit=1)[0].strip()
        if ic_raw:
            out["notes"] = ic_raw

    # —— Alıcı adı (full text; telefon/adres/gönderenden önce kes)
    name_m = re.search(
        r"(?is)(?:alıcı|alici)\s*[:\.]?\s*(.+?)(?=\s*\+?\s*90\s*\d|\s*0\s*5\d{2}\s*\d{3}|\badres\s*[:\.]|\bgönderen\s*[:\.]|\btelefon\b|\btar[iı]h\s*[:\.])",
        raw,
    )
    if name_m:
        out["name"] = _reject_bad_name(name_m.group(1).strip(), tracking_hint)

    # —— Satır tabanlı alıcı (OCR ':'' kaybederse)
    if not out.get("name"):
        for i, ln in enumerate(lines):
            low = _low_tr(ln)
            if "alıcı" in low or low.startswith("alici"):
                if ":" in ln:
                    rest = ln.split(":", 1)[1].strip()
                    out["name"] = _reject_bad_name(rest, tracking_hint)
                if not out.get("name") and i + 1 < len(lines):
                    cand = lines[i + 1].strip()
                    clow = _low_tr(cand)
                    if (
                        cand
                        and "adres" not in clow
                        and "gönderen" not in clow
                        and "alıcı" not in clow
                    ):
                        out["name"] = _reject_bad_name(cand, tracking_hint)
                break

    # —— Telefon
    pm = re.search(r"(\+90\s*[\d\s]{10,14}|0\s*5\d{2}\s*\d{3}\s*\d{2}\s*\d{2})", big)
    if pm:
        out["phone"] = _normalize_phone(re.sub(r"\s+", "", pm.group(1)))

    # —— Adres (tek satırlık OCR: "Adres: ... İçerik:")
    if not out.get("address"):
        adr_m = re.search(
            r"(?is)\badres\s*[:\.]?\s*(.+?)(?=\s*(?:i̇çerik|içerik|icerik)\s*[:\.]?|\s*tahsilat\s*[:\.]?\s*\d|tahsilat\s*\d)",
            raw,
        )
        if adr_m:
            addr_blob = adr_m.group(1).strip()
            addr_clean, ic_from_addr = _strip_trailing_ic_from_blob(addr_blob)
            if ic_from_addr and not out.get("notes"):
                out["notes"] = ic_from_addr
            if addr_clean:
                street, city = _split_address_city(addr_clean)
                if city:
                    city2, ic_from_city = _strip_trailing_ic_from_blob(city)
                    if ic_from_city and not out.get("notes"):
                        out["notes"] = ic_from_city
                    city = city2
                out["address"] = street or addr_clean
                out["city"] = city

    # —— Adres (satır satır)
    addr_start = None
    for i, ln in enumerate(lines):
        low = _low_tr(ln)
        if out.get("address"):
            break
        if addr_start is None and ("adres:" in low or low.startswith("adres")):
            addr_start = i
            first = ln.split(":", 1)[1].strip() if ":" in ln else ""
            chunks = [first] if first else []
            for j in range(i + 1, len(lines)):
                l2 = lines[j]
                l2l = _low_tr(l2)
                if any(
                    l2l.startswith(x) or (x in l2l and x.endswith(":"))
                    for x in ("içerik:", "icerik:", "tahsilat", "gönderen:", "gonderen:")
                ):
                    break
                if "tahsilat" in l2l and "adres" not in l2l:
                    break
                chunks.append(l2)
            addr_full = " ".join(chunks).strip()
            addr_clean, ic_from_addr = _strip_trailing_ic_from_blob(addr_full)
            if ic_from_addr and not out.get("notes"):
                out["notes"] = ic_from_addr
            if addr_clean:
                street, city = _split_address_city(addr_clean)
                if city:
                    city2, ic_from_city = _strip_trailing_ic_from_blob(city)
                    if ic_from_city and not out.get("notes"):
                        out["notes"] = ic_from_city
                    city = city2
                out["address"] = street or addr_clean
                out["city"] = city
            break

    # İçerik bloğu satır bazlı (full-text kaçırdıysa)
    if not out.get("notes"):
        for i, ln in enumerate(lines):
            low = _low_tr(ln)
            if "içerik:" in low or "icerik:" in low:
                rest = ln.split(":", 1)[1].strip()
                parts = [rest] if rest else []
                for j in range(i + 1, len(lines)):
                    l2 = lines[j]
                    l2l = _low_tr(l2)
                    if "tahsilat" in l2l or "sürat" in l2l or "surat" in l2l:
                        break
                    parts.append(l2)
                ic = " ".join(parts).strip()
                if ic:
                    out["notes"] = ic
                break

    if not out.get("tracking_no"):
        tm = re.search(r"\b(\d{12,16})\b", big)
        if tm:
            out["tracking_no"] = tm.group(1)

    b = _low_tr(big)
    if not out.get("shipping_company") and ("sürat" in b or "surat" in b):
        out["shipping_company"] = "surat"

    return out


def ocr_to_label_fields(merged: Dict[str, Any]) -> Dict[str, Any]:
    """API response slice for mobile UI (after merge_kargo_fields + order logic)."""
    addr = merged.get("address") or ""
    city = merged.get("city")
    full_addr = addr
    if city and city not in addr:
        full_addr = f"{addr} / {city}" if addr else str(city)
    full_addr, ic_tail = _strip_trailing_ic_from_blob(full_addr)
    content = (merged.get("notes") or "") or None
    if ic_tail and not (content and ic_tail in content):
        content = f"{content} | {ic_tail}" if content else ic_tail
    tot = merged.get("total_amount")
    if tot is None:
        tot = merged.get("payment_amount")
    try:
        cod = float(tot) if tot is not None else None
    except (TypeError, ValueError):
        cod = None
    return {
        "recipient_name": (merged.get("name") or "") or None,
        "phone": merged.get("phone"),
        "address": full_addr or None,
        "content": (content or "") or None,
        "cod_amount": cod,
        "tracking_no": (merged.get("tracking_no") or "") or None,
    }
