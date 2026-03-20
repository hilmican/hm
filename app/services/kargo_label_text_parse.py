"""Extract carrier label fields from raw OCR text (Sürat / Focus-style Turkish layouts)."""
from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple


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


def parse_kargo_label_ocr_text(text: str) -> Dict[str, Any]:
    """
    Return keys compatible with merge_kargo_fields:
    name, phone, address, city, notes (içerik), total_amount, payment_amount,
    tracking_no (if visible under barcode).
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

    raw = re.sub(r"\r\n?", "\n", str(text))
    lines = [ln.strip() for ln in raw.split("\n") if ln.strip()]
    big = " ".join(lines)

    # Tahsilat : 1530.00 ₺
    for m in re.finditer(
        r"Tahsilat\s*:?\s*([\d]+(?:[.,][\d]+)?)\s*(?:₺|TL)?", big, re.IGNORECASE
    ):
        amt = _parse_money(m.group(1))
        if amt is not None:
            out["total_amount"] = amt
            out["payment_amount"] = amt
            break

    # Alıcı: NAME (same line or next)
    for i, ln in enumerate(lines):
        low = _low_tr(ln)
        if "alıcı" in low or "alici:" in low:
            if ":" in ln:
                rest = ln.split(":", 1)[1].strip()
                if rest and len(rest) > 2:
                    out["name"] = rest
                elif i + 1 < len(lines):
                    cand = lines[i + 1].strip()
                    clow = _low_tr(cand)
                    if cand and "adres" not in clow and "gönderen" not in clow:
                        out["name"] = cand
            break

    # Phone +90 / 05xx
    pm = re.search(r"(\+90\s*[\d\s]{10,14}|0\s*5\d{2}\s*\d{3}\s*\d{2}\s*\d{2})", big)
    if pm:
        out["phone"] = _normalize_phone(re.sub(r"\s+", "", pm.group(1)))

    # Adres: ... until İçerik: or Tahsilat or Gönderen block
    addr_start = None
    for i, ln in enumerate(lines):
        low = _low_tr(ln)
        if addr_start is None and ("adres:" in low or low.startswith("adres")):
            addr_start = i
            first = ln.split(":", 1)[1].strip() if ":" in ln else ""
            chunks = [first] if first else []
            for j in range(i + 1, len(lines)):
                l2 = lines[j]
                l2l = _low_tr(l2)
                if any(
                    l2l.startswith(x) or x in l2l
                    for x in ("içerik:", "icerik:", "tahsilat", "gönderen:", "gonderen:", "sürat", "surat")
                ):
                    break
                chunks.append(l2)
            addr_full = " ".join(chunks).strip()
            if addr_full:
                street, city = _split_address_city(addr_full)
                out["address"] = street or addr_full
                out["city"] = city
            break

    # İçerik: ...
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

    # Long digit string as tracking (barcode under label)
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
        "content": (merged.get("notes") or "") or None,
        "cod_amount": cod,
        "tracking_no": (merged.get("tracking_no") or "") or None,
    }
