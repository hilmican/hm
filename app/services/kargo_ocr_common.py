"""OCR çıkarımı için paylaşılan yardımcılar (tüm kargo şablonları)."""
from __future__ import annotations

import re
from typing import Any, Dict, Optional, Tuple


def low_tr(s: str) -> str:
    return (s or "").replace("İ", "i").replace("I", "ı").lower()


def parse_money(s: str) -> Optional[float]:
    s = s.strip().replace("₺", "").replace("TL", "").replace("tl", "").strip()
    s = s.replace(".", "").replace(",", ".") if "," in s and "." in s else s.replace(",", ".")
    try:
        v = float(s)
        return v if v == v and v >= 0 else None
    except ValueError:
        return None


def normalize_phone(raw: str) -> Optional[str]:
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


def split_address_city(address: str) -> Tuple[str, Optional[str]]:
    a = (address or "").strip()
    if not a:
        return "", None
    parts = [p.strip() for p in a.split("/") if p.strip()]
    if len(parts) >= 2:
        city = parts[-1].strip()
        street = " / ".join(parts[:-1]).strip()
        return street, city or None
    return a, None


def strip_trailing_ic_from_blob(blob: str) -> Tuple[str, Optional[str]]:
    b = (blob or "").strip()
    if not b:
        return "", None
    m = re.split(r"(?is)\s+(?:i̇çerik|içerik|icerik)\s*[:\.]?\s*", b, maxsplit=1)
    if len(m) == 2 and m[1].strip():
        left, right = m[0].strip(), m[1].strip()
        right = re.split(r"(?is)\s*tahsilat\s*[:\.]?", right, maxsplit=1)[0].strip()
        return left, right or None
    return b, None


def reject_bad_name(name: Optional[str], tracking_hint: Optional[str]) -> Optional[str]:
    if not name:
        return None
    n = name.strip()
    if len(n) < 2:
        return None
    low = low_tr(n)
    if tracking_hint and tracking_hint in n.replace(" ", ""):
        return None
    if re.fullmatch(r"[\d\s\-]+", n):
        return None
    if re.match(r"(?i)kargo\s*\d+", n):
        return None
    if "adres" in low and ":" in n:
        return None
    return n


def empty_label_dict() -> Dict[str, Any]:
    return {
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
