"""Parse QR payloads for mobile stock/kargo flows (kargo label + HMA stock labels)."""
from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse


def parse_stock_qr(qr_content: str) -> Optional[Dict[str, Any]]:
    """
    Returns dict with item_id, stock_unit_id, or sku, or None if unparseable.
    Formats: hma:item:123, hma:unit:456, hma:sku:ABC-123
    """
    raw = (qr_content or "").strip()
    if not raw:
        return None
    low = raw.lower()
    if low.startswith("hma:item:"):
        rest = raw.split(":", 2)[-1].strip()
        try:
            return {"item_id": int(rest)}
        except ValueError:
            return None
    if low.startswith("hma:unit:"):
        rest = raw.split(":", 2)[-1].strip()
        try:
            return {"stock_unit_id": int(rest)}
        except ValueError:
            return None
    if low.startswith("hma:sku:"):
        sku = raw.split(":", 2)[-1].strip()
        return {"sku": sku} if sku else None
    return None


def parse_kargo_qr(qr_content: str) -> Dict[str, Any]:
    """
    Extract tracking_no, name, phone, address, city from various carrier label formats.
    """
    raw = (qr_content or "").strip()
    out: Dict[str, Any] = {
        "tracking_no": None,
        "name": None,
        "phone": None,
        "address": None,
        "city": None,
    }
    if not raw:
        return out

    # JSON
    if raw.startswith("{") and raw.endswith("}"):
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                tn = obj.get("tracking_no") or obj.get("takip_no") or obj.get("TakipNo") or obj.get("barcode")
                out["tracking_no"] = str(tn).strip() if tn else None
                out["name"] = _s(obj.get("name") or obj.get("alici") or obj.get("recipient"))
                out["phone"] = _s(obj.get("phone") or obj.get("telefon") or obj.get("tel"))
                out["address"] = _s(obj.get("address") or obj.get("adres"))
                out["city"] = _s(obj.get("city") or obj.get("il") or obj.get("sehir"))
                return out
        except json.JSONDecodeError:
            pass

    # URL with query params
    if "://" in raw or (raw.startswith("http") and "?" in raw):
        try:
            p = urlparse(raw if "://" in raw else f"https://dummy/{raw}")
            qs = parse_qs(p.query or "")
            flat = {k: (v[0] if v else "") for k, v in qs.items()}

            def _qk(*keys: str) -> Optional[str]:
                for k in keys:
                    v = flat.get(k) or flat.get(k.lower()) or flat.get(k.upper())
                    if v:
                        return str(v).strip()
                return None

            out["tracking_no"] = _qk("tracking_no", "takip_no", "TakipNo", "barcode", "gonderino")
            out["name"] = _qk("name", "alici", "recipient")
            out["phone"] = _qk("phone", "telefon", "tel")
            out["address"] = _qk("address", "adres")
            out["city"] = _qk("city", "il", "sehir")
            if out["tracking_no"]:
                return out
        except Exception:
            pass

    # Delimiter-separated: tracking|name|phone|address|city
    if "|" in raw or ";" in raw:
        sep = "|" if raw.count("|") >= raw.count(";") else ";"
        parts = [p.strip() for p in raw.split(sep)]
        if len(parts) >= 1:
            out["tracking_no"] = parts[0] or None
            if len(parts) > 1:
                out["name"] = parts[1] or None
            if len(parts) > 2:
                out["phone"] = parts[2] or None
            if len(parts) > 3:
                out["address"] = parts[3] or None
            if len(parts) > 4:
                out["city"] = parts[4] or None
            return out

    # Plain tracking number (digits / alphanumeric typical for carriers)
    if re.fullmatch(r"[\dA-Za-z\-]{8,32}", raw.replace(" ", "")):
        out["tracking_no"] = raw.replace(" ", "").strip()
        return out

    return out


def _s(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def merge_kargo_fields(parsed: Dict[str, Any], overrides: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Apply explicit body fields over parsed QR."""
    base = dict(parsed)
    if not overrides:
        return base
    for k in ("tracking_no", "name", "phone", "address", "city"):
        v = overrides.get(k)
        if v is not None and str(v).strip():
            base[k] = str(v).strip()
    return base
