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


def _parse_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        try:
            f = float(val)
            return f if f == f else None  # nan check
        except Exception:
            return None
    s = str(val).strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_kargo_qr(qr_content: str) -> Dict[str, Any]:
    """
    Extract tracking_no, name, phone, address, city, totals and description from carrier label formats.
    Aligns with KargoRow-ish fields where possible (total_amount, unit_price, notes).
    """
    raw = (qr_content or "").strip()
    out: Dict[str, Any] = {
        "tracking_no": None,
        "name": None,
        "phone": None,
        "address": None,
        "city": None,
        "total_amount": None,
        "unit_price": None,
        "payment_amount": None,
        "quantity": None,
        "notes": None,
        "shipping_company": None,
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
                out["total_amount"] = _parse_float(
                    obj.get("total_amount")
                    or obj.get("total")
                    or obj.get("toplam")
                    or obj.get("tutar")
                    or obj.get("amount")
                )
                out["unit_price"] = _parse_float(
                    obj.get("unit_price") or obj.get("birim_fiyat") or obj.get("fiyat") or obj.get("price")
                )
                out["payment_amount"] = _parse_float(obj.get("payment_amount") or obj.get("odeme"))
                qraw = obj.get("quantity") or obj.get("adet") or obj.get("qty")
                if qraw is not None:
                    try:
                        out["quantity"] = int(float(str(qraw).replace(",", ".")))
                    except ValueError:
                        out["quantity"] = None
                desc = _s(
                    obj.get("notes")
                    or obj.get("description")
                    or obj.get("aciklama")
                    or obj.get("urun")
                    or obj.get("item_name")
                    or obj.get("product")
                )
                out["notes"] = desc
                out["shipping_company"] = _s(obj.get("shipping_company") or obj.get("kargo"))
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
            out["total_amount"] = _parse_float(
                flat.get("total_amount") or flat.get("total") or flat.get("toplam") or flat.get("tutar")
            )
            out["unit_price"] = _parse_float(flat.get("unit_price") or flat.get("fiyat") or flat.get("price"))
            out["payment_amount"] = _parse_float(flat.get("payment_amount") or flat.get("odeme"))
            qflat = flat.get("quantity") or flat.get("adet") or flat.get("qty")
            if qflat:
                try:
                    out["quantity"] = int(float(str(qflat).replace(",", ".")))
                except ValueError:
                    pass
            out["notes"] = _s(
                flat.get("notes")
                or flat.get("description")
                or flat.get("aciklama")
                or flat.get("urun")
                or flat.get("item_name")
            )
            out["shipping_company"] = _s(flat.get("shipping_company") or flat.get("kargo"))
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
            if len(parts) > 5:
                out["total_amount"] = _parse_float(parts[5])
            if len(parts) > 6:
                out["notes"] = parts[6] or None
            if len(parts) > 7:
                out["unit_price"] = _parse_float(parts[7])
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
    for k in (
        "tracking_no",
        "name",
        "phone",
        "address",
        "city",
        "total_amount",
        "unit_price",
        "payment_amount",
        "quantity",
        "notes",
        "description",
        "shipping_company",
    ):
        v = overrides.get(k)
        if v is None or (isinstance(v, str) and not str(v).strip()):
            continue
        if k in ("total_amount", "unit_price", "payment_amount"):
            base[k] = _parse_float(v)
        elif k == "quantity":
            try:
                base["quantity"] = int(float(str(v).replace(",", ".")))
            except ValueError:
                pass
        elif k == "description":
            base["notes"] = str(v).strip()
        else:
            base[k] = str(v).strip() if k != "notes" else str(v).strip()
    return base
