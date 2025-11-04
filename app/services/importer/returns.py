from __future__ import annotations

from typing import Any, Dict, List

from .common import read_sheet_rows, row_to_dict, parse_float, parse_date
from ...utils.normalize import strip_parenthetical_suffix, normalize_text


# Headers are normalized via common.normalize_header -> normalize_text
RETURNS_MAPPING: dict[str, str] = {
    "ad-soyad": "name",
    "ad soyad": "name",
    "ad": "name",
    "soyad": "name",
    "telefon": "phone",
    "tel": "phone",
    "urun": "item_name",
    "urun adi": "item_name",
    "urun adı": "item_name",
    "urunadi": "item_name",
    "kac tl geri geldi": "amount",
    "degisim-iade": "action",
    "degisim iade": "action",
    "iade-degisim": "action",
    "aciklama": "notes",
    "açıklama": "notes",
    "tarih": "date",
}


def _normalize_action(val: Any) -> str | None:
    s = normalize_text(val)
    if not s:
        return None
    # accept values like: iade, iadE, degisim, değişim
    if "iade" in s:
        return "refund"
    if "degisim" in s or "degis" in s:
        return "switch"
    return None


def map_row(raw: dict[str, Any]) -> dict[str, Any]:
    mapped: dict[str, Any] = {}
    for k, v in raw.items():
        key = RETURNS_MAPPING.get(k)
        if not key:
            continue
        if key == "item_name" and isinstance(v, str):
            mapped[key] = strip_parenthetical_suffix(v)
        else:
            mapped[key] = v
    # types
    if "amount" in mapped:
        mapped["amount"] = parse_float(mapped.get("amount"))
    if "action" in mapped:
        mapped["action"] = _normalize_action(mapped.get("action"))
    if "date" in mapped:
        mapped["date"] = parse_date(mapped.get("date"))
    # derive a loose base to aid matching (strip leading size token like XL-, 30-, etc.)
    try:
        txt = str(mapped.get("item_name") or "").strip()
        if txt:
            import re as _re
            base = _re.sub(r"^(?:\d{2,3}|xs|s|m|l|xl|xxl|3xl)\s*[- ]\s*", "", txt, flags=_re.IGNORECASE)
            base = base.strip()
            mapped["item_name_base"] = base or txt
    except Exception:
        pass
    return mapped


def read_returns_file(file_path: str) -> list[dict[str, Any]]:
    headers, rows = read_sheet_rows(file_path)
    records: list[dict[str, Any]] = []
    for row in rows:
        raw = row_to_dict(headers, row)
        mapped = map_row(raw)
        # skip empty
        try:
            meaningful = (mapped.get("name"), mapped.get("phone"), mapped.get("item_name"), mapped.get("action"))
            if not any(meaningful):
                continue
        except Exception:
            pass
        records.append(mapped)
    return records


