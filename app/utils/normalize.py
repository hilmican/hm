import re
import unicodedata
from typing import Any


def normalize_text(s: Any) -> str:
    if s is None:
        return ''
    s = str(s)
    s = s.strip().lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return re.sub(r'\s+', ' ', s)


def normalize_phone(p: Any) -> str:
    try:
        if p is None:
            return ''
        # Coerce numeric cells from spreadsheets to integer digits
        if isinstance(p, (int, float)):
            try:
                s = str(int(p))
            except Exception:
                s = str(p)
        else:
            s = str(p)
        return re.sub(r'\D', '', s)
    except Exception:
        return ''


def client_unique_key(name: Any, phone: Any) -> str:
    n = normalize_text(name)
    ph = normalize_phone(phone)
    return f"{n}|{ph}" if n or ph else ''
