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


# Turkish-specific transliteration map to ASCII
TURKISH_MAP = str.maketrans({
    'ı': 'i', 'İ': 'i', 'ş': 's', 'Ş': 's', 'ğ': 'g', 'Ğ': 'g',
    'ç': 'c', 'Ç': 'c', 'ö': 'o', 'Ö': 'o', 'ü': 'u', 'Ü': 'u'
})


def normalize_key(s: Any) -> str:
    """Normalize text to a stable ASCII underscore key.

    - Lowercase, transliterate Turkish chars, remove accents
    - Replace non-alphanumerics with underscores
    - Collapse repeats and trim underscores
    """
    if s is None:
        return ''
    s = str(s).strip().lower().translate(TURKISH_MAP)
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'[^a-z0-9]+', '_', s)
    return s.strip('_')


def client_name_key(name: Any) -> str:
    return normalize_key(name)


def client_unique_key(name: Any, phone: Any) -> str:
    n = client_name_key(name)
    ph = normalize_phone(phone)
    return f"{n}|{ph}" if n or ph else ''


def legacy_client_unique_key(name: Any, phone: Any) -> str:
    """Previous unique key behavior: space-normalized lowercase with phone digits.

    Kept for backward matching of existing clients in DB.
    """
    n = normalize_text(name)
    ph = normalize_phone(phone)
    return f"{n}|{ph}" if n or ph else ''
