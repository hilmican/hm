import re
import unicodedata


def normalize_text(s: str) -> str:
	s = s.strip().lower()
	s = unicodedata.normalize('NFKD', s)
	s = ''.join(c for c in s if not unicodedata.combining(c))
	return re.sub(r'\s+', ' ', s)


def normalize_phone(p: str | None) -> str:
	if not p:
		return ''
	return re.sub(r'\D', '', p)


def client_unique_key(name: str | None, phone: str | None) -> str:
	n = normalize_text(name) if name else ''
	ph = normalize_phone(phone)
	return f"{n}|{ph}" if n or ph else ''
