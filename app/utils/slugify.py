import re
import unicodedata


def slugify(value: str) -> str:
	value = unicodedata.normalize("NFKD", value)
	value = value.encode("ascii", "ignore").decode("ascii")
	value = re.sub(r"[^a-zA-Z0-9]+", "-", value)
	value = value.strip("-")
	return value.lower() or "item"
