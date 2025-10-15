import hashlib
import json
from typing import Any


def stable_dumps(obj: Any) -> str:
	return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compute_row_hash(mapped_row: dict) -> str:
	payload = stable_dumps(mapped_row)
	return hashlib.sha256(payload.encode("utf-8")).hexdigest()
