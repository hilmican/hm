import hashlib
import json
import datetime as dt
from typing import Any


def stable_dumps(obj: Any) -> str:
	def _default(o: Any):
		if isinstance(o, (dt.date, dt.datetime)):
			return o.isoformat()
		return str(o)
	return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_default)


def compute_row_hash(mapped_row: dict) -> str:
	payload = stable_dumps(mapped_row)
	return hashlib.sha256(payload.encode("utf-8")).hexdigest()
