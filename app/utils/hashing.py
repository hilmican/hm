import hashlib
import json
import datetime as dt
from typing import Any
import hmac
import os


def stable_dumps(obj: Any) -> str:
	def _default(o: Any):
		if isinstance(o, (dt.date, dt.datetime)):
			return o.isoformat()
		return str(o)
	return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_default)


def compute_row_hash(mapped_row: dict) -> str:
	payload = stable_dumps(mapped_row)
	return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def hash_password(password: str, salt: bytes | None = None) -> str:
    if salt is None:
        salt = os.urandom(16)
    if isinstance(salt, str):
        salt = salt.encode("utf-8")
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return salt.hex() + ":" + dk.hex()


def verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, dk_hex = stored.split(":", 1)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(dk_hex)
        test = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
        return hmac.compare_digest(test, expected)
    except Exception:
        return False
