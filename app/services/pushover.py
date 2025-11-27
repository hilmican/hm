from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

import httpx

log = logging.getLogger("services.pushover")

PUSHOVER_ENDPOINT = "https://api.pushover.net/1/messages.json"


def _get_app_token() -> str:
	for key in ("PUSHOVER_APP_TOKEN", "PUSHOVER_API_TOKEN", "PUSHOVER_TOKEN"):
		val = (os.getenv(key) or "").strip()
		if val:
			return val
	return ""


def is_configured() -> bool:
	return bool(_get_app_token())


def send_pushover_message(
	user_key: str,
	message: str,
	*,
	title: Optional[str] = None,
	url: Optional[str] = None,
	url_title: Optional[str] = None,
	priority: Optional[int] = None,
) -> bool:
	token = _get_app_token()
	if not token:
		log.debug("pushover skipped: missing app token")
		return False
	user_key = (user_key or "").strip()
	if not user_key:
		log.debug("pushover skipped: missing user key")
		return False
	message = (message or "").strip()
	if not message:
		log.debug("pushover skipped: empty message")
		return False

	payload: Dict[str, Any] = {
		"token": token,
		"user": user_key,
		"message": message[:1024],
	}
	if title:
		payload["title"] = title[:250]
	if url:
		payload["url"] = url
	if url_title:
		payload["url_title"] = url_title[:100]
	if priority is not None:
		payload["priority"] = priority

	try:
		with httpx.Client(timeout=10.0) as client:
			resp = client.post(PUSHOVER_ENDPOINT, data=payload)
			resp.raise_for_status()
			return True
	except httpx.HTTPStatusError as exc:
		try:
			body = exc.response.json()
		except Exception:
			body = exc.response.text
		log.warning("pushover HTTP error status=%s body=%s", exc.response.status_code, body)
		return False
	except Exception as exc:
		log.warning("pushover send failed err=%s", exc)
		return False

