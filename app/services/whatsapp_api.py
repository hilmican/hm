import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

import httpx


GRAPH_VERSION = os.getenv("WA_GRAPH_API_VERSION", os.getenv("IG_GRAPH_API_VERSION", "v22.0"))
_log = logging.getLogger("whatsapp.api")


def _get_token_and_phone_number_id() -> tuple[str, str]:
    token = os.getenv("WA_ACCESS_TOKEN", "") or os.getenv("IG_ACCESS_TOKEN", "")
    phone_number_id = os.getenv("WA_PHONE_NUMBER_ID", "")
    if not token or not phone_number_id:
        raise RuntimeError("Missing WA_PHONE_NUMBER_ID (and WA_ACCESS_TOKEN/IG_ACCESS_TOKEN)")
    return token, phone_number_id


async def send_message(
    recipient_id: str,
    text: str,
    image_urls: Optional[List[str]] = None,
) -> Dict[str, Any]:
    token, phone_number_id = _get_token_and_phone_number_id()
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{phone_number_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    if not recipient_id:
        raise RuntimeError("recipient_id is required")

    message_ids: List[str] = []
    image_delay_sec = float(os.getenv("WA_IMAGE_SEND_DELAY_SEC", os.getenv("IG_IMAGE_SEND_DELAY_SEC", "1.2")))
    image_delay_after_fail_sec = float(os.getenv("WA_IMAGE_DELAY_AFTER_FAIL_SEC", os.getenv("IG_IMAGE_DELAY_AFTER_FAIL_SEC", "2.5")))
    urls_to_send = [u for u in (image_urls or []) if u and str(u).strip() and str(u).strip().startswith(("http://", "https://"))]

    async with httpx.AsyncClient() as client:
        for i, image_url in enumerate(urls_to_send):
            if i > 0 and image_delay_sec > 0:
                await asyncio.sleep(image_delay_sec)
            sent = False
            for attempt in range(2):
                try:
                    payload = {
                        "messaging_product": "whatsapp",
                        "to": str(recipient_id),
                        "type": "image",
                        "image": {"link": str(image_url).strip()},
                    }
                    r_img = await client.post(url, headers=headers, json=payload, timeout=25)
                    r_img.raise_for_status()
                    data_img = r_img.json() if r_img.content else {}
                    msg_id = ((data_img.get("messages") or [{}])[0]).get("id")
                    if msg_id:
                        message_ids.append(str(msg_id))
                        sent = True
                    break
                except Exception as e:
                    _log.warning(
                        "WhatsApp image send failed attempt=%s url=%s err=%s",
                        attempt + 1,
                        (str(image_url))[:80],
                        str(e)[:200],
                    )
                    if attempt == 0:
                        await asyncio.sleep(1.0)
            if not sent:
                _log.warning("WhatsApp image skipped after retries url=%s", (str(image_url))[:80])
                if image_delay_after_fail_sec > 0:
                    await asyncio.sleep(image_delay_after_fail_sec)

        text_lines = [line.strip() for line in (text or "").split("\n") if line.strip()]
        if not text_lines:
            text_lines = [str(text or "").strip()]
        for line in text_lines:
            payload = {
                "messaging_product": "whatsapp",
                "to": str(recipient_id),
                "type": "text",
                "text": {"body": line},
            }
            r = await client.post(url, headers=headers, json=payload, timeout=20)
            r.raise_for_status()
            data = r.json() if r.content else {}
            msg_id = ((data.get("messages") or [{}])[0]).get("id")
            if msg_id:
                message_ids.append(str(msg_id))

    return {
        "message_id": (message_ids[0] if message_ids else None),
        "message_ids": message_ids,
    }
