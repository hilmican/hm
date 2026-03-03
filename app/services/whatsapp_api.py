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
    from .image_urls import normalize_image_urls_for_send
    urls_to_send = normalize_image_urls_for_send(image_urls or [])
    received_n = len(image_urls or [])
    after_filter_n = len(urls_to_send)
    _log.info(
        "WhatsApp send_message: image_urls received=%d after_absolute_filter=%d to=%s",
        received_n,
        after_filter_n,
        (recipient_id[:20] if recipient_id else ""),
    )
    if received_n and after_filter_n == 0:
        _log.warning(
            "WhatsApp send_message: all %d image URL(s) dropped (none absolute). First: %s. Set IMAGE_CDN_BASE_URL or APP_URL.",
            received_n,
            ((image_urls or [])[0][:120] if image_urls else ""),
        )

    sent_image_count = 0
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
                        sent_image_count += 1
                    break
                except httpx.HTTPStatusError as e:
                    try:
                        body = (e.response.text or "")[:500]
                        code = getattr(e.response, "status_code", None)
                    except Exception:
                        body = str(e)[:500]
                        code = None
                    _log.warning(
                        "WhatsApp image API error attempt=%s status=%s url=%s body=%s",
                        attempt + 1,
                        code,
                        (str(image_url))[:80],
                        body,
                    )
                    if attempt == 0:
                        await asyncio.sleep(1.0)
                except Exception as e:
                    _log.warning(
                        "WhatsApp image send exception attempt=%s url=%s err=%s",
                        attempt + 1,
                        (str(image_url))[:80],
                        str(e)[:300],
                    )
                    if attempt == 0:
                        await asyncio.sleep(1.0)
            if not sent:
                _log.warning("WhatsApp image skipped after retries url=%s", (str(image_url))[:80])
                if image_delay_after_fail_sec > 0:
                    await asyncio.sleep(image_delay_after_fail_sec)

        if urls_to_send:
            _log.info(
                "WhatsApp send_message: image summary sent=%d requested=%d to=%s",
                sent_image_count,
                len(urls_to_send),
                (recipient_id[:20] if recipient_id else ""),
            )
            if sent_image_count < len(urls_to_send):
                _log.warning(
                    "WhatsApp images partial send: %d/%d succeeded (check logs above for API errors)",
                    sent_image_count,
                    len(urls_to_send),
                )

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

    result: Dict[str, Any] = {
        "message_id": (message_ids[0] if message_ids else None),
        "message_ids": message_ids,
    }
    if urls_to_send:
        result["image_message_count"] = sent_image_count
    return result
