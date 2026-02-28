import os
from typing import Any, Dict, List, Optional

import httpx


GRAPH_VERSION = os.getenv("WA_GRAPH_API_VERSION", os.getenv("IG_GRAPH_API_VERSION", "v22.0"))


def _get_token_and_phone_number_id() -> tuple[str, str]:
    token = os.getenv("WA_ACCESS_TOKEN", "")
    phone_number_id = os.getenv("WA_PHONE_NUMBER_ID", "")
    if not token or not phone_number_id:
        raise RuntimeError("Missing WA_ACCESS_TOKEN or WA_PHONE_NUMBER_ID")
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
    async with httpx.AsyncClient() as client:
        for image_url in image_urls or []:
            payload = {
                "messaging_product": "whatsapp",
                "to": str(recipient_id),
                "type": "image",
                "image": {"link": str(image_url)},
            }
            r_img = await client.post(url, headers=headers, json=payload, timeout=20)
            r_img.raise_for_status()
            data_img = r_img.json() if r_img.content else {}
            msg_id = ((data_img.get("messages") or [{}])[0]).get("id")
            if msg_id:
                message_ids.append(str(msg_id))

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
