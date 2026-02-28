from typing import Any, Dict, List, Optional

from .instagram_api import send_message as send_instagram_message
from .whatsapp_api import send_message as send_whatsapp_message


async def send_message(
    *,
    platform: str,
    recipient_id: str,
    conversation_id: Optional[str],
    text: str,
    image_urls: Optional[List[str]] = None,
) -> Dict[str, Any]:
    normalized = str(platform or "instagram").strip().lower()
    if normalized == "whatsapp":
        return await send_whatsapp_message(
            recipient_id=str(recipient_id),
            text=text,
            image_urls=image_urls,
        )
    return await send_instagram_message(
        conversation_id=str(conversation_id or ""),
        text=text,
        image_urls=image_urls,
    )
