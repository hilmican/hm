"""
Synchronous media download functions for Instagram webhook processing.
Downloads images and videos immediately when processing webhook events.
"""

import logging
import os
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, List
import httpx

_log = logging.getLogger("instagram.webhook.media")


def get_media_storage_path(media_url: str, ig_message_id: str, position: int, kind: str = "file") -> str:
    """
    Generate a storage path for media based on URL hash and message info.
    Returns relative path from media directory.
    """
    # Create hash of URL for unique filename
    url_hash = hashlib.md5(media_url.encode()).hexdigest()[:8]

    # Get file extension from URL or default
    ext = ".bin"
    if "." in media_url.split("?")[0].split("/")[-1]:
        ext = "." + media_url.split("?")[0].split("/")[-1].split(".")[-1].lower()

    # For images/videos, use appropriate extensions
    if kind == "image":
        if not ext or ext == ".bin":
            ext = ".jpg"
        elif ext not in [".jpg", ".jpeg", ".png", ".gif", ".webp"]:
            ext = ".jpg"
    elif kind == "video":
        if not ext or ext == ".bin":
            ext = ".mp4"
        elif ext not in [".mp4", ".mov", ".avi", ".mkv"]:
            ext = ".mp4"

    filename = f"{ig_message_id}_{position}_{url_hash}{ext}"
    return f"media/{filename}"


async def download_media_attachment(session, message_id: int, ig_message_id: str, attachment: Dict[str, Any], position: int, client: httpx.AsyncClient) -> Optional[str]:
    """
    Download a single media attachment synchronously.
    Returns the storage path if successful, None otherwise.
    """
    try:
        # Determine media type
        kind = "file"
        media_url = None
        mime_type = attachment.get("mime_type", "").lower()

        if "image" in mime_type:
            kind = "image"
        elif "video" in mime_type:
            kind = "video"

        # Try different ways to get the media URL
        if attachment.get("file_url"):
            media_url = attachment["file_url"]
        elif attachment.get("payload", {}).get("url"):
            media_url = attachment["payload"]["url"]
        elif attachment.get("image_data", {}).get("url"):
            media_url = attachment["image_data"]["url"]
        elif attachment.get("image_data", {}).get("preview_url"):
            media_url = attachment["image_data"]["preview_url"]

        if not media_url:
            _log.debug("webhook.media: no downloadable URL found for msg %s pos %d", ig_message_id, position)
            return None

        # Generate storage path
        storage_path = get_media_storage_path(media_url, ig_message_id, position, kind)

        # Create media directory if needed
        media_dir = Path("data/media")
        media_dir.mkdir(parents=True, exist_ok=True)

        full_path = Path("data") / storage_path

        # Check if file already exists
        if full_path.exists():
            _log.debug("webhook.media: file already exists %s", storage_path)
            return storage_path

        # Download the file
        _log.info("webhook.media: downloading %s -> %s", media_url[:100], storage_path)

        response = await client.get(media_url, timeout=30, follow_redirects=True)
        response.raise_for_status()

        # Save to file
        with open(full_path, "wb") as f:
            f.write(response.content)

        file_size = len(response.content)
        _log.info("webhook.media: downloaded %s (%d bytes)", storage_path, file_size)

        # Update attachment record with storage path and status
        try:
            from sqlalchemy import text
            session.exec(text("""
                UPDATE attachments SET
                  storage_path=:path,
                  fetch_status='ok',
                  mime=:mime,
                  file_size=:size
                WHERE message_id=:mid AND position=:pos
            """).params(
                path=storage_path,
                mime=response.headers.get("content-type", mime_type or "application/octet-stream"),
                size=file_size,
                mid=message_id,
                pos=position
            ))
        except Exception as e:
            _log.warning("webhook.media: failed to update attachment record: %s", str(e))

        return storage_path

    except Exception as e:
        _log.warning("webhook.media: failed to download attachment msg %s pos %d: %s", ig_message_id, position, str(e)[:200])

        # Mark as failed in database
        try:
            from sqlalchemy import text
            session.exec(text("""
                UPDATE attachments SET
                  fetch_status='error',
                  fetch_error=:error
                WHERE message_id=:mid AND position=:pos
            """).params(
                error=str(e)[:500],
                mid=message_id,
                pos=position
            ))
        except Exception as e2:
            _log.warning("webhook.media: failed to mark attachment as error: %s", str(e2))

        return None


async def download_message_attachments(session, message_id: int, ig_message_id: str, attachments: List[Dict[str, Any]], client: httpx.AsyncClient) -> List[str]:
    """
    Download all attachments for a message synchronously.
    Returns list of storage paths for successfully downloaded files.
    """
    downloaded_paths = []

    if not attachments:
        return downloaded_paths

    for idx, attachment in enumerate(attachments):
        try:
            storage_path = await download_media_attachment(
                session, message_id, ig_message_id, attachment, idx, client
            )
            if storage_path:
                downloaded_paths.append(storage_path)
        except Exception as e:
            _log.warning("webhook.media: error downloading attachment %d for msg %s: %s", idx, ig_message_id, str(e))

    return downloaded_paths


def create_attachment_records(session, message_id: int, ig_message_id: str, attachments: List[Dict[str, Any]]) -> None:
    """
    Create attachment records in database for tracking download status.
    """
    if not attachments:
        return

    try:
        from sqlalchemy import text

        for idx, attachment in enumerate(attachments):
            # Determine media type
            kind = "file"
            graph_id = None
            mime_type = attachment.get("mime_type", "").lower()

            if "image" in mime_type:
                kind = "image"
            elif "video" in mime_type:
                kind = "video"
            elif "audio" in mime_type:
                kind = "audio"

            # Extract Graph API ID if available
            if attachment.get("id"):
                graph_id = str(attachment["id"])
            elif attachment.get("payload", {}).get("id"):
                graph_id = str(attachment["payload"]["id"])

            # Insert attachment record
            try:
                session.exec(text("""
                    INSERT INTO attachments(message_id, ig_message_id, position, kind, graph_id, mime, fetch_status)
                    VALUES (:mid, :ig_mid, :pos, :kind, :gid, :mime, 'pending')
                    ON DUPLICATE KEY UPDATE
                      kind=VALUES(kind),
                      graph_id=VALUES(graph_id),
                      mime=VALUES(mime)
                """).params(
                    mid=message_id,
                    ig_mid=ig_message_id,
                    pos=idx,
                    kind=kind,
                    gid=graph_id,
                    mime=mime_type or None
                ))
            except Exception as e:
                try:
                    session.exec(text("""
                        UPDATE attachments SET
                          kind=COALESCE(:kind, kind),
                          graph_id=COALESCE(:gid, graph_id),
                          mime=COALESCE(:mime, mime)
                        WHERE message_id=:mid AND position=:pos
                    """).params(
                        mid=message_id,
                        pos=idx,
                        kind=kind,
                        gid=graph_id,
                        mime=mime_type or None
                    ))
                except Exception as e2:
                    _log.warning("webhook.media: failed to create attachment record: %s", str(e2))

    except Exception as e:
        _log.warning("webhook.media: error creating attachment records for msg %s: %s", ig_message_id, str(e))
