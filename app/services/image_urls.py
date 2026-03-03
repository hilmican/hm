"""
Shared helpers for image URLs used when sending to Meta (Instagram/WhatsApp).
Meta requires absolute, publicly accessible URLs; relative paths must be resolved.
"""
from __future__ import annotations

import logging
import os
from typing import List

_log = logging.getLogger("image_urls")


def get_image_base_url() -> str:
    """Return the base URL for resolving relative image paths (no trailing slash)."""
    base = (os.getenv("IMAGE_CDN_BASE_URL", "") or "").strip().rstrip("/")
    if base:
        return base
    base = (os.getenv("APP_URL", "") or os.getenv("BASE_URL", "") or "").strip().rstrip("/")
    return base


def make_absolute_image_url(url: str) -> str:
    """
    Convert a relative image URL to absolute for Meta Graph API.
    If already absolute (http/https), returns as-is.
    Uses IMAGE_CDN_BASE_URL, then APP_URL, then BASE_URL.
    """
    if not url:
        return url
    raw = str(url).strip()
    if raw.startswith(("http://", "https://")):
        return raw
    base = get_image_base_url()
    if not base:
        _log.warning(
            "IMAGE_CDN_BASE_URL and APP_URL/BASE_URL not set; relative image URL will be rejected: %s",
            raw[:80],
        )
        return raw
    path = raw.lstrip("/")
    return f"{base}/{path}"


def normalize_image_urls_for_send(urls: List[str]) -> List[str]:
    """
    Normalize a list of image URLs for sending: convert to absolute and keep only valid http(s) URLs.
    Returns only URLs that are absolute so Meta can fetch them.
    """
    out: List[str] = []
    for u in urls or []:
        if not u or not str(u).strip():
            continue
        raw = str(u).strip()
        abs_url = make_absolute_image_url(raw)
        if abs_url.startswith(("http://", "https://")):
            if abs_url not in out:
                out.append(abs_url)
    return out
