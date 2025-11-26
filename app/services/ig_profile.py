"""
Utility helpers for fetching/caching the Instagram Business profile metadata that is
required to justify the `instagram_business_basic` permission.

The functions here are intentionally small so they can be reused by routers, workers,
or cron jobs without pulling in the much larger inbox/thread modules.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from typing import Any, Dict, Optional

import httpx
from sqlmodel import select

from ..db import get_session
from ..models import IGProfileSnapshot
from .instagram_api import GRAPH_VERSION, _get_base_token_and_id

_log = logging.getLogger("instagram.profile")

PROFILE_FIELDS = [
	"id",
	"username",
	"name",
	"biography",
	"profile_picture_url",
	"followers_count",
	"follows_count",
	"media_count",
	"website",
]

DEFAULT_CACHE_TTL_SECONDS = int(os.getenv("IG_PROFILE_CACHE_TTL", "3600"))


def _now() -> dt.datetime:
	return dt.datetime.utcnow()


def get_cached_profile(max_age_seconds: Optional[int] = None) -> Optional[IGProfileSnapshot]:
	"""
	Return the freshest cached profile snapshot, respecting an optional max age.
	"""
	max_age = max_age_seconds or DEFAULT_CACHE_TTL_SECONDS
	cutoff = _now() - dt.timedelta(seconds=max_age)
	with get_session() as session:
		stmt = (
			select(IGProfileSnapshot)
			.order_by(IGProfileSnapshot.refreshed_at.desc())
		)
		if max_age > 0:
			stmt = stmt.where(IGProfileSnapshot.refreshed_at >= cutoff)
		return session.exec(stmt).first()


def refresh_profile_snapshot(force: bool = False, timeout: int = 20) -> IGProfileSnapshot:
	"""
	Fetch the latest business profile metadata from Graph and persist it.
	"""
	token, entity_id, _ = _get_base_token_and_id()
	url = f"https://graph.facebook.com/{GRAPH_VERSION}/{entity_id}"
	params = {
		"access_token": token,
		"fields": ",".join(PROFILE_FIELDS),
		"platform": "instagram",
	}
	with httpx.Client(timeout=timeout) as client:
		resp = client.get(url, params=params)
		resp.raise_for_status()
		data: Dict[str, Any] = resp.json()

	expires_at = _now() + dt.timedelta(seconds=DEFAULT_CACHE_TTL_SECONDS)
	snapshot = IGProfileSnapshot(
		igba_id=str(entity_id),
		username=data.get("username"),
		name=data.get("name"),
		profile_picture_url=data.get("profile_picture_url"),
		biography=data.get("biography"),
		followers_count=data.get("followers_count"),
		follows_count=data.get("follows_count"),
		media_count=data.get("media_count"),
		website=data.get("website"),
		refreshed_at=_now(),
		expires_at=expires_at,
	)
	with get_session() as session:
		session.add(snapshot)
		session.commit()
		session.refresh(snapshot)
	try:
		_log.info(
			"profile.refresh ok username=%s followers=%s",
			snapshot.username,
			snapshot.followers_count,
		)
	except Exception:
		pass
	return snapshot


def ensure_profile_snapshot(max_age_seconds: Optional[int] = None) -> IGProfileSnapshot:
	"""
	Return a cached snapshot, refreshing from Graph if necessary.
	"""
	cached = get_cached_profile(max_age_seconds=max_age_seconds)
	if cached:
		return cached
	return refresh_profile_snapshot()

