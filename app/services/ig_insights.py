"""
Lightweight helpers for fetching/caching Instagram account & media insights.

Routers and background jobs can import these functions without needing to touch
the heavier inbox/thread modules.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
from typing import Any, Dict, Iterable, Optional

import httpx
from sqlmodel import select

from ..db import get_session
from ..models import IGInsightsSnapshot
from .instagram_api import GRAPH_VERSION, _get_base_token_and_id

_log = logging.getLogger("instagram.insights")

DEFAULT_INSIGHTS_TTL = int(os.getenv("IG_INSIGHTS_CACHE_TTL", str(24 * 3600)))


def _now() -> dt.datetime:
	return dt.datetime.utcnow()


def _build_cache_key(scope: str, subject_id: str, metrics: Iterable[str], params: Dict[str, Any]) -> str:
	key_obj = {
		"scope": scope,
		"subject": subject_id,
		"metrics": sorted(set(metrics)),
		"params": params,
	}
	blob = json.dumps(key_obj, sort_keys=True)
	return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _resolve_subject_id(scope: str, subject_id: Optional[str], default_entity_id: str) -> str:
	if subject_id:
		return str(subject_id)
	if scope == "account":
		ig_user_id = os.getenv("IG_USER_ID")
		if ig_user_id:
			return str(ig_user_id)
	return str(default_entity_id)


def _cache_insights(scope: str, subject_id: str, cache_key: str, payload: Dict[str, Any], ttl_seconds: int) -> IGInsightsSnapshot:
	expires_at = _now() + dt.timedelta(seconds=ttl_seconds)
	snapshot = IGInsightsSnapshot(
		scope=scope,
		subject_id=subject_id,
		cache_key=cache_key,
		payload_json=json.dumps(payload),
		captured_at=_now(),
		expires_at=expires_at,
	)
	with get_session() as session:
		session.add(snapshot)
		session.commit()
		session.refresh(snapshot)
	return snapshot


def get_cached_insights(scope: str, subject_id: str, cache_key: str) -> Optional[IGInsightsSnapshot]:
	with get_session() as session:
		stmt = (
			select(IGInsightsSnapshot)
			.where(IGInsightsSnapshot.scope == scope)
			.where(IGInsightsSnapshot.subject_id == subject_id)
			.where(IGInsightsSnapshot.cache_key == cache_key)
			.order_by(IGInsightsSnapshot.captured_at.desc())
		)
		return session.exec(stmt).first()


def fetch_insights(scope: str, subject_id: Optional[str], metrics: Iterable[str], params: Optional[Dict[str, Any]] = None, ttl_seconds: Optional[int] = None) -> IGInsightsSnapshot:
	"""
	Fetch insights from Graph regardless of cache state.
	"""
	params = params or {}
	metric_list = sorted({m.strip() for m in metrics if m})
	if not metric_list:
		raise ValueError("metrics must not be empty")
	token, entity_id, _ = _get_base_token_and_id()
	target_id = _resolve_subject_id(scope, subject_id, entity_id)
	url = f"https://graph.facebook.com/{GRAPH_VERSION}/{target_id}/insights"
	query = {
		"access_token": token,
		"metric": ",".join(metric_list),
		"platform": "instagram",
	}
	query.update(params)
	with httpx.Client(timeout=30) as client:
		resp = client.get(url, params=query)
		resp.raise_for_status()
		payload = resp.json()
	cache_key = _build_cache_key(scope, str(target_id), metric_list, params)
	snapshot = _cache_insights(scope, str(target_id), cache_key, payload, ttl_seconds or DEFAULT_INSIGHTS_TTL)
	try:
		_log.info("insights.fetch scope=%s subject=%s metrics=%s", scope, target_id, metric_list)
	except Exception:
		pass
	return snapshot


def ensure_insights(scope: str, subject_id: Optional[str], metrics: Iterable[str], params: Optional[Dict[str, Any]] = None, max_age_seconds: Optional[int] = None) -> IGInsightsSnapshot:
	"""
	Return cached insights or refresh if data is stale/missing.
	"""
	params = params or {}
	metric_list = sorted({m.strip() for m in metrics if m})
	if not metric_list:
		raise ValueError("metrics must not be empty")
	_, entity_id, _ = _get_base_token_and_id()
	target_id = _resolve_subject_id(scope, subject_id, entity_id)
	cache_key = _build_cache_key(scope, str(target_id), metric_list, params)
	cached = get_cached_insights(scope, str(target_id), cache_key)
	if cached and cached.expires_at > _now():
		return cached
	return fetch_insights(scope, str(target_id), metric_list, params=params, ttl_seconds=max_age_seconds or DEFAULT_INSIGHTS_TTL)

