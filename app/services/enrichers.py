import os
import datetime as dt
from typing import Optional

from sqlalchemy import text

from ..db import get_session
from .instagram_api import fetch_user_username, _get_base_token_and_id, GRAPH_VERSION, _get as graph_get
import httpx


def _ttl_hours(env_key: str, default: int) -> int:
	try:
		return int(os.getenv(env_key, str(default)))
	except Exception:
		return default


async def enrich_user(ig_user_id: str) -> bool:
	# TTL check
	with get_session() as session:
		row = session.exec(text("SELECT fetched_at FROM ig_users WHERE ig_user_id = :id").params(id=ig_user_id)).first()
		if row and (row.fetched_at if hasattr(row, "fetched_at") else row[0]):
			fa = row.fetched_at if hasattr(row, "fetched_at") else row[0]
			if isinstance(fa, str):
				try:
					fa = dt.datetime.fromisoformat(fa)
				except Exception:
					fa = None
			if fa and (dt.datetime.utcnow() - fa) < dt.timedelta(hours=_ttl_hours("USER_TTL_HOURS", 48)):
				return False
	# Fetch full profile fields instead of only username to enable avatars in UI
	try:
		from .instagram_api import _get as graph_get, GRAPH_VERSION, _get_base_token_and_id
		import httpx
		token, _, _ = _get_base_token_and_id()
		base = f"https://graph.facebook.com/{GRAPH_VERSION}"
		# Fetch only username and name (no profile picture)
		async with httpx.AsyncClient() as client:
			data_basic = await graph_get(client, base + f"/{ig_user_id}", {"access_token": token, "fields": "username,name"})
		username = data_basic.get("username") or data_basic.get("name")
		name = data_basic.get("name")
	except Exception as e:
		with get_session() as session:
			session.exec(
				text("UPDATE ig_users SET fetch_status='error', fetch_error=:e WHERE ig_user_id=:id").params(
					id=ig_user_id, e=str(e)
				)
			)
		return False
	with get_session() as session:
		session.exec(
			text(
				"""
				UPDATE ig_users
				SET username=:u, name=:n, fetched_at=CURRENT_TIMESTAMP, fetch_status='ok', fetch_error=NULL
				WHERE ig_user_id=:id
				"""
			).params(u=username, n=name, id=ig_user_id)
		)
	return True


async def enrich_page(igba_id: str) -> bool:
	# TTL check on ig_accounts.updated_at
	with get_session() as session:
		row = session.exec(text("SELECT updated_at FROM ig_accounts WHERE igba_id=:id").params(id=igba_id)).first()
		if row and (row.updated_at if hasattr(row, "updated_at") else row[0]):
			ua = row.updated_at if hasattr(row, "updated_at") else row[0]
			if isinstance(ua, str):
				try:
					ua = dt.datetime.fromisoformat(ua)
				except Exception:
					ua = None
			if ua and (dt.datetime.utcnow() - ua) < dt.timedelta(hours=_ttl_hours("PAGE_TTL_HOURS", 48)):
				return False
	# Call Graph to resolve page/account fields
	token, entity_id, is_page = _get_base_token_and_id()
	base = f"https://graph.facebook.com/{GRAPH_VERSION}"
	path = f"/{igba_id}"
	params = {"access_token": token, "fields": "username,name,profile_picture_url"}
	async with httpx.AsyncClient() as client:
		data = await graph_get(client, base + path, params)
		username = data.get("username")
		name = data.get("name")
		pp = data.get("profile_picture_url") or data.get("profile_pic")
	with get_session() as session:
		session.exec(
			text(
				"UPDATE ig_accounts SET username=:u, name=:n, profile_pic_url=:p, updated_at=CURRENT_TIMESTAMP WHERE igba_id=:id"
			).params(u=username, n=name, p=pp, id=igba_id)
		)
	return True


