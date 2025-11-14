import os
import datetime as dt
from typing import Optional

from sqlalchemy import text
import logging

from ..db import get_session
from .instagram_api import fetch_user_username, _get_base_token_and_id, GRAPH_VERSION, _get as graph_get
import httpx

_log = logging.getLogger("enricher")


def _ttl_hours(env_key: str, default: int) -> int:
	try:
		return int(os.getenv(env_key, str(default)))
	except Exception:
		return default


async def enrich_user(ig_user_id: str) -> bool:
	# TTL check - only skip if we have successful recent data
	with get_session() as session:
		row = session.exec(text("SELECT fetched_at, fetch_status FROM ig_users WHERE ig_user_id = :id").params(id=ig_user_id)).first()
		if row:
			fa = row.fetched_at if hasattr(row, "fetched_at") else (row[0] if isinstance(row, (list, tuple)) else None)
			fs = row.fetch_status if hasattr(row, "fetch_status") else (row[1] if isinstance(row, (list, tuple)) else None)
			if isinstance(fa, str):
				try:
					fa = dt.datetime.fromisoformat(fa)
				except Exception:
					fa = None
			if fa and str(fs).lower() == 'ok' and (dt.datetime.utcnow() - fa) < dt.timedelta(hours=_ttl_hours("USER_TTL_HOURS", 48)):
				try:
					_log.info("enrich_user: skip TTL uid=%s fetched_at=%s", ig_user_id, fa)
				except Exception:
					pass
				return False
	# Fetch username and name; profile picture URL is not available for all node types (e.g., IGBusinessScopedID)
	try:
		from .instagram_api import _get as graph_get, GRAPH_VERSION, _get_base_token_and_id
		import httpx
		token, _, _ = _get_base_token_and_id()
		base = f"https://graph.facebook.com/{GRAPH_VERSION}"
		# Fetch username and name only (avoid profile_picture_url to prevent Graph 400 on some node types)
		async with httpx.AsyncClient() as client:
			data_basic = await graph_get(client, base + f"/{ig_user_id}", {"access_token": token, "fields": "username,name"})
		username = data_basic.get("username") or data_basic.get("name")
		name = data_basic.get("name")
		profile_pic_url = data_basic.get("profile_picture_url")
		try:
			_log.info("enrich_user: graph ok uid=%s username=%s name=%s profile_pic=%s", ig_user_id, username, name, profile_pic_url)
		except Exception:
			pass
	except Exception as e:
		try:
			_log.warning("enrich_user: graph error uid=%s err=%s", ig_user_id, e)
		except Exception:
			pass
		with get_session() as session:
			session.exec(
				text("UPDATE ig_users SET fetch_status='error', fetch_error=:e WHERE ig_user_id=:id").params(
					id=ig_user_id, e=str(e)
				)
			)
		return False
	# Fallback: if username is still empty, try to infer from recent messages we have
	if not username:
		try:
			from sqlalchemy import text as _text
			with get_session() as session:
				row = session.exec(
					_text(
						"SELECT sender_username FROM message WHERE ig_sender_id=:u AND sender_username IS NOT NULL ORDER BY timestamp_ms DESC, id DESC LIMIT 1"
					).params(u=str(ig_user_id))
				).first()
				if row:
					# row could be RowMapping or tuple
					val = row.sender_username if hasattr(row, "sender_username") else (row[0] if isinstance(row, (list, tuple)) else None)
					if isinstance(val, str) and val.strip():
						username = val.strip()
			try:
				_log.info("enrich_user: fallback from messages uid=%s username=%s", ig_user_id, username)
			except Exception:
				pass
		except Exception:
			pass
	with get_session() as session:
		res = session.exec(
			text(
				"""
				UPDATE ig_users
				SET username=:u, name=:n, profile_pic_url=:p, fetched_at=CURRENT_TIMESTAMP, fetch_status='ok', fetch_error=NULL
				WHERE ig_user_id=:id
				"""
			).params(u=username, n=name, p=profile_pic_url, id=ig_user_id)
		)
	try:
		updated = 0
		try:
			updated = int(getattr(res, "rowcount", 0))  # type: ignore
		except Exception:
			updated = 0
		_log.info("enrich_user: done uid=%s username=%s updated_rows=%s", ig_user_id, username, updated)
	except Exception:
		pass
	# If no row was updated, insert a new ig_users row (idempotent)
	if 'updated' in locals() and updated == 0:
		try:
			with get_session() as session:
				# Try SQLite-style first; fall back to MySQL
				try:
					session.exec(
						text(
							"INSERT OR IGNORE INTO ig_users(ig_user_id, username, name, profile_pic_url, fetched_at, fetch_status) VALUES (:id, :u, :n, :p, CURRENT_TIMESTAMP, 'ok')"
						).params(id=ig_user_id, u=username, n=name, p=profile_pic_url)
					)
				except Exception:
					session.exec(
						text(
							"INSERT IGNORE INTO ig_users(ig_user_id, username, name, profile_pic_url, fetched_at, fetch_status) VALUES (:id, :u, :n, :p, CURRENT_TIMESTAMP, 'ok')"
						).params(id=ig_user_id, u=username, n=name, p=profile_pic_url)
					)
		except Exception:
			pass
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
	params = {"access_token": token, "fields": "username,name"}
	async with httpx.AsyncClient() as client:
		data = await graph_get(client, base + path, params)
		username = data.get("username")
		name = data.get("name")
	with get_session() as session:
		# Try UPDATE first; if no row affected, INSERT
		res = session.exec(
			text(
				"UPDATE ig_accounts SET username=:u, name=:n, updated_at=CURRENT_TIMESTAMP WHERE igba_id=:id"
			).params(u=username, n=name, id=igba_id)
		)
		rc = 0
		try:
			rc = int(getattr(res, "rowcount", 0))
		except Exception:
			rc = 0
		if rc == 0:
			try:
				# Dialect-aware insert
				with session.get_bind().begin() as conn:  # type: ignore
					try:
						conn.exec_driver_sql(
							"INSERT OR IGNORE INTO ig_accounts(igba_id, username, name, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
							(igba_id, username, name),
						)
					except Exception:
						# MySQL
						conn.exec_driver_sql(
							"INSERT IGNORE INTO ig_accounts(igba_id, username, name, updated_at) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)",
							(igba_id, username, name),
						)
			except Exception:
				pass
	return True


