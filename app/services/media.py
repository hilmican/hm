import os
import io
import hashlib
import datetime as dt
from pathlib import Path
from typing import Any, Dict, Optional

import httpx
from sqlalchemy import text

from ..db import get_session
from ..services.instagram_api import GRAPH_VERSION, _get as graph_get, _get_base_token_and_id

try:
	from PIL import Image
	_PIL_AVAILABLE = True
except Exception:
	_PIL_AVAILABLE = False


def _media_roots() -> tuple[Path, Path]:
	media_root = Path(os.getenv("MEDIA_ROOT", "data/media")).resolve()
	thumbs_root = Path(os.getenv("THUMBS_ROOT", "data/thumbs")).resolve()
	media_root.mkdir(parents=True, exist_ok=True)
	thumbs_root.mkdir(parents=True, exist_ok=True)
	return media_root, thumbs_root


async def _resolve_attachment_url(ig_message_id: str, position: int) -> tuple[Optional[str], Optional[str]]:
	"""Return (url, mime) for a message's attachment index via Graph attachments."""
	token, _, _ = _get_base_token_and_id()
	base = f"https://graph.facebook.com/{GRAPH_VERSION}"
	path = f"/{ig_message_id}/attachments"
	params = {"access_token": token, "fields": "mime_type,file_url,image_data{url,preview_url},name"}
	async with httpx.AsyncClient() as client:
		data = await graph_get(client, base + path, params)
		arr = data.get("data") or []
		if isinstance(arr, list) and position < len(arr):
			att = arr[position] or {}
			mime = att.get("mime_type")
			url = att.get("file_url") or ((att.get("image_data") or {}).get("url")) or ((att.get("image_data") or {}).get("preview_url"))
			return url, mime
	return None, None


def _make_paths(kind: str, ig_message_id: str, position: int, mime: Optional[str]) -> tuple[Path, Optional[Path]]:
	media_root, thumbs_root = _media_roots()
	now = dt.datetime.utcnow()
	subdir = Path(kind + "s") / f"{now.year:04d}" / f"{now.month:02d}" / f"{now.day:02d}"
	# pick ext from mime
	ext = "bin"
	if mime:
		if "/" in mime:
			ext = mime.split("/")[-1]
		elif mime.startswith("image"):
			ext = "jpg"
	name = f"{ig_message_id}_{position}.{ext}"
	thumb_name = f"{ig_message_id}_{position}_thumb.jpg"
	return (media_root / subdir / name, thumbs_root / subdir / thumb_name)


def _sha256_bytes(buf: bytes) -> str:
	return hashlib.sha256(buf).hexdigest()


def _write_file(path: Path, content: bytes) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	path.write_bytes(content)


def _make_thumb(src_bytes: bytes, thumb_path: Path) -> None:
	if not _PIL_AVAILABLE:
		return
	try:
		thumb_path.parent.mkdir(parents=True, exist_ok=True)
		im = Image.open(io.BytesIO(src_bytes))
		im.thumbnail((512, 512))
		im.save(thumb_path, format="JPEG", quality=85)
	except Exception:
		pass


async def fetch_and_store(attachment_id: int) -> bool:
	"""Fetch one attachment if pending/error and store to local FS, update DB."""
	with get_session() as session:
		row = session.exec(
			text(
				"""
				SELECT a.id, a.graph_id, a.position, m.ig_message_id
				FROM attachments a JOIN message m ON a.message_id = m.id
				WHERE a.id = :id
				"""
			)
		).params(id=attachment_id).first()
		if not row:
			return False
		att_id = row.id if hasattr(row, "id") else row[0]
		graph_id = row.graph_id if hasattr(row, "graph_id") else row[1]
		position = row.position if hasattr(row, "position") else row[2]
		ig_mid = row.ig_message_id if hasattr(row, "ig_message_id") else row[3]
		url: Optional[str] = None
		mime: Optional[str] = None
		# Prefer resolving by message attachments + index
		url, mime = await _resolve_attachment_url(str(ig_mid), int(position or 0))
		if not url:
			return False
		# download
		async with httpx.AsyncClient() as client:
			r = await client.get(url, timeout=60, follow_redirects=True)
			r.raise_for_status()
			content = r.content
			mime = mime or r.headers.get("content-type")
		checksum = _sha256_bytes(content)
		# determine kind by mime
		kind = "file"
		if mime and mime.startswith("image"):
			kind = "image"
		elif mime and mime.startswith("video"):
			kind = "video"
		elif mime and mime.startswith("audio"):
			kind = "audio"
		path, thumb_path = _make_paths(kind, str(ig_mid), int(position or 0), mime)
		_write_file(path, content)
		if kind == "image" and thumb_path is not None:
			_make_thumb(content, thumb_path)
		size_bytes = len(content)
		# Update row
		session.exec(
			text(
				"""
				UPDATE attachments
				SET mime=:mime, size_bytes=:size, checksum_sha256=:sum, storage_path=:sp, thumb_path=:tp, fetched_at=CURRENT_TIMESTAMP, fetch_status='ok'
				WHERE id=:id
				"""
			)
		).params(mime=mime or "application/octet-stream", size=size_bytes, sum=checksum, sp=str(path), tp=str(thumb_path) if thumb_path else None, id=attachment_id)
		# increment per-mime counters for NOC
		try:
			from .monitoring import increment_counter as _inc
			_inc("media_fetch", 1)
			if kind == "image":
				_inc("media_image", 1)
			elif kind == "video":
				_inc("media_video", 1)
			elif kind == "audio":
				_inc("media_audio", 1)
		except Exception:
			pass
		return True


