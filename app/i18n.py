from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import Request


class I18n:
	"""Lightweight JSON-based i18n manager."""

	def __init__(self, catalogs: Dict[str, Dict[str, Any]] | None = None, default_lang: str = "tr") -> None:
		self.catalogs: Dict[str, Dict[str, Any]] = catalogs or {}
		self.default_lang: str = default_lang

	@classmethod
	def load_from_dir(cls, directory: str | Path, default_lang: str = "tr") -> "I18n":
		base = Path(directory)
		catalogs: Dict[str, Dict[str, Any]] = {}
		if base.exists():
			for p in base.glob("*.json"):
				try:
					with p.open("r", encoding="utf-8") as f:
						catalogs[p.stem] = json.load(f) or {}
				except Exception:
					catalogs[p.stem] = {}
		return cls(catalogs=catalogs, default_lang=default_lang)

	def available_languages(self) -> list[str]:
		return sorted(self.catalogs.keys())

	def _lookup(self, lang: str, key: str) -> Optional[str]:
		data = self.catalogs.get(lang) or {}
		node: Any = data
		for part in key.split("."):
			if isinstance(node, dict) and part in node:
				node = node[part]
			else:
				return None
		if isinstance(node, str):
			return node
		return None

	def translate(self, lang: str, key: str, **kwargs: Any) -> str:
		# primary
		text = self._lookup(lang, key)
		# fallback to default language
		if text is None and self.default_lang and lang != self.default_lang:
			text = self._lookup(self.default_lang, key)
		# fallback to key
		if text is None:
			text = key
		if kwargs:
			try:
				text = text.format_map(_SafeDict(kwargs))
			except Exception:
				# best-effort formatting
				pass
		return text


def current_lang(request: Request, default: str = "tr") -> str:
	# Prefer session language (set via /i18n/set or user preference)
	try:
		s = getattr(request, "session", None)
		if isinstance(s, dict):
			lang = s.get("lang")
			if isinstance(lang, str) and lang:
				return lang
	except Exception:
		pass
	# Fallback to request.state.lang (may be set by middleware)
	try:
		lang = getattr(request.state, "lang", None)
		if isinstance(lang, str) and lang:
			return lang
	except Exception:
		pass
	# last-resort
	return default


def t(request: Request, key: str, **kwargs: Any) -> str:
	i18n = _ensure_i18n_loaded(request)
	lang = current_lang(request, default=(i18n.default_lang if i18n else "tr"))
	return i18n.translate(lang, key, **kwargs)


class _SafeDict(dict):
	"""Avoid KeyErrors in format_map by returning a placeholder for missing keys."""

	def __missing__(self, key: str) -> str:
		return "{" + key + "}"


def _ensure_i18n_loaded(request: Request) -> I18n:
	"""Lazy loader to guarantee an I18n instance is available at render time.
	Attempts to locate catalogs relative to the package and common working dirs.
	"""
	try:
		existing = getattr(request.app.state, "i18n", None)
		# Use an already-loaded manager when it has catalogs
		if isinstance(existing, I18n) and (existing.catalogs or {}):
			return existing
	except Exception:
		pass
	# Try to load catalogs now (best-effort)
	try:
		import os as _os
		from pathlib import Path as _Path
		default_lang = _os.getenv("DEFAULT_LANG", "tr")
		candidates = [
			_Path(__file__).resolve().parent / "locales",
			_Path.cwd() / "app" / "locales",
			_Path.cwd() / "locales",
			_Path("app/locales"),
		]
		catalog_dir = None
		for c in candidates:
			try:
				if c.exists() and any(c.glob("*.json")):
					catalog_dir = c
					break
			except Exception:
				continue
		if catalog_dir is None:
			catalog_dir = _Path(__file__).resolve().parent / "locales"
		mgr = I18n.load_from_dir(str(catalog_dir), default_lang=default_lang)
		try:
			request.app.state.i18n = mgr
		except Exception:
			# ignore inability to set; still return the manager
			pass
		return mgr
	except Exception:
		# Fallback empty manager; returns keys to surface missing translations
		mgr = I18n()
		try:
			request.app.state.i18n = mgr
		except Exception:
			pass
		return mgr


