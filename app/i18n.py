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
	lang = getattr(request.state, "lang", None)
	if isinstance(lang, str) and lang:
		return lang
	# last-resort
	return default


def t(request: Request, key: str, **kwargs: Any) -> str:
	i18n: I18n | None = getattr(request.app.state, "i18n", None)
	lang = current_lang(request, default=(i18n.default_lang if i18n else "tr"))
	if i18n is None:
		# no manager yet; return key
		return key
	return i18n.translate(lang, key, **kwargs)


class _SafeDict(dict):
	"""Avoid KeyErrors in format_map by returning a placeholder for missing keys."""

	def __missing__(self, key: str) -> str:
		return "{" + key + "}"


