from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, Set, Tuple


def _norm(value: Any) -> Optional[str]:
	try:
		if value is None:
			return None
		text = str(value).strip().lower()
		return text or None
	except Exception:
		return None


@dataclass
class VariantExclusions:
	colors: Set[str] = field(default_factory=set)
	sizes: Set[str] = field(default_factory=set)
	combos: Set[Tuple[str, str]] = field(default_factory=set)

	def is_empty(self) -> bool:
		return not (self.colors or self.sizes or self.combos)


def _add_entry(exclusions: VariantExclusions, *, color: Any = None, size: Any = None) -> None:
	c = _norm(color)
	s = _norm(size)
	if c and s:
		exclusions.combos.add((c, s))
	elif c:
		exclusions.colors.add(c)
	elif s:
		exclusions.sizes.add(s)


def _ingest_iterable(exclusions: VariantExclusions, entries: Iterable[Any]) -> None:
	for entry in entries:
		if entry is None:
			continue
		if isinstance(entry, dict):
			_add_entry(
				exclusions,
				color=entry.get("color") or entry.get("renk"),
				size=entry.get("size") or entry.get("beden"),
			)
		else:
			_parse_fallback_token(exclusions, entry)


def _parse_fallback_token(exclusions: VariantExclusions, token: Any) -> None:
	text = _norm(token)
	if not text:
		return
	parts = (
		text.replace("=", ":")
		.replace("|", ":")
		.replace("/", ":")
		.replace(";", ",")
	)
	if ":" in parts:
		key, val = parts.split(":", 1)
		key = key.strip().lower()
		val = val.strip()
		if not val:
			return
		if key in ("color", "renk", "c"):
			_add_entry(exclusions, color=val)
		elif key in ("size", "beden", "s"):
			_add_entry(exclusions, size=val)
		elif key in ("variant", "v"):
			# allow format variant:red-s
			if "-" in val:
				candidate_color, candidate_size = val.split("-", 1)
				_add_entry(exclusions, color=candidate_color, size=candidate_size)
			else:
				_add_entry(exclusions, size=val)
		else:
			_add_entry(exclusions, size=val)
	else:
		if "-" in text:
			candidate_color, candidate_size = text.split("-", 1)
			_add_entry(exclusions, color=candidate_color, size=candidate_size)
		else:
			_add_entry(exclusions, size=text)


def parse_variant_exclusions(raw: Optional[Any]) -> VariantExclusions:
	"""
	Parse a product's variant exclusion configuration into a normalized structure.

	Accepted formats:
	- JSON string representing a dict: {"colors": ["red"], "sizes": ["s"], "variants": [{"color": "blue", "size": "m"}]}
	- JSON array of dicts / strings
	- Plain comma/semicolon separated tokens like "s,m" or "color:red,size:xl"
	"""
	exclusions = VariantExclusions()
	if raw is None:
		return exclusions
	data: Any = raw
	if isinstance(raw, str):
		text = raw.strip()
		if not text:
			return exclusions
		try:
			data = json.loads(text)
		except Exception:
			for token in text.replace(";", ",").split(","):
				_parse_fallback_token(exclusions, token)
			return exclusions
	if isinstance(data, dict):
		for key in ("colors", "color_list", "renkler"):
			if key in data and data[key] is not None:
				_ingest_iterable(exclusions, data[key] if isinstance(data[key], list) else [data[key]])
		for key in ("sizes", "size_list", "bedenler"):
			if key in data and data[key] is not None:
				_ingest_iterable(exclusions, data[key] if isinstance(data[key], list) else [data[key]])
		for key in ("variants", "entries", "exclude"):
			if key in data and data[key] is not None:
				_ingest_iterable(exclusions, data[key] if isinstance(data[key], list) else [data[key]])
	elif isinstance(data, list):
		_ingest_iterable(exclusions, data)
	else:
		_parse_fallback_token(exclusions, data)
	return exclusions


def variant_is_excluded(exclusions: VariantExclusions, color: Any = None, size: Any = None) -> bool:
	if exclusions.is_empty():
		return False
	c = _norm(color)
	s = _norm(size)
	if c and c in exclusions.colors:
		return True
	if s and s in exclusions.sizes:
		return True
	if c and s and (c, s) in exclusions.combos:
		return True
	return False

