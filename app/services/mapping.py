from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple
import re

from sqlmodel import Session, select

from ..models import Item, Product, ItemMappingRule, ItemMappingOutput
from ..utils.slugify import slugify
from ..utils.normalize import strip_parenthetical_suffix


class MappingResult(Tuple[List[Dict[str, Any]], Optional[str]]):
	pass


def find_or_create_variant(session: Session, *, product: Product, size: Optional[str], color: Optional[str], pack_type: Optional[str], pair_multiplier: Optional[int]) -> Item:
	sku_parts = [product.slug]
	if size:
		sku_parts.append(slugify(size))
	if color:
		sku_parts.append(slugify(color))
	if pack_type:
		sku_parts.append(slugify(pack_type))
	sku = "-".join([p for p in sku_parts if p]) or product.slug

	item = session.exec(select(Item).where(Item.sku == sku)).first()
	if item:
		return item

	name_parts = [product.name]
	if size:
		name_parts.append(size)
	if color:
		name_parts.append(color)
	if pack_type:
		name_parts.append(pack_type.upper())
	item = Item(
		sku=sku,
		name=" - ".join([p for p in name_parts if p]),
		product_id=product.id,
		size=size,
		color=color,
		pack_type=pack_type,
		pair_multiplier=pair_multiplier or 1,
		unit=product.default_unit or "adet",
	)
	session.add(item)
	session.flush()
	return item


def resolve_mapping(session: Session, text: Optional[str]) -> Tuple[List[ItemMappingOutput], Optional[ItemMappingRule]]:
	"""Resolve text to mapping outputs using rules. First match wins by priority and mode.
	Returns (outputs, matched_rule) where outputs may be empty if unmatched.
	"""
	if not text:
		return [], None
	# normalize: strip a trailing parenthetical like "(175,75)" before matching
	raw = strip_parenthetical_suffix(text).strip()
	if not raw:
		return [], None

	rules = session.exec(
		select(ItemMappingRule).where(ItemMappingRule.is_active == True).order_by(ItemMappingRule.priority.desc(), ItemMappingRule.id.asc())
	).all()
	for rule in rules:
		pat = rule.source_pattern or ""
		matched = False
		if rule.match_mode == "exact":
			matched = (raw == pat) or (slugify(raw) == slugify(pat))
		elif rule.match_mode == "icontains":
			matched = pat.lower() in raw.lower()
		elif rule.match_mode == "regex":
			try:
				matched = bool(re.search(pat, raw, flags=re.IGNORECASE))
			except Exception:
				matched = False
		if not matched:
			continue
		outs = session.exec(select(ItemMappingOutput).where(ItemMappingOutput.rule_id == rule.id)).all()
		return outs, rule
	return [], None


