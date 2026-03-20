from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple
import re

from sqlmodel import Session, select

from ..models import Item, Product, ItemMappingRule, ItemMappingOutput
from ..utils.slugify import slugify
from ..utils.normalize import strip_parenthetical_suffix


class MappingResult(Tuple[List[Dict[str, Any]], Optional[str]]):
	pass


def build_variant_sku(product: Product, size: Optional[str], color: Optional[str]) -> str:
    """Deterministic SKU string for product+size+color (same as stored on Item)."""
    sku_parts = [product.slug or ""]
    if size:
        sku_parts.append(slugify(size))
    if color:
        sku_parts.append(slugify(color))
    return "-".join([p for p in sku_parts if p]) or (product.slug or "item")


def find_variant_if_exists(
    session: Session, *, product: Product, size: Optional[str], color: Optional[str]
) -> Optional[Item]:
    sku = build_variant_sku(product, size, color)
    return session.exec(select(Item).where(Item.sku == sku)).first()


def find_or_create_variant(session: Session, *, product: Product, size: Optional[str], color: Optional[str]) -> Item:
    item = find_variant_if_exists(session, product=product, size=size, color=color)
    if item:
        return item

    sku = build_variant_sku(product, size, color)
    name_parts = [product.name]
    if size:
        name_parts.append(size)
    if color:
        name_parts.append(color)
    item = Item(
        sku=sku,
        name=" - ".join([p for p in name_parts if p]),
        product_id=product.id,
        size=size,
        color=color,
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


