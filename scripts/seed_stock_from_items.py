from __future__ import annotations

import re
from typing import Optional

from sqlmodel import select

from app.db import get_session
from app.models import Item, Product, ItemMappingRule, ItemMappingOutput
from app.utils.slugify import slugify


SIZE_TOKENS = ["S","M","L","XL","XXL","XS","XXXL","3XL","4XL","30","31","32","33","34","35","36","38","40","42"]
PACK_KEYWORDS = {"TEK": ("tek", 1), "ÇİFT": ("cift", 2), "CIFT": ("cift", 2), "CIFt": ("cift", 2)}


def guess_attributes(name: str) -> tuple[str, Optional[str], Optional[str], Optional[str]]:
	base = name
	size = None
	color = None
	pack = None
	# basic heuristics
	up = name.upper()
	for k, (p, mult) in PACK_KEYWORDS.items():
		if k in up:
			pack = p
			break
	for tok in SIZE_TOKENS:
		if re.search(rf"\b{re.escape(tok)}\b", up):
			size = tok
			break
	# color hints
	for col in ["SİYAH","SIYAH","LACİVERT","LACIVERT","KREM","AÇIK GRİ","ACIK GRI","ACIK GRİ","GRI","GRİ","BEYAZ"]:
		if col in up:
			color = col.title()
			break
	# derive base by removing found tokens
	rem = up
	if size:
		rem = re.sub(rf"\b{re.escape(size)}\b", "", rem)
	if color:
		rem = rem.replace(color.upper(), "")
	for k in PACK_KEYWORDS.keys():
		rem = rem.replace(k, "")
	base = re.sub(r"\s+", " ", rem).strip().title()
	return base or name, size, color, pack


def main() -> None:
	with get_session() as session:
		rows = session.exec(select(Item).order_by(Item.id.asc())).all()
		for it in rows:
			base, size, color, pack = guess_attributes(it.name or it.sku)
			pslug = slugify(base)
			prod = session.exec(select(Product).where(Product.slug == pslug)).first()
			if not prod:
				prod = Product(name=base, slug=pslug)
				session.add(prod)
				session.flush()
			if not it.product_id:
				it.product_id = prod.id  # type: ignore
			if size and not it.size:
				it.size = size
			if color and not it.color:
				it.color = color
			if pack and not it.pack_type:
				it.pack_type = pack
			# create an exact mapping rule for this original name -> this item
			existing_rule = session.exec(select(ItemMappingRule).where(ItemMappingRule.source_pattern == it.name, ItemMappingRule.match_mode == "exact")).first()
			if not existing_rule:
				r = ItemMappingRule(source_pattern=it.name, match_mode="exact", priority=100, is_active=True)
				session.add(r)
				session.flush()
				out = ItemMappingOutput(rule_id=r.id or 0, item_id=it.id, quantity=1)
				session.add(out)


if __name__ == "__main__":
	main()


