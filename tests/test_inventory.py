from sqlmodel import Session

from app.models import Item, Product, StockMovement, ItemMappingRule, ItemMappingOutput
from app.utils.slugify import slugify
from app.services.inventory import compute_on_hand_for_items


def test_compute_on_hand(tmp_path):
	# basic in/out aggregation
	from app.db import engine
	with Session(engine) as s:
		prod = Product(name="TEST", slug=slugify("TEST"))
		s.add(prod)
		s.flush()
		it = Item(sku="test-sku", name="TEST", product_id=prod.id)
		s.add(it)
		s.flush()
		s.add(StockMovement(item_id=it.id, direction="in", quantity=10))
		s.add(StockMovement(item_id=it.id, direction="out", quantity=3))
		s.commit()
		m = compute_on_hand_for_items(s, [it.id])
		assert m.get(it.id or 0, 0) == 7


def test_mapping_rule_exact(tmp_path):
	from sqlmodel import select
	from app.db import engine
	from app.services.mapping import resolve_mapping
	with Session(engine) as s:
		r = ItemMappingRule(source_pattern="DERI TRENCH TEK", match_mode="exact", priority=10, is_active=True)
		s.add(r)
		s.flush()
		out = ItemMappingOutput(rule_id=r.id or 0, quantity=1)
		s.add(out)
		s.commit()
		outs, rule = resolve_mapping(s, "DERI TRENCH TEK")
		assert rule is not None
		assert len(outs) == 1


