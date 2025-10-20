from sqlmodel import Session

from app.models import Item, Product, StockMovement, ItemMappingRule, ItemMappingOutput, Order, OrderItem
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


def test_bizim_package_mapping_creates_order_items(tmp_path):
	from sqlmodel import select
	from app.db import engine
	from app.services.importer.committers import process_bizim_row
	with Session(engine) as s:
		# create a package rule that yields two different outputs and a duplicate
		r = ItemMappingRule(source_pattern="HILMI OZEL PAKET", match_mode="exact", priority=50, is_active=True)
		s.add(r)
		s.flush()
		# output 1: product-less (generic item by product created via base name)
		o1 = ItemMappingOutput(rule_id=r.id or 0, quantity=1)
		s.add(o1)
		# output 2: another generic line, quantity 2
		o2 = ItemMappingOutput(rule_id=r.id or 0, quantity=2)
		s.add(o2)
		s.commit()

		run = type("Run", (), {"data_date": None, "created_clients": 0, "updated_clients": 0, "created_items": 0, "created_orders": 0, "created_payments": 0, "unmatched_count": 0})()
		rec = {
			"name": "MEHMET OZ",
			"phone": "5370000000",
			"city": "ADANA",
			"item_name": "HILMI OZEL PAKET(170,80)",
			"quantity": 1,
			"total_amount": 1400,
		}
		status, message, client_id, order_id = process_bizim_row(s, run, rec)
		s.commit()
		assert order_id is not None
		# verify order items exist: expected 1*1 + 1*2 = 3 total across 2 lines
		rows = s.exec(select(OrderItem).where(OrderItem.order_id == order_id)).all()
		assert len(rows) == 2
		qsum = sum(r.quantity for r in rows)
		assert qsum == 3


