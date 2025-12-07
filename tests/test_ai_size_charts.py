from contextlib import contextmanager

from sqlmodel import SQLModel, Session, create_engine

import app.services.ai_utils as ai_utils
from app.models import Product, Item, SizeChart, SizeChartEntry, ProductSizeChart


def _build_session():
	engine = create_engine("sqlite:///:memory:")
	SQLModel.metadata.create_all(engine)
	return Session(engine)


def _assign_chart(session: Session, product: Product, size_label: str):
	chart = SizeChart(name="Test Chart")
	session.add(chart)
	session.flush()
	entry = SizeChartEntry(
		size_chart_id=chart.id,
		size_label=size_label,
		height_min=170,
		height_max=180,
		weight_min=65,
		weight_max=80,
	)
	session.add(entry)
	session.add(ProductSizeChart(product_id=product.id, size_chart_id=chart.id))
	session.commit()


def test_size_chart_overrides_matrix(monkeypatch):
	session = _build_session()
	p = Product(name="Tshirt", slug="tshirt")
	session.add(p)
	session.flush()
	item_m = Item(sku="TS-M", name="Tshirt M", product_id=p.id, size="M")
	item_l = Item(sku="TS-L", name="Tshirt L", product_id=p.id, size="L")
	session.add(item_m)
	session.add(item_l)
	session.commit()

	_assign_chart(session, p, "M")

	@contextmanager
	def _fake_session():
		yield session
	monkeypatch.setattr(ai_utils, "get_session", _fake_session)

	size = ai_utils.calculate_size_suggestion(175, 75, p.id)
	assert size == "M"


def test_fallback_to_matrix_when_no_chart(monkeypatch):
	session = _build_session()
	p = Product(name="Jean", slug="jean")
	session.add(p)
	session.flush()
	session.add(Item(sku="JN-30", name="Jean 30", product_id=p.id, size="30"))
	session.add(Item(sku="JN-31", name="Jean 31", product_id=p.id, size="31"))
	session.commit()

	@contextmanager
	def _fake_session():
		yield session
	monkeypatch.setattr(ai_utils, "get_session", _fake_session)

	size = ai_utils.calculate_size_suggestion(170, 65, p.id)
	assert size == "30"

