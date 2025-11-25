import pytest
from sqlmodel import SQLModel, create_engine, Session

from app.services import ai_orders


@pytest.fixture(autouse=True)
def in_memory_db(monkeypatch):
	engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
	SQLModel.metadata.create_all(engine)

	def _get_session():
		with Session(engine) as session:
			yield session

	monkeypatch.setattr("app.services.ai_orders.get_session", _get_session)
	yield


def test_mark_candidate_interested_creates_snapshot():
	result = ai_orders.mark_candidate_interested(101, note="first contact")
	assert result["status"] == ai_orders.STATUS_INTERESTED
	assert result["status_reason"] == "first contact"
	snapshot = ai_orders.get_candidate_snapshot(101)
	assert snapshot is not None
	assert snapshot["status_history"]
	assert snapshot["status_history"][-1]["status"] == ai_orders.STATUS_INTERESTED


def test_submit_candidate_order_sets_payload_and_status():
	ai_orders.mark_candidate_very_interested(202, note="address requested")
	payload = {
		"product": {
			"name": "Oversize Hoodie",
			"sku": "HD-001",
			"quantity": 1,
		},
		"customer": {
			"name": "Test User",
			"phone": "+905551112233",
			"address": "Atasehir, Istanbul",
		},
		"notes": "Teslimat gün içi olsun",
	}
	result = ai_orders.submit_candidate_order(202, payload, note="ai-order-ready")
	assert result["status"] == ai_orders.STATUS_PLACED
	assert result["order_payload"]["product"]["name"] == "Oversize Hoodie"
	assert result["order_payload"]["customer"]["phone"] == "+905551112233"
	assert result["placed_at"] is not None

