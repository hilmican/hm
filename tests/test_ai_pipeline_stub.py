from typing import Any, Dict

from app.services import ai_reply


class DummyClient:
	model = "dummy-model"
	enabled = True

	def __init__(self, *args: Any, **kwargs: Any) -> None:
		pass

	def generate_chat(self, **kwargs: Any) -> str:
		# Simulate Agent stage text output
		return "DM cevap"

	def generate_json(self, **kwargs: Any) -> Dict[str, Any]:
		# Simulate Serializer stage JSON output
		return {
			"should_reply": True,
			"reply_text": "DM cevap",
			"confidence": 0.9,
			"reason": "ok",
			"notes": None,
			"state": {"cart": []},
		}


def test_draft_reply_two_stage_stub(monkeypatch):
	# Stub heavy dependencies
	monkeypatch.setattr(ai_reply, "AIClient", DummyClient)
	monkeypatch.setattr(
		ai_reply,
		"_load_focus_product_and_stock",
		lambda cid: (
			{"id": 1, "name": "KAŞE CEKET", "slug_or_sku": "kase-ceket"},
			[{"sku": "kase-ceket", "name": "KAŞE CEKET", "color": "ANTRASİT", "size": "L", "price": 899}],
		),
	)
	monkeypatch.setattr(ai_reply, "_load_history", lambda cid, limit=40: ([], ""))
	monkeypatch.setattr(ai_reply, "_select_product_images_for_reply", lambda pid, variant_key=None: [])
	monkeypatch.setattr(ai_reply, "_detect_conversation_flags", lambda history, product_info: {})
	monkeypatch.setattr(ai_reply, "_load_customer_info", lambda cid: {"username": "test", "name": "Test", "contact_name": None})
	monkeypatch.setattr(ai_reply, "get_candidate_snapshot", lambda cid: None)
	monkeypatch.setattr(ai_reply, "get_ai_shadow_model_from_settings", lambda default="gpt-4o-mini": "dummy-model")

	reply = ai_reply.draft_reply(1, limit=5, include_meta=False, state={"upsell_offered": False})

	assert reply["reply_text"] == "DM cevap"
	assert reply["should_reply"] is True
	assert reply["state"] == {"cart": []}

