from __future__ import annotations

from typing import Any, List, Tuple

from rapidfuzz import fuzz
from sqlmodel import select

from ..models import Client, Order


def score_candidate(row: dict[str, Any], client: Client) -> int:
	name = (row.get("name") or "")
	address = (row.get("address") or "")
	city = (row.get("city") or "")
	s1 = fuzz.token_set_ratio(name, client.name or "")
	s2 = fuzz.token_set_ratio(address, client.address or "")
	s3 = fuzz.token_set_ratio(city, client.city or "")
	return round(0.5 * s1 + 0.35 * s2 + 0.15 * s3)


def find_client_candidates(session, row: dict[str, Any], limit: int = 5) -> list[Tuple[Client, int]]:
	clients = session.exec(select(Client)).all()
	scored: list[Tuple[Client, int]] = []
	for c in clients:
		scored.append((c, score_candidate(row, c)))
	scored.sort(key=lambda x: x[1], reverse=True)
	return scored[:limit]


def find_order_by_tracking(session, tracking_no: str | None) -> Order | None:
	if not tracking_no:
		return None
	return session.exec(select(Order).where(Order.tracking_no == tracking_no)).first()
