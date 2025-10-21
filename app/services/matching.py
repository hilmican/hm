from __future__ import annotations

from typing import Any, List, Tuple

from rapidfuzz import fuzz
from sqlmodel import select
from sqlalchemy import or_, and_

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


def find_order_by_client_and_date(session, client_id: int | None, date_val) -> Order | None:
    """Find an order for a client around a date, preferring source='bizim'.

    Matches if either data_date OR shipment_date falls within ±7 days.
    """
    if not client_id or not date_val:
        return None
    from datetime import timedelta
    start = date_val - timedelta(days=7)
    end = date_val + timedelta(days=7)
    rows = session.exec(
        select(Order).where(
            and_(
                Order.client_id == client_id,
                or_(
                    and_(Order.data_date >= start, Order.data_date <= end),
                    and_(Order.shipment_date >= start, Order.shipment_date <= end),
                ),
            )
        )
    ).all()
    if not rows:
        return None
    bizim = [o for o in rows if (o.source or "") == "bizim"]
    if bizim:
        return bizim[0]
    return rows[0]


def find_recent_placeholder_kargo_for_client(session, client_id: int, days: int = 7) -> Order | None:
    """Find the most recent kargo placeholder order for a client within N days.

    Used when a bizim row lacks a date but we want to upgrade an existing kargo placeholder.
    """
    from datetime import date, timedelta
    cutoff = date.today() - timedelta(days=days)
    rows = session.exec(
        select(Order)
        .where(
            Order.client_id == client_id,
            Order.source == "kargo",
            Order.status == "placeholder",
            or_(Order.shipment_date == None, Order.shipment_date >= cutoff),
        )
        .order_by(Order.shipment_date.desc().nullslast(), Order.id.desc())
    ).all()
    return rows[0] if rows else None
