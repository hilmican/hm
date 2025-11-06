from __future__ import annotations

from typing import Any, List, Tuple

from rapidfuzz import fuzz
from sqlmodel import select
from sqlalchemy import or_, and_

from ..models import Client, Order
from ..utils.normalize import normalize_phone, normalize_text
import datetime as dt


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
        .order_by(Order.shipment_date.is_(None), Order.shipment_date.desc(), Order.id.desc())
    ).all()
    return rows[0] if rows else None


def _orders_for_client_in_window(session, client_id: int, date_from: dt.date | None, date_to: dt.date | None) -> list[Order]:
    q = select(Order).where(Order.client_id == client_id)
    if date_from and date_to:
        q = q.where(
            or_(
                and_(Order.shipment_date.is_not(None), Order.shipment_date >= date_from, Order.shipment_date <= date_to),
                and_(Order.data_date.is_not(None), Order.data_date >= date_from, Order.data_date <= date_to),
            )
        )
    return session.exec(q.order_by(Order.id.desc())).all()


def link_order_for_extraction(session, extracted: dict[str, Any], *, date_from: dt.date | None, date_to: dt.date | None) -> int | None:
    """Given extracted buyer info from IG, attempt to link to a single existing order.

    Strategy:
    1) If phone present, find client by normalized phone; if exactly one client and exactly one order in window, link it.
    2) Else fuzzy match client by buyer_name/address; if one strong candidate and one order in window, link it.
    """
    phone = normalize_phone(extracted.get("phone"))
    buyer_name = (extracted.get("buyer_name") or "").strip()
    addr = (extracted.get("address") or "").strip()

    # 1) Phone-based client match
    client_by_phone: Client | None = None
    if phone:
        try:
            candidates = session.exec(select(Client).where(Client.phone.is_not(None))).all()
        except Exception:
            candidates = []
        matches = []
        for c in candidates:
            ph = normalize_phone(c.phone)
            if ph and (ph == phone or (len(phone) >= 7 and phone.endswith(ph[-7:]) or ph.endswith(phone[-7:]))):
                matches.append(c)
        if len(matches) == 1:
            client_by_phone = matches[0]
            orders = _orders_for_client_in_window(session, int(client_by_phone.id), date_from, date_to) if client_by_phone.id else []
            if len(orders) == 1:
                return int(orders[0].id)

    # 2) Fuzzy name/address match
    row = {"name": buyer_name, "address": addr, "city": ""}
    candidates = session.exec(select(Client)).all()
    best: tuple[Client, int] | None = None
    for c in candidates:
        sc = score_candidate(row, c)
        if best is None or sc > best[1]:
            best = (c, sc)
    if best and best[1] >= 80:
        c = best[0]
        orders = _orders_for_client_in_window(session, int(c.id), date_from, date_to) if c.id else []
        if len(orders) == 1:
            return int(orders[0].id)

    return None
