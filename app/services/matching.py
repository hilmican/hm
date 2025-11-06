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
    rows = session.exec(q.order_by(Order.id.desc())).all()
    return rows


def _order_priority(order: Order) -> tuple[int, dt.date, dt.date, int]:
    src_score = 1 if (order.source or "").lower() == "bizim" else 0
    data_date = order.data_date or dt.date.min
    ship_date = order.shipment_date or dt.date.min
    # prefer whichever date is more recent
    best_date = data_date if data_date >= ship_date else ship_date
    return (src_score, best_date, ship_date, int(order.id or 0))


def _choose_best_order(orders: list[Order]) -> Order | None:
    if not orders:
        return None
    scored = sorted(((o, _order_priority(o)) for o in orders), key=lambda item: item[1], reverse=True)
    top_order, top_score = scored[0]
    if len(scored) == 1:
        return top_order
    second_score = scored[1][1]
    if top_score == second_score:
        return None
    return top_order


def link_order_for_extraction(session, extracted: dict[str, Any], *, date_from: dt.date | None, date_to: dt.date | None) -> int | None:
    """Given extracted buyer info from IG, attempt to link to a single existing order.

    Strategy:
    1) If phone present, find client by normalized phone; if exactly one client and exactly one order in window, link it.
    2) Else fuzzy match client by buyer_name/address; if one strong candidate and one order in window, link it.
    """
    phone_digits = normalize_phone(extracted.get("phone"))
    phone_last10 = phone_digits[-10:] if len(phone_digits) >= 10 else phone_digits
    buyer_name = (extracted.get("buyer_name") or "").strip()
    addr = (extracted.get("address") or "").strip()
    price_val = extracted.get("price")

    def _price_num(v: Any) -> float | None:
        if v is None:
            return None
        try:
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v)
            import re
            cleaned = re.sub(r"[^0-9,\.]", "", s).replace(",", ".")
            if not cleaned:
                return None
            return float(cleaned)
        except Exception:
            return None

    price_num = _price_num(price_val)

    # 1) Phone-based client match
    client_by_phone: Client | None = None
    if phone_last10:
        try:
            candidates = session.exec(select(Client).where(Client.phone.is_not(None))).all()
        except Exception:
            candidates = []
        matches = []
        for c in candidates:
            ph_digits = normalize_phone(c.phone)
            if not ph_digits:
                continue
            # Prefer exact last-10 match, else allow substring contains
            c_last10 = ph_digits[-10:] if len(ph_digits) >= 10 else ph_digits
            if (c_last10 and c_last10 == phone_last10) or (phone_last10 in ph_digits):
                matches.append(c)
        # If exactly one client matches by phone, try to link order
        if len(matches) == 1:
            client_by_phone = matches[0]
            orders = _orders_for_client_in_window(session, int(client_by_phone.id), date_from, date_to) if client_by_phone.id else []
            best_order = None
            if price_num is not None and orders:
                # choose order by closest amount to extracted price
                def _order_amount(o: Order) -> float | None:
                    if o.total_amount is not None:
                        return float(o.total_amount)
                    amt = 0.0
                    if o.unit_price is not None and o.quantity is not None:
                        amt += float(o.unit_price) * float(o.quantity)
                    if o.shipping_fee is not None:
                        amt += float(o.shipping_fee)
                    return amt or None
                scored: list[tuple[Order, float]] = []
                for o in orders:
                    oa = _order_amount(o)
                    if oa is None:
                        continue
                    scored.append((o, abs(oa - float(price_num))))
                if scored:
                    scored.sort(key=lambda t: t[1])
                    top_o, top_delta = scored[0]
                    # accept within reasonable tolerance (<= 50 TL or <= 8%)
                    tol = max(50.0, 0.08 * float(price_num))
                    if top_delta <= tol:
                        best_order = top_o
            if best_order is None:
                best_order = _choose_best_order(orders)
            if best_order and best_order.id is not None:
                return int(best_order.id)
        # If multiple clients matched by phone substring, try to disambiguate by having orders in window
        if len(matches) > 1:
            viable: list[tuple[Client, Order]] = []
            for c in matches:
                orders = _orders_for_client_in_window(session, int(c.id), date_from, date_to) if c.id else []
                best_order = _choose_best_order(orders)
                if best_order and best_order.id is not None:
                    viable.append((c, best_order))
            if len(viable) == 1:
                return int(viable[0][1].id) if viable[0][1].id is not None else None
            if len(viable) > 1 and price_num is not None:
                # multiple viable: use price to pick best
                def _order_amount(o: Order) -> float | None:
                    if o.total_amount is not None:
                        return float(o.total_amount)
                    amt = 0.0
                    if o.unit_price is not None and o.quantity is not None:
                        amt += float(o.unit_price) * float(o.quantity)
                    if o.shipping_fee is not None:
                        amt += float(o.shipping_fee)
                    return amt or None
                best_pair = None
                best_delta = None
                for c, o in viable:
                    oa = _order_amount(o)
                    if oa is None:
                        continue
                    d = abs(oa - float(price_num))
                    if best_pair is None or d < float(best_delta):
                        best_pair = (c, o)
                        best_delta = d
                if best_pair is not None and best_pair[1].id is not None:
                    return int(best_pair[1].id)

    # 2) Exact slug/name key match before fuzzy
    if buyer_name:
        try:
            from ..utils.normalize import normalize_key

            buyer_key = normalize_key(buyer_name)
            if buyer_key:
                clients = session.exec(select(Client)).all()
                exact_candidates: list[Client] = []
                for c in clients:
                    c_key = normalize_key(c.name)
                    if c_key and c_key == buyer_key:
                        exact_candidates.append(c)
                if len(exact_candidates) == 1:
                    orders = _orders_for_client_in_window(session, int(exact_candidates[0].id), date_from, date_to) if exact_candidates[0].id else []
                    best_order = _choose_best_order(orders)
                    if best_order and best_order.id is not None:
                        return int(best_order.id)
        except Exception:
            pass

    # 3) Fuzzy name/address match
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
        best_order = _choose_best_order(orders)
        if best_order and best_order.id is not None:
            return int(best_order.id)

    return None
