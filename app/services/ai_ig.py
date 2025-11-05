from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, Optional, Tuple, List

from sqlmodel import select

from ..db import get_session
from ..models import Message
from .ai import AIClient
from .prompts import IG_PURCHASE_SYSTEM_PROMPT


def _format_transcript(messages: List[Message], max_chars: int = 15000) -> str:
    parts: List[str] = []
    for m in messages:
        role = (m.direction or "in").lower()
        ts = m.timestamp_ms or 0
        txt = (m.text or "").strip()
        parts.append(f"[{role}] {ts}: {txt}")
    txt = "\n".join(parts)
    # trim to model budget if needed
    if len(txt) > max_chars:
        return txt[-max_chars:]
    return txt


def analyze_conversation(conversation_id: str, *, limit: int = 200) -> Dict[str, Any]:
    """Run AI over a single conversation to detect purchase and extract contacts.

    Returns a dict with keys: purchase_detected, buyer_name, phone, address, notes,
    product_mentions (list), possible_order_ids (list)
    """
    client = AIClient()
    if not client.enabled:
        raise RuntimeError("AI client is not configured. Set OPENAI_API_KEY.")
    with get_session() as session:
        msgs = session.exec(
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.timestamp_ms.asc())
            .limit(min(max(limit, 1), 500))
        ).all()
    transcript = _format_transcript(msgs)
    schema_hint = (
        '{"purchase_detected": true|false, "buyer_name": "str|null", "phone": "str|null", '
        '"address": "str|null", "notes": "str|null", "product_mentions": ["str"], '
        '"possible_order_ids": ["str"]}'
    )
    user_prompt = (
        "Aşağıda bir DM konuşması transkripti var. \n"
        "Lütfen kesin satın alma olup olmadığını belirle ve bilgileri çıkar.\n\n"
        f"Şema: {schema_hint}\n\n"
        f"Transkript:\n{transcript}"
    )
    data = client.generate_json(system_prompt=IG_PURCHASE_SYSTEM_PROMPT, user_prompt=user_prompt)
    if not isinstance(data, dict):
        raise RuntimeError("AI returned non-dict JSON")
    # Normalize keys presence
    out: Dict[str, Any] = {
        "purchase_detected": bool(data.get("purchase_detected", False)),
        "buyer_name": data.get("buyer_name"),
        "phone": data.get("phone"),
        "address": data.get("address"),
        "notes": data.get("notes"),
        "product_mentions": data.get("product_mentions") or [],
        "possible_order_ids": data.get("possible_order_ids") or [],
    }
    return out


def process_run(*, run_id: int, date_from: Optional[dt.date], date_to: Optional[dt.date], min_age_minutes: int = 60, limit: int = 200) -> Dict[str, Any]:
    """Process eligible conversations for a run id and persist results.

    Returns counters summary.
    """
    from sqlalchemy import text as _text
    from .matching import link_order_for_extraction  # lazy import

    now = dt.datetime.utcnow()
    min_age_delta = dt.timedelta(minutes=max(0, int(min_age_minutes)))
    cutoff_dt = now - min_age_delta

    considered = 0
    processed = 0
    linked = 0
    purchases = 0
    purchases_unlinked = 0
    errors: List[str] = []

    with get_session() as session:
        # Select eligible conversations by last_message_at and ai_processed_at is NULL
        params: Dict[str, Any] = {"cutoff": cutoff_dt.isoformat(" ")}
        where = ["ai_processed_at IS NULL", "last_message_at <= :cutoff"]
        if date_from and date_to and date_from <= date_to:
            dt_end = date_to + dt.timedelta(days=1)
            params["df"] = f"{date_from.isoformat()} 00:00:00"
            params["dte"] = f"{dt_end.isoformat()} 00:00:00"
            where.append("last_message_at >= :df AND last_message_at < :dte")
        elif date_from:
            params["df"] = f"{date_from.isoformat()} 00:00:00"
            where.append("last_message_at >= :df")
        elif date_to:
            dt_end = date_to + dt.timedelta(days=1)
            params["dte"] = f"{dt_end.isoformat()} 00:00:00"
            where.append("last_message_at < :dte")
        sql = (
            "SELECT convo_id FROM conversations WHERE " + " AND ".join(where) + " ORDER BY last_message_at DESC LIMIT :lim"
        )
        params["lim"] = int(limit)
        rows = session.exec(_text(sql)).params(**params).all()
        convo_ids = [r.convo_id if hasattr(r, "convo_id") else r[0] for r in rows]
        considered = len(convo_ids)

    # Helper to check cancellation flag quickly
    def _is_cancelled() -> bool:
        try:
            with get_session() as s2:
                row = s2.exec(_text("SELECT cancelled_at FROM ig_ai_run WHERE id=:id").params(id=int(run_id))).first()
                if not row:
                    return False
                val = row.cancelled_at if hasattr(row, 'cancelled_at') else (row[0] if isinstance(row, (list, tuple)) else None)
                return bool(val)
        except Exception:
            return False

    if _is_cancelled():
        errors.append("cancelled")
        with get_session() as session:
            try:
                session.exec(_text("UPDATE ig_ai_run SET completed_at=CURRENT_TIMESTAMP WHERE id=:id").params(id=int(run_id)))
            except Exception:
                pass
        return {
            "considered": 0,
            "processed": 0,
            "linked": 0,
            "purchases": 0,
            "purchases_unlinked": 0,
            "errors": errors,
        }

    for cid in convo_ids:
        try:
            data = analyze_conversation(cid)
        except Exception as e:
            errors.append(f"{cid}: {e}")
            # persist error status so we don't spin forever; keep ai_json for debugging
            with get_session() as session:
                try:
                    session.exec(
                        _text(
                            "UPDATE conversations SET ai_status=:s, ai_json=:j, ai_processed_at=CURRENT_TIMESTAMP, ai_run_id=:rid WHERE convo_id=:cid"
                        )
                    ).params(s="error", j=json.dumps({"error": str(e)}), rid=run_id, cid=cid)
                except Exception:
                    pass
            continue

        status = "no_purchase"
        linked_order_id: Optional[int] = None
        if bool(data.get("purchase_detected")):
            purchases += 1
            try:
                with get_session() as session:
                    linked_order_id = link_order_for_extraction(session, data, date_from=date_from, date_to=date_to)
            except Exception as le:
                errors.append(f"{cid}: match_err {le}")
                linked_order_id = None
            if linked_order_id:
                linked += 1
                status = "ok"
            else:
                purchases_unlinked += 1
                status = "ambiguous"

        with get_session() as session:
            try:
                # Persist conversations fields
                session.exec(
                    _text(
                        """
                        UPDATE conversations SET
                          contact_name = COALESCE(contact_name, :name),
                          contact_phone = COALESCE(contact_phone, :phone),
                          contact_address = COALESCE(contact_address, :addr),
                          ai_status = :st,
                          ai_json = :js,
                          ai_processed_at = CURRENT_TIMESTAMP,
                          linked_order_id = COALESCE(linked_order_id, :oid),
                          ai_run_id = :rid
                        WHERE convo_id = :cid
                        """
                    )
                ).params(
                    name=(data.get("buyer_name") or None),
                    phone=(data.get("phone") or None),
                    addr=(data.get("address") or None),
                    st=status,
                    js=json.dumps(data, ensure_ascii=False),
                    oid=linked_order_id,
                    rid=run_id,
                    cid=cid,
                )
                # Also back-fill order.ig_conversation_id if linked
                if linked_order_id and cid:
                    session.exec(
                        _text('UPDATE "order" SET ig_conversation_id = COALESCE(ig_conversation_id, :cid) WHERE id=:oid')
                    ).params(cid=cid, oid=int(linked_order_id))
                processed += 1
            except Exception as pe:
                errors.append(f"{cid}: persist_err {pe}")

        # cancellation check between items
        if _is_cancelled():
            errors.append("cancelled")
            break

    # Update run row
    with get_session() as session:
        try:
            session.exec(
                _text(
                    """
                    UPDATE ig_ai_run SET
                      completed_at = CURRENT_TIMESTAMP,
                      conversations_considered = :cns,
                      conversations_processed = :prs,
                      orders_linked = :lnk,
                      purchases_detected = :pur,
                      purchases_unlinked = :pun,
                      errors_json = :err
                    WHERE id = :rid
                    """
                )
            ).params(
                cns=considered,
                prs=processed,
                lnk=linked,
                pur=purchases,
                pun=purchases_unlinked,
                err=json.dumps(errors, ensure_ascii=False) if errors else None,
                rid=run_id,
            )
        except Exception:
            pass

    return {
        "considered": considered,
        "processed": processed,
        "linked": linked,
        "purchases": purchases,
        "purchases_unlinked": purchases_unlinked,
        "errors": errors,
    }


