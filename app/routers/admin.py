import os
import json
from pathlib import Path
from typing import Optional, Dict, Any

from fastapi import APIRouter, Request, HTTPException
from sqlalchemy import text
from sqlmodel import select

from ..db import get_session
from ..services.queue import _get_redis
from ..services.queue import enqueue
from ..models import Message, Client, Order


router = APIRouter(prefix="/admin", tags=["admin"])


def _read_git_version(repo_root: Path = Path(".")) -> Optional[str]:
	# Prefer explicit env override
	for key in ("APP_VERSION", "HM_VERSION", "RELEASE_VERSION"):
		val = os.getenv(key)
		if val:
			return str(val)
	git_dir = repo_root / ".git"
	try:
		head = (git_dir / "HEAD").read_text().strip()
		if head.startswith("ref:"):
			ref = head.split(" ", 1)[1].strip()
			ref_path = git_dir / ref
			if ref_path.exists():
				sha = ref_path.read_text().strip()
				return sha[:12]
			# packed-refs fallback
			packed = (git_dir / "packed-refs").read_text().splitlines()
			for line in packed:
				if line.startswith("#") or not line.strip():
					continue
				if line.endswith(" " + ref):
					sha = line.split(" ", 1)[0].strip()
					return sha[:12]
		else:
			# Detached HEAD contains SHA directly
			return head[:12]
	except Exception:
		return None


def _check_db() -> Dict[str, Any]:
	try:
		with get_session() as session:
			session.exec(text("SELECT 1")).first()
			# quick counts (cheap) with robust row extraction
			counts: Dict[str, Any] = {}
			for tbl in ("message", "attachments", "raw_events", "jobs"):
				try:
					row = session.exec(text(f"SELECT COUNT(1) AS c FROM {tbl}")).first()
					val = None
					if row is None:
						val = 0
					elif isinstance(row, (list, tuple)):
						val = int(row[0])
					else:
						# RowMapping-like
						try:
							val = int(getattr(row, "c", 0))
						except Exception:
							# attempt index access
							try:
								val = int(row[0])  # type: ignore
							except Exception:
								val = None
					counts[tbl] = val
				except Exception:
					counts[tbl] = None
			return {"ok": True, "counts": counts}
	except Exception as e:
		return {"ok": False, "error": str(e)}


def _check_redis() -> Dict[str, Any]:
	try:
		from ..services.queue import _get_redis  # lazy import
		r = _get_redis()
		pong = r.ping()
		return {"ok": bool(pong)}
	except Exception as e:
		return {"ok": False, "error": str(e)}


@router.get("/version")
def version() -> Dict[str, Any]:
	return {"version": _read_git_version() or "unknown"}


@router.get("/health")
def health() -> Dict[str, Any]:
	media_root = Path(os.getenv("MEDIA_ROOT", "data/media"))
	thumbs_root = Path(os.getenv("THUMBS_ROOT", "data/thumbs"))
	return {
		"status": "ok",
		"version": _read_git_version() or "unknown",
		"db": _check_db(),
		"redis": _check_redis(),
		"media_root_exists": media_root.exists(),
		"thumbs_root_exists": thumbs_root.exists(),
	}


@router.get("/status")
def status_page(request: Request):
	templates = request.app.state.templates
	ctx = {
		"version": _read_git_version() or "unknown",
		"db": _check_db(),
		"redis": _check_redis(),
	}
	return templates.TemplateResponse("admin_status.html", {"request": request, **ctx})


@router.post("/clients/merge")
def merge_clients(source_id: int, target_id: int):
    """Manually merge/link two clients.

    - Reassign all orders from source -> target
    - Mark source as merged into target
    """
    if not source_id or not target_id or source_id == target_id:
        raise HTTPException(status_code=400, detail="Invalid source/target")
    with get_session() as session:
        source = session.get(Client, source_id)
        target = session.get(Client, target_id)
        if not source or not target:
            raise HTTPException(status_code=404, detail="Client not found")
        # reassign orders
        reassigned = 0
        try:
            rows = session.exec(select(Order).where(Order.client_id == source_id)).all()
            for o in rows:
                o.client_id = target_id
                session.add(o)
                reassigned += 1
        except Exception:
            pass
        # mark source merged
        source.merged_into_client_id = target_id
        source.status = "merged"
        session.add(source)
        try:
            session.commit()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"merge failed: {e}")
        return {"status": "ok", "reassigned_orders": reassigned, "source_id": source_id, "target_id": target_id}


@router.post("/orders/fix-cancelled")
def fix_cancelled_orders():
    """Fix all cancelled orders that have non-zero financial values.
    
    Sets total_amount, total_cost, and shipping_fee to 0.0
    for all orders with status='cancelled' that still have non-zero values.
    """
    from sqlalchemy import or_, and_
    fixed_count = 0
    fixed_orders = []
    with get_session() as session:
        # Find all cancelled orders with non-zero financials
        cancelled_orders = session.exec(
            select(Order).where(
                Order.status == "cancelled",
                # At least one financial field is non-zero
                or_(
                    and_(Order.total_amount.is_not(None), Order.total_amount != 0.0),
                    and_(Order.total_cost.is_not(None), Order.total_cost != 0.0),
                    and_(Order.shipping_fee.is_not(None), Order.shipping_fee != 0.0),
                )
            )
        ).all()
        
        for o in cancelled_orders:
            original_amount = float(o.total_amount or 0.0)
            original_cost = float(o.total_cost or 0.0)
            original_shipping = float(o.shipping_fee or 0.0)
            
            # Zero out financials
            o.total_amount = 0.0
            o.total_cost = 0.0
            o.shipping_fee = 0.0
            
            fixed_orders.append({
                "id": o.id,
                "original_amount": original_amount,
                "original_cost": original_cost,
                "original_shipping": original_shipping,
            })
            fixed_count += 1
        
        # Session commits automatically via context manager
    return {
        "status": "ok",
        "fixed_count": fixed_count,
        "fixed_orders": fixed_orders[:100],  # Limit response size
    }


@router.post("/cache/invalidate")
def cache_invalidate(request: Request):
    uid = request.session.get("uid")
    if not uid:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        from ..services.cache import bump_namespace
        ns = bump_namespace()
        return {"status": "ok", "namespace": ns}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"invalidate failed: {e}")


@router.get("/debug/jobs")
def debug_jobs(kind: str, limit: int = 50):
    try:
        r = _get_redis()
        ll = int(r.llen(f"jobs:{kind}"))
        items = [r.lindex(f"jobs:{kind}", i) for i in range(0, min(max(limit, 1), ll))]
        return {"kind": kind, "length": ll, "items": items}
    except Exception as e:
        return {"error": str(e)}


@router.get("/debug/attachments")
def debug_attachments(limit: int = 50):
    with get_session() as session:
        safe_n = max(1, min(int(limit or 50), 1000))
        # Embed sanitized value in LIMIT (avoid parameterizing LIMIT across drivers)
        rows = session.exec(text(f"SELECT id, message_id, kind, fetch_status, fetched_at FROM attachments ORDER BY id DESC LIMIT {safe_n}")) .all()
        out = []
        for r in rows:
            try:
                out.append({
                    "id": int(r.id if hasattr(r, 'id') else r[0]),
                    "message_id": int(r.message_id if hasattr(r, 'message_id') else r[1]),
                    "kind": (r.kind if hasattr(r, 'kind') else r[2]),
                    "fetch_status": (r.fetch_status if hasattr(r, 'fetch_status') else r[3]),
                    "fetched_at": (r.fetched_at if hasattr(r, 'fetched_at') else r[4]),
                })
            except Exception:
                continue
        return {"attachments": out}


@router.get("/debug/conversations")
def debug_conversations(limit: int = 20):
    with get_session() as session:
        safe_n = max(1, min(int(limit or 20), 200))
        rows = session.exec(text(f"SELECT convo_id, igba_id, ig_user_id, hydrated_at, last_message_at FROM conversations ORDER BY last_message_at DESC LIMIT {safe_n}")).all()
        out = []
        for r in rows:
            try:
                out.append({
                    "convo_id": (r.convo_id if hasattr(r, 'convo_id') else r[0]),
                    "igba_id": (r.igba_id if hasattr(r, 'igba_id') else r[1]),
                    "ig_user_id": (r.ig_user_id if hasattr(r, 'ig_user_id') else r[2]),
                    "hydrated_at": (r.hydrated_at if hasattr(r, 'hydrated_at') else r[3]),
                    "last_message_at": (r.last_message_at if hasattr(r, 'last_message_at') else r[4]),
                })
            except Exception:
                continue
        return {"conversations": out}


@router.post("/debug/hydrate")
def debug_hydrate(igba_id: str, ig_user_id: str):
    try:
        from ..services.queue import enqueue
        enqueue("hydrate_conversation", key=f"{igba_id}:{ig_user_id}", payload={"igba_id": str(igba_id), "ig_user_id": str(ig_user_id), "max_messages": 200})
        return {"status": "queued"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.post("/debug/backfill")
def debug_backfill(conv_limit: int = 50, media_limit: int = 100):
    """Enqueue enrich_page, enrich_user, hydrate_conversation and pending media fetch.

    This mirrors scripts/backfill_enqueue.py but is callable via HTTP for ops.
    """
    ep = 0
    eu = 0
    hy = 0
    md = 0
    # conversations backfill
    try:
        with get_session() as session:
            rows = session.exec(text("SELECT igba_id, ig_user_id FROM conversations ORDER BY last_message_at DESC LIMIT :n").params(n=int(conv_limit))).all()
            for r in rows:
                igba_id = r.igba_id if hasattr(r, "igba_id") else r[0]
                ig_user_id = r.ig_user_id if hasattr(r, "ig_user_id") else r[1]
                try:
                    enqueue("enrich_page", key=str(igba_id), payload={"igba_id": str(igba_id)})
                    ep += 1
                except Exception:
                    pass
                try:
                    enqueue("enrich_user", key=str(ig_user_id), payload={"ig_user_id": str(ig_user_id)})
                    eu += 1
                except Exception:
                    pass
                try:
                    cid = f"{igba_id}:{ig_user_id}"
                    enqueue("hydrate_conversation", key=cid, payload={"igba_id": str(igba_id), "ig_user_id": str(ig_user_id), "max_messages": 200})
                    hy += 1
                except Exception:
                    pass
    except Exception as e:
        return {"status": "error", "error": f"conversations backfill: {e}"}
    # media backfill
    try:
        with get_session() as session:
            rows = session.exec(text(
                """
                SELECT id FROM attachments
                WHERE fetch_status IS NULL OR fetch_status IN ('pending','error')
                ORDER BY id DESC
                LIMIT :n
                """
            ).params(n=int(media_limit))).all()
            for r in rows:
                att_id = r.id if hasattr(r, "id") else r[0]
                try:
                    enqueue("fetch_media", key=str(att_id), payload={"attachment_id": int(att_id)})
                    md += 1
                except Exception:
                    pass
    except Exception as e:
        return {"status": "partial", "enrich_page": ep, "enrich_user": eu, "hydrate": hy, "media": md, "error": f"media backfill: {e}"}
    return {"status": "ok", "enrich_page": ep, "enrich_user": eu, "hydrate": hy, "media": md}


@router.get("/debug/coverage")
def debug_coverage():
    """Return coverage metrics for messages, conversations, and attachments."""
    out: Dict[str, Any] = {}
    with get_session() as session:
        def _scalar(sql: str) -> int:
            try:
                row = session.exec(text(sql)).first()
                if row is None:
                    return 0
                if isinstance(row, (list, tuple)):
                    return int(row[0] or 0)
                try:
                    # use first attribute
                    return int(list(row)[0])  # type: ignore
                except Exception:
                    return 0
            except Exception:
                return 0

        total_msgs = _scalar("SELECT COUNT(1) FROM message")
        msgs_with_username = _scalar("SELECT COUNT(1) FROM message WHERE sender_username IS NOT NULL AND sender_username <> ''")
        total_convs = _scalar("SELECT COUNT(1) FROM conversations")
        convs_hydrated = _scalar("SELECT COUNT(1) FROM conversations WHERE hydrated_at IS NOT NULL")
        total_atts = _scalar("SELECT COUNT(1) FROM attachments")
        atts_ok = _scalar("SELECT COUNT(1) FROM attachments WHERE fetch_status='ok' AND storage_path IS NOT NULL AND storage_path <> ''")
        atts_pending = _scalar("SELECT COUNT(1) FROM attachments WHERE fetch_status IS NULL OR fetch_status='pending'")
        atts_error = _scalar("SELECT COUNT(1) FROM attachments WHERE fetch_status='error'")
        users_ok = _scalar("SELECT COUNT(1) FROM ig_users WHERE fetch_status='ok'")
        users_total = _scalar("SELECT COUNT(1) FROM ig_users")

        out["messages"] = {
            "total": total_msgs,
            "with_username": msgs_with_username,
            "pct_with_username": (round(100.0 * msgs_with_username / total_msgs, 1) if total_msgs else 0.0),
        }
        out["conversations"] = {
            "total": total_convs,
            "hydrated": convs_hydrated,
            "pct_hydrated": (round(100.0 * convs_hydrated / total_convs, 1) if total_convs else 0.0),
        }
        out["attachments"] = {
            "total": total_atts,
            "ok": atts_ok,
            "pending": atts_pending,
            "error": atts_error,
            "pct_ok": (round(100.0 * atts_ok / total_atts, 1) if total_atts else 0.0),
        }
        out["ig_users"] = {
            "total": users_total,
            "ok": users_ok,
            "pct_ok": (round(100.0 * users_ok / users_total, 1) if users_total else 0.0),
        }
    return out


@router.post("/debug/backfill_usernames")
def debug_backfill_usernames(limit: int = 2000):
    """Ensure ig_users rows exist for both senders and recipients, enqueue enrich, and update message.sender_username."""
    created_users = 0
    enqueued = 0
    updated_msgs = 0
    ids: list[str] = []
    # Collect distinct user ids from senders and recipients
    with get_session() as session:
        try:
            rows = session.exec(text("SELECT DISTINCT ig_sender_id FROM message WHERE ig_sender_id IS NOT NULL")).all()
            for r in rows:
                val = r.ig_sender_id if hasattr(r, "ig_sender_id") else (r[0] if isinstance(r, (list, tuple)) else None)
                if val:
                    ids.append(str(val))
            rows = session.exec(text("SELECT DISTINCT ig_recipient_id FROM message WHERE ig_recipient_id IS NOT NULL")).all()
            for r in rows:
                val = r.ig_recipient_id if hasattr(r, "ig_recipient_id") else (r[0] if isinstance(r, (list, tuple)) else None)
                if val:
                    ids.append(str(val))
            # de-dup and cap by limit
            uniq = []
            seen = set()
            for uid in ids:
                if uid not in seen:
                    seen.add(uid); uniq.append(uid)
            for uid in uniq[: max(1, min(int(limit or 2000), len(uniq)) )]:
                try:
                    session.exec(text("INSERT IGNORE INTO ig_users(ig_user_id) VALUES(:id)").params(id=uid))
                    created_users += 1
                except Exception:
                    pass
        except Exception as e:
            return {"status": "error", "error": f"scan ids: {e}"}
    # Enqueue enrich for all collected ids
    try:
        for uid in seen:
            try:
                enqueue("enrich_user", key=str(uid), payload={"ig_user_id": str(uid)})
                enqueued += 1
            except Exception:
                pass
    except Exception:
        pass
    # Update message.sender_username from ig_users (applies to inbound messages display)
    with get_session() as session:
        try:
            session.exec(text(
                """
                UPDATE message
                SET sender_username = (
                  SELECT username FROM ig_users WHERE ig_users.ig_user_id = message.ig_sender_id
                )
                WHERE (sender_username IS NULL OR sender_username='')
                AND ig_sender_id IS NOT NULL
                """
            ))
            row = session.exec(text("SELECT COUNT(1) FROM message WHERE sender_username IS NOT NULL AND sender_username<>''")).first()
            updated_msgs = int((row[0] if isinstance(row, (list, tuple)) else (row if isinstance(row, int) else 0)) or 0)
        except Exception:
            pass
    return {"status": "ok", "users_created": created_users, "enrich_enqueued": enqueued, "messages_with_username": updated_msgs}


@router.post("/debug/backfill_media")
def debug_backfill_media(limit: int = 200):
    """Enqueue media fetch for pending/error attachments."""
    md = 0
    try:
        with get_session() as session:
            rows = session.exec(text(
                """
                SELECT id FROM attachments
                WHERE fetch_status IS NULL OR fetch_status IN ('pending','error')
                ORDER BY id DESC
                LIMIT :n
                """
            ).params(n=int(limit))).all()
            for r in rows:
                att_id = r.id if hasattr(r, "id") else r[0]
                try:
                    enqueue("fetch_media", key=str(att_id), payload={"attachment_id": int(att_id)})
                    md += 1
                except Exception:
                    pass
    except Exception as e:
        return {"status": "error", "error": str(e), "media_enqueued": md}
    return {"status": "ok", "media_enqueued": md}


@router.get("/fix/message-timestamps")
def fix_message_timestamps(request: Request):
	templates = request.app.state.templates
	fixed = 0
	skipped = 0
	errors: list[dict[str, Any]] = []
	total = 0
	with get_session() as session:
		try:
			rows = session.exec(
				select(Message).where((Message.timestamp_ms.is_(None)) | (Message.timestamp_ms >= 2147480000))
			).all()
			total = len(rows)
			for msg in rows:
				try:
					raw = msg.raw_json or "{}"
					data = json.loads(raw)
					ts = data.get("timestamp")
					if ts is None:
						skipped += 1
						continue
					if isinstance(ts, (int, float)):
						val = int(ts if ts > 10_000_000_000 else ts * 1000)
						msg.timestamp_ms = val
						session.add(msg)
						fixed += 1
				except Exception as e:
					errors.append({"id": msg.id, "error": str(e)})
			try:
				session.commit()
			except Exception as e:
				# best-effort commit; surface error
				errors.append({"id": None, "error": f"commit: {e}"})
		except Exception as e:
			errors.append({"id": None, "error": f"query: {e}"})
	ctx = {
		"total": total,
		"fixed": fixed,
		"skipped": skipped,
		"error_count": len(errors),
		"errors": errors[:50],
	}
	return templates.TemplateResponse("admin_fix_timestamps.html", {"request": request, **ctx})
