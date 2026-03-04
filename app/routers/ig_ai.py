from __future__ import annotations

import datetime as dt
import math
import os
import time
from pathlib import Path
from typing import Optional, Any, Dict, List

from fastapi import APIRouter, Request, HTTPException, Form, UploadFile, File, Body, Query
from sqlalchemy import text, func as _func
from sqlmodel import select
import datetime as dt

from ..db import get_session
from ..services.queue import enqueue, delete_job
from ..services.monitoring import get_ai_run_logs
from ..services.ai import get_shadow_temperature_setting
from ..services.ai_models import (
    get_model_whitelist,
    group_model_names,
    normalize_model_choice,
    refresh_openai_model_whitelist,
)
from urllib.parse import quote as _quote
import logging

log = logging.getLogger("ig_ai.settings")


router = APIRouter(prefix="/ig/ai", tags=["instagram-ai"])


def _row_get(row: Any, key: str) -> Any:
    try:
        mapping = getattr(row, "_mapping", None)
        if mapping and key in mapping:
            return mapping[key]
    except Exception:
        pass
    if hasattr(row, key):
        return getattr(row, key)
    try:
        return row[key]
    except Exception:
        return None


def _ms_to_datetime(value: Any) -> Optional[dt.datetime]:
    try:
        if value is None:
            return None
        ms = int(value)
        if ms <= 0:
            return None
        return dt.datetime.utcfromtimestamp(ms / 1000.0)
    except Exception:
        return None


def _utc_to_turkey_time(utc_dt: Optional[dt.datetime]) -> Optional[dt.datetime]:
    """Convert UTC datetime to Turkey timezone (GMT+3)."""
    if utc_dt is None:
        return None
    return utc_dt + dt.timedelta(hours=3)


def _collect_shadow_metrics(limit: int = 100, status_filter: str | None = None) -> Dict[str, Any]:
    n = max(1, min(int(limit or 100), 200))
    now = dt.datetime.utcnow()
    status_counts: Dict[str, int] = {}
    entries: List[Dict[str, Any]] = []
    ready_count = 0
    normalized_filter = (status_filter or "").strip().lower() or None

    with get_session() as session:
        try:
            rows = session.exec(
                text(
                    """
                    SELECT COALESCE(status, 'pending') AS st, COUNT(*) AS cnt
                    FROM ai_shadow_state
                    GROUP BY COALESCE(status, 'pending')
                    """
                )
            ).all()
            for row in rows:
                status = str(_row_get(row, "st") or "pending")
                cnt = int(_row_get(row, "cnt") or 0)
                status_counts[status] = cnt
        except Exception:
            session.rollback()
            status_counts = {}

        try:
            row_ready = session.exec(
                text(
                    """
                    SELECT COUNT(*) AS c
                    FROM ai_shadow_state
                    WHERE (status = 'pending' OR status IS NULL)
                      AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
                    """
                )
            ).first()
            ready_count = int(_row_get(row_ready, "c") or 0) if row_ready else 0
        except Exception:
            session.rollback()
            ready_count = 0

        try:
            where_clause = " WHERE COALESCE(s.status, 'pending') = :status_filter" if normalized_filter else ""
            list_sql = (
                """
                SELECT
                    s.conversation_id,
                    COALESCE(s.status, 'pending') AS status,
                    s.last_inbound_ms,
                    s.next_attempt_at,
                    s.updated_at,
                    c.graph_conversation_id,
                    c.last_message_at,
                    u.username,
                    u.name AS contact_name,
                    (SELECT MIN(m.timestamp_ms) FROM message m WHERE m.conversation_id = s.conversation_id) AS first_msg_ms,
                    (SELECT MAX(m.timestamp_ms) FROM message m WHERE m.conversation_id = s.conversation_id) AS last_msg_ms,
                    (SELECT COUNT(*) FROM ai_shadow_reply r WHERE r.conversation_id = s.conversation_id) AS reply_count,
                    (SELECT MIN(r.created_at) FROM ai_shadow_reply r WHERE r.conversation_id = s.conversation_id) AS first_reply_at,
                    (SELECT MAX(r.created_at) FROM ai_shadow_reply r WHERE r.conversation_id = s.conversation_id) AS last_reply_at
                FROM ai_shadow_state s
                LEFT JOIN conversations c ON c.id = s.conversation_id
                LEFT JOIN ig_users u ON u.ig_user_id = c.ig_user_id
                """
                + where_clause
                + """
                ORDER BY
                    COALESCE(
                        (SELECT MAX(r.created_at) FROM ai_shadow_reply r WHERE r.conversation_id = s.conversation_id),
                        (SELECT MAX(m.timestamp_ms) FROM message m WHERE m.conversation_id = s.conversation_id),
                        s.updated_at
                    ) DESC
                LIMIT :n
                """
            )
            list_params: dict[str, object] = {"n": n}
            if normalized_filter:
                list_params["status_filter"] = normalized_filter
            rows = session.exec(text(list_sql).params(**list_params)).all()
        except Exception:
            session.rollback()
            rows = []

    total_replies = 0
    total_first_reply_latency = 0.0
    first_reply_samples = 0

    for row in rows:
        convo_id = _row_get(row, "conversation_id")
        status = _row_get(row, "status") or "pending"
        last_inbound_at = _ms_to_datetime(_row_get(row, "last_inbound_ms"))
        first_message_at = _ms_to_datetime(_row_get(row, "first_msg_ms"))
        last_message_at = _ms_to_datetime(_row_get(row, "last_msg_ms"))
        first_reply_at = _row_get(row, "first_reply_at")
        last_reply_at = _row_get(row, "last_reply_at")
        if isinstance(first_reply_at, str):
            try:
                first_reply_at = dt.datetime.fromisoformat(first_reply_at)
            except Exception:
                first_reply_at = None
        if isinstance(last_reply_at, str):
            try:
                last_reply_at = dt.datetime.fromisoformat(last_reply_at)
            except Exception:
                last_reply_at = None

        reply_count = int(_row_get(row, "reply_count") or 0)
        total_replies += reply_count

        wait_seconds = None
        if last_inbound_at:
            wait_seconds = max(0.0, (now - last_inbound_at).total_seconds())

        time_to_first_reply = None
        if first_reply_at and last_message_at:
            time_to_first_reply = (first_reply_at - last_message_at).total_seconds()
            if time_to_first_reply is not None and time_to_first_reply >= 0:
                total_first_reply_latency += time_to_first_reply
                first_reply_samples += 1

        time_since_last_reply = None
        if last_reply_at:
            time_since_last_reply = max(0.0, (now - last_reply_at).total_seconds())

        # Convert UTC datetimes to Turkey time (GMT+3) for display
        next_attempt_at_raw = _row_get(row, "next_attempt_at")
        updated_at_raw = _row_get(row, "updated_at")
        
        def _parse_datetime(val):
            """Parse datetime from string or return as-is if already datetime."""
            if val is None:
                return None
            if isinstance(val, dt.datetime):
                return val
            if isinstance(val, str):
                try:
                    # Try ISO format
                    return dt.datetime.fromisoformat(val.replace('Z', '+00:00'))
                except Exception:
                    try:
                        # Try MySQL datetime format
                        return dt.datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
                    except Exception:
                        pass
            return None
        
        next_attempt_at_turkey = _utc_to_turkey_time(_parse_datetime(next_attempt_at_raw)) if next_attempt_at_raw else None
        updated_at_turkey = _utc_to_turkey_time(_parse_datetime(updated_at_raw)) if updated_at_raw else None
        
        last_message_at_turkey = _utc_to_turkey_time(last_message_at) if last_message_at else None
        first_reply_at_turkey = _utc_to_turkey_time(first_reply_at) if first_reply_at else None
        last_reply_at_turkey = _utc_to_turkey_time(last_reply_at) if last_reply_at else None
        last_inbound_at_turkey = _utc_to_turkey_time(last_inbound_at) if last_inbound_at else None
        first_message_at_turkey = _utc_to_turkey_time(first_message_at) if first_message_at else None
        
        entry_status = str(status)
        entry = {
            "conversation_id": int(convo_id) if convo_id is not None else None,
            "status": entry_status,
            "last_inbound_at": last_inbound_at_turkey,
            "first_message_at": first_message_at_turkey,
            "last_message_at": last_message_at_turkey,
            "next_attempt_at": next_attempt_at_turkey,
            "updated_at": updated_at_turkey,
            "graph_conversation_id": _row_get(row, "graph_conversation_id"),
            "username": _row_get(row, "username"),
            "contact_name": _row_get(row, "contact_name"),
            "reply_count": reply_count,
            "first_reply_at": first_reply_at_turkey,
            "last_reply_at": last_reply_at_turkey,
            "wait_seconds": wait_seconds,
            "time_to_first_reply_seconds": time_to_first_reply,
            "time_since_last_reply_seconds": time_since_last_reply,
        }
        if normalized_filter and entry_status.lower() != normalized_filter:
            continue
        entries.append(entry)

    oldest_pending = max(
        [e["wait_seconds"] or 0 for e in entries if e["status"] == "pending" and e.get("wait_seconds") is not None],
        default=0,
    )
    summary = {
        "total_queue": sum(status_counts.values()),
        "ready_to_run": ready_count,
        "reply_total": total_replies,
        "with_replies": sum(1 for e in entries if (e.get("reply_count") or 0) > 0),
        "avg_first_reply_seconds": (total_first_reply_latency / first_reply_samples) if first_reply_samples else None,
        "avg_reply_count": (total_replies / len(entries)) if entries else 0,
        "oldest_pending_seconds": oldest_pending,
    }
    # Worker scope: who consumes this queue and can skip/exhaust items
    shadow_scope = "all"
    try:
        from ..models import SystemSetting
        from sqlmodel import select as _select
        with get_session() as session:
            row = session.exec(_select(SystemSetting).where(SystemSetting.key == "ai_shadow_scope")).first()
            if row and getattr(row, "value", None):
                v = str(row.value).strip().lower()
                if v in ("all", "linked_only", "off"):
                    shadow_scope = v
    except Exception:
        pass

    # Convert generated_at to Turkey time for display
    return {
        "generated_at": _utc_to_turkey_time(now) or now,
        "status_counts": status_counts,
        "entries": entries,
        "summary": summary,
        "limit": n,
        "shadow_scope": shadow_scope,
    }


def _serialize_shadow_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    """Convert datetime objects to ISO strings so templates/JSON can consume them easily."""
    out: Dict[str, Any] = {}
    for key, value in entry.items():
        if isinstance(value, dt.datetime):
            out[key] = value.isoformat()
        else:
            out[key] = value
    return out


@router.get("/process")
def process_page(request: Request):
    templates = request.app.state.templates
    # Render immediately; client will fetch runs via /ig/ai/process/runs
    return templates.TemplateResponse("ig_ai_process.html", {"request": request, "runs": []})


@router.post("/process/run")
def start_process(body: dict):
    # Parse inputs
    date_from_s: Optional[str] = (body or {}).get("date_from")
    date_to_s: Optional[str] = (body or {}).get("date_to")
    min_age_minutes: int = int((body or {}).get("min_age_minutes") or 60)
    limit: int = int((body or {}).get("limit") or 200)
    reprocess: bool = bool((body or {}).get("reprocess") not in (False, 0, "0", "false", "False", None))

    def _parse_date(v: Optional[str]) -> Optional[dt.date]:
        try:
            return dt.date.fromisoformat(str(v)) if v else None
        except Exception:
            return None

    date_from = _parse_date(date_from_s)
    date_to = _parse_date(date_to_s)

    # Create run row
    with get_session() as session:
        stmt = text(
            """
            INSERT INTO ig_ai_run(started_at, date_from, date_to, min_age_minutes)
            VALUES (CURRENT_TIMESTAMP, :df, :dt, :age)
            """
        ).bindparams(
            df=(date_from.isoformat() if date_from else None),
            dt=(date_to.isoformat() if date_to else None),
            age=int(min_age_minutes),
        )
        session.exec(stmt)
        run_id = None
        # Try MySQL first
        try:
            rid_row = session.exec(text("SELECT LAST_INSERT_ID() AS id")).first()
            if rid_row is not None:
                run_id = int(getattr(rid_row, "id", rid_row[0]))
        except Exception:
            pass
        # Backend fallback
        if run_id is None:
            try:
                rid_row = session.exec(text("SELECT last_insert_rowid() AS id")).first()
                if rid_row is not None:
                    run_id = int(getattr(rid_row, "id", rid_row[0]))
            except Exception:
                pass
        if run_id is None:
            raise HTTPException(status_code=500, detail="Could not create run")

    # Enqueue background job to process
    job_id = enqueue("ig_ai_process_run", key=str(run_id), payload={
        "run_id": run_id,
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,
        "min_age_minutes": min_age_minutes,
        "limit": limit,
        "reprocess": reprocess,
    })
    with get_session() as session:
        session.exec(text("UPDATE ig_ai_run SET job_id=:jid WHERE id=:id").params(jid=int(job_id), id=int(run_id)))
    return {"status": "ok", "run_id": run_id}


@router.get("/products")
def product_ai_page(request: Request, focus: str):
    """
    Edit AI instructions and product details for a single product identified by slug or name.
    Renders ig_ai_products.html with product details, categories, ai_system_msg / ai_prompt_msg
    and product image configuration.
    """
    from ..models import Product, AIPretext, ProductImage, Item, ProductCategory, ProductCategoryLink

    focus_s = (focus or "").strip()
    if not focus_s:
        raise HTTPException(status_code=400, detail="focus is required")
    with get_session() as session:
        try:
            row = session.exec(
                select(Product).where((Product.slug == focus_s) | (Product.name == focus_s)).limit(1)
            ).first()
        except Exception:
            row = None
        if not row:
            raise HTTPException(status_code=404, detail="Product not found for focus")
        
        # Load all pretexts for dropdown
        pretexts = session.exec(
            select(AIPretext).order_by(AIPretext.is_default.desc(), AIPretext.id.asc())
        ).all()
        pretext_list = [
            {"id": p.id, "name": p.name, "is_default": p.is_default} for p in pretexts
        ]

        # Load product images
        images = session.exec(
            select(ProductImage)
            .where(ProductImage.product_id == row.id)
            .order_by(ProductImage.position.asc(), ProductImage.id.asc())
        ).all()
        image_list = [
            {
                "id": img.id,
                "url": img.url,
                "variant_key": img.variant_key,
                "position": img.position,
                "ai_send": bool(img.ai_send),
                "ai_send_order": img.ai_send_order,
            }
            for img in images
        ]

        # Collect SKUs to help the operator understand the folder naming
        items = session.exec(
            select(Item).where(Item.product_id == row.id).order_by(Item.id.asc())
        ).all()
        sku_list = [it.sku for it in items if getattr(it, "sku", None)]

        # All categories (for multi-select); himan.com.tr sync can populate these
        all_categories = session.exec(
            select(ProductCategory).order_by(ProductCategory.position.asc(), ProductCategory.name.asc())
        ).all()
        category_list = [{"id": c.id, "name": c.name, "slug": c.slug} for c in all_categories]

        # This product's category ids
        links = session.exec(
            select(ProductCategoryLink).where(ProductCategoryLink.product_id == row.id)
        ).all()
        product_category_ids = [link.category_id for link in links]
        
        name = row.name or focus_s
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "ig_ai_products.html",
        {
            "request": request,
            "focus": row.slug or focus_s,
            "name": name,
            "product_id": row.id,
            "default_price": row.default_price,
            "default_color": row.default_color or "",
            "default_unit": row.default_unit or "adet",
            "default_cost": row.default_cost,
            "description": getattr(row, "description", None) or "",
            "himan_price": getattr(row, "himan_price", None),
            "himan_sale_price": getattr(row, "himan_sale_price", None),
            "ai_system_msg": row.ai_system_msg or "",
            "ai_prompt_msg": row.ai_prompt_msg or "",
            "pretext_id": row.pretext_id,
            "pretexts": pretext_list,
            "images": image_list,
            "skus": sku_list,
            "ai_reply_sending_enabled": getattr(row, "ai_reply_sending_enabled", True),
            "categories": category_list,
            "product_category_ids": product_category_ids,
        },
    )


@router.post("/products/save")
def save_product_ai(
    focus: str = Form(...),
    ai_system_msg: str = Form(default=""),
    ai_prompt_msg: str = Form(default=""),
    pretext_id: str = Form(default=""),
    ai_reply_sending_enabled: Optional[str] = Form(default=None),
):
    """
    Persist AI instructions for the focused product.
    """
    from ..models import Product

    focus_s = (focus or "").strip()
    if not focus_s:
        raise HTTPException(status_code=400, detail="focus is required")
    with get_session() as session:
        try:
            prod = session.exec(
                select(Product).where((Product.slug == focus_s) | (Product.name == focus_s)).limit(1)
            ).first()
        except Exception:
            prod = None
        if not prod:
            raise HTTPException(status_code=404, detail="Product not found for focus")
        # Normalize empty strings to None
        msg_sys = ai_system_msg.strip() if isinstance(ai_system_msg, str) else ""
        msg_prompt = ai_prompt_msg.strip() if isinstance(ai_prompt_msg, str) else ""
        prod.ai_system_msg = msg_sys or None
        prod.ai_prompt_msg = msg_prompt or None
        # Handle pretext_id
        pretext_id_val = None
        if pretext_id and isinstance(pretext_id, str) and pretext_id.strip():
            try:
                pretext_id_val = int(pretext_id.strip())
                if pretext_id_val <= 0:
                    pretext_id_val = None
            except Exception:
                pretext_id_val = None
        prod.pretext_id = pretext_id_val
        # Handle ai_reply_sending_enabled (checkbox is absent when unchecked)
        enabled = False
        if isinstance(ai_reply_sending_enabled, str):
            enabled = ai_reply_sending_enabled.lower() in ("true", "1", "yes", "on")
        prod.ai_reply_sending_enabled = enabled
        session.add(prod)
    # Redirect back to the edit page for this product
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=f"/ig/ai/products?focus={prod.slug or focus_s}", status_code=303)


def _woo_client():
    """Return (base_url, consumer_key, consumer_secret) if WooCommerce API is configured."""
    import os
    base = os.getenv("HIMAN_WOO_BASE_URL", "").strip()
    key = os.getenv("HIMAN_WOO_CONSUMER_KEY", "").strip()
    secret = os.getenv("HIMAN_WOO_CONSUMER_SECRET", "").strip()
    if base and key and secret:
        return base, key, secret
    return None


def _woo_get_product_by_slug(base_url: str, auth: tuple, slug: str) -> Optional[dict]:
    """Get single product by slug from WooCommerce REST API. Returns product dict or None."""
    import httpx
    base = base_url.rstrip("/")
    with httpx.Client(timeout=15.0) as client:
        r = client.get(
            f"{base}/wp-json/wc/v3/products",
            auth=auth,
            params={"slug": slug, "per_page": 1},
        )
        r.raise_for_status()
        items = r.json()
        return items[0] if items else None


def _woo_parse_price(s: str) -> Optional[float]:
    """Parse WooCommerce price string (e.g. '944.27' or '944,27')."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip().replace(",", ".")
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _woo_get_product_prices_light(woo_prod: dict) -> tuple[Optional[float], Optional[float]]:
    """
    Extract regular_price and sale_price from WooCommerce product dict only (no extra HTTP).
    For variable products uses top-level 'price' as fallback. Use in bulk sync to avoid timeouts.
    """
    reg = (woo_prod.get("regular_price") or "").strip()
    sale = (woo_prod.get("sale_price") or "").strip()
    price_display = (woo_prod.get("price") or "").strip()
    regular_float = _woo_parse_price(reg) if reg else None
    sale_float = _woo_parse_price(sale) if sale else None
    if price_display and regular_float is None:
        regular_float = _woo_parse_price(price_display)
    return regular_float, sale_float


def _woo_get_product_prices(base_url: str, auth: tuple, woo_prod: dict) -> tuple[Optional[float], Optional[float]]:
    """
    Resolve regular_price and sale_price from WooCommerce product.
    Variable products often have empty regular_price/sale_price on parent; then use 'price' or first variation.
    Returns (regular_price, sale_price).
    """
    import httpx
    reg = (woo_prod.get("regular_price") or "").strip()
    sale = (woo_prod.get("sale_price") or "").strip()
    price_display = (woo_prod.get("price") or "").strip()
    regular_float = _woo_parse_price(reg) if reg else None
    sale_float = _woo_parse_price(sale) if sale else None
    if regular_float is not None and sale_float is not None:
        return regular_float, sale_float
    if regular_float is not None and sale_float is None:
        return regular_float, None
    # Variable product: parent often has empty regular_price; use "price" or fetch variations
    if price_display:
        fallback = _woo_parse_price(price_display)
        if fallback is not None:
            if regular_float is None:
                regular_float = fallback
            if sale_float is None and sale:
                sale_float = _woo_parse_price(sale)
            return regular_float, sale_float
    # Fetch first variation for variable products (single-product pull only; timeout generous)
    woo_id = woo_prod.get("id")
    if woo_id and (woo_prod.get("type") == "variable" or (regular_float is None and not price_display)):
        base = base_url.rstrip("/")
        with httpx.Client(timeout=45.0) as client:
            r = client.get(
                f"{base}/wp-json/wc/v3/products/{woo_id}/variations",
                auth=auth,
                params={"per_page": 1},
            )
            if r.status_code == 200:
                variations = r.json()
                if variations:
                    v = variations[0]
                    reg_v = (v.get("regular_price") or v.get("price") or "").strip()
                    sale_v = (v.get("sale_price") or "").strip()
                    if regular_float is None:
                        regular_float = _woo_parse_price(reg_v) or _woo_parse_price(v.get("price"))
                    if sale_float is None and sale_v:
                        sale_float = _woo_parse_price(sale_v)
    return regular_float, sale_float


def _woo_format_price(value: Optional[float]) -> str:
    """WooCommerce'a gönderilecek fiyat string'i; virgülden sonrasız tam sayı ise '778' gibi."""
    if value is None:
        return ""
    if value == int(value):
        return str(int(value))
    return str(round(value, 2))


def _woo_update_product(base_url: str, auth: tuple, woo_id: int, payload: dict) -> tuple[bool, int, str]:
    """PATCH product on WooCommerce. Returns (success, status_code, response_text)."""
    import httpx
    base = base_url.rstrip("/")
    with httpx.Client(timeout=15.0) as client:
        r = client.patch(f"{base}/wp-json/wc/v3/products/{woo_id}", auth=auth, json=payload)
        body = (r.text or "")[:500]
        return (r.status_code in (200, 201), r.status_code, body)


def _woo_update_variations_prices(
    base_url: str, auth: tuple, parent_woo_id: int, regular_price: Optional[float], sale_price: Optional[str]
) -> None:
    """Variable ürünün tüm varyasyonlarının fiyatını günceller; sitede görünen fiyat varyasyondan gelir."""
    import httpx
    base = base_url.rstrip("/")
    reg_str = _woo_format_price(regular_price) if regular_price is not None else ""
    with httpx.Client(timeout=30.0) as client:
        page = 1
        per_page = 100
        while True:
            r = client.get(
                f"{base}/wp-json/wc/v3/products/{parent_woo_id}/variations",
                auth=auth,
                params={"per_page": per_page, "page": page},
            )
            if r.status_code != 200:
                break
            variations = r.json()
            if not variations:
                break
            for v in variations:
                vid = v.get("id")
                if not vid:
                    continue
                patch = {}
                if reg_str:
                    patch["regular_price"] = reg_str
                patch["sale_price"] = sale_price if sale_price else ""
                if patch:
                    client.patch(f"{base}/wp-json/wc/v3/products/{parent_woo_id}/variations/{vid}", auth=auth, json=patch)
            if len(variations) < per_page:
                break
            page += 1


@router.post("/products/details/save")
def save_product_details(
    focus: str = Form(...),
    name: str = Form(default=""),
    default_price: str = Form(default=""),
    default_color: str = Form(default=""),
    default_unit: str = Form(default="adet"),
    default_cost: str = Form(default=""),
    description: str = Form(default=""),
    himan_price: str = Form(default=""),
    himan_sale_price: str = Form(default=""),
):
    """Persist product details including description and himan.com.tr fiyat; optionally push to WooCommerce."""
    from ..models import Product
    from ..utils.slugify import slugify
    from fastapi.responses import RedirectResponse
    import httpx

    focus_s = (focus or "").strip()
    name_s = (name or "").strip()
    if not focus_s:
        raise HTTPException(status_code=400, detail="focus is required")
    if not name_s:
        raise HTTPException(status_code=400, detail="name is required")
    with get_session() as session:
        prod = session.exec(
            select(Product).where((Product.slug == focus_s) | (Product.name == focus_s)).limit(1)
        ).first()
        if not prod:
            raise HTTPException(status_code=404, detail="Product not found for focus")
        new_slug = slugify(name_s)
        if new_slug != (prod.slug or ""):
            existing = session.exec(
                select(Product).where(Product.slug == new_slug, Product.id != prod.id)
            ).first()
            if existing:
                raise HTTPException(status_code=409, detail="Başka bir ürün aynı isim/slug ile mevcut")
        prod.name = name_s
        prod.slug = new_slug
        try:
            prod.default_price = float(default_price) if default_price and default_price.strip() else None
        except ValueError:
            prod.default_price = None
        prod.default_color = default_color.strip() or None
        prod.default_unit = default_unit.strip() or "adet"
        try:
            prod.default_cost = float(default_cost) if default_cost and default_cost.strip() else None
        except ValueError:
            prod.default_cost = None
        prod.description = description.strip() or None
        try:
            prod.himan_price = float(himan_price) if himan_price and himan_price.strip() else None
        except ValueError:
            prod.himan_price = None
        try:
            prod.himan_sale_price = float(himan_sale_price) if himan_sale_price and himan_sale_price.strip() else None
        except ValueError:
            prod.himan_sale_price = None
        session.add(prod)
        session.flush()
        # Push fiyat (ve açıklama) to himan.com.tr if WooCommerce API configured
        woo = _woo_client()
        if woo:
            base_url, key, secret = woo
            auth = (key, secret)
            try:
                woo_prod = _woo_get_product_by_slug(base_url, auth, new_slug)
                if woo_prod and woo_prod.get("id"):
                    woo_id = woo_prod["id"]
                    # Fiyatı tam sayı ise "778" gönder (778.0 değil), sitede 778,70 görünmesin
                    reg_str = _woo_format_price(prod.himan_price)
                    sale_str = _woo_format_price(prod.himan_sale_price) if prod.himan_sale_price is not None else ""
                    if prod.himan_price is not None and not sale_str:
                        sale_str = ""
                    patch_payload = {}
                    if prod.description is not None:
                        patch_payload["description"] = prod.description or ""
                    if reg_str:
                        patch_payload["regular_price"] = reg_str
                    if prod.himan_price is not None:
                        patch_payload["sale_price"] = sale_str
                    if patch_payload:
                        ok, status, body = _woo_update_product(base_url, auth, woo_id, patch_payload)
                        if ok:
                            log.info("himan push ok slug=%s woo_id=%s payload=%s", new_slug, woo_id, patch_payload)
                        else:
                            log.warning("himan push failed slug=%s woo_id=%s status=%s body=%s", new_slug, woo_id, status, body)
                    # Variable ürünlerde sitede görünen fiyat varyasyonlardan gelir; hepsini güncelle
                    if woo_prod.get("type") == "variable" and (reg_str or sale_str is not None):
                        _woo_update_variations_prices(base_url, auth, woo_id, prod.himan_price, sale_str)
                        log.info("himan variations updated slug=%s woo_id=%s", new_slug, woo_id)
            except Exception as e:
                log.warning("himan.com.tr fiyat push failed: %s", e)
    return RedirectResponse(url=f"/ig/ai/products?focus={new_slug}", status_code=303)


def _woo_get_category_ids_by_slugs(base_url: str, auth: tuple, slugs: list[str]) -> list[int]:
    """Resolve WooCommerce category slugs to term ids."""
    import httpx
    if not slugs:
        return []
    base = base_url.rstrip("/")
    ids = []
    with httpx.Client(timeout=15.0) as client:
        page = 1
        per_page = 100
        slug_set = set(s.strip().lower() for s in slugs if (s or "").strip())
        while True:
            r = client.get(
                f"{base}/wp-json/wc/v3/products/categories",
                auth=auth,
                params={"per_page": per_page, "page": page},
            )
            r.raise_for_status()
            chunk = r.json()
            if not chunk:
                break
            for c in chunk:
                s = (c.get("slug") or "").strip().lower()
                if s in slug_set:
                    tid = c.get("id")
                    if tid and tid not in ids:
                        ids.append(tid)
            if len(chunk) < per_page:
                break
            page += 1
    return ids


@router.post("/products/categories/save")
async def save_product_categories(request: Request, focus: str = Form(...)):
    """Persist product category links (multi-select) and push to himan.com.tr."""
    from ..models import Product, ProductCategoryLink, ProductCategory
    from fastapi.responses import RedirectResponse

    focus_s = (focus or "").strip()
    if not focus_s:
        raise HTTPException(status_code=400, detail="focus is required")
    form = await request.form()
    category_ids = form.getlist("category_ids[]") if hasattr(form, "getlist") else []
    if not category_ids:
        cat = form.get("category_ids[]") or form.get("category_ids")
        category_ids = [cat] if isinstance(cat, str) else (list(cat) if cat else [])

    prod_slug_after = focus_s
    with get_session() as session:
        prod = session.exec(
            select(Product).where((Product.slug == focus_s) | (Product.name == focus_s)).limit(1)
        ).first()
        if not prod:
            raise HTTPException(status_code=404, detail="Product not found for focus")
        prod_slug_after = prod.slug or focus_s
        for link in session.exec(select(ProductCategoryLink).where(ProductCategoryLink.product_id == prod.id)).all():
            session.delete(link)
        valid_ids = set()
        for cid in category_ids:
            try:
                valid_ids.add(int(cid))
            except (TypeError, ValueError):
                pass
        category_slugs = []
        for cid in valid_ids:
            cat = session.exec(select(ProductCategory).where(ProductCategory.id == cid)).first()
            if cat:
                session.add(ProductCategoryLink(product_id=prod.id, category_id=cat.id))
                category_slugs.append(cat.slug or "")
    # Push categories to himan.com.tr (WooCommerce)
    woo = _woo_client()
    if woo and category_slugs:
        base_url, key, secret = woo
        auth = (key, secret)
        try:
            woo_prod = _woo_get_product_by_slug(base_url, auth, prod_slug_after)
            if woo_prod:
                woo_id = woo_prod.get("id")
                woo_cat_ids = _woo_get_category_ids_by_slugs(base_url, auth, category_slugs)
                if woo_id and woo_cat_ids:
                    _woo_update_product(
                        base_url, auth, woo_id,
                        {"categories": [{"id": i} for i in woo_cat_ids]},
                    )
        except Exception:
            pass
    return RedirectResponse(url=f"/ig/ai/products?focus={prod_slug_after}", status_code=303)


@router.get("/products/pull-from-himan")
def pull_product_from_himan(focus: str):
    """himan.com.tr (WooCommerce) ürün verisini çekip HMA ürününü günceller: açıklama, fiyat, indirim, kategoriler."""
    from ..models import Product, ProductCategory, ProductCategoryLink
    from fastapi.responses import RedirectResponse

    focus_s = (focus or "").strip()
    if not focus_s:
        raise HTTPException(status_code=400, detail="focus is required")
    woo = _woo_client()
    if not woo:
        raise HTTPException(
            status_code=503,
            detail="HIMAN_WOO_* ortam değişkenleri tanımlı değil; himan.com.tr'den veri alınamaz.",
        )
    base_url, key, secret = woo
    auth = (key, secret)
    try:
        woo_prod = _woo_get_product_by_slug(base_url, auth, focus_s)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"himan.com.tr isteği başarısız: {e}")
    if not woo_prod:
        raise HTTPException(status_code=404, detail="himan.com.tr'de bu slug ile ürün bulunamadı")
    with get_session() as session:
        prod = session.exec(
            select(Product).where((Product.slug == focus_s) | (Product.name == focus_s)).limit(1)
        ).first()
        if not prod:
            raise HTTPException(status_code=404, detail="HMA'da ürün bulunamadı")
        # Woo description (can be HTML)
        desc = (woo_prod.get("description") or "").strip()
        prod.description = desc or None
        # Fiyat: variable ürünlerde ana üründe boş olabilir; helper price veya varyasyon kullanır
        regular_price, sale_price = _woo_get_product_prices(base_url, auth, woo_prod)
        prod.himan_price = regular_price
        prod.himan_sale_price = sale_price
        # Satış durumu (publish=Satışta, draft=Taslak, private=Gizli)
        prod.himan_status = (woo_prod.get("status") or "").strip() or None
        # Sync categories: Woo category slugs -> HMA ProductCategoryLink
        woo_cats = woo_prod.get("categories") or []
        woo_slugs = [str(c.get("slug") or "").strip() for c in woo_cats if (c.get("slug") or "").strip()]
        for link in session.exec(select(ProductCategoryLink).where(ProductCategoryLink.product_id == prod.id)).all():
            session.delete(link)
        for slug in woo_slugs:
            if slug == "uncategorized":
                continue
            cat = session.exec(select(ProductCategory).where(ProductCategory.slug == slug)).first()
            if cat:
                session.add(ProductCategoryLink(product_id=prod.id, category_id=cat.id))
        session.add(prod)
    return RedirectResponse(url=f"/ig/ai/products?focus={focus_s}", status_code=303)


@router.get("/products/sync-all-from-himan")
def sync_all_products_from_himan(
    redirect: str = Query(default="/products/table"),
):
    """
    himan.com.tr (WooCommerce) tüm ürünleri çekip HMA ürünleriyle eşleştirir.
    Fiyat, indirim, satış durumu (publish/draft/private), açıklama ve kategoriler güncellenir.
    Ürünler tablosundan 'himan.com.tr\'den topluca veri al' ile tetiklenir.
    """
    from ..models import Product, ProductCategory, ProductCategoryLink
    from fastapi.responses import RedirectResponse
    import httpx

    woo = _woo_client()
    if not woo:
        return RedirectResponse(
            url=f"{redirect}?sync_error=woo_not_configured",
            status_code=303,
        )
    base_url, key, secret = woo
    auth = (key, secret)
    base = base_url.rstrip("/")
    updated = 0
    missing_in_hma = 0
    missing_list: list[dict] = []  # himan.com.tr'de olup HMA'da eşleşmeyenler: [{slug, name}]
    page = 1
    per_page = 100
    with get_session() as session:
        while True:
            try:
                with httpx.Client(timeout=60.0) as client:
                    r = client.get(
                        f"{base}/wp-json/wc/v3/products",
                        auth=auth,
                        params={"per_page": per_page, "page": page},
                    )
                    r.raise_for_status()
                    products = r.json()
            except Exception:
                return RedirectResponse(url=f"{redirect}?sync_error=fetch_failed", status_code=303)
            if not products:
                break
            for woo_prod in products:
                slug = (woo_prod.get("slug") or "").strip()
                if not slug:
                    continue
                prod = session.exec(
                    select(Product).where(Product.slug == slug)).first()
                if not prod:
                    missing_in_hma += 1
                    name = (woo_prod.get("name") or "").strip()
                    missing_list.append({"slug": slug, "name": name or slug})
                    continue
                desc = (woo_prod.get("description") or "").strip()
                prod.description = desc or None
                # Toplu senkron: ek HTTP (varyasyon) isteği atma; sadece ürün cevabındaki price kullan (timeout önlenir)
                regular_price, sale_price = _woo_get_product_prices_light(woo_prod)
                prod.himan_price = regular_price
                prod.himan_sale_price = sale_price
                prod.himan_status = (woo_prod.get("status") or "").strip() or None
                woo_cats = woo_prod.get("categories") or []
                woo_slugs = [str(c.get("slug") or "").strip() for c in woo_cats if (c.get("slug") or "").strip()]
                for link in session.exec(select(ProductCategoryLink).where(ProductCategoryLink.product_id == prod.id)).all():
                    session.delete(link)
                for cat_slug in woo_slugs:
                    if cat_slug == "uncategorized":
                        continue
                    cat = session.exec(select(ProductCategory).where(ProductCategory.slug == cat_slug)).first()
                    if cat:
                        session.add(ProductCategoryLink(product_id=prod.id, category_id=cat.id))
                session.add(prod)
                updated += 1
            if len(products) < per_page:
                break
            page += 1
    # Sonuç: eşleşmeyen ürün listesini Redis'e yaz (ürünler tablosunda gösterilecek)
    try:
        from ..services.queue import _get_redis
        import json
        r = _get_redis()
        r.setex("hm:sync_all_missing", 3600, json.dumps(missing_list, ensure_ascii=False))
    except Exception:
        pass
    from urllib.parse import urlencode
    q = urlencode({"synced": updated, "missing_in_hma": missing_in_hma})
    return RedirectResponse(url=f"{redirect}?{q}", status_code=303)


@router.get("/categories")
def categories_page(request: Request):
    """List product categories and form to add new. Used for multi-select on product page."""
    from ..models import ProductCategory

    with get_session() as session:
        cats = session.exec(
            select(ProductCategory).order_by(ProductCategory.position.asc(), ProductCategory.name.asc())
        ).all()
        category_list = [{"id": c.id, "name": c.name, "slug": c.slug, "position": c.position} for c in cats]
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "ig_ai_categories.html",
        {"request": request, "categories": category_list},
    )


@router.post("/categories/add")
def add_category(
    name: str = Form(...),
    slug: str = Form(default=""),
):
    """Add a product category. Slug defaults to slugify(name)."""
    from ..models import ProductCategory
    from ..utils.slugify import slugify
    from fastapi.responses import RedirectResponse

    name_s = (name or "").strip()
    if not name_s:
        raise HTTPException(status_code=400, detail="name is required")
    slug_s = (slug or "").strip() or slugify(name_s)
    with get_session() as session:
        existing = session.exec(select(ProductCategory).where(ProductCategory.slug == slug_s)).first()
        if existing:
            raise HTTPException(status_code=409, detail="Bu slug ile kategori zaten var")
        session.add(ProductCategory(name=name_s, slug=slug_s))
    return RedirectResponse(url="/ig/ai/categories", status_code=303)


def _fetch_categories_from_woo(
    base_url: str, consumer_key: str, consumer_secret: str
) -> tuple[list[dict], list[dict]]:
    """
    Fetch categories and product->category assignments from WooCommerce REST API (himan.com.tr).
    Returns (categories list, product_categories list) for sync payload.
    """
    import httpx

    base = base_url.rstrip("/")
    auth = (consumer_key, consumer_secret)
    categories: list[dict] = []
    product_categories: list[dict] = []

    with httpx.Client(timeout=30.0) as client:
        # WooCommerce product_cat: /wp-json/wc/v3/products/categories
        page = 1
        per_page = 100
        while True:
            r = client.get(
                f"{base}/wp-json/wc/v3/products/categories",
                auth=auth,
                params={"per_page": per_page, "page": page},
            )
            r.raise_for_status()
            chunk = r.json()
            if not chunk:
                break
            for c in chunk:
                name_s = (c.get("name") or "").strip()
                slug_s = (c.get("slug") or "").strip()
                if name_s and slug_s and (slug_s != "uncategorized"):
                    categories.append({"name": name_s, "slug": slug_s})
            if len(chunk) < per_page:
                break
            page += 1

        # Products with their categories (product slug -> category slugs)
        page = 1
        while True:
            r = client.get(
                f"{base}/wp-json/wc/v3/products",
                auth=auth,
                params={"per_page": per_page, "page": page},
            )
            r.raise_for_status()
            products = r.json()
            if not products:
                break
            for p in products:
                prod_slug = (p.get("slug") or "").strip()
                if not prod_slug:
                    continue
                for cat in p.get("categories") or []:
                    cat_slug = (cat.get("slug") or "").strip()
                    if cat_slug and cat_slug != "uncategorized":
                        product_categories.append(
                            {"product_slug": prod_slug, "category_slug": cat_slug}
                        )
            if len(products) < per_page:
                break
            page += 1

    return categories, product_categories


@router.get("/categories/sync-from-himan")
def sync_categories_from_himan():
    """
    Sync categories and product–category assignments from himan.com.tr (WooCommerce).
    Configure either:
    - WooCommerce REST API: HIMAN_WOO_BASE_URL, HIMAN_WOO_CONSUMER_KEY, HIMAN_WOO_CONSUMER_SECRET
    - Or custom JSON endpoint: HIMAN_CATEGORIES_API_URL returning {categories, product_categories}
    """
    import os
    import httpx

    from ..models import ProductCategory, ProductCategoryLink, Product

    data: dict = {}
    woo_base = os.getenv("HIMAN_WOO_BASE_URL", "").strip()
    woo_key = os.getenv("HIMAN_WOO_CONSUMER_KEY", "").strip()
    woo_secret = os.getenv("HIMAN_WOO_CONSUMER_SECRET", "").strip()
    api_url = os.getenv("HIMAN_CATEGORIES_API_URL", "").strip()

    if woo_base and woo_key and woo_secret:
        try:
            categories_list, product_categories_list = _fetch_categories_from_woo(
                woo_base, woo_key, woo_secret
            )
            data = {
                "categories": categories_list,
                "product_categories": product_categories_list,
            }
        except Exception as e:
            return {"ok": False, "message": f"WooCommerce API hatası: {e}"}
    elif api_url:
        try:
            with httpx.Client(timeout=15.0) as client:
                r = client.get(api_url)
                r.raise_for_status()
                data = r.json()
        except Exception as e:
            return {"ok": False, "message": f"himan.com.tr isteği başarısız: {e}"}
    else:
        return {
            "ok": False,
            "message": "himan.com.tr senkronu için tanım yok. WooCommerce kullanıyorsanız HIMAN_WOO_BASE_URL, HIMAN_WOO_CONSUMER_KEY, HIMAN_WOO_CONSUMER_SECRET tanımlayın.",
            "hint": "himansite projesi /Users/hilmibaycan/Projeler-Aktif/himansite; WooCommerce REST API (himan.com.tr) için Consumer Key/Secret: WooCommerce → Ayarlar → Gelişmiş → REST API.",
            "alt_hint": "Kategorileri manuel eklemek için /ig/ai/categories sayfasını kullanın.",
        }

    categories = data.get("categories") or []
    product_categories = data.get("product_categories") or []
    created = 0
    with get_session() as session:
        for c in categories:
            name_s = (c.get("name") or "").strip()
            slug_s = (c.get("slug") or "").strip()
            if not name_s or not slug_s:
                continue
            existing = session.exec(select(ProductCategory).where(ProductCategory.slug == slug_s)).first()
            if not existing:
                session.add(ProductCategory(name=name_s, slug=slug_s))
                created += 1
        session.flush()
        for pc in product_categories:
            prod_slug = (pc.get("product_slug") or "").strip()
            cat_slug = (pc.get("category_slug") or "").strip()
            if not prod_slug or not cat_slug:
                continue
            prod = session.exec(select(Product).where(Product.slug == prod_slug)).first()
            cat = session.exec(select(ProductCategory).where(ProductCategory.slug == cat_slug)).first()
            if prod and cat:
                existing_link = session.exec(
                    select(ProductCategoryLink).where(
                        ProductCategoryLink.product_id == prod.id,
                        ProductCategoryLink.category_id == cat.id,
                    )
                ).first()
                if not existing_link:
                    session.add(ProductCategoryLink(product_id=prod.id, category_id=cat.id))
    return {
        "ok": True,
        "categories_synced": len(categories),
        "created": created,
        "product_links": len(product_categories),
    }


@router.post("/products/images/upload")
async def upload_product_images(
    focus: str = Form(...),
    files: List[UploadFile] = File(...),
):
    """
    Upload one or more images for the focused product.

    Files are stored under IMAGE_UPLOAD_ROOT/products/{folder}/filename where:
      - folder is the primary SKU when available, otherwise product slug.

    The stored URL is:
      {IMAGE_CDN_BASE_URL.rstrip('/')}/{relative_path}
    where relative_path = 'products/{folder}/{filename}'.
    If IMAGE_CDN_BASE_URL is not set, fall back to '/products/{folder}/{filename}'.
    """
    from ..models import Product, Item, ProductImage

    focus_s = (focus or "").strip()
    if not focus_s:
        raise HTTPException(status_code=400, detail="focus is required")

    with get_session() as session:
        prod = session.exec(
            select(Product)
            .where((Product.slug == focus_s) | (Product.name == focus_s))
            .limit(1)
        ).first()
        if not prod:
            raise HTTPException(status_code=404, detail="Product not found for focus")

        # Determine folder name: prefer first SKU, else product slug
        item = session.exec(
            select(Item).where(Item.product_id == prod.id).order_by(Item.id.asc())
        ).first()
        folder = (item.sku if item and item.sku else (prod.slug or focus_s)).strip()

        # Determine starting position for new images
        max_pos_row = session.exec(
            select(_func.max(ProductImage.position)).where(
                ProductImage.product_id == prod.id
            )
        ).first()
        base_pos = int(max_pos_row) if max_pos_row is not None else 0

        root = Path(os.getenv("IMAGE_UPLOAD_ROOT", "static")).resolve()
        product_dir = root / "products" / folder
        product_dir.mkdir(parents=True, exist_ok=True)

        cdn_base = (os.getenv("IMAGE_CDN_BASE_URL", "") or "").rstrip("/")

        for idx, file in enumerate(files):
            content = await file.read()
            if not content:
                continue
            original_name = file.filename or f"image-{idx}.jpg"
            _, ext = os.path.splitext(original_name)
            if not ext:
                ext = ".jpg"
            filename = f"image-{int(time.time() * 1000)}-{idx}{ext}"
            target = product_dir / filename
            target.write_bytes(content)

            relative_path = f"products/{folder}/{filename}".lstrip("/")
            if cdn_base:
                url = f"{cdn_base}/{relative_path}"
            else:
                url = f"/static/{relative_path}"

            img = ProductImage(
                product_id=prod.id,
                url=url,
                position=base_pos + idx + 1,
                ai_send=True,
                ai_send_order=base_pos + idx + 1,
            )
            session.add(img)

        # Flush so ids are assigned if needed by follow-up requests
        session.flush()

    from fastapi.responses import RedirectResponse

    return RedirectResponse(
        url=f"/ig/ai/products?focus={prod.slug or focus_s}", status_code=303
    )


@router.post("/products/images/save")
async def save_product_images(request: Request):
    """
    Persist per-image AI configuration (ai_send, ai_send_order, variant_key)
    for the focused product.
    """
    from ..models import Product, ProductImage

    form = await request.form()
    focus = (form.get("focus") or "").strip()
    if not focus:
        raise HTTPException(status_code=400, detail="focus is required")

    # Use image_ids[] as the canonical list of rows present in the table
    image_ids = (
        form.getlist("image_ids[]") if hasattr(form, "getlist") else form.getlist("image_ids")  # type: ignore[attr-defined]
    )

    with get_session() as session:
        prod = session.exec(
            select(Product)
            .where((Product.slug == focus) | (Product.name == focus))
            .limit(1)
        ).first()
        if not prod:
            raise HTTPException(status_code=404, detail="Product not found for focus")

        for sid in image_ids:
            try:
                iid = int(sid)
            except Exception:
                continue
            img = session.exec(
                select(ProductImage).where(ProductImage.id == iid)
            ).first()
            if not img:
                continue

            # Optional delete
            if form.get(f"delete_{iid}"):
                session.delete(img)
                continue

            # ai_send checkbox
            img.ai_send = bool(form.get(f"ai_send_{iid}"))

            # ai_send_order integer or None
            raw_order = (form.get(f"ai_send_order_{iid}") or "").strip()
            if raw_order:
                try:
                    img.ai_send_order = int(raw_order)
                except Exception:
                    img.ai_send_order = None
            else:
                img.ai_send_order = None

            # variant_key (may be empty)
            raw_variant = (form.get(f"variant_key_{iid}") or "").strip()
            img.variant_key = raw_variant or None

            session.add(img)

    from fastapi.responses import RedirectResponse

    return RedirectResponse(
        url=f"/ig/ai/products?focus={focus}", status_code=303
    )


@router.get("/products/{product_slug}/review")
def product_review_page(request: Request, product_slug: str, limit: int = Query(default=50, ge=1, le=500), offset: int = Query(default=0, ge=0, description="Offset for pagination")):
    """
    Review page for a product showing:
    - Product data and prompts
    - All conversations linked to this product (via ads)
    - Client questions (inbound messages)
    - Shadow replies
    - Actual agent replies (outbound messages)
    """
    from ..models import Product, Conversation, Message, AiShadowReply
    
    with get_session() as session:
        # Get product
        product = session.exec(
            select(Product).where(Product.slug == product_slug).limit(1)
        ).first()
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        
        # Find all conversations linked to this product via ads
        # Get ad_ids for this product from ads_products table
        ad_ids_rows = session.exec(
            text(
                """
                SELECT DISTINCT ad_id FROM ads_products 
                WHERE product_id = :pid
                """
            ).params(pid=product.id)
        ).all()
        
        ad_ids = [str(r[0] if isinstance(r, tuple) else getattr(r, "ad_id", r)) for r in ad_ids_rows if r]
        
        # Find conversations that have messages with these ad_ids
        conversations = []
        total_count = 0
        if ad_ids:
            # Build parameter list for IN clause
            params = {}
            placeholders = []
            for i, aid in enumerate(ad_ids):
                param_name = f"aid_{i}"
                placeholders.append(f":{param_name}")
                params[param_name] = str(aid)
            params["lim"] = limit
            params["off"] = offset
            
            placeholders_str = ",".join(placeholders)
            
            # Get total count for pagination info
            count_params = {k: v for k, v in params.items() if k != "lim" and k != "off"}
            total_count_row = session.exec(
                text(
                    f"""
                    SELECT COUNT(DISTINCT c.id) as cnt
                    FROM conversations c
                    INNER JOIN message m ON m.conversation_id = c.id
                    WHERE m.ad_id IN ({placeholders_str})
                    """
                ).params(**count_params)
            ).first()
            # Extract count value - handle SQLAlchemy Row objects properly
            if total_count_row is None:
                total_count = 0
            else:
                # SQLAlchemy Row objects - extract the actual integer value
                try:
                    # Try accessing as tuple/list first
                    if isinstance(total_count_row, (list, tuple)):
                        total_count = int(total_count_row[0]) if total_count_row else 0
                    # Otherwise try attribute access or index access
                    elif hasattr(total_count_row, '__getitem__'):
                        val = total_count_row[0]
                        total_count = int(val) if val is not None else 0
                    else:
                        # Direct value
                        total_count = int(total_count_row) if total_count_row is not None else 0
                except (ValueError, TypeError, IndexError, AttributeError) as e:
                    import logging
                    logging.getLogger("ig_ai").warning(f"Error extracting total_count: {e}, type={type(total_count_row)}, row={total_count_row}")
                    total_count = 0
            
            # Ensure total_count is an integer for comparisons
            total_count = int(total_count) if total_count else 0
            
            convo_rows = session.exec(
                text(
                    f"""
                    SELECT DISTINCT c.id, c.ig_user_id, c.graph_conversation_id,
                           c.last_message_at, c.last_message_text, c.last_sender_username,
                           u.username, u.contact_name, u.contact_phone
                    FROM conversations c
                    LEFT JOIN ig_users u ON u.ig_user_id = c.ig_user_id
                    INNER JOIN message m ON m.conversation_id = c.id
                    WHERE m.ad_id IN ({placeholders_str})
                    ORDER BY c.last_message_at DESC
                    LIMIT :lim OFFSET :off
                    """
                ).params(**params)
            ).all()
        else:
            convo_rows = []
        
        # Build conversation data with messages, shadow replies, and actual replies
        conversation_data = []
        for row in convo_rows:
            try:
                convo_id = row[0] if isinstance(row, tuple) else getattr(row, "id", row)
                ig_user_id = row[1] if isinstance(row, tuple) else getattr(row, "ig_user_id", None)
                graph_convo_id = row[2] if isinstance(row, tuple) else getattr(row, "graph_conversation_id", None)
                last_msg_at = row[3] if isinstance(row, tuple) else getattr(row, "last_message_at", None)
                last_msg_text = row[4] if isinstance(row, tuple) else getattr(row, "last_message_text", None)
                last_sender = row[5] if isinstance(row, tuple) else getattr(row, "last_sender_username", None)
                username = row[6] if isinstance(row, tuple) else getattr(row, "username", None)
                contact_name = row[7] if isinstance(row, tuple) else getattr(row, "contact_name", None)
                contact_phone = row[8] if isinstance(row, tuple) else getattr(row, "contact_phone", None)
                
                # Get all messages for this conversation
                messages = session.exec(
                    select(Message)
                    .where(Message.conversation_id == convo_id)
                    .order_by(Message.timestamp_ms.asc())
                ).all()
                
                # Get shadow replies for this conversation
                shadow_replies = session.exec(
                    select(AiShadowReply)
                    .where(AiShadowReply.conversation_id == convo_id)
                    .order_by(AiShadowReply.created_at.asc())
                ).all()
                
                # Separate messages by direction
                client_questions = [m for m in messages if m.direction == "in"]
                actual_replies = [m for m in messages if m.direction == "out"]
                
                conversation_data.append({
                    "conversation_id": convo_id,
                    "graph_conversation_id": graph_convo_id,
                    "ig_user_id": ig_user_id,
                    "username": username,
                    "contact_name": contact_name,
                    "contact_phone": contact_phone,
                    "last_message_at": last_msg_at,
                    "last_message_text": last_msg_text,
                    "last_sender_username": last_sender,
                    "client_questions": [
                        {
                            "id": m.id,
                            "text": m.text,
                            "timestamp_ms": m.timestamp_ms,
                            "sender_username": m.sender_username,
                            "created_at": m.created_at,
                        }
                        for m in client_questions
                    ],
                    "shadow_replies": [
                        {
                            "id": r.id,
                            "reply_text": r.reply_text,
                            "model": r.model,
                            "confidence": r.confidence,
                            "reason": r.reason,
                            "status": r.status,
                            "created_at": r.created_at,
                        }
                        for r in shadow_replies
                    ],
                    "actual_replies": [
                        {
                            "id": m.id,
                            "text": m.text,
                            "timestamp_ms": m.timestamp_ms,
                            "sender_username": m.sender_username,
                            "created_at": m.created_at,
                            "ai_status": m.ai_status,
                        }
                        for m in actual_replies
                    ],
                    "question_count": len(client_questions),
                    "shadow_reply_count": len(shadow_replies),
                    "actual_reply_count": len(actual_replies),
                })
            except Exception as e:
                import logging
                logging.getLogger("ig_ai").error(f"Error processing conversation: {e}")
                continue
        
        # Get pretext if exists
        pretext = None
        if product.pretext_id:
            from ..models import AIPretext
            pretext = session.get(AIPretext, product.pretext_id)
        
        # Calculate pagination info
        has_prev = offset > 0
        has_next = offset + len(conversation_data) < total_count
        prev_offset = max(0, offset - limit)
        next_offset = offset + limit
        
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "ig_ai_product_review.html",
            {
                "request": request,
                "product": product,
                "pretext": pretext,
                "conversations": conversation_data,
                "total_conversations": total_count,
                "current_page_count": len(conversation_data),
                "total_questions": sum(c["question_count"] for c in conversation_data),
                "total_shadow_replies": sum(c["shadow_reply_count"] for c in conversation_data),
                "total_actual_replies": sum(c["actual_reply_count"] for c in conversation_data),
                "limit": limit,
                "offset": offset,
                "has_prev": has_prev,
                "has_next": has_next,
                "prev_offset": prev_offset,
                "next_offset": next_offset,
            },
        )


@router.get("/products/{product_slug}/export")
def export_product_data(
    request: Request,
    product_slug: str,
    format: str = "json",
    conversation_limit: int = 100,
    conversation_offset: int = Query(default=0, ge=0, description="Offset for pagination"),
):
    """
    Export product data including conversations, shadow replies, and actual replies.
    
    Supports pagination via conversation_offset parameter:
    - offset=0, limit=50  → conversations 0-49
    - offset=50, limit=50 → conversations 50-99
    - offset=100, limit=50 → conversations 100-149
    """
    from ..models import Product, Conversation, Message, AiShadowReply
    from fastapi.responses import JSONResponse, Response
    import json
    
    with get_session() as session:
        # Get product
        product = session.exec(
            select(Product).where(Product.slug == product_slug).limit(1)
        ).first()
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        
        # Find all conversations linked to this product via ads
        ad_ids_rows = session.exec(
            text(
                """
                SELECT DISTINCT ad_id FROM ads_products 
                WHERE product_id = :pid
                """
            ).params(pid=product.id)
        ).all()
        
        ad_ids = [str(r[0] if isinstance(r, tuple) else getattr(r, "ad_id", r)) for r in ad_ids_rows if r]
        
        conversations = []
        if ad_ids:
            # Build parameter list for IN clause
            params = {}
            placeholders = []
            for i, aid in enumerate(ad_ids):
                param_name = f"aid_{i}"
                placeholders.append(f":{param_name}")
                params[param_name] = str(aid)
            params["lim"] = conversation_limit
            params["off"] = conversation_offset
            
            placeholders_str = ",".join(placeholders)
            
            convo_rows = session.exec(
                text(
                    f"""
                    SELECT DISTINCT c.id, c.ig_user_id, c.graph_conversation_id,
                           c.last_message_at, u.username, u.contact_name, u.contact_phone
                    FROM conversations c
                    LEFT JOIN ig_users u ON u.ig_user_id = c.ig_user_id
                    INNER JOIN message m ON m.conversation_id = c.id
                    WHERE m.ad_id IN ({placeholders_str})
                    ORDER BY c.last_message_at DESC
                    LIMIT :lim OFFSET :off
                    """
                ).params(**params)
            ).all()
        else:
            convo_rows = []
        
        # Build export data
        export_data = {
            "product": {
                "id": product.id,
                "name": product.name,
                "slug": product.slug,
                "ai_system_msg": product.ai_system_msg,
                "ai_prompt_msg": product.ai_prompt_msg,
            },
            "conversation_count": len(convo_rows),
            "conversation_offset": conversation_offset,
            "conversation_limit": conversation_limit,
            "has_more": len(convo_rows) == conversation_limit,  # Indicates if there might be more
            "exported_at": dt.datetime.utcnow().isoformat(),
            "conversations": [],
        }
        
        for row in convo_rows:
            try:
                convo_id = row[0] if isinstance(row, tuple) else getattr(row, "id", row)
                
                # Get all messages
                messages = session.exec(
                    select(Message)
                    .where(Message.conversation_id == convo_id)
                    .order_by(Message.timestamp_ms.asc())
                ).all()
                
                # Get shadow replies
                shadow_replies = session.exec(
                    select(AiShadowReply)
                    .where(AiShadowReply.conversation_id == convo_id)
                    .order_by(AiShadowReply.created_at.asc())
                ).all()
                
                export_data["conversations"].append({
                    "conversation_id": convo_id,
                    "graph_conversation_id": row[2] if isinstance(row, tuple) else getattr(row, "graph_conversation_id", None),
                    "ig_user_id": row[1] if isinstance(row, tuple) else getattr(row, "ig_user_id", None),
                    "username": row[4] if isinstance(row, tuple) else getattr(row, "username", None),
                    "contact_name": row[5] if isinstance(row, tuple) else getattr(row, "contact_name", None),
                    "contact_phone": row[6] if isinstance(row, tuple) else getattr(row, "contact_phone", None),
                    "client_questions": [
                        {
                            "id": m.id,
                            "text": m.text,
                            "timestamp_ms": m.timestamp_ms,
                            "sender_username": m.sender_username,
                            "created_at": m.created_at.isoformat() if m.created_at else None,
                        }
                        for m in messages if m.direction == "in"
                    ],
                    "shadow_replies": [
                        {
                            "id": r.id,
                            "reply_text": r.reply_text,
                            "model": r.model,
                            "confidence": r.confidence,
                            "reason": r.reason,
                            "status": r.status,
                            "created_at": r.created_at.isoformat() if r.created_at else None,
                        }
                        for r in shadow_replies
                    ],
                    "actual_replies": [
                        {
                            "id": m.id,
                            "text": m.text,
                            "timestamp_ms": m.timestamp_ms,
                            "sender_username": m.sender_username,
                            "created_at": m.created_at.isoformat() if m.created_at else None,
                            "ai_status": m.ai_status,
                        }
                        for m in messages if m.direction == "out"
                    ],
                })
            except Exception as e:
                import logging
                logging.getLogger("ig_ai").error(f"Error exporting conversation: {e}")
                continue
        
        if format.lower() == "json":
            return JSONResponse(content=export_data)
        else:
            # CSV format (simplified - could be enhanced)
            import csv
            import io
            
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Header
            writer.writerow([
                "Conversation ID", "Graph Conversation ID", "Username", "Contact Name", "Contact Phone",
                "Question Text", "Question Timestamp", "Shadow Reply Text", "Shadow Confidence", "Shadow Status",
                "Actual Reply Text", "Actual Reply Timestamp", "AI Status"
            ])
            
            # Data rows
            for conv in export_data["conversations"]:
                # Get max length to iterate
                max_len = max(
                    len(conv["client_questions"]),
                    len(conv["shadow_replies"]),
                    len(conv["actual_replies"]),
                    1
                )
                
                for i in range(max_len):
                    row = [
                        conv["conversation_id"],
                        conv["graph_conversation_id"],
                        conv["username"],
                        conv["contact_name"],
                        conv["contact_phone"],
                    ]
                    
                    # Question
                    if i < len(conv["client_questions"]):
                        q = conv["client_questions"][i]
                        row.extend([q["text"], q["timestamp_ms"]])
                    else:
                        row.extend(["", ""])
                    
                    # Shadow reply
                    if i < len(conv["shadow_replies"]):
                        sr = conv["shadow_replies"][i]
                        row.extend([sr["reply_text"], sr["confidence"], sr["status"]])
                    else:
                        row.extend(["", "", ""])
                    
                    # Actual reply
                    if i < len(conv["actual_replies"]):
                        ar = conv["actual_replies"][i]
                        row.extend([ar["text"], ar["timestamp_ms"], ar["ai_status"]])
                    else:
                        row.extend(["", "", ""])
                    
                    writer.writerow(row)
            
            output.seek(0)
            return Response(
                content=output.getvalue(),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f'attachment; filename="product_{product_slug}_export.csv"'
                }
            )


@router.get("/products/reviews")
def product_reviews_list(request: Request, limit: int = 500):
    """
    List all products with links to their review pages.
    Shows products that have conversations linked to them.
    """
    from ..models import Product
    from sqlalchemy import text as _text
    
    with get_session() as session:
        # Get all products that have ads linked to them (meaning they might have conversations)
        products_rows = session.exec(
            _text(
                """
                SELECT DISTINCT p.id, p.name, p.slug, p.ai_system_msg, p.ai_prompt_msg,
                       (SELECT COUNT(DISTINCT c.id)
                        FROM conversations c
                        INNER JOIN message m ON m.conversation_id = c.id
                        INNER JOIN ads_products ap ON ap.ad_id = m.ad_id
                        WHERE ap.product_id = p.id) AS conversation_count
                FROM product p
                INNER JOIN ads_products ap ON ap.product_id = p.id
                ORDER BY p.name ASC
                LIMIT :lim
                """
            ).params(lim=limit)
        ).all()
        
        products = []
        for row in products_rows:
            try:
                products.append({
                    "id": row[0] if isinstance(row, tuple) else getattr(row, "id", row),
                    "name": row[1] if isinstance(row, tuple) else getattr(row, "name", row),
                    "slug": row[2] if isinstance(row, tuple) else getattr(row, "slug", row),
                    "ai_system_msg": row[3] if isinstance(row, tuple) else getattr(row, "ai_system_msg", None),
                    "ai_prompt_msg": row[4] if isinstance(row, tuple) else getattr(row, "ai_prompt_msg", None),
                    "conversation_count": int(row[5] if isinstance(row, tuple) else getattr(row, "conversation_count", 0) or 0),
                })
            except Exception:
                continue
        
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "ig_ai_product_reviews_list.html",
            {
                "request": request,
                "products": products,
                "total": len(products),
            },
        )


@router.get("/process/runs")
def list_runs(limit: int = 50):
    with get_session() as session:
        nint = int(max(1, min(limit, 200)))
        # Embed LIMIT as a literal integer to avoid driver param binding issues
        rows = session.exec(text(f"""
            SELECT id, started_at, completed_at, cancelled_at, job_id, date_from, date_to, min_age_minutes,
                   conversations_considered, conversations_processed, orders_linked,
                   purchases_detected, purchases_unlinked, errors_json
            FROM ig_ai_run ORDER BY id DESC LIMIT {nint}
        """)).all()
        out = []
        for r in rows:
            out.append({
                "id": getattr(r, "id", r[0]),
                "started_at": getattr(r, "started_at", r[1]),
                "completed_at": getattr(r, "completed_at", r[2]),
                "cancelled_at": getattr(r, "cancelled_at", r[3]),
                "job_id": getattr(r, "job_id", r[4]),
                "date_from": getattr(r, "date_from", r[5]),
                "date_to": getattr(r, "date_to", r[6]),
                "min_age_minutes": getattr(r, "min_age_minutes", r[7]),
                "conversations_considered": getattr(r, "conversations_considered", r[8]),
                "conversations_processed": getattr(r, "conversations_processed", r[9]),
                "orders_linked": getattr(r, "orders_linked", r[10]),
                "purchases_detected": getattr(r, "purchases_detected", r[11]),
                "purchases_unlinked": getattr(r, "purchases_unlinked", r[12]),
                "errors_json": getattr(r, "errors_json", r[13]),
            })
        return {"runs": out}


@router.get("/process/run/{run_id}")
def run_details(run_id: int):
    with get_session() as session:
        stmt = text(
            """
            SELECT id, started_at, completed_at, cancelled_at, job_id, date_from, date_to, min_age_minutes,
                   conversations_considered, conversations_processed, orders_linked,
                   purchases_detected, purchases_unlinked, errors_json
            FROM ig_ai_run WHERE id = :id
            """
        ).bindparams(id=int(run_id))
        row = session.exec(stmt).first()
        if not row:
            raise HTTPException(status_code=404, detail="Run not found")
        return {
            "id": getattr(row, "id", row[0]),
            "started_at": getattr(row, "started_at", row[1]),
            "completed_at": getattr(row, "completed_at", row[2]),
            "cancelled_at": getattr(row, "cancelled_at", row[3]),
            "job_id": getattr(row, "job_id", row[4]),
            "date_from": getattr(row, "date_from", row[5]),
            "date_to": getattr(row, "date_to", row[6]),
            "min_age_minutes": getattr(row, "min_age_minutes", row[7]),
            "conversations_considered": getattr(row, "conversations_considered", row[8]),
            "conversations_processed": getattr(row, "conversations_processed", row[9]),
            "orders_linked": getattr(row, "orders_linked", row[10]),
            "purchases_detected": getattr(row, "purchases_detected", row[11]),
            "purchases_unlinked": getattr(row, "purchases_unlinked", row[12]),
            "errors_json": getattr(row, "errors_json", row[13]),
        }


@router.post("/process/run/{run_id}/cancel")
def cancel_run(run_id: int):
    with get_session() as session:
        row = session.exec(text("SELECT job_id FROM ig_ai_run WHERE id=:id").params(id=int(run_id))).first()
        session.exec(text("UPDATE ig_ai_run SET cancelled_at=CURRENT_TIMESTAMP, completed_at=COALESCE(completed_at, CURRENT_TIMESTAMP) WHERE id=:id").params(id=int(run_id)))
        jid = None
        if row:
            jid = getattr(row, 'job_id', None) if hasattr(row, 'job_id') else (row[0] if isinstance(row, (list, tuple)) else None)
    try:
        if jid:
            delete_job(int(jid))
    except Exception:
        pass
    return {"status": "ok"}


@router.post("/process/preview")
def preview_process(body: dict):
    # Parse inputs like start_process, but only compute counts
    date_from_s: Optional[str] = (body or {}).get("date_from")
    date_to_s: Optional[str] = (body or {}).get("date_to")
    min_age_minutes: int = int((body or {}).get("min_age_minutes") or 60)
    reprocess: bool = bool((body or {}).get("reprocess") not in (False, 0, "0", "false", "False", None))

    def _parse_date(v: Optional[str]) -> Optional[dt.date]:
        try:
            return dt.date.fromisoformat(str(v)) if v else None
        except Exception:
            return None

    date_from = _parse_date(date_from_s)
    date_to = _parse_date(date_to_s)

    # Compute cutoff
    now = dt.datetime.utcnow()
    cutoff_dt = now - dt.timedelta(minutes=max(0, min_age_minutes))
    # For message timestamp_ms comparisons (ms since epoch)
    cutoff_ms = int(cutoff_dt.timestamp() * 1000)

    with get_session() as session:
        # Conversations count derived from messages grouped by conversation_id (conversations table has no ai_process_time)
        cutoff_ms = int(cutoff_dt.timestamp() * 1000)
        msg_where = ["m.conversation_id IS NOT NULL", "(m.timestamp_ms IS NULL OR m.timestamp_ms <= :cutoff_ms)"]
        msg_params: dict[str, object] = {"cutoff_ms": int(cutoff_ms)}
        if date_from and date_to and date_from <= date_to:
            ms_from = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
            ms_to = int(dt.datetime.combine(date_to + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
            msg_where.append("(m.timestamp_ms IS NULL OR (m.timestamp_ms >= :ms_from AND m.timestamp_ms < :ms_to))")
            msg_params["ms_from"] = int(ms_from)
            msg_params["ms_to"] = int(ms_to)
        elif date_from:
            ms_from = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
            msg_where.append("(m.timestamp_ms IS NULL OR m.timestamp_ms >= :ms_from)")
            msg_params["ms_from"] = int(ms_from)
        elif date_to:
            ms_to = int(dt.datetime.combine(date_to + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
            msg_where.append("(m.timestamp_ms IS NULL OR m.timestamp_ms < :ms_to)")
            msg_params["ms_to"] = int(ms_to)
        sql_conv = (
            "SELECT COUNT(1) AS c FROM ("
            " SELECT m.conversation_id, MAX(COALESCE(m.timestamp_ms,0)) AS last_ts"
            " FROM message m WHERE " + " AND ".join(msg_where) +
            " GROUP BY m.conversation_id"
            ") t"
        )
        rowc = session.exec(text(sql_conv).params(**msg_params)).first()
        conv_count = int((getattr(rowc, "c", None) if rowc is not None else 0) or (rowc[0] if rowc else 0) or 0)

        # Messages count in scope (no ai_process_time watermark; conversations table does not have it)
        msg_where = ["m.conversation_id IS NOT NULL", "COALESCE(m.timestamp_ms,0) <= :cutoff_ms"]
        msg_params = {"cutoff_ms": int(cutoff_ms)}
        if date_from and date_to and date_from <= date_to:
            ms_from = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
            ms_to = int(dt.datetime.combine(date_to + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
            msg_where.append("(m.timestamp_ms IS NULL OR (m.timestamp_ms >= :ms_from AND m.timestamp_ms < :ms_to))")
            msg_params["ms_from"] = int(ms_from)
            msg_params["ms_to"] = int(ms_to)
        elif date_from:
            ms_from = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
            msg_where.append("(m.timestamp_ms IS NULL OR m.timestamp_ms >= :ms_from)")
            msg_params["ms_from"] = int(ms_from)
        elif date_to:
            ms_to = int(dt.datetime.combine(date_to + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
            msg_where.append("(m.timestamp_ms IS NULL OR m.timestamp_ms < :ms_to)")
            msg_params["ms_to"] = int(ms_to)
        sql_msg = (
            "SELECT COUNT(1) AS mc, SUM(CASE WHEN m.timestamp_ms IS NULL THEN 1 ELSE 0 END) AS mt0 "
            "FROM message m WHERE "
            + " AND ".join(msg_where)
        )
        rowm = session.exec(text(sql_msg).params(**msg_params)).first()
        msg_count = int((getattr(rowm, "mc", None) if rowm is not None else 0) or (rowm[0] if rowm else 0) or 0)
        msg_ts_missing = int((getattr(rowm, "mt0", None) if rowm is not None else 0) or (rowm[1] if rowm and len(rowm) > 1 else 0) or 0)

        # Fallbacks when conversations table filters produce 0 due to missing/old data
        if conv_count == 0:
            ms_from = None
            ms_to = None
            if date_from:
                ms_from = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
            if date_to:
                ms_to = int(dt.datetime.combine(date_to + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
            where_msg = ["m.conversation_id IS NOT NULL", "(m.timestamp_ms IS NULL OR m.timestamp_ms <= :cutoff_ms)"]
            params_f = {"cutoff_ms": int(cutoff_ms)}
            if ms_from is not None and ms_to is not None:
                where_msg.append("(m.timestamp_ms IS NULL OR (m.timestamp_ms >= :ms_from AND m.timestamp_ms < :ms_to))")
                params_f["ms_from"] = int(ms_from)
                params_f["ms_to"] = int(ms_to)
            elif ms_from is not None:
                where_msg.append("(m.timestamp_ms IS NULL OR m.timestamp_ms >= :ms_from)")
                params_f["ms_from"] = int(ms_from)
            elif ms_to is not None:
                where_msg.append("(m.timestamp_ms IS NULL OR m.timestamp_ms < :ms_to)")
                params_f["ms_to"] = int(ms_to)
            sql_conv_fb = "SELECT COUNT(DISTINCT m.conversation_id) AS c FROM message m WHERE " + " AND ".join(where_msg)
            rowfb = session.exec(text(sql_conv_fb).params(**params_f)).first()
            conv_count = int((getattr(rowfb, "c", None) if rowfb is not None else 0) or (rowfb[0] if rowfb else 0) or 0)

    return {
        "eligible_conversations": conv_count,
        "messages_in_scope": msg_count,
        "messages_without_timestamp": msg_ts_missing,
        "cutoff": cutoff_dt.isoformat(),
    }


@router.get("/shadow/monitor")
def shadow_monitor(request: Request, limit: int = 50, status: str | None = None):
    safe_status = (status or "").strip().lower() or None
    data = _collect_shadow_metrics(limit, status_filter=safe_status)
    templates = request.app.state.templates
    ctx = {
        "request": request,
        **data,
        "status_filter": safe_status or "",
        "entries_serialized": [_serialize_shadow_entry(e) for e in data["entries"]],
    }
    return templates.TemplateResponse("ig_ai_shadow.html", ctx)


@router.get("/shadow/monitor/data")
def shadow_monitor_data(limit: int = 50, status: str | None = None):
    safe_status = (status or "").strip().lower() or None
    data = _collect_shadow_metrics(limit, status_filter=safe_status)
    payload = {
        "generated_at": data["generated_at"].isoformat(),
        "status_counts": data["status_counts"],
        "summary": data["summary"],
        "limit": data["limit"],
        "entries": [_serialize_shadow_entry(e) for e in data["entries"]],
        "status_filter": safe_status or "",
        "shadow_scope": data.get("shadow_scope", "all"),
    }
    return payload


@router.post("/shadow/reset")
def shadow_reset(body: dict | None = Body(default=None)):
    """Reset one or all shadow states back to pending so worker can retry."""
    from ..models import AiShadowState

    target_cid = None
    if body and body.get("conversation_id") is not None:
        try:
            target_cid = int(body.get("conversation_id"))
        except Exception:
            raise HTTPException(status_code=400, detail="conversation_id must be an integer")

    with get_session() as session:
        query = select(AiShadowState)
        if target_cid:
            query = query.where(AiShadowState.conversation_id == target_cid)
        else:
            query = query.where(AiShadowState.status == "error")
        rows = session.exec(query).all()
        if not rows:
            return {"status": "ok", "reset": 0}

        now = dt.datetime.utcnow()
        count = 0
        for row in rows:
            row.status = "pending"
            row.next_attempt_at = None
            row.postpone_count = 0
            row.updated_at = now
            session.add(row)
            count += 1
        session.commit()
    return {"status": "ok", "reset": count}


@router.get("/run/{run_id}/logs")
def run_logs(run_id: int, limit: int = 200):
    n = int(max(1, min(limit, 2000)))
    logs = get_ai_run_logs(int(run_id), n)
    return {"logs": logs}


@router.get("/process/run/{run_id}/results")
def run_results(request: Request, run_id: int, limit: int = 200, status: str | None = None, linked: str | None = None, q: str | None = None, start: str | None = None, end: str | None = None):
    """List per-conversation results for a given AI run."""
    n = int(max(1, min(limit or 200, 1000)))
    # Build filters
    where = ["r.run_id = :rid"]
    params: dict[str, object] = {"rid": int(run_id), "lim": int(n)}
    st = (status or "").strip().lower()
    if st and st not in ("all", "*"):
        where.append("r.status = :st")
        params["st"] = st
    lk = (linked or "").strip().lower()
    if lk in ("yes", "true", "1"):
        where.append("r.linked_order_id IS NOT NULL")
    elif lk in ("no", "false", "0"):
        where.append("r.linked_order_id IS NULL")
    qq = (q or "").strip()
    if qq:
        where.append("("
                     "LOWER(COALESCE(r.convo_id,'')) LIKE :qq OR "
                     "LOWER(COALESCE(r.ai_json,'')) LIKE :qq OR "
                     "LOWER(COALESCE(u.contact_name,'')) LIKE :qq OR "
                     "COALESCE(u.contact_phone,'') LIKE :qp"
                     ")")
        params["qq"] = f"%{qq.lower()}%"
        # for phone, do not lower or strip digits only; a simple contains helps
        params["qp"] = f"%{qq}%"
    # Date filter on last_ts using HAVING after aggregation
    having: list[str] = []
    def _parse_date(s: str | None):
        try:
            return dt.date.fromisoformat(str(s)) if s else None
        except Exception:
            return None
    sd = _parse_date(start)
    ed = _parse_date(end)
    if sd:
        ms_from = int(dt.datetime.combine(sd, dt.time.min).timestamp() * 1000)
        having.append("last_ts >= :ms_from")
        params["ms_from"] = int(ms_from)
    if ed:
        ms_to = int(dt.datetime.combine(ed + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
        having.append("last_ts < :ms_to")
        params["ms_to"] = int(ms_to)
    sql = (
        "SELECT r.convo_id, r.status, r.linked_order_id, r.ai_json, r.created_at, "
        "       MAX(COALESCE(m.timestamp_ms,0)) AS last_ts, "
        "       u.contact_name, u.contact_phone "
        "FROM ig_ai_result r "
        "LEFT JOIN message m ON m.conversation_id = r.convo_id "
        "LEFT JOIN ig_users u "
        "  ON u.ig_user_id = COALESCE("
        "       CASE WHEN m.direction = 'in' THEN m.ig_sender_id ELSE m.ig_recipient_id END, "
        "       m.ig_sender_id, m.ig_recipient_id"
        "     ) "
        "WHERE " + " AND ".join(where) + " "
        "GROUP BY r.convo_id, r.status, r.linked_order_id, r.ai_json, r.created_at, "
        "         u.contact_name, u.contact_phone "
        + ("HAVING " + " AND ".join(having) + " " if having else "")
        + "ORDER BY last_ts DESC, r.convo_id DESC "
        "LIMIT :lim"
    )
    with get_session() as session:
        rows = session.exec(text(sql).params(**params)).all()
        items: list[dict] = []
        for r in rows:
            try:
                items.append({
                    "convo_id": getattr(r, "convo_id", r[0]),
                    "status": getattr(r, "status", r[1]),
                    "linked_order_id": getattr(r, "linked_order_id", r[2]),
                    "ai_json": getattr(r, "ai_json", r[3]),
                    "created_at": getattr(r, "created_at", r[4]),
                    "last_ts": getattr(r, "last_ts", r[5]),
                    "contact_name": getattr(r, "contact_name", r[6]) if len(r) > 6 else None,
                    "contact_phone": getattr(r, "contact_phone", r[7]) if len(r) > 7 else None,
                })
            except Exception:
                continue
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "ig_ai_run_results.html",
        {
            "request": request,
            "run_id": int(run_id),
            "rows": items,
            "status": status or "",
            "linked": linked or "",
            "q": q or "",
            "start": start or "",
            "end": end or "",
        },
    )


@router.get("/process/run/{run_id}/result/{convo_id}")
def run_result_detail(request: Request, run_id: int, convo_id: str, limit: int = 120):
    """Detail view for a conversation result in a given run, including recent messages and bind UI."""
    n = int(max(20, min(limit or 120, 500)))
    with get_session() as session:
        # latest result for run+convo
        row = session.exec(
            text(
                """
                SELECT id, status, ai_json, linked_order_id, created_at
                FROM ig_ai_result
                WHERE run_id=:rid AND convo_id=:cid
                ORDER BY id DESC LIMIT 1
                """
            ).params(rid=int(run_id), cid=str(convo_id))
        ).first()
        if not row:
            raise HTTPException(status_code=404, detail="Result not found for this run/conversation")
        res = {
            "id": getattr(row, "id", row[0]),
            "status": getattr(row, "status", row[1]),
            "ai_json": getattr(row, "ai_json", row[2]),
            "linked_order_id": getattr(row, "linked_order_id", row[3]),
            "created_at": getattr(row, "created_at", row[4]) if len(row) > 4 else None,
        }
        # messages (chronological)
        msgs = session.exec(
            text(
                """
                SELECT timestamp_ms, direction, text
                FROM message
                WHERE conversation_id=:cid
                ORDER BY COALESCE(timestamp_ms,0) ASC, id ASC
                LIMIT :lim
                """
            ).params(cid=str(convo_id), lim=int(n))
        ).all()
        messages: list[dict] = []
        for m in msgs:
            try:
                ts_ms = getattr(m, "timestamp_ms", m[0])
                ts_h = None
                try:
                    if ts_ms and int(ts_ms) > 0:
                        from datetime import datetime as _dt
                        ts_h = _dt.utcfromtimestamp(int(ts_ms) / 1000).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    ts_h = None
                messages.append({
                    "timestamp_ms": ts_ms,
                    "ts": ts_h,
                    "direction": getattr(m, "direction", m[1]),
                    "text": getattr(m, "text", m[2]),
                })
            except Exception:
                continue
        # contact info (from ig_users via latest message for this conversation)
        contact = {}
        try:
            rowu = session.exec(
                text(
                    """
                    SELECT
                      CASE
                        WHEN m.direction = 'in' THEN m.ig_sender_id
                        ELSE m.ig_recipient_id
                      END AS ig_user_id
                    FROM message m
                    WHERE m.conversation_id=:cid
                    ORDER BY COALESCE(m.timestamp_ms,0) DESC, m.id DESC
                    LIMIT 1
                    """
                ).params(cid=str(convo_id))
            ).first()
            ig_user_id = rowu.ig_user_id if rowu and hasattr(rowu, "ig_user_id") else (rowu[0] if rowu else None)
            if ig_user_id:
                rc = session.exec(
                    text(
                        "SELECT contact_name, contact_phone, contact_address "
                        "FROM ig_users WHERE ig_user_id=:uid LIMIT 1"
                    ).params(uid=str(ig_user_id))
                ).first()
                if rc:
                    contact = {
                        "name": getattr(rc, "contact_name", rc[0]) or None,
                        "phone": getattr(rc, "contact_phone", rc[1]) or None,
                        "address": getattr(rc, "contact_address", rc[2]) or None,
                    }
        except Exception:
            contact = {}
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "ig_ai_run_result_detail.html",
        {
            "request": request,
            "run_id": int(run_id),
            "convo_id": str(convo_id),
            "result": res,
            "messages": messages,
            "contact": contact,
        },
    )


@router.get("/pretexts")
def pretexts_page(request: Request):
    """List and manage AI pretexts."""
    from ..models import AIPretext
    
    with get_session() as session:
        pretexts = session.exec(select(AIPretext).order_by(AIPretext.is_default.desc(), AIPretext.id.asc())).all()
        pretext_list = []
        for p in pretexts:
            pretext_list.append({
                "id": p.id,
                "name": p.name,
                "content": p.content or "",
                "is_default": p.is_default,
                "created_at": p.created_at,
                "updated_at": p.updated_at,
            })
    
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "ig_ai_pretexts.html",
        {"request": request, "pretexts": pretext_list},
    )


@router.post("/pretexts/create")
def create_pretext(
    name: str = Form(...),
    content: str = Form(...),
    is_default: str = Form(default=""),
):
    """Create a new pretext."""
    from ..models import AIPretext
    
    with get_session() as session:
        # If this is set as default, unset other defaults
        set_as_default = is_default.lower() in ("true", "1", "yes", "on")
        if set_as_default:
            session.exec(text("UPDATE ai_pretext SET is_default = 0"))
        
        pretext = AIPretext(
            name=name.strip(),
            content=content.strip(),
            is_default=set_as_default,
        )
        session.add(pretext)
        session.flush()
        pretext_id = pretext.id
    
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/ig/ai/pretexts", status_code=303)


@router.post("/pretexts/{pretext_id}/update")
def update_pretext(
    pretext_id: int,
    name: str = Form(...),
    content: str = Form(...),
    is_default: str = Form(default=""),
):
    """Update an existing pretext."""
    from ..models import AIPretext
    
    with get_session() as session:
        pretext = session.exec(select(AIPretext).where(AIPretext.id == pretext_id)).first()
        if not pretext:
            raise HTTPException(status_code=404, detail="Pretext not found")
        
        # If this is set as default, unset other defaults
        set_as_default = is_default.lower() in ("true", "1", "yes", "on")
        if set_as_default:
            session.exec(text("UPDATE ai_pretext SET is_default = 0 WHERE id != :id").params(id=pretext_id))
        
        pretext.name = name.strip()
        pretext.content = content.strip()
        pretext.is_default = set_as_default
        session.add(pretext)
    
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/ig/ai/pretexts", status_code=303)


@router.post("/pretexts/{pretext_id}/delete")
def delete_pretext(pretext_id: int):
    """Delete a pretext. If products use it, set their pretext_id to NULL (they fall back to default) then delete."""
    from ..models import AIPretext, Product

    with get_session() as session:
        pretext = session.exec(select(AIPretext).where(AIPretext.id == pretext_id)).first()
        if not pretext:
            raise HTTPException(status_code=404, detail="Pretext not found")

        # Kullanan ürünleri varsayılana çek (pretext_id = NULL → default pretext kullanılır)
        products_using = session.exec(
            select(Product).where(Product.pretext_id == pretext_id)
        ).all()
        for prod in products_using:
            prod.pretext_id = None
            session.add(prod)

        session.delete(pretext)
        session.commit()

    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/ig/ai/pretexts", status_code=303)


@router.get("/link-suggest")
def link_suggest_page(request: Request, start: str | None = None, end: str | None = None, limit: int = 200):
    """Suggest linking orders to IG conversations by matching client phone in conversations/messages.

    - Only orders without ig_conversation_id
    - Default date window: last 7 days
    - Date criterion: shipment_date if present else data_date
    """
    n = int(max(1, min(limit or 200, 1000)))
    def _parse_date(s: str | None) -> dt.date | None:
        try:
            return dt.date.fromisoformat(str(s)) if s else None
        except Exception:
            return None
    today = dt.date.today()
    start_d = _parse_date(start) or (today - dt.timedelta(days=7))
    end_d = _parse_date(end) or today
    if end_d < start_d:
        start_d, end_d = end_d, start_d
    from ..models import Order, Client, Item
    with get_session() as session:
        # Orders without link in date window
        rows = session.exec(
            select(Order, Client, Item)
            .where(Order.ig_conversation_id.is_(None))
            .where(
                (
                    ((Order.shipment_date.is_not(None)) & (Order.shipment_date >= start_d) & (Order.shipment_date <= end_d))
                    | ((Order.shipment_date.is_(None)) & (Order.data_date.is_not(None)) & (Order.data_date >= start_d) & (Order.data_date <= end_d))
                )
            )
            .where(Order.client_id == Client.id)
            .where((Order.item_id.is_(None)) | (Order.item_id == Item.id))
            .order_by(Order.id.desc())
            .limit(n)
        ).all()
        suggestions: list[dict] = []
        for o, c, it in rows:
            phone = (c.phone or "").strip() if c.phone else ""
            # normalize digits, prefer last 10
            digits = "".join([ch for ch in phone if ch.isdigit()])
            last10 = digits[-10:] if len(digits) >= 10 else digits
            convo_id = None
            msg_preview = None
            # Try ig_users.contact_phone first (via messages to identify ig_user_id)
            if last10:
                try:
                    rowc = session.exec(
                        text(
                            """
                            SELECT m.conversation_id, u.contact_phone
                            FROM message m
                            JOIN ig_users u
                              ON u.ig_user_id = COALESCE(
                                   CASE WHEN m.direction='in' THEN m.ig_sender_id ELSE m.ig_recipient_id END,
                                   m.ig_sender_id, m.ig_recipient_id
                                 )
                            WHERE u.contact_phone IS NOT NULL
                              AND REPLACE(REPLACE(REPLACE(u.contact_phone,' ',''),'-',''),'+','') LIKE :p
                            ORDER BY COALESCE(m.timestamp_ms,0) DESC, m.id DESC
                            LIMIT 1
                            """
                        ).params(p=f"%{last10}%")
                    ).first()
                    if rowc:
                        convo_id = rowc.conversation_id if hasattr(rowc, "conversation_id") else rowc[0]
                except Exception:
                    convo_id = None
            # Fallback: search messages.text for digits
            if (convo_id is None) and last10:
                try:
                    rowm = session.exec(
                        text(
                            """
                            SELECT conversation_id, text, timestamp_ms
                            FROM message
                            WHERE text IS NOT NULL AND REPLACE(REPLACE(REPLACE(text,' ',''),'-',''),'+','') LIKE :p
                            ORDER BY COALESCE(timestamp_ms,0) DESC LIMIT 1
                            """
                        ).params(p=f"%{last10}%")
                    ).first()
                    if rowm:
                        convo_id = rowm.conversation_id if hasattr(rowm, "conversation_id") else rowm[0]
                        msg_preview = rowm.text if hasattr(rowm, "text") else (rowm[1] if len(rowm) > 1 else None)
                except Exception:
                    pass
            suggestions.append({
                "order_id": int(o.id or 0),
                "client_id": int(c.id or 0),
                "client_name": c.name,
                "client_phone": phone,
                "item_name": (it.name if it else None),
                "total": float(o.total_amount or 0.0) if o.total_amount is not None else None,
                "date": (o.shipment_date or o.data_date),
                "convo_id": convo_id,
                "msg_preview": msg_preview,
            })
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "ig_link_suggest.html",
        {"request": request, "rows": suggestions, "start": start_d, "end": end_d},
    )


@router.post("/link-suggest/apply")
async def link_suggest_apply(request: Request):
    """Apply linking selections from the UI."""
    form = await request.form()
    # Expect arrays: sel[] (order_id strings), and conv[order_id]=convo_id
    selected = form.getlist("sel[]") if hasattr(form, "getlist") else []
    # Build conv map
    conv_map: dict[int, str] = {}
    try:
        for k, v in form.multi_items():  # type: ignore[attr-defined]
            if str(k).startswith("conv[") and str(k).endswith("]"):
                try:
                    oid = int(str(k)[5:-1])
                    conv_map[oid] = str(v)
                except Exception:
                    continue
    except Exception:
        # fallback: scan all keys
        for k in form.keys():  # type: ignore
            if str(k).startswith("conv[") and str(k).endswith("]"):
                try:
                    val = form.get(k)  # type: ignore
                    oid = int(str(k)[5:-1])
                    conv_map[oid] = str(val or "")
                except Exception:
                    continue
    updated = 0
    with get_session() as session:
        from ..models import Order
        for s in selected:
            try:
                oid = int(s)
            except Exception:
                continue
            cv = conv_map.get(oid)
            if not cv:
                continue
            o = session.exec(select(Order).where(Order.id == oid)).first()
            if not o:
                continue
            # Resolve ig_user_id for this conversation from latest message
            ig_user_id = None
            try:
                rowu = session.exec(
                    text(
                        """
                        SELECT
                          CASE
                            WHEN m.direction = 'in' THEN m.ig_sender_id
                            ELSE m.ig_recipient_id
                          END AS ig_user_id
                        FROM message m
                        WHERE m.conversation_id=:cid
                        ORDER BY COALESCE(m.timestamp_ms,0) DESC, m.id DESC
                        LIMIT 1
                        """
                    ).params(cid=str(cv))
                ).first()
                ig_user_id = rowu.ig_user_id if rowu and hasattr(rowu, "ig_user_id") else (rowu[0] if rowu else None)
            except Exception:
                ig_user_id = None
            if ig_user_id:
                try:
                    session.exec(
                        text(
                            "UPDATE ig_users SET linked_order_id=:oid "
                            "WHERE ig_user_id=:uid AND linked_order_id IS NULL"
                        ).params(oid=int(oid), uid=str(ig_user_id))
                    )
                except Exception:
                    pass
            if not o.ig_conversation_id:
                o.ig_conversation_id = str(cv)
                session.add(o)
            updated += 1
    return {"status": "ok", "linked": updated}


@router.get("/unlinked")
def unlinked_purchases(request: Request, q: str | None = None, start: str | None = None, end: str | None = None, limit: int = 200):
    """List conversations where a purchase was detected but not linked to an order.

    Uses ig_users AI status and orders by latest message timestamp.
    """
    n = int(max(1, min(limit or 200, 1000)))
    where = ["(u.ai_status = 'ambiguous' OR u.ai_status IS NULL)"]
    params: dict[str, object] = {}
    # Parse start/end as dates and convert to ms window over last_ts
    def _parse_date(s: str | None):
        try:
            return dt.date.fromisoformat(str(s)) if s else None
        except Exception:
            return None
    start_d = _parse_date(start)
    end_d = _parse_date(end)
    ms_from = None
    ms_to = None
    if start_d:
        ms_from = int(dt.datetime.combine(start_d, dt.time.min).timestamp() * 1000)
    if end_d:
        ms_to = int(dt.datetime.combine(end_d + dt.timedelta(days=1), dt.time.min).timestamp() * 1000)
    if q:
        # Search in contact fields (when present) or AI JSON (on ig_users)
        where.append("(LOWER(COALESCE(u.contact_name,'')) LIKE :qq OR COALESCE(u.contact_phone,'') LIKE :qp OR LOWER(COALESCE(u.ai_json,'')) LIKE :qa)")
        qs = f"%{q.lower()}%"
        params.update({"qq": qs, "qa": qs, "qp": f"%{q}%"} )
    # Build via subquery to filter on last_ts window
    sql = (
        "SELECT t.convo_id, u.contact_name, u.contact_phone, u.contact_address, "
        "       u.ai_status, u.ai_json, NULL AS linked_order_id, t.last_ts "
        "FROM ("
        "  SELECT m.conversation_id AS convo_id, "
        "         MAX(COALESCE(m.timestamp_ms,0)) AS last_ts, "
        "         MAX(CASE WHEN m.direction='in' THEN m.ig_sender_id ELSE m.ig_recipient_id END) AS ig_user_id "
        "  FROM message m "
        "  GROUP BY m.conversation_id"
        ") t "
        "JOIN ig_users u ON u.ig_user_id = t.ig_user_id "
        "WHERE " + " AND ".join(where)
        + (" AND t.last_ts >= :ms_from" if ms_from is not None else "")
        + (" AND t.last_ts < :ms_to" if ms_to is not None else "")
        + " ORDER BY t.last_ts DESC, t.convo_id DESC "
        "LIMIT :lim"
    )
    params["lim"] = int(n)
    if ms_from is not None:
        params["ms_from"] = int(ms_from)
    if ms_to is not None:
        params["ms_to"] = int(ms_to)
    with get_session() as session:
        rows = session.exec(text(sql).params(**params)).all()
        items = []
        for r in rows:
            try:
                convo_id = getattr(r, "convo_id", r[0])
                contact_name = getattr(r, "contact_name", None if len(r) < 2 else r[1])
                contact_phone = getattr(r, "contact_phone", None if len(r) < 3 else r[2])
                contact_address = getattr(r, "contact_address", None if len(r) < 4 else r[3])
                ai_status = getattr(r, "ai_status", None if len(r) < 5 else r[4])
                ai_json = getattr(r, "ai_json", None if len(r) < 6 else r[5])
                last_ts = getattr(r, "last_ts", None if len(r) < 8 else r[7])
                # Fallback contact info from AI JSON if conversations row is missing/empty
                if (not contact_name or not contact_phone or not contact_address) and ai_json:
                    try:
                        data = __import__("json").loads(ai_json)
                        if isinstance(data, dict):
                            contact_name = contact_name or data.get("buyer_name")
                            contact_phone = contact_phone or data.get("phone")
                            contact_address = contact_address or data.get("address")
                    except Exception:
                        pass
                # Convert last_ts ms to ISO string
                last_dt = None
                try:
                    if last_ts and int(last_ts) > 0:
                        last_dt = __import__("datetime").datetime.utcfromtimestamp(int(last_ts) / 1000).isoformat()
                except Exception:
                    last_dt = None
                items.append({
                    "convo_id": convo_id,
                    "contact_name": contact_name,
                    "contact_phone": contact_phone,
                    "contact_address": contact_address,
                    "ai_status": ai_status,
                    "ai_json": ai_json,
                    "linked_order_id": None,
                    "last_message_at": last_dt,
                })
            except Exception:
                continue
    templates = request.app.state.templates
    return templates.TemplateResponse("ig_ai_unlinked.html", {"request": request, "rows": items, "q": q or "", "start": start or "", "end": end or ""})


@router.get("/unlinked/search")
def search_orders(q: str, limit: int = 20):
    """Search orders by client name or phone digits."""
    if not q or not isinstance(q, str):
        raise HTTPException(status_code=400, detail="q required")
    q = q.strip()
    from ..models import Client, Order
    with get_session() as session:
        # Normalize phone digits
        phone_digits = "".join([c for c in q if c.isdigit()])
        # Build base query
        qry = select(Order, Client).where(Order.client_id == Client.id)
        if phone_digits:
            qry = qry.where((Client.phone.is_not(None)) & (Client.phone.contains(phone_digits)))
        else:
            from sqlalchemy import func as _func
            qry = qry.where(_func.lower(Client.name).like(f"%{q.lower()}%"))
        rows = session.exec(qry.order_by(Order.id.desc()).limit(max(1, min(limit, 50)))).all()
        out = []
        for o, c in rows:
            out.append({
                "order_id": int(o.id or 0),
                "client_id": int(c.id or 0),
                "client_name": c.name,
                "client_phone": c.phone,
                "total": float(o.total_amount or 0.0) if o.total_amount is not None else None,
                "shipment_date": o.shipment_date.isoformat() if o.shipment_date else None,
                "data_date": o.data_date.isoformat() if o.data_date else None,
                "source": o.source,
            })
        return {"results": out}


@router.post("/unlinked/bind")
def bind_conversation(body: dict):
    """Bind a conversation to an order: sets conversations.linked_order_id and order.ig_conversation_id if empty."""
    convo_id = (body or {}).get("conversation_id")
    order_id = (body or {}).get("order_id")
    if not convo_id or not order_id:
        raise HTTPException(status_code=400, detail="conversation_id and order_id required")
    try:
        oid = int(order_id)
    except Exception:
        raise HTTPException(status_code=400, detail="order_id must be integer")
    with get_session() as session:
        # Ensure order exists
        from ..models import Order
        row = session.exec(select(Order).where(Order.id == oid)).first()
        if not row:
            raise HTTPException(status_code=404, detail="Order not found")
        # Update conversations and order
        try:
            session.exec(text("UPDATE conversations SET linked_order_id=:oid WHERE convo_id=:cid").params(oid=oid, cid=str(convo_id)))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"link_failed: {e}")
        # Reflect link in ai_conversations as well
        try:
            session.exec(text("UPDATE ai_conversations SET linked_order_id=:oid WHERE convo_id=:cid").params(oid=oid, cid=str(convo_id)))
        except Exception:
            pass
        try:
            if not row.ig_conversation_id:
                row.ig_conversation_id = str(convo_id)
                session.add(row)
        except Exception:
            pass
    return {"status": "ok"}


@router.post("/unlinked/mark")
def mark_unlinked(body: dict):
    """Mark an unlinked conversation's AI status (e.g., no_purchase) so it no longer appears in the list."""
    convo_id = (body or {}).get("conversation_id")
    status = (body or {}).get("status") or "no_purchase"
    allowed = {"no_purchase", "ambiguous", "ok"}
    if not convo_id:
        raise HTTPException(status_code=400, detail="conversation_id required")
    if status not in allowed:
        raise HTTPException(status_code=400, detail=f"invalid status; allowed: {', '.join(sorted(allowed))}")
    with get_session() as session:
        # Resolve ig_user_id for this conversation and update ig_users.ai_status
        ig_user_id = None
        try:
            rowu = session.exec(
                text(
                    """
                    SELECT
                      CASE
                        WHEN m.direction = 'in' THEN m.ig_sender_id
                        ELSE m.ig_recipient_id
                      END AS ig_user_id
                    FROM message m
                    WHERE m.conversation_id=:cid
                    ORDER BY COALESCE(m.timestamp_ms,0) DESC, m.id DESC
                    LIMIT 1
                    """
                ).params(cid=str(convo_id))
            ).first()
            ig_user_id = rowu.ig_user_id if rowu and hasattr(rowu, "ig_user_id") else (rowu[0] if rowu else None)
        except Exception:
            ig_user_id = None
        if ig_user_id:
            try:
                session.exec(
                    text(
                        "UPDATE ig_users SET ai_status=:st WHERE ig_user_id=:uid"
                    ).params(st=status, uid=str(ig_user_id))
                )
            except Exception:
                pass
    return {"status": "ok"}


@router.get("/settings")
def ai_settings_page(request: Request):
    """Global AI reply settings page."""
    from ..models import SystemSetting
    
    status_msg = request.query_params.get("msg")
    log.debug("Rendering AI settings page msg=%s", status_msg)
    model_whitelist = get_model_whitelist()
    model_groups = group_model_names(model_whitelist)
    
    shadow_temperature = get_shadow_temperature_setting()

    with get_session() as session:
        # Get global AI reply sending enabled setting
        setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_reply_sending_enabled_global")
        ).first()
        
        global_enabled = False  # Default to disabled for safety
        if setting:
            try:
                global_enabled = setting.value.lower() in ("true", "1", "yes")
            except Exception:
                global_enabled = False
        
        # Get AI model setting
        model_setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_model")
        ).first()
        
        ai_model = "gpt-4o-mini"  # Default model
        if model_setting:
            ai_model = model_setting.value
        ai_model = normalize_model_choice(ai_model, log_prefix="AI settings page model")
        
        # Get AI shadow model setting
        shadow_model_setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_shadow_model")
        ).first()
        
        ai_shadow_model = os.getenv("AI_SHADOW_MODEL") or ai_model
        if shadow_model_setting and shadow_model_setting.value:
            ai_shadow_model = shadow_model_setting.value
        ai_shadow_model = normalize_model_choice(ai_shadow_model, default=ai_model, log_prefix="AI settings page shadow")

        temp_opt_out_setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_shadow_temperature_opt_out")
        ).first()
        ai_shadow_temp_opt_out = False
        if temp_opt_out_setting and temp_opt_out_setting.value:
            ai_shadow_temp_opt_out = str(temp_opt_out_setting.value).strip().lower() in ("1","true","yes","on")
        
        # Get intro_only_mode setting
        intro_only_setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_reply_intro_only_mode")
        ).first()
        intro_only_mode = False
        if intro_only_setting and intro_only_setting.value:
            intro_only_mode = str(intro_only_setting.value).strip().lower() in ("1","true","yes","on")

        # Get shadow scope setting
        shadow_scope_setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_shadow_scope")
        ).first()
        ai_shadow_scope = "all"
        if shadow_scope_setting and shadow_scope_setting.value:
            candidate = str(shadow_scope_setting.value).strip().lower()
            if candidate in ("all", "linked_only", "off"):
                ai_shadow_scope = candidate
    
    templates = request.app.state.templates
    return templates.TemplateResponse(
        "ig_ai_settings.html",
        {
            "request": request,
            "ai_reply_sending_enabled": global_enabled,
            "ai_model": ai_model,
            "ai_shadow_model": ai_shadow_model,
            "ai_shadow_temperature": shadow_temperature,
            "ai_shadow_temp_opt_out": ai_shadow_temp_opt_out,
            "ai_reply_intro_only_mode": intro_only_mode,
            "ai_shadow_scope": ai_shadow_scope,
            "model_groups": model_groups,
            "model_refresh_msg": status_msg,
        },
    )


@router.post("/settings/save")
def save_ai_settings(
    ai_reply_sending_enabled: str = Form(default="false"),
    ai_model: str = Form(default="gpt-4o-mini"),
    ai_shadow_model: str = Form(default="gpt-4o-mini"),
    ai_shadow_temperature: str = Form(default="0.1"),
    ai_shadow_temp_opt_out: str = Form(default="0"),
    ai_reply_intro_only_mode: str = Form(default="false"),
    ai_shadow_scope: str = Form(default="all"),
):
    """Save global AI reply settings."""
    from ..models import SystemSetting
    
    enabled = ai_reply_sending_enabled.lower() in ("true", "1", "yes", "on")
    raw_model = ai_model
    raw_shadow = ai_shadow_model
    
    ai_model = normalize_model_choice(ai_model, log_prefix="AI settings save model")
    ai_shadow_model = normalize_model_choice(ai_shadow_model, default=ai_model, log_prefix="AI settings save shadow")

    def _normalize_temp(raw: str, fallback: float = 0.1) -> float:
        try:
            value = float(raw)
            if not math.isfinite(value):
                raise ValueError("non-finite")
        except Exception:
            value = fallback
        return max(0.0, min(value, 2.0))

    shadow_temperature = _normalize_temp(ai_shadow_temperature)
    temp_opt_out = ai_shadow_temp_opt_out.lower() in ("true", "1", "yes", "on")
    intro_only_mode = ai_reply_intro_only_mode.lower() in ("true", "1", "yes", "on")
    shadow_scope = (ai_shadow_scope or "all").strip().lower()
    if shadow_scope not in ("all", "linked_only", "off"):
        shadow_scope = "all"

    log.info(
        "Saving AI settings enabled=%s intro_only_mode=%s raw_model=%s raw_shadow=%s normalized_model=%s normalized_shadow=%s temp=%.2f",
        enabled,
        intro_only_mode,
        raw_model,
        raw_shadow,
        ai_model,
        ai_shadow_model,
        shadow_temperature,
    )
    
    with get_session() as session:
        # Save global AI reply sending enabled setting
        setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_reply_sending_enabled_global")
        ).first()
        
        if setting:
            setting.value = "true" if enabled else "false"
            setting.updated_at = dt.datetime.utcnow()
            session.add(setting)
        else:
            setting = SystemSetting(
                key="ai_reply_sending_enabled_global",
                value="true" if enabled else "false",
                description="Global toggle for AI reply sending (shadow replies always run)",
            )
            session.add(setting)
        
        # Save AI model setting
        model_setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_model")
        ).first()
        
        if model_setting:
            model_setting.value = ai_model
            model_setting.updated_at = dt.datetime.utcnow()
            session.add(model_setting)
        else:
            model_setting = SystemSetting(
                key="ai_model",
                value=ai_model,
                description="AI model to use for generating replies (shadow replies use separate model)",
            )
            session.add(model_setting)
        
        # Ensure selected shadow model exists in whitelist; otherwise fall back to main model
        whitelist = set(get_model_whitelist() or [])
        if whitelist and ai_shadow_model not in whitelist:
            log.warning(
                "AI settings save shadow model %s not in whitelist; falling back to %s",
                ai_shadow_model,
                ai_model,
            )
            ai_shadow_model = ai_model
        
        # Save AI shadow model setting
        shadow_model_setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_shadow_model")
        ).first()
        
        if shadow_model_setting:
            shadow_model_setting.value = ai_shadow_model
            shadow_model_setting.updated_at = dt.datetime.utcnow()
            session.add(shadow_model_setting)
        else:
            shadow_model_setting = SystemSetting(
                key="ai_shadow_model",
                value=ai_shadow_model,
                description="AI model to use for generating shadow replies (test/draft replies)",
            )
            session.add(shadow_model_setting)

        # Save temperature
        temp_setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_shadow_temperature")
        ).first()

        if temp_setting:
            temp_setting.value = f"{shadow_temperature:.3f}"
            temp_setting.updated_at = dt.datetime.utcnow()
            session.add(temp_setting)
        else:
            temp_setting = SystemSetting(
                key="ai_shadow_temperature",
                value=f"{shadow_temperature:.3f}",
                description="Temperature parameter for AI shadow replies (0-2)",
            )
            session.add(temp_setting)

        # Save temperature opt-out flag
        temp_opt_setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_shadow_temperature_opt_out")
        ).first()

        opt_value = "true" if temp_opt_out else "false"
        if temp_opt_setting:
            temp_opt_setting.value = opt_value
            temp_opt_setting.updated_at = dt.datetime.utcnow()
            session.add(temp_opt_setting)
        else:
            temp_opt_setting = SystemSetting(
                key="ai_shadow_temperature_opt_out",
                value=opt_value,
                description="If true, do not send temperature param (use model default)",
            )
            session.add(temp_opt_setting)
        
        # Save intro_only_mode setting
        intro_only_setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_reply_intro_only_mode")
        ).first()

        intro_value = "true" if intro_only_mode else "false"
        if intro_only_setting:
            intro_only_setting.value = intro_value
            intro_only_setting.updated_at = dt.datetime.utcnow()
            session.add(intro_only_setting)
        else:
            intro_only_setting = SystemSetting(
                key="ai_reply_intro_only_mode",
                value=intro_value,
                description="If true, only send first intro messages (with images), subsequent messages will be shadow replies only",
            )
            session.add(intro_only_setting)

        # Save shadow scope setting
        shadow_scope_setting = session.exec(
            select(SystemSetting).where(SystemSetting.key == "ai_shadow_scope")
        ).first()
        if shadow_scope_setting:
            shadow_scope_setting.value = shadow_scope
            shadow_scope_setting.updated_at = dt.datetime.utcnow()
            session.add(shadow_scope_setting)
        else:
            session.add(
                SystemSetting(
                    key="ai_shadow_scope",
                    value=shadow_scope,
                    description="Scope for shadow replies: all|linked_only|off",
                )
            )
        
        session.commit()
    
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/ig/ai/settings", status_code=303)


@router.post("/settings/refresh-models")
def refresh_ai_models():
    """Sync the model whitelist with OpenAI /models endpoint."""
    try:
        summary = refresh_openai_model_whitelist()
        msg = f"Model listesi güncellendi. +{len(summary['added'])} / -{len(summary['removed'])} model"
    except Exception as exc:
        msg = f"Model listesi güncellenemedi: {exc}"
    from fastapi.responses import RedirectResponse

    return RedirectResponse(url=f"/ig/ai/settings?msg={_quote(msg)}", status_code=303)


