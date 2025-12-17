from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import RedirectResponse, JSONResponse
from starlette.status import HTTP_303_SEE_OTHER
from typing import Any, Optional, List, Dict
from sqlalchemy import text as _text
import json
import logging

from ..db import get_session
from ..models import Message, Product
from sqlmodel import select
from ..utils.slugify import slugify
from ..services.prompts import AD_PRODUCT_MATCH_SYSTEM_PROMPT

router = APIRouter(prefix="/posts", tags=["posts"])
_log = logging.getLogger("posts")


@router.get("/auto-linked")
def list_auto_linked_posts(request: Request, limit: int = 100):
	"""List posts that were automatically linked by AI (for review and correction)."""
	posts: list[dict[str, Any]] = []
	with get_session() as session:
		try:
			rows = session.exec(
				_text(
					"""
					SELECT pp.post_id, pp.product_id, pp.auto_linked, pp.created_at,
					       pst.title, pst.url, pst.ig_post_media_id, pst.message_id,
					       pr.name AS product_name
					FROM posts_products pp
					LEFT JOIN posts pst ON pst.post_id = pp.post_id
					LEFT JOIN product pr ON pr.id = pp.product_id
					WHERE pp.auto_linked = 1
					ORDER BY pp.created_at DESC
					LIMIT :lim
					"""
				).bindparams(lim=int(limit))
			).all()
		except Exception:
			rows = []
		
		for r in rows:
			try:
				post_id = getattr(r, "post_id", None) if hasattr(r, "post_id") else (r[0] if len(r) > 0 else None)
				product_id = getattr(r, "product_id", None) if hasattr(r, "product_id") else (r[1] if len(r) > 1 else None)
				auto_linked = bool(getattr(r, "auto_linked", None) if hasattr(r, "auto_linked") else (r[2] if len(r) > 2 else False))
				created_at = getattr(r, "created_at", None) if hasattr(r, "created_at") else (r[3] if len(r) > 3 else None)
				title = getattr(r, "title", None) if hasattr(r, "title") else (r[4] if len(r) > 4 else None)
				url = getattr(r, "url", None) if hasattr(r, "url") else (r[5] if len(r) > 5 else None)
				ig_post_media_id = getattr(r, "ig_post_media_id", None) if hasattr(r, "ig_post_media_id") else (r[6] if len(r) > 6 else None)
				message_id = getattr(r, "message_id", None) if hasattr(r, "message_id") else (r[7] if len(r) > 7 else None)
				product_name = getattr(r, "product_name", None) if hasattr(r, "product_name") else (r[8] if len(r) > 8 else None)
				
				if post_id:
					posts.append({
						"post_id": str(post_id),
						"product_id": int(product_id) if product_id is not None else None,
						"product_name": product_name,
						"title": title,
						"url": url,
						"ig_post_media_id": ig_post_media_id,
						"message_id": message_id,
						"created_at": created_at,
						"auto_linked": auto_linked,
					})
			except Exception:
				continue
	
	templates = request.app.state.templates
	return templates.TemplateResponse(
		"posts_auto_linked.html",
		{
			"request": request,
			"posts": posts,
			"count": len(posts),
		},
	)


def _extract_post_info_from_message(msg: Message) -> Optional[Dict[str, Any]]:
    """Extract Instagram post information from a message's attachments_json."""
    if not msg or not msg.attachments_json:
        return None
    
    try:
        atts = json.loads(msg.attachments_json)
        if not isinstance(atts, list):
            return None
        
        for att in atts:
            if att.get("type") in ("ig_post", "share"):
                payload = att.get("payload", {})
                media_id = payload.get("ig_post_media_id")
                if media_id:
                    return {
                        "ig_post_media_id": str(media_id),
                        "title": payload.get("title"),
                        "url": payload.get("url"),
                    }
    except Exception as e:
        _log.debug("Error extracting post info: %s", e)
    
    return None


@router.get("/unlinked")
def list_unlinked_posts(request: Request, limit: int = 100):
    """List messages with Instagram posts that are not linked to products."""
    with get_session() as session:
        # Get all messages with attachments
        messages_query = select(Message).where(
            Message.attachments_json.isnot(None)
        ).order_by(Message.timestamp_ms.desc()).limit(limit * 2)  # Get more to filter
        
        all_messages = session.exec(messages_query).all()
        
        # Get all existing post IDs
        try:
            existing_posts = session.exec(_text("SELECT ig_post_media_id FROM posts WHERE ig_post_media_id IS NOT NULL")).all()
            existing_media_ids = {str(row[0] if isinstance(row, tuple) else getattr(row, "ig_post_media_id", None)) for row in existing_posts if row}
        except Exception:
            existing_media_ids = set()
        
        unlinked_messages = []
        seen_media_ids = set()
        
        for msg in all_messages:
            if len(unlinked_messages) >= limit:
                break
            
            post_info = _extract_post_info_from_message(msg)
            if not post_info:
                continue
            
            media_id = post_info["ig_post_media_id"]
            
            # Skip if we've already seen this media_id or it's already linked
            if media_id in seen_media_ids or media_id in existing_media_ids:
                continue
            
            seen_media_ids.add(media_id)
            
            # Check if linked to product and get auto_linked status
            auto_linked = False
            try:
                linked_row = session.exec(
                    _text("SELECT post_id, auto_linked FROM posts_products WHERE post_id=:pid").bindparams(pid=media_id)
                ).first()
                if linked_row:
                    # Extract auto_linked value
                    auto_linked = bool(
                        getattr(linked_row, "auto_linked", None) if hasattr(linked_row, "auto_linked") 
                        else (linked_row[1] if len(linked_row) > 1 else False)
                    )
                    # Only show in unlinked list if we want to show all (including linked for review)
                    # For now, skip if already linked
                    continue
            except Exception:
                pass
            
            unlinked_messages.append({
                "message_id": msg.id,
                "ig_message_id": msg.ig_message_id,
                "text": msg.text,
                "post_info": post_info,
                "timestamp_ms": msg.timestamp_ms,
                "conversation_id": msg.conversation_id,
                "auto_linked": auto_linked,
            })
        
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "posts_unlinked.html",
            {
                "request": request,
                "messages": unlinked_messages,
            },
        )


@router.post("/unlink/{post_id}")
def unlink_post(post_id: str):
	"""Remove product link from a post (sets auto_linked=0 and clears product_id)."""
	with get_session() as session:
		try:
			stmt_update = _text("""
				UPDATE posts_products 
				SET product_id=NULL, auto_linked=0 
				WHERE post_id=:pid
			""").bindparams(pid=str(post_id))
			session.exec(stmt_update)
			session.commit()
			return JSONResponse({"status": "ok", "post_id": post_id})
		except Exception as e:
			_log.error("Error unlinking post: %s", e)
			raise HTTPException(status_code=500, detail="Failed to unlink post")


@router.post("/link/{message_id}")
def link_post_to_product(message_id: int, product_id: Optional[int] = Form(default=None)):
    """Link an Instagram post from a message to a product."""
    if not product_id:
        raise HTTPException(status_code=400, detail="product_id required")
    
    with get_session() as session:
        # Get message
        msg = session.exec(select(Message).where(Message.id == message_id)).first()
        if not msg:
            raise HTTPException(status_code=404, detail="Message not found")
        
        post_info = _extract_post_info_from_message(msg)
        if not post_info or not post_info.get("ig_post_media_id"):
            raise HTTPException(status_code=400, detail="No Instagram post found in message")
        
        post_id = post_info["ig_post_media_id"]
        
        # Create or update post record
        try:
            stmt_upsert = _text("""
                INSERT INTO posts(post_id, ig_post_media_id, title, url, message_id, updated_at)
                VALUES (:pid, :media_id, :title, :url, :msg_id, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                    title=VALUES(title),
                    url=VALUES(url),
                    message_id=VALUES(message_id),
                    updated_at=CURRENT_TIMESTAMP
            """).bindparams(
                pid=str(post_id),
                media_id=str(post_info["ig_post_media_id"]),
                title=post_info.get("title"),
                url=post_info.get("url"),
                msg_id=int(message_id),
            )
            session.exec(stmt_upsert)
        except Exception:
            # Backend fallback
            try:
                stmt_sel = _text("SELECT post_id FROM posts WHERE post_id=:pid").bindparams(pid=str(post_id))
                existing = session.exec(stmt_sel).first()
                if existing:
                    stmt_update = _text("""
                        UPDATE posts SET title=:title, url=:url, message_id=:msg_id, updated_at=CURRENT_TIMESTAMP
                        WHERE post_id=:pid
                    """).bindparams(
                        pid=str(post_id),
                        title=post_info.get("title"),
                        url=post_info.get("url"),
                        msg_id=int(message_id),
                    )
                    session.exec(stmt_update)
                else:
                    stmt_insert = _text("""
                        INSERT INTO posts(post_id, ig_post_media_id, title, url, message_id)
                        VALUES (:pid, :media_id, :title, :url, :msg_id)
                    """).bindparams(
                        pid=str(post_id),
                        media_id=str(post_info["ig_post_media_id"]),
                        title=post_info.get("title"),
                        url=post_info.get("url"),
                        msg_id=int(message_id),
                    )
                    session.exec(stmt_insert)
            except Exception as e:
                _log.error("Error upserting post: %s", e)
                raise HTTPException(status_code=500, detail="Failed to create post record")
        
        # Link post to product (manual save clears auto_linked flag)
        try:
            stmt_link = _text("""
                INSERT INTO posts_products(post_id, product_id, sku, auto_linked)
                VALUES (:pid, :prod_id, NULL, 0)
                ON DUPLICATE KEY UPDATE product_id=VALUES(product_id), auto_linked=0
            """).bindparams(
                pid=str(post_id),
                prod_id=int(product_id),
            )
            session.exec(stmt_link)
        except Exception:
            # Backend fallback
            try:
                stmt_sel = _text("SELECT post_id FROM posts_products WHERE post_id=:pid").bindparams(pid=str(post_id))
                existing = session.exec(stmt_sel).first()
                if existing:
                    stmt_update = _text("""
                        UPDATE posts_products SET product_id=:prod_id, auto_linked=0 WHERE post_id=:pid
                    """).bindparams(pid=str(post_id), prod_id=int(product_id))
                    session.exec(stmt_update)
                else:
                    stmt_insert = _text("""
                        INSERT INTO posts_products(post_id, product_id, sku, auto_linked)
                        VALUES (:pid, :prod_id, NULL, 0)
                    """).bindparams(pid=str(post_id), prod_id=int(product_id))
                    session.exec(stmt_insert)
            except Exception as e:
                _log.error("Error linking post to product: %s", e)
                raise HTTPException(status_code=500, detail="Failed to link post to product")
        
        session.commit()
    
    return RedirectResponse(url="/posts/unlinked", status_code=HTTP_303_SEE_OTHER)


@router.post("/ai/suggest/{message_id}")
def ai_suggest_product(message_id: int, request: Request):
    """Use AI to suggest which product an Instagram post could be."""
    ai = getattr(request.app.state, "ai", None)
    if not ai or not getattr(ai, "enabled", False):
        raise HTTPException(status_code=503, detail="AI not configured")
    
    with get_session() as session:
        # Get message
        msg = session.exec(select(Message).where(Message.id == message_id)).first()
        if not msg:
            raise HTTPException(status_code=404, detail="Message not found")
        
        post_info = _extract_post_info_from_message(msg)
        if not post_info:
            raise HTTPException(status_code=400, detail="No Instagram post found in message")
        
        # Get list of existing products for context
        products = session.exec(select(Product).limit(500)).all()
        product_list = [{"id": p.id, "name": p.name, "slug": p.slug} for p in products]
        
        # Build AI prompt
        system_prompt = """Sen bir Instagram gönderisi analiz uzmanısın. 
Bir Instagram gönderisinin başlığını ve açıklamasını analiz ederek, bu gönderinin hangi ürünü tanıttığını belirlemen gerekiyor.

Mevcut ürün listesini incele ve gönderinin içeriğine göre en uygun ürünü öner. Eğer hiçbiri uygun değilse, yeni bir ürün adı öner.

Yanıtını JSON formatında döndür:
{
  "suggested_product_id": <ürün_id veya null>,
  "suggested_product_name": "<ürün adı veya null>",
  "confidence": <0.0-1.0 arası güven skoru>,
  "reasoning": "<neden bu ürünü seçtiğin açıklaması>"
}"""
        
        user_prompt = f"""Instagram Gönderisi:
Başlık: {post_info.get('title', 'Yok')}
Mesaj Metni: {msg.text or 'Yok'}

Mevcut Ürünler:
{json.dumps(product_list, ensure_ascii=False, indent=2)}

Bu gönderi hangi ürünü tanıtıyor? Lütfen JSON formatında yanıt ver."""
        
        try:
            result = ai.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
            return {
                "message_id": message_id,
                "post_info": post_info,
                "suggestion": result,
            }
        except Exception as e:
            _log.error("AI suggestion failed: %s", e)
            raise HTTPException(status_code=502, detail=f"AI suggestion failed: {e}")


@router.post("/ai/link/{message_id}")
def ai_link_post(message_id: int, request: Request):
    """Use AI to identify product and automatically link the post."""
    ai = getattr(request.app.state, "ai", None)
    if not ai or not getattr(ai, "enabled", False):
        raise HTTPException(status_code=503, detail="AI not configured")
    
    with get_session() as session:
        # Get message
        msg = session.exec(select(Message).where(Message.id == message_id)).first()
        if not msg:
            raise HTTPException(status_code=404, detail="Message not found")
        
        post_info = _extract_post_info_from_message(msg)
        if not post_info or not post_info.get("ig_post_media_id"):
            raise HTTPException(status_code=400, detail="No Instagram post found in message")
        
        # Get products for context
        products = session.exec(select(Product).limit(500)).all()
        product_list = [{"id": p.id, "name": p.name, "slug": p.slug} for p in products]
        
        # Use the same prompt system as ads for consistency
        system_prompt = AD_PRODUCT_MATCH_SYSTEM_PROMPT
        
        # Build prompt with post title and message text
        post_text = f"{post_info.get('title', '')} {msg.text or ''}".strip()
        
        body = {
            "ad_title": post_text,  # Reuse the same structure
            "known_products": [{"id": p.id, "name": p.name} for p in products],
            "schema": {
                "product_id": "int|null",
                "product_name": "str|null",
                "confidence": "float",
                "notes": "str|null",
            },
        }
        
        user_prompt = (
            "Lütfen SADECE geçerli JSON döndür. Markdown/kod bloğu/yorum ekleme. "
            "Tüm alanlar çift tırnaklı olmalı.\nGirdi:\n" + json.dumps(body, ensure_ascii=False)
        )
        
        try:
            suggestion = ai.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
        except Exception as e:
            _log.error("AI suggestion failed: %s", e)
            raise HTTPException(status_code=502, detail=f"AI suggestion failed: {e}")
        
        # Map response to expected format (using same schema as ads)
        product_id = suggestion.get("product_id") or suggestion.get("suggested_product_id")
        product_name = suggestion.get("product_name") or suggestion.get("suggested_product_name")
        
        # Create product if needed
        if not product_id and product_name:
            slug = slugify(product_name)
            existing = session.exec(select(Product).where(Product.slug == slug)).first()
            if existing:
                product_id = existing.id
            else:
                new_product = Product(
                    name=product_name,
                    slug=slug,
                    default_unit="adet",
                    default_price=None,
                )
                session.add(new_product)
                session.flush()
                if new_product.id:
                    product_id = new_product.id
        
        if not product_id:
            raise HTTPException(status_code=400, detail="Could not determine or create product")
        
        # Create post and link
        post_id = post_info["ig_post_media_id"]
        
        # Create post record
        try:
            stmt_upsert = _text("""
                INSERT INTO posts(post_id, ig_post_media_id, title, url, message_id, updated_at)
                VALUES (:pid, :media_id, :title, :url, :msg_id, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                    title=VALUES(title),
                    url=VALUES(url),
                    message_id=VALUES(message_id),
                    updated_at=CURRENT_TIMESTAMP
            """).bindparams(
                pid=str(post_id),
                media_id=str(post_info["ig_post_media_id"]),
                title=post_info.get("title"),
                url=post_info.get("url"),
                msg_id=int(message_id),
            )
            session.exec(stmt_upsert)
        except Exception:
            # Backend fallback
            stmt_sel = _text("SELECT post_id FROM posts WHERE post_id=:pid").bindparams(pid=str(post_id))
            existing = session.exec(stmt_sel).first()
            if existing:
                stmt_update = _text("""
                    UPDATE posts SET title=:title, url=:url, message_id=:msg_id, updated_at=CURRENT_TIMESTAMP
                    WHERE post_id=:pid
                """).bindparams(
                    pid=str(post_id),
                    title=post_info.get("title"),
                    url=post_info.get("url"),
                    msg_id=int(message_id),
                )
                session.exec(stmt_update)
            else:
                stmt_insert = _text("""
                    INSERT INTO posts(post_id, ig_post_media_id, title, url, message_id)
                    VALUES (:pid, :media_id, :title, :url, :msg_id)
                """).bindparams(
                    pid=str(post_id),
                    media_id=str(post_info["ig_post_media_id"]),
                    title=post_info.get("title"),
                    url=post_info.get("url"),
                    msg_id=int(message_id),
                )
                session.exec(stmt_insert)
        
        # Link to product (AI link keeps auto_linked=1)
        try:
            stmt_link = _text("""
                INSERT INTO posts_products(post_id, product_id, sku, auto_linked)
                VALUES (:pid, :prod_id, NULL, 1)
                ON DUPLICATE KEY UPDATE product_id=VALUES(product_id), auto_linked=1
            """).bindparams(
                pid=str(post_id),
                prod_id=int(product_id),
            )
            session.exec(stmt_link)
        except Exception:
            # Backend fallback
            stmt_sel = _text("SELECT post_id FROM posts_products WHERE post_id=:pid").bindparams(pid=str(post_id))
            existing = session.exec(stmt_sel).first()
            if existing:
                stmt_update = _text("""
                    UPDATE posts_products SET product_id=:prod_id, auto_linked=1 WHERE post_id=:pid
                """).bindparams(pid=str(post_id), prod_id=int(product_id))
                session.exec(stmt_update)
            else:
                stmt_insert = _text("""
                    INSERT INTO posts_products(post_id, product_id, sku, auto_linked)
                    VALUES (:pid, :prod_id, NULL, 1)
                """).bindparams(pid=str(post_id), prod_id=int(product_id))
                session.exec(stmt_insert)
        
        session.commit()
        
        return {
            "status": "ok",
            "post_id": post_id,
            "product_id": product_id,
            "product_name": product_name or "Unknown",
            "suggestion": suggestion,
        }


@router.post("/ai/batch-link")
def batch_ai_link(request: Request, limit: int = 50):
    """Process multiple unlinked posts and link them to products using AI."""
    ai = getattr(request.app.state, "ai", None)
    if not ai or not getattr(ai, "enabled", False):
        raise HTTPException(status_code=503, detail="AI not configured")
    
    results = []
    errors = []
    
    with get_session() as session:
        # Get unlinked messages (similar to list_unlinked_posts)
        messages_query = select(Message).where(
            Message.attachments_json.isnot(None)
        ).order_by(Message.timestamp_ms.desc()).limit(limit * 2)
        
        all_messages = session.exec(messages_query).all()
        
        # Get existing posts
        try:
            existing_posts = session.exec(_text("SELECT ig_post_media_id FROM posts WHERE ig_post_media_id IS NOT NULL")).all()
            existing_media_ids = {str(row[0] if isinstance(row, tuple) else getattr(row, "ig_post_media_id", None)) for row in existing_posts if row}
        except Exception:
            existing_media_ids = set()
        
        # Get products for context (once)
        products = session.exec(select(Product).limit(500)).all()
        product_list = [{"id": p.id, "name": p.name, "slug": p.slug} for p in products]
        
        processed = 0
        seen_media_ids = set()
        
        for msg in all_messages:
            if processed >= limit:
                break
            
            post_info = _extract_post_info_from_message(msg)
            if not post_info:
                continue
            
            media_id = post_info["ig_post_media_id"]
            
            # Skip if already processed or linked
            if media_id in seen_media_ids or media_id in existing_media_ids:
                continue
            
            # Check if already linked to product
            try:
                linked = session.exec(
                    _text("SELECT post_id FROM posts_products WHERE post_id=:pid").bindparams(pid=media_id)
                ).first()
                if linked:
                    continue
            except Exception:
                pass
            
            seen_media_ids.add(media_id)
            
            try:
                # Use the same prompt system as ads for consistency
                system_prompt = AD_PRODUCT_MATCH_SYSTEM_PROMPT
                
                # Build prompt with post title and message text
                post_text = f"{post_info.get('title', '')} {msg.text or ''}".strip()
                
                body = {
                    "ad_title": post_text,  # Reuse the same structure
                    "known_products": [{"id": p.id, "name": p.name} for p in products],
                    "schema": {
                        "product_id": "int|null",
                        "product_name": "str|null",
                        "confidence": "float",
                        "notes": "str|null",
                    },
                }
                
                user_prompt = (
                    "Lütfen SADECE geçerli JSON döndür. Markdown/kod bloğu/yorum ekleme. "
                    "Tüm alanlar çift tırnaklı olmalı.\nGirdi:\n" + json.dumps(body, ensure_ascii=False)
                )
                
                suggestion = ai.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
                
                # Map response to expected format (using same schema as ads)
                product_id = suggestion.get("product_id") or suggestion.get("suggested_product_id")
                product_name = suggestion.get("product_name") or suggestion.get("suggested_product_name")
                
                # Create product if needed
                if not product_id and product_name:
                    slug = slugify(product_name)
                    existing = session.exec(select(Product).where(Product.slug == slug)).first()
                    if existing:
                        product_id = existing.id
                    else:
                        new_product = Product(
                            name=product_name,
                            slug=slug,
                            default_unit="adet",
                            default_price=None,
                        )
                        session.add(new_product)
                        session.flush()
                        if new_product.id:
                            product_id = new_product.id
                
                if not product_id:
                    errors.append({
                        "message_id": msg.id,
                        "error": "Could not determine or create product",
                        "suggestion": suggestion,
                    })
                    continue
                
                # Create post and link (same logic as ai_link_post)
                post_id = post_info["ig_post_media_id"]
                
                # Create post record
                try:
                    stmt_upsert = _text("""
                        INSERT INTO posts(post_id, ig_post_media_id, title, url, message_id, updated_at)
                        VALUES (:pid, :media_id, :title, :url, :msg_id, CURRENT_TIMESTAMP)
                        ON DUPLICATE KEY UPDATE
                            title=VALUES(title),
                            url=VALUES(url),
                            message_id=VALUES(message_id),
                            updated_at=CURRENT_TIMESTAMP
                    """).bindparams(
                        pid=str(post_id),
                        media_id=str(post_info["ig_post_media_id"]),
                        title=post_info.get("title"),
                        url=post_info.get("url"),
                        msg_id=int(msg.id or 0),
                    )
                    session.exec(stmt_upsert)
                except Exception:
                    # Backend fallback
                    stmt_sel = _text("SELECT post_id FROM posts WHERE post_id=:pid").bindparams(pid=str(post_id))
                    existing_post = session.exec(stmt_sel).first()
                    if existing_post:
                        stmt_update = _text("""
                            UPDATE posts SET title=:title, url=:url, message_id=:msg_id, updated_at=CURRENT_TIMESTAMP
                            WHERE post_id=:pid
                        """).bindparams(
                            pid=str(post_id),
                            title=post_info.get("title"),
                            url=post_info.get("url"),
                            msg_id=int(msg.id or 0),
                        )
                        session.exec(stmt_update)
                    else:
                        stmt_insert = _text("""
                            INSERT INTO posts(post_id, ig_post_media_id, title, url, message_id)
                            VALUES (:pid, :media_id, :title, :url, :msg_id)
                        """).bindparams(
                            pid=str(post_id),
                            media_id=str(post_info["ig_post_media_id"]),
                            title=post_info.get("title"),
                            url=post_info.get("url"),
                            msg_id=int(msg.id or 0),
                        )
                        session.exec(stmt_insert)
                
                # Link to product (batch AI link - mark as auto_linked)
                try:
                    stmt_link = _text("""
                        INSERT INTO posts_products(post_id, product_id, sku, auto_linked)
                        VALUES (:pid, :prod_id, NULL, 1)
                        ON DUPLICATE KEY UPDATE product_id=VALUES(product_id), auto_linked=1
                    """).bindparams(
                        pid=str(post_id),
                        prod_id=int(product_id),
                    )
                    session.exec(stmt_link)
                except Exception:
                    # Backend fallback
                    stmt_sel = _text("SELECT post_id FROM posts_products WHERE post_id=:pid").bindparams(pid=str(post_id))
                    existing_link = session.exec(stmt_sel).first()
                    if existing_link:
                        stmt_update = _text("""
                            UPDATE posts_products SET product_id=:prod_id, auto_linked=1 WHERE post_id=:pid
                        """).bindparams(pid=str(post_id), prod_id=int(product_id))
                        session.exec(stmt_update)
                    else:
                        stmt_insert = _text("""
                            INSERT INTO posts_products(post_id, product_id, sku, auto_linked)
                            VALUES (:pid, :prod_id, NULL, 1)
                        """).bindparams(pid=str(post_id), prod_id=int(product_id))
                        session.exec(stmt_insert)
                
                results.append({
                    "message_id": msg.id,
                    "post_id": post_id,
                    "product_id": product_id,
                    "product_name": product_name or "Unknown",
                    "confidence": suggestion.get("confidence"),
                })
                processed += 1
                
            except Exception as e:
                _log.error("Error processing message %s: %s", msg.id, e)
                errors.append({
                    "message_id": msg.id,
                    "error": str(e),
                })
                continue
        
        session.commit()
    
    return {
        "status": "ok",
        "processed": processed,
        "results": results,
        "errors": errors,
    }

