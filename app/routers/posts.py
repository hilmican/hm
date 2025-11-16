from fastapi import APIRouter, Request, HTTPException, Form
from fastapi.responses import RedirectResponse
from starlette.status import HTTP_303_SEE_OTHER
from typing import Any, Optional, List, Dict
from sqlalchemy import text as _text
import json
import logging

from ..db import get_session
from ..models import Message, Product
from sqlmodel import select
from ..utils.slugify import slugify

router = APIRouter(prefix="/posts", tags=["posts"])
_log = logging.getLogger("posts")


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
            
            # Check if linked to product
            try:
                linked = session.exec(
                    _text("SELECT post_id FROM posts_products WHERE post_id=:pid").bindparams(pid=media_id)
                ).first()
                if linked:
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
            })
        
        templates = request.app.state.templates
        return templates.TemplateResponse(
            "posts_unlinked.html",
            {
                "request": request,
                "messages": unlinked_messages,
            },
        )


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
            # Fallback for SQLite
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
        
        # Link post to product
        try:
            stmt_link = _text("""
                INSERT INTO posts_products(post_id, product_id, sku)
                VALUES (:pid, :prod_id, NULL)
                ON DUPLICATE KEY UPDATE product_id=VALUES(product_id)
            """).bindparams(
                pid=str(post_id),
                prod_id=int(product_id),
            )
            session.exec(stmt_link)
        except Exception:
            # Fallback for SQLite
            try:
                stmt_sel = _text("SELECT post_id FROM posts_products WHERE post_id=:pid").bindparams(pid=str(post_id))
                existing = session.exec(stmt_sel).first()
                if existing:
                    stmt_update = _text("""
                        UPDATE posts_products SET product_id=:prod_id WHERE post_id=:pid
                    """).bindparams(pid=str(post_id), prod_id=int(product_id))
                    session.exec(stmt_update)
                else:
                    stmt_insert = _text("""
                        INSERT INTO posts_products(post_id, product_id, sku)
                        VALUES (:pid, :prod_id, NULL)
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
        
        # AI suggestion
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
            suggestion = ai.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
        except Exception as e:
            _log.error("AI suggestion failed: %s", e)
            raise HTTPException(status_code=502, detail=f"AI suggestion failed: {e}")
        
        product_id = suggestion.get("suggested_product_id")
        product_name = suggestion.get("suggested_product_name")
        
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
            # SQLite fallback
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
        
        # Link to product
        try:
            stmt_link = _text("""
                INSERT INTO posts_products(post_id, product_id, sku)
                VALUES (:pid, :prod_id, NULL)
                ON DUPLICATE KEY UPDATE product_id=VALUES(product_id)
            """).bindparams(
                pid=str(post_id),
                prod_id=int(product_id),
            )
            session.exec(stmt_link)
        except Exception:
            # SQLite fallback
            stmt_sel = _text("SELECT post_id FROM posts_products WHERE post_id=:pid").bindparams(pid=str(post_id))
            existing = session.exec(stmt_sel).first()
            if existing:
                stmt_update = _text("""
                    UPDATE posts_products SET product_id=:prod_id WHERE post_id=:pid
                """).bindparams(pid=str(post_id), prod_id=int(product_id))
                session.exec(stmt_update)
            else:
                stmt_insert = _text("""
                    INSERT INTO posts_products(post_id, product_id, sku)
                    VALUES (:pid, :prod_id, NULL)
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
                # AI suggestion
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
                
                suggestion = ai.generate_json(system_prompt=system_prompt, user_prompt=user_prompt)
                
                product_id = suggestion.get("suggested_product_id")
                product_name = suggestion.get("suggested_product_name")
                
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
                    # SQLite fallback
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
                
                # Link to product
                try:
                    stmt_link = _text("""
                        INSERT INTO posts_products(post_id, product_id, sku)
                        VALUES (:pid, :prod_id, NULL)
                        ON DUPLICATE KEY UPDATE product_id=VALUES(product_id)
                    """).bindparams(
                        pid=str(post_id),
                        prod_id=int(product_id),
                    )
                    session.exec(stmt_link)
                except Exception:
                    # SQLite fallback
                    stmt_sel = _text("SELECT post_id FROM posts_products WHERE post_id=:pid").bindparams(pid=str(post_id))
                    existing_link = session.exec(stmt_sel).first()
                    if existing_link:
                        stmt_update = _text("""
                            UPDATE posts_products SET product_id=:prod_id WHERE post_id=:pid
                        """).bindparams(pid=str(post_id), prod_id=int(product_id))
                        session.exec(stmt_update)
                    else:
                        stmt_insert = _text("""
                            INSERT INTO posts_products(post_id, product_id, sku)
                            VALUES (:pid, :prod_id, NULL)
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

