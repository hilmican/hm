"""
Synchronous enrichment functions for Instagram webhook processing.
These functions call Instagram Graph API immediately when data is missing.
"""

import logging
from typing import Dict, Any, Optional, Tuple
import httpx
from sqlalchemy import text

from ..db import get_session
from ..services.instagram_api import _get_base_token_and_id, GRAPH_VERSION, _get as graph_get

_log = logging.getLogger("instagram.webhook.enrich")


async def enrich_user_if_missing(session, ig_user_id: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    """
    Check if user exists in database. If not, fetch from Graph API synchronously.
    Returns user data dict if fetched, None if already exists or failed.
    """
    try:
        # Check if user exists
        row = session.exec(text("SELECT ig_user_id, username, name, fetched_at, fetch_status FROM ig_users WHERE ig_user_id=:id LIMIT 1")).params(id=str(ig_user_id)).first()
        if row:
            username = getattr(row, "username", None) or (row[1] if len(row) > 1 else None)
            name = getattr(row, "name", None) or (row[2] if len(row) > 2 else None)
            fetched_at = getattr(row, "fetched_at", None) or (row[3] if len(row) > 3 else None)
            fetch_status = getattr(row, "fetch_status", None) or (row[4] if len(row) > 4 else None)

            # If we have username and it's been fetched successfully, consider it complete
            if username and str(fetch_status or "").lower() == "ok":
                return None

        # User doesn't exist or incomplete - fetch from API
        _log.info("webhook.enrich: fetching user %s", ig_user_id)

        token, _, _ = _get_base_token_and_id()
        base = f"https://graph.facebook.com/{GRAPH_VERSION}"

        # Try to get user info
        user_url = f"{base}/{ig_user_id}"
        params = {
            "access_token": token,
            "fields": "id,username,name,biography,followers_count,follows_count,media_count,profile_picture_url"
        }

        user_data = await graph_get(client, user_url, params)

        if user_data:
            # Update or insert user
            username = user_data.get("username")
            name = user_data.get("name")
            bio = user_data.get("biography")
            followers = user_data.get("followers_count")
            following = user_data.get("follows_count")
            media_count = user_data.get("media_count")
            profile_pic = user_data.get("profile_picture_url")

            try:
                session.exec(text("""
                    INSERT INTO ig_users(ig_user_id, username, name, biography, followers_count, follows_count, media_count, profile_picture_url, fetched_at, fetch_status)
                    VALUES (:id, :uname, :name, :bio, :followers, :following, :media_count, :profile_pic, CURRENT_TIMESTAMP, 'ok')
                    ON DUPLICATE KEY UPDATE
                      username=VALUES(username),
                      name=VALUES(name),
                      biography=VALUES(biography),
                      followers_count=VALUES(followers_count),
                      follows_count=VALUES(follows_count),
                      media_count=VALUES(media_count),
                      profile_picture_url=VALUES(profile_picture_url),
                      fetched_at=CURRENT_TIMESTAMP,
                      fetch_status='ok'
                """).params(
                    id=str(ig_user_id),
                    uname=username,
                    name=name,
                    bio=bio,
                    followers=followers,
                    following=following,
                    media_count=media_count,
                    profile_pic=profile_pic
                ))
                _log.info("webhook.enrich: user %s enriched successfully", ig_user_id)
                return user_data
            except Exception as e:
                # Fallback for SQLite
                try:
                    session.exec(text("""
                        INSERT OR IGNORE INTO ig_users(ig_user_id, username, name, biography, followers_count, follows_count, media_count, profile_picture_url, fetched_at, fetch_status)
                        VALUES (:id, :uname, :name, :bio, :followers, :following, :media_count, :profile_pic, CURRENT_TIMESTAMP, 'ok')
                    """).params(
                        id=str(ig_user_id),
                        uname=username,
                        name=name,
                        bio=bio,
                        followers=followers,
                        following=following,
                        media_count=media_count,
                        profile_pic=profile_pic
                    ))
                    # Update if insert was ignored (user exists)
                    session.exec(text("""
                        UPDATE ig_users SET
                          username=COALESCE(:uname, username),
                          name=COALESCE(:name, name),
                          biography=COALESCE(:bio, biography),
                          followers_count=COALESCE(:followers, followers_count),
                          follows_count=COALESCE(:following, follows_count),
                          media_count=COALESCE(:media_count, media_count),
                          profile_picture_url=COALESCE(:profile_pic, profile_picture_url),
                          fetched_at=CURRENT_TIMESTAMP,
                          fetch_status='ok'
                        WHERE ig_user_id=:id
                    """).params(
                        id=str(ig_user_id),
                        uname=username,
                        name=name,
                        bio=bio,
                        followers=followers,
                        following=following,
                        media_count=media_count,
                        profile_pic=profile_pic
                    ))
                    _log.info("webhook.enrich: user %s enriched successfully", ig_user_id)
                    return user_data
                except Exception as e2:
                    _log.warning("webhook.enrich: failed to save user %s: %s", ig_user_id, str(e2))
                    return None

    except Exception as e:
        _log.warning("webhook.enrich: failed to enrich user %s: %s", ig_user_id, str(e)[:200])
        return None


async def enrich_conversation_if_missing(session, igba_id: str, ig_user_id: str, client: httpx.AsyncClient) -> Optional[str]:
    """
    Check if conversation mapping exists. If not, fetch conversation info from Graph API.
    Returns the graph_conversation_id if found, None otherwise.
    """
    try:
        # Check if conversation mapping exists
        row = session.exec(text("""
            SELECT graph_conversation_id FROM conversations
            WHERE igba_id=:g AND ig_user_id=:u AND graph_conversation_id IS NOT NULL
            ORDER BY last_message_at DESC LIMIT 1
        """)).params(g=str(igba_id), u=str(ig_user_id)).first()

        if row:
            gcid = getattr(row, "graph_conversation_id", None) or (row[0] if len(row) > 0 else None)
            if gcid:
                return str(gcid)

        # No mapping found - try to get conversations from Graph API
        _log.info("webhook.enrich: fetching conversations for igba=%s user=%s", igba_id, ig_user_id)

        token, _, _ = _get_base_token_and_id()
        base = f"https://graph.facebook.com/{GRAPH_VERSION}"

        # Get conversations for the business account
        conv_url = f"{base}/{igba_id}/conversations"
        params = {
            "access_token": token,
            "fields": "id,participants,updated_time",
            "platform": "instagram"
        }

        conv_data = await graph_get(client, conv_url, params)

        if conv_data and conv_data.get("data"):
            for conv in conv_data["data"]:
                participants = conv.get("participants", {}).get("data", [])
                # Check if this conversation includes our user
                has_user = any(str(p.get("id")) == str(ig_user_id) for p in participants)
                if has_user:
                    graph_conv_id = conv.get("id")
                    if graph_conv_id:
                        # Save the mapping
                        try:
                            session.exec(text("""
                                INSERT INTO conversations(convo_id, igba_id, ig_user_id, graph_conversation_id, last_message_at)
                                VALUES (:cid, :g, :u, :gcid, CURRENT_TIMESTAMP)
                                ON DUPLICATE KEY UPDATE
                                  graph_conversation_id=VALUES(graph_conversation_id),
                                  last_message_at=CURRENT_TIMESTAMP
                            """).params(
                                cid=f"{igba_id}:{ig_user_id}",
                                g=str(igba_id),
                                u=str(ig_user_id),
                                gcid=str(graph_conv_id)
                            ))
                            _log.info("webhook.enrich: conversation mapping saved %s:%s -> %s", igba_id, ig_user_id, graph_conv_id)
                            return str(graph_conv_id)
                        except Exception as e:
                            # SQLite fallback
                            try:
                                session.exec(text("""
                                    INSERT OR IGNORE INTO conversations(convo_id, igba_id, ig_user_id, graph_conversation_id, last_message_at)
                                    VALUES (:cid, :g, :u, :gcid, CURRENT_TIMESTAMP)
                                """).params(
                                    cid=f"{igba_id}:{ig_user_id}",
                                    g=str(igba_id),
                                    u=str(ig_user_id),
                                    gcid=str(graph_conv_id)
                                ))
                                session.exec(text("""
                                    UPDATE conversations SET
                                      graph_conversation_id=:gcid,
                                      last_message_at=CURRENT_TIMESTAMP
                                    WHERE convo_id=:cid
                                """).params(
                                    cid=f"{igba_id}:{ig_user_id}",
                                    gcid=str(graph_conv_id)
                                ))
                                _log.info("webhook.enrich: conversation mapping saved %s:%s -> %s", igba_id, ig_user_id, graph_conv_id)
                                return str(graph_conv_id)
                            except Exception as e2:
                                _log.warning("webhook.enrich: failed to save conversation mapping: %s", str(e2))

    except Exception as e:
        _log.warning("webhook.enrich: failed to enrich conversation %s:%s: %s", igba_id, ig_user_id, str(e)[:200])

    return None


async def enrich_ad_if_missing(session, ad_id: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    """
    Check if ad exists in database. If not, fetch from Graph API synchronously.
    """
    try:
        # Check if ad exists
        row = session.exec(text("SELECT ad_id, name, link FROM ads WHERE ad_id=:id LIMIT 1")).params(id=str(ad_id)).first()
        if row:
            name = getattr(row, "name", None) or (row[1] if len(row) > 1 else None)
            link = getattr(row, "link", None) or (row[2] if len(row) > 2 else None)
            if name and link:  # Consider it complete if we have basic info
                return None

        # Ad doesn't exist or incomplete - fetch from API
        _log.info("webhook.enrich: fetching ad %s", ad_id)

        token, _, _ = _get_base_token_and_id()
        base = f"https://graph.facebook.com/{GRAPH_VERSION}"

        # Get ad creative info
        ad_url = f"{base}/{ad_id}"
        params = {
            "access_token": token,
            "fields": "id,name,creative{title,body,link_url,image_url,video_id,object_story_spec}"
        }

        ad_data = await graph_get(client, ad_url, params)

        if ad_data:
            name = ad_data.get("name")
            creative = ad_data.get("creative", {})

            # Extract info from creative
            title = creative.get("title")
            body = creative.get("body")
            link_url = creative.get("link_url")
            image_url = creative.get("image_url")
            video_id = creative.get("video_id")

            # Try to get more info from object_story_spec if available
            story_spec = creative.get("object_story_spec", {})
            if story_spec:
                page_id = story_spec.get("page_id")
                link_url = link_url or story_spec.get("link_data", {}).get("link")

            try:
                session.exec(text("""
                    INSERT INTO ads(ad_id, name, image_url, link, updated_at)
                    VALUES (:id, :name, :img, :link, CURRENT_TIMESTAMP)
                    ON DUPLICATE KEY UPDATE
                      name=VALUES(name),
                      image_url=VALUES(image_url),
                      link=VALUES(link),
                      updated_at=CURRENT_TIMESTAMP
                """).params(
                    id=str(ad_id),
                    name=title or name or f"Ad {ad_id}",
                    img=image_url,
                    link=link_url or f"https://www.facebook.com/ads/library/?id={ad_id}"
                ))
                _log.info("webhook.enrich: ad %s enriched successfully", ad_id)
                return ad_data
            except Exception as e:
                # SQLite fallback
                try:
                    session.exec(text("""
                        INSERT OR IGNORE INTO ads(ad_id, name, image_url, link, updated_at)
                        VALUES (:id, :name, :img, :link, CURRENT_TIMESTAMP)
                    """).params(
                        id=str(ad_id),
                        name=title or name or f"Ad {ad_id}",
                        img=image_url,
                        link=link_url or f"https://www.facebook.com/ads/library/?id={ad_id}"
                    ))
                    session.exec(text("""
                        UPDATE ads SET
                          name=COALESCE(:name, name),
                          image_url=COALESCE(:img, image_url),
                          link=COALESCE(:link, link),
                          updated_at=CURRENT_TIMESTAMP
                        WHERE ad_id=:id
                    """).params(
                        id=str(ad_id),
                        name=title or name or f"Ad {ad_id}",
                        img=image_url,
                        link=link_url or f"https://www.facebook.com/ads/library/?id={ad_id}"
                    ))
                    _log.info("webhook.enrich: ad %s enriched successfully", ad_id)
                    return ad_data
                except Exception as e2:
                    _log.warning("webhook.enrich: failed to save ad %s: %s", ad_id, str(e2))

    except Exception as e:
        _log.warning("webhook.enrich: failed to enrich ad %s: %s", ad_id, str(e)[:200])

    return None


async def enrich_story_if_missing(session, story_id: str, client: httpx.AsyncClient) -> Optional[Dict[str, Any]]:
    """
    Check if story exists in database. If not, fetch from Graph API synchronously.
    """
    try:
        # Check if story exists
        row = session.exec(text("SELECT story_id, url FROM stories WHERE story_id=:id LIMIT 1")).params(id=str(story_id)).first()
        if row:
            url = getattr(row, "url", None) or (row[1] if len(row) > 1 else None)
            if url:  # Consider it complete if we have URL
                return None

        # Story doesn't exist or incomplete - fetch from API
        _log.info("webhook.enrich: fetching story %s", story_id)

        token, _, _ = _get_base_token_and_id()
        base = f"https://graph.facebook.com/{GRAPH_VERSION}"

        # Get story info
        story_url = f"{base}/{story_id}"
        params = {
            "access_token": token,
            "fields": "id,media_url,permalink"
        }

        story_data = await graph_get(client, story_url, params)

        if story_data:
            media_url = story_data.get("media_url")
            permalink = story_data.get("permalink")

            try:
                session.exec(text("""
                    INSERT INTO stories(story_id, url, updated_at)
                    VALUES (:id, :url, CURRENT_TIMESTAMP)
                    ON DUPLICATE KEY UPDATE
                      url=VALUES(url),
                      updated_at=CURRENT_TIMESTAMP
                """).params(
                    id=str(story_id),
                    url=media_url or permalink
                ))
                _log.info("webhook.enrich: story %s enriched successfully", story_id)
                return story_data
            except Exception as e:
                # SQLite fallback
                try:
                    session.exec(text("""
                        INSERT OR IGNORE INTO stories(story_id, url, updated_at)
                        VALUES (:id, :url, CURRENT_TIMESTAMP)
                    """).params(
                        id=str(story_id),
                        url=media_url or permalink
                    ))
                    session.exec(text("""
                        UPDATE stories SET
                          url=COALESCE(:url, url),
                          updated_at=CURRENT_TIMESTAMP
                        WHERE story_id=:id
                    """).params(
                        id=str(story_id),
                        url=media_url or permalink
                    ))
                    _log.info("webhook.enrich: story %s enriched successfully", story_id)
                    return story_data
                except Exception as e2:
                    _log.warning("webhook.enrich: failed to save story %s: %s", story_id, str(e2))

    except Exception as e:
        _log.warning("webhook.enrich: failed to enrich story %s: %s", story_id, str(e)[:200])

    return None
