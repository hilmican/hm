from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Request, Form, HTTPException
import logging
import os
import hmac
import hashlib
import base64
import json
import secrets
from sqlmodel import select

from ..db import get_session
from ..models import User
from ..utils.hashing import hash_password, verify_password


router = APIRouter(prefix="/auth", tags=["auth"])
_log = logging.getLogger("meta.auth")


def is_locked(user: User) -> bool:
    if not user.locked_until:
        return False
    return datetime.utcnow() < user.locked_until


@router.get("/login")
def login_form(request: Request):
    templates = request.app.state.templates
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@router.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    # basic rate limiting (per IP) could be added later; here we use per-user lockouts
    with get_session() as session:
        user = session.exec(select(User).where(User.username == username)).first()
        if not user:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        if is_locked(user):
            raise HTTPException(status_code=429, detail="Account temporarily locked. Try later.")
        if not verify_password(password, user.password_hash):
            user.failed_attempts = (user.failed_attempts or 0) + 1
            if user.failed_attempts >= 5:
                user.locked_until = datetime.utcnow() + timedelta(minutes=15)
                user.failed_attempts = 0
            session.add(user)
            session.commit()
            raise HTTPException(status_code=401, detail="Invalid credentials")
        # success
        user.failed_attempts = 0
        user.locked_until = None
        session.add(user)
        session.commit()
        request.session["uid"] = user.id
        request.session["uname"] = user.username
        return {"status": "ok"}


@router.post("/logout")
def logout(request: Request):
    request.session.clear()
    return {"status": "ok"}


def require_auth(request: Request) -> int:
    uid = request.session.get("uid")
    if not uid:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return int(uid)

# ---- Instagram Business Login minimal endpoints ----

@router.get("/instagram/callback")
def ig_business_login_callback(request: Request):
    # Meta will redirect here with ?code=...&state=...
    # We simply acknowledge for now since we only receive messages.
    params = dict(request.query_params)
    try:
        # basic arrival log, avoid leaking full codes
        code = params.get("code")
        state = params.get("state")
        _log.info(
            "Meta callback: code_len=%s code_sfx=%s state_len=%s state_sfx=%s",
            (len(code) if code else 0),
            (code[-4:] if code else None),
            (len(state) if state else 0),
            (state[-4:] if state else None),
        )
    except Exception:
        pass
    return {"status": "ok", "received": params}


def _decode_signed_request(signed_request: str, app_secret: str) -> dict:
    try:
        sig_b64, payload_b64 = signed_request.split(".", 1)
        # base64url decode
        def b64urldecode(b: str) -> bytes:
            padding = '=' * ((4 - len(b) % 4) % 4)
            return base64.urlsafe_b64decode(b + padding)

        sig = b64urldecode(sig_b64)
        payload_bytes = b64urldecode(payload_b64)
        expected = hmac.new(app_secret.encode("utf-8"), payload_bytes, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, expected):
            raise ValueError("bad signature")
        return json.loads(payload_bytes.decode("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid signed_request: {e}")


@router.post("/instagram/deauthorize")
async def ig_deauthorize(request: Request):
    body = await request.body()
    app_secret = os.getenv("IG_APP_SECRET", "")
    signed_request = None
    ct = request.headers.get("content-type", "")
    if "application/json" in ct:
        try:
            data = json.loads(body.decode("utf-8"))
            signed_request = data.get("signed_request")
        except Exception:
            pass
    if signed_request is None and "application/x-www-form-urlencoded" in ct:
        form = await request.form()
        signed_request = form.get("signed_request")  # type: ignore
    if not signed_request:
        raise HTTPException(status_code=400, detail="missing signed_request")
    payload = _decode_signed_request(str(signed_request), app_secret)
    # Here we would delete user-scoped data. We only operate our own business account, so minimal no-op.
    return {"status": "success", "user": payload.get("user_id")}


@router.post("/instagram/data_deletion")
async def ig_data_deletion(request: Request):
    app_secret = os.getenv("IG_APP_SECRET", "")
    signed_request = None
    ct = request.headers.get("content-type", "")
    body = await request.body()
    try:
        if "application/json" in ct:
            signed_request = (json.loads(body.decode("utf-8")) or {}).get("signed_request")
        elif "application/x-www-form-urlencoded" in ct:
            form = await request.form()
            signed_request = form.get("signed_request")  # type: ignore
    except Exception:
        pass
    if signed_request:
        _ = _decode_signed_request(str(signed_request), app_secret)
    confirmation_code = secrets.token_hex(8)
    return {
        "url": "https://hma.cdn.com.tr/legal/data-deletion",
        "confirmation_code": confirmation_code,
    }


