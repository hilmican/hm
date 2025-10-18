from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Request, Form, HTTPException
from sqlmodel import select

from ..db import get_session
from ..models import User
from ..utils.hashing import hash_password, verify_password


router = APIRouter(prefix="/auth", tags=["auth"])


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


