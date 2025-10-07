from dotenv import load_dotenv
load_dotenv()

from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from .db import get_db
from .models import User
import os, jwt

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")
JWT_SECRET = os.getenv("JWT_SECRET", "devsecret")
DEV_ALLOW_ALL = os.getenv("DEV_ALLOW_ALL", "false").lower() == "true"
TEST_EMAIL = (os.getenv("TEST_EMAIL") or "").strip().lower()

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        uid = int(payload.get("sub"))
        user = db.query(User).get(uid)
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return user
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

def require_active_subscription(user: User = Depends(get_current_user)) -> User:
    if DEV_ALLOW_ALL or (TEST_EMAIL and user.email.lower() == TEST_EMAIL):
        return user
    if user.subscription_status != "active":
        raise HTTPException(status_code=402, detail="Subscription required")
    return user
