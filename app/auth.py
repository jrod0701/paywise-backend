from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from passlib.context import CryptContext
import jwt, datetime as dt, os
from pydantic import BaseModel, EmailStr
from .db import get_db
from .models import User

JWT_SECRET = os.getenv("JWT_SECRET", "devsecret")
pwd_context = CryptContext(schemes=["pbkdf2_sha256","bcrypt_sha256","bcrypt"], default="pbkdf2_sha256", deprecated="auto")

router = APIRouter(prefix="/auth", tags=["auth"])

class RegisterIn(BaseModel):
    email: EmailStr
    password: str

@router.post("/register")
def register(payload: RegisterIn, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == payload.email.lower()).first():
        raise HTTPException(status_code=400, detail="Email already registered")
    password_hash = pwd_context.hash(payload.password[:72])
    user = User(email=payload.email.lower(), password_hash=password_hash)
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"ok": True}

@router.post("/login")
def login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == form.username.lower()).first()
    if not user or not pwd_context.verify(form.password[:72], user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    now = dt.datetime.utcnow()
    token = jwt.encode({"sub": user.id, "email": user.email, "exp": now + dt.timedelta(hours=12)}, JWT_SECRET, algorithm="HS256")
    return {"access_token": token, "token_type": "bearer"}
