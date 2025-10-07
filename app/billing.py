from fastapi import APIRouter, Depends, Request, HTTPException
from sqlalchemy.orm import Session
from .deps import get_current_user
from .db import get_db
from .models import User
import os, stripe

router = APIRouter(prefix="/billing", tags=["billing"])

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID")
if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

@router.get("/status")
def status(user: User = Depends(get_current_user)):
    return {"subscription_status": user.subscription_status}

@router.post("/create-checkout-session")
def create_checkout_session(request: Request, db: Session = Depends(get_db), user: User = Depends(get_current_user)):
    if not STRIPE_SECRET_KEY or not STRIPE_PRICE_ID:
        raise HTTPException(status_code=400, detail="Stripe not configured")
    origin = request.headers.get("origin") or request.url.scheme + "://" + request.url.netloc
    session = stripe.checkout.Session.create(
        mode="subscription",
        line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
        success_url=origin + "/?success=true",
        cancel_url=origin + "/?canceled=true",
        customer_email=user.email,
        client_reference_id=str(user.id)
    )
    return {"checkout_url": session.url}
