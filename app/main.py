from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException

from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

from .db import init_db
from .deps import get_current_user, require_active_subscription
from . import auth, billing, logic
import pandas as pd

app = FastAPI(title="Paywise (HTML fixed)")
init_db()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def landing():
    html = Path("static/login.html").read_text(encoding="utf-8")
    return HTMLResponse(content=html, media_type="text/html")

@app.get("/app", response_class=HTMLResponse)
def app_page():
    html = Path("static/app.html").read_text(encoding="utf-8")
    return HTMLResponse(content=html, media_type="text/html")

@app.get("/health")
def health():
    return JSONResponse({"ok": True})

@app.get("/favicon.ico")
def favicon():
    path = Path("static/favicon.ico")
    if path.exists():
        return FileResponse(path)
    return PlainTextResponse("", status_code=204)

app.include_router(auth.router)
app.include_router(billing.router)

OUT_DIR = Path("data")
OUT_DIR.mkdir(exist_ok=True)

@app.post("/ingest")
async def ingest(
    payroll: UploadFile = File(...),
    commission: UploadFile = File(...),
    location: str = Form(""),
    user=Depends(require_active_subscription)
):
    p_bytes = await payroll.read()
    c_bytes = await commission.read()

    p_df = logic.read_payroll_by_totalfor(p_bytes, payroll.filename)
    c_df = logic.read_commission_sections(c_bytes, commission.filename)

    p_norm = logic.normalize_payroll(p_df, payroll.filename)
    c_norm = logic.normalize_commission(c_df, commission.filename)

    user_dir = OUT_DIR / f"user_{user.id}"
    user_dir.mkdir(exist_ok=True, parents=True)
    merged_csv = user_dir / "merged_normalized.csv"
    emp_csv = user_dir / "employee_totals.csv"

    payload = logic.build_payload(p_norm, c_norm, location=location)

    canonical = [
        "employee_id","employee_name","date","location","source","pay_component",
        "gross_amount","net_amount","commission_pct","commission_amount",
        "tips","bonus","notes","original_row_id","original_source_file"
    ]
    merged = pd.concat([p_norm, c_norm], ignore_index=True)[canonical]
    merged.to_csv(merged_csv, index=False)

    emp = pd.DataFrame(payload["employee_totals"])
    emp.to_csv(emp_csv, index=False)

    return JSONResponse({
        **payload,
        "download_url": f"/download/{user.id}/merged_normalized.csv",
        "employee_totals_url": f"/download/{user.id}/employee_totals.csv",
        "rows_merged": int(len(merged)),
        "rows_payroll": int(len(p_norm)),
        "rows_commission": int(len(c_norm)),
        "issues": []
    })

@app.get("/download/{user_id}/{name}")
async def download(user_id: int, name: str, user=Depends(get_current_user)):
    if user.id != user_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    path = OUT_DIR / f"user_{user_id}" / name
    if not path.exists():
        return JSONResponse({"error":"not found"}, status_code=404)
    return FileResponse(path)

@app.exception_handler(404)
def not_found(request: Request, exc: StarletteHTTPException):
    path = Path("static/404.html")
    if path.exists():
        return FileResponse(path, status_code=404)
    return HTMLResponse("<h1>404 Not Found</h1>", status_code=404)
