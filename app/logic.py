# logic.py — strict Mindbody XLS payroll parser (drop-in replacement)
from io import BytesIO, StringIO
import pandas as pd
import re

def _to_money(x) -> float:
    if x is None: 
        return 0.0
    t = str(x).strip()
    if not t: 
        return 0.0
    neg = t.startswith("(") and t.endswith(")")
    t = t.replace("$","").replace(",","").replace("(","").replace(")","")
    m = re.search(r"-?\d+(?:\.\d+)?", t)
    v = float(m.group(0)) if m else 0.0
    return -v if neg else v

def read_raw(upload: bytes, filename: str) -> pd.DataFrame:
    fn = (filename or "").lower()
    if fn.endswith(".xls") and (upload[:1000].lower().find(b"<html") != -1 or upload[:1000].lower().find(b"<!doctype") != -1):
        try:
            text = upload.decode("utf-8")
        except UnicodeDecodeError:
            text = upload.decode("utf-16", errors="ignore")
        try:
            dfs = pd.read_html(StringIO(text))
            return dfs[0] if dfs else pd.DataFrame()
        except Exception:
            return pd.DataFrame()
    try:
        if fn.endswith(".csv"):
            return pd.read_csv(BytesIO(upload), dtype=str).fillna("")
        if fn.endswith(".xlsx"):
            return pd.read_excel(BytesIO(upload), dtype=str, engine="openpyxl").fillna("")
        if fn.endswith(".xls"):
            return pd.read_excel(BytesIO(upload), dtype=str, engine="xlrd").fillna("")
        if fn.endswith(".xml"):
            return pd.DataFrame()
    except Exception:
        pass
    try:
        dfs = pd.read_html(BytesIO(upload))
        return dfs[0] if dfs else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def parse_mindbody_payroll_xls(upload: bytes, filename: str) -> pd.DataFrame:
    """Strict Mindbody Payroll Detail (HTML-in-.xls) parser."""
    from bs4 import BeautifulSoup
    from io import StringIO
    import pandas as pd
    import re

    def to_money(x):
        t = str(x or "").strip()
        if not t: return 0.0
        neg = t.startswith("(") and t.endswith(")")
        t = t.replace("$","").replace(",","").replace("(","").replace(")","")
        m = re.search(r"-?\d+(?:\.\d+)?", t)
        v = float(m.group(0)) if m else 0.0
        return -v if neg else v

    # decode HTML-ish XLS
    try:
        text = upload.decode("utf-8")
    except UnicodeDecodeError:
        text = upload.decode("utf-16", errors="ignore")

    soup = BeautifulSoup(text, "lxml")
    out_rows = []


    # loop per staff section
    for staff_div in soup.select("div.staffHeader"):
        name_el = staff_div.select_one(".staffName")
        employee_name = (name_el.get_text(strip=True) if name_el else "").strip()
        if not employee_name:
            continue  # ← inside loop (valid)

        # find next appointments table
        table = None
        cur = staff_div
        while True:
            cur = cur.find_next_sibling()
            if cur is None:
                break
            if getattr(cur, "name", None) == "table" and "results" in (cur.get("class") or []) and "appointments" in (cur.get("class") or []):
                table = cur
                break
            if getattr(cur, "name", None) in ("div","section"):
                t = cur.find("table", class_=["results","appointments"])
                if t and "results" in (t.get("class") or []) and "appointments" in (t.get("class") or []):
                    table = t
                    break
        if table is None:
            continue  # ← inside loop (valid)

        # read table
        try:
            df = pd.read_html(StringIO(str(table)))[0]
        except Exception:
            continue  # ← inside loop (valid)

        # tidy & pick strict columns
        df.columns = [str(c).strip() for c in df.columns]
        df = df.loc[:, ~(df.columns.astype(str).str.strip() == "")]
        df = df[df.astype(str).apply(lambda r: "".join(r.values).strip(), axis=1) != ""]
        if df.empty:
            continue  # ← inside loop (valid)

        # ----- choose columns (supports both header text & numeric headers) -----
        cols = [str(c).strip() for c in df.columns]

        def all_numeric_headers(cs):
            try:
                return all(str(c).strip().isdigit() for c in cs)
            except Exception:
                return False

        if all_numeric_headers(cols):
            # Mindbody HTML table with numeric headers; use stable positions:
            # 0: Appointment Date, 6: Base Pay, 8: Earnings
            date_col = cols[0] if len(cols) > 0 else None
            base_col = cols[6] if len(cols) > 6 else None
            earn_col = cols[8] if len(cols) > 8 else None
        else:
            # header text path
            def pick_contains(options):
                for c in df.columns:
                    lc = str(c).strip().lower()
                    if any(opt in lc for opt in options):
                        return c
                return None

            date_col = pick_contains(["appointment date", "date"])
            base_col = pick_contains(["base pay", "base"])
            earn_col = pick_contains(["earnings", "net pay", "total pay", "total", "pay"])

        # optional one-line debug to confirm picks
        print("[payroll pick]", {"date": date_col, "base": base_col, "earn": earn_col})

        # if no earnings-like column, skip this staff block (accuracy-first)
        if earn_col is None:
            continue

        # coerce money
        gross_series = df[base_col].map(to_money) if base_col in df.columns else pd.Series(0.0, index=df.index)
        earn_series  = df[earn_col].map(to_money)

        # build rows
        for i in range(len(df)):
            out_rows.append({
                "employee_id": "",
                "employee_name": employee_name,
                "date": df.iloc[i][date_col] if date_col else "",
                "location": "",
                "source": "payroll",
                "pay_component": "",
                "gross_amount": float(gross_series.iloc[i]),
                "net_amount":   float(earn_series.iloc[i]),
                "commission_pct": 0.0,
                "commission_amount": 0.0,
                "tips": 0.0,
                "bonus": 0.0,
                "notes": "",
                "original_row_id": "",
                "original_source_file": filename
            })

    return pd.DataFrame(out_rows, columns=[
        "employee_id","employee_name","date","location","source","pay_component",
        "gross_amount","net_amount","commission_pct","commission_amount",
        "tips","bonus","notes","original_row_id","original_source_file"
    ])

def read_payroll_by_totalfor(upload: bytes, filename: str) -> pd.DataFrame:
    """Dispatcher for payroll: prefer strict HTML-in-.xls parsing; no guessing."""
    from io import BytesIO
    import pandas as pd

    fn = (filename or "").lower()
    ul = upload.lower()
    # robust HTML-in-.xls detection
    if fn.endswith(".xls") and (b"<html" in ul or b"<!doctype" in ul):
        return parse_mindbody_payroll_xls(upload, filename)

    # true Excel fallback (rare for detail)
    try:
        if fn.endswith(".xlsx"):
            raw = pd.read_excel(BytesIO(upload), dtype=str, engine="openpyxl")
        else:
            raw = pd.read_excel(BytesIO(upload), dtype=str, engine="xlrd")
    except Exception:
        return pd.DataFrame(columns=[
            "employee_id","employee_name","date","location","source","pay_component",
            "gross_amount","net_amount","commission_pct","commission_amount",
            "tips","bonus","notes","original_row_id","original_source_file"
        ])

    cols_lower = {str(c).strip().lower(): c for c in raw.columns}
    date_col = cols_lower.get("appointment date") or cols_lower.get("date")
    base_col = cols_lower.get("base pay")
    earn_col = cols_lower.get("earnings") or cols_lower.get("net pay") or cols_lower.get("total pay") or cols_lower.get("total")
    if earn_col is None:
        return pd.DataFrame(columns=[
            "employee_id","employee_name","date","location","source","pay_component",
            "gross_amount","net_amount","commission_pct","commission_amount",
            "tips","bonus","notes","original_row_id","original_source_file"
        ])

    def to_money(x):
        t = str(x or "").strip()
        if not t: return 0.0
        neg = t.startswith("(") and t.endswith(")")
        t = t.replace("$","").replace(",","").replace("(","").replace(")","")
        try:
            v = float(t);  return -v if neg else v
        except:          return 0.0

    gross_series = raw[base_col].map(to_money) if base_col else pd.Series(0.0, index=raw.index)
    earn_series  = raw[earn_col].map(to_money)

    out = pd.DataFrame({
        "employee_id": "",
        "employee_name": "",  # true .xls often lacks per-section names
        "date": raw[date_col] if date_col else "",
        "location": "",
        "source": "payroll",
        "pay_component": "",
        "gross_amount": gross_series,
        "net_amount":   earn_series,
        "commission_pct": 0.0,
        "commission_amount": 0.0,
        "tips": 0.0,
        "bonus": 0.0,
        "notes": "",
        "original_row_id": "",
        "original_source_file": filename
    })
    return out.fillna(0)

def normalize_payroll(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    needed = {
        "employee_id","employee_name","date","location","source","pay_component",
        "gross_amount","net_amount","commission_pct","commission_amount",
        "tips","bonus","notes","original_row_id","original_source_file"
    }
    if isinstance(df, pd.DataFrame) and needed.issubset(set(df.columns)):
        return df.fillna(0)
    return pd.DataFrame(columns=list(needed))

def read_commission_sections(upload: bytes, filename: str) -> pd.DataFrame:
    fn = (filename or "").lower()
    try:
        if fn.endswith(".csv"):
            df = pd.read_csv(BytesIO(upload), dtype=str)
            return df
        if fn.endswith(".xlsx"):
            df = pd.read_excel(BytesIO(upload), dtype=str, engine="openpyxl")
            return df
        if fn.endswith(".xls"):
            if upload[:1000].lower().find(b"<html") != -1 or upload[:1000].lower().find(b"<!doctype") != -1:
                try:
                    text = upload.decode("utf-8")
                except UnicodeDecodeError:
                    text = upload.decode("utf-16", errors="ignore")
                dfs = pd.read_html(StringIO(text))
                df = dfs[0] if dfs else pd.DataFrame()
                return df
            df = pd.read_excel(BytesIO(upload), dtype=str, engine="xlrd")
            return df
    except Exception:
        try:
            dfs = pd.read_html(BytesIO(upload))
            df = dfs[0] if dfs else pd.DataFrame()
            return df
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def normalize_commission(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    Robust commission normalizer:
    - Works with Mindbody Commission Detail exported as HTML-in-.xls (often numeric headers) or real Excel/CSV
    - Picks commission amount by header tokens OR by detecting the column with most money-looking cells
    - Picks employee name by header tokens OR by name-like density (e.g., "Last, First")
    - Falls back to employee_name = "Unassigned" when missing so per-employee totals always render
    """
    import re
    import pandas as pd

    needed = [
        "employee_id","employee_name","date","location","source","pay_component",
        "gross_amount","net_amount","commission_pct","commission_amount",
        "tips","bonus","notes","original_row_id","original_source_file"
    ]

    def empty_df():
        return pd.DataFrame(columns=needed)

    if df is None or df.empty:
        return empty_df()

    # Helper: money detector
    money_re = re.compile(r"^\s*\(?\$?-?\d{1,3}(?:,\d{3})*(?:\.\d{2})?\)?\s*$|^\s*\(?\$?-?\d+(?:\.\d{2})?\)?\s*$")

    def looks_money(x: object) -> bool:
        s = str(x or "").strip()
        return bool(money_re.match(s))

    # Helper: simple name heuristic (e.g., "Last, First" or has space & letters)
    def looks_name(x: object) -> bool:
        s = str(x or "").strip()
        if not s:
            return False
        if "," in s and any(c.isalpha() for c in s):
            return True
        return (" " in s) and any(c.isalpha() for c in s)

    # Helper: detect service/product-like catalog strings (not person names)
    def looks_catalog(x: object) -> bool:
        s = str(x or "").strip().lower()
        if not s:
            return False
        bad_kw = [
            "service","product","sku","qty","quantity","item","description","benefit","benefits",
            "set","refill","refills","package","membership","member","add-on","addon","brow","lash",
            "gel","lift","tint","classic","hybrid","volume","clear","mask","payscale","pay rate","|"
        ]
        # obvious product/service keywords
        if any(k in s for k in bad_kw):
            return True
        # long marketing-like strings without comma pattern
        if (len(s) > 40 and "," not in s) or s.count(" ") >= 6:
            return True
        # contains digits but not a comma name (e.g., "Set 1" etc.)
        if any(ch.isdigit() for ch in s) and "," not in s:
            return True
        return False

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    cols = list(df.columns)

    # 1) Employee name column
    name_tokens = ["employee", "employee name", "staff", "staff name", "provider", "service provider"]
    emp_col = next((c for c in cols if any(tok in c.lower() for tok in name_tokens)), None)
    if emp_col is None:
        # score columns: name-likeness minus catalog-likeness
        best, best_score = None, -1e9
        sample = df.head(200)
        for c in cols:
            try:
                name_score = int(sample[c].apply(looks_name).sum())
                catalog_pen = int(sample[c].apply(looks_catalog).sum())
                score = name_score - (2 * catalog_pen)
            except Exception:
                score = -1e9
            if score > best_score:
                best, best_score = c, score
        emp_col = best

    # 2) Date column (optional)
    date_col = next((c for c in cols if any(k in c.lower() for k in ["date","sales date","transaction date"])), None)

    # 3) Commission amount column
    amt_tokens = ["commission amount","commission","total commission","amount","amt"]
    amt_col = next((c for c in cols if any(tok in c.lower() for tok in amt_tokens)), None)
    if amt_col is None:
        # pick column with highest count of money-looking cells
        best, score = None, -1
        sample = df.head(200)
        for c in cols:
            try:
                sc = int(sample[c].apply(looks_money).sum())
            except Exception:
                sc = 0
            if sc > score:
                best, score = c, sc
        amt_col = best

    # Convert amounts to float using existing helper
    commission_amount = df[amt_col].map(_to_money) if amt_col in df.columns else 0.0

    # Build normalized output
    out = pd.DataFrame()
    out["employee_id"] = ""
    if emp_col in df.columns:
        # strip whitespace; fill blanks with fallback label so grouping works
        en = df[emp_col].astype(str).str.strip()
        # sanitize: if detected as catalog/service string, mark Unassigned
        en = en.apply(lambda v: "Unassigned" if (v == "" or looks_catalog(v)) else v)
        out["employee_name"] = en
    else:
        out["employee_name"] = "Unassigned"

    out["date"] = df[date_col] if date_col in df.columns else ""
    out["location"] = ""
    out["source"] = "commission"
    out["pay_component"] = "commission"
    out["gross_amount"] = 0.0
    out["net_amount"] = 0.0
    out["commission_pct"] = 0.0
    out["commission_amount"] = commission_amount
    out["tips"] = 0.0
    out["bonus"] = 0.0
    out["notes"] = ""
    out["original_row_id"] = ""
    out["original_source_file"] = filename

    return out.fillna(0)

def build_payload(payroll_norm: pd.DataFrame, commission_norm: pd.DataFrame, location: str = ""):
    canonical = [
        "employee_id","employee_name","date","location","source","pay_component",
        "gross_amount","net_amount","commission_pct","commission_amount",
        "tips","bonus","notes","original_row_id","original_source_file"
    ]
    def ensure(df):
        for c in canonical:
            if c not in df.columns:
                df[c] = "" if c in ("employee_id","employee_name","date","location","source","pay_component","notes","original_row_id","original_source_file") else 0.0
        return df[canonical]
    p = ensure(payroll_norm.copy() if isinstance(payroll_norm, pd.DataFrame) else pd.DataFrame(columns=canonical))
    c = ensure(commission_norm.copy() if isinstance(commission_norm, pd.DataFrame) else pd.DataFrame(columns=canonical))
    if location:
        p["location"] = location
        c["location"] = location
    merged = pd.concat([p, c], ignore_index=True)
    merged["gross_amount"] = pd.to_numeric(merged["gross_amount"], errors="coerce").fillna(0.0)
    merged["net_amount"] = pd.to_numeric(merged["net_amount"], errors="coerce").fillna(0.0)
    merged["commission_amount"] = pd.to_numeric(merged["commission_amount"], errors="coerce").fillna(0.0)
    emp = merged.groupby(["employee_id","employee_name"], dropna=False).agg(
        payroll_total=("net_amount","sum"),
        commission_total=("commission_amount","sum"),
    ).reset_index()
    emp["combined_total"] = emp["payroll_total"] + emp["commission_total"]
    grand = {
        "payroll_total": float(merged["net_amount"].sum()),
        "commission_total": float(merged["commission_amount"].sum()),
        "combined_total": float(merged["net_amount"].sum() + merged["commission_amount"].sum()),
    }
    breakdowns = []
    for (emp_id, emp_name), sub in merged.groupby(["employee_id","employee_name"], dropna=False):
        rows_payload = sub.head(50).infer_objects(copy=False).fillna("").to_dict(orient="records")
        breakdowns.append({
            "employee_id": emp_id or "",
            "employee_name": emp_name or "",
            "rows_count": int(len(sub)),
            "rows": rows_payload
        })
    return {
        "employee_totals": emp.to_dict(orient="records"),
        "grand_totals": grand,
        "employee_breakdowns": breakdowns,
    }