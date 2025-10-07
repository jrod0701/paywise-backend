import pandas as pd, numpy as np, math, re
from io import BytesIO

def _sanitize_json(v):
    try:
        if v != v: return None
    except: pass
    if isinstance(v, (np.floating, float)):
        if math.isinf(v) or math.isnan(v): return None
        return float(v)
    if isinstance(v, (np.integer, int)): return int(v)
    return v

def read_raw(upload: bytes, filename: str):
    fn = filename.lower()
    if fn.endswith(".csv"):
        return pd.read_csv(BytesIO(upload), dtype=str, header=None).fillna("")
    elif fn.endswith(".xlsx"):
        return pd.read_excel(BytesIO(upload), dtype=str, header=None, engine="openpyxl").fillna("")
    else:
        return pd.read_excel(BytesIO(upload), dtype=str, header=None, engine="xlrd").fillna("")

def read_raw_then_header(upload: bytes, filename: str, header_hint_contains=None, header_row_index=None) -> pd.DataFrame:
    raw = read_raw(upload, filename)
    if header_row_index is not None:
        hdr_idx = header_row_index
    elif header_hint_contains:
        mask = raw.apply(lambda row: row.astype(str).str.contains(header_hint_contains, case=False, na=False)).any(axis=1)
        idxs = list(raw[mask].index)
        hdr_idx = idxs[0] if idxs else 0
    else:
        hdr_idx = 0
    headers = [str(x).strip() for x in raw.iloc[hdr_idx].tolist()]
    df = raw.iloc[hdr_idx+1:].copy()
    df.columns = headers
    df = df.loc[:, ~(df.columns.astype(str).str.strip() == "")]
    df = df[(df.astype(str).apply(lambda r: "".join(r.values), axis=1).str.strip() != "")]
    return df.reset_index(drop=True)

def read_payroll_by_totalfor(upload: bytes, filename: str) -> pd.DataFrame:
    raw = read_raw(upload, filename)
    n = len(raw)
    total_rows = []
    for i in range(n):
        vals = [str(x).strip() for x in raw.iloc[i].tolist()]
        for v in vals:
            m = re.match(r"^Total\s+for\s+(.+)$", v, flags=re.IGNORECASE)
            if m:
                name = m.group(1).strip()
                total_rows.append((i, name))
                break
    if not total_rows:
        return read_raw_then_header(upload, filename, header_hint_contains="Appointment Date")
    sections = []
    for idx, name in total_rows:
        hdr = None
        for j in range(idx-1, -1, -1):
            if raw.iloc[j].astype(str).str.contains("Appointment Date", case=False, na=False).any():
                hdr = j
                break
        if hdr is None:
            continue
        start = hdr + 1
        end = idx
        if end <= start:
            continue
        data = raw.iloc[start:end].copy()
        header_row = [str(x).strip() for x in raw.iloc[hdr].tolist()]
        data.columns = header_row
        data = data.loc[:, ~(data.columns.astype(str).str.strip() == "")]
        data = data[(data.astype(str).apply(lambda r: "".join(r.values), axis=1).str.strip() != "")]
        if data.empty:
            continue
        data["__employee_name_hint"] = name
        sections.append(data)
    if not sections:
        return read_raw_then_header(upload, filename, header_hint_contains="Appointment Date")
    return pd.concat(sections, ignore_index=True).reset_index(drop=True)

def read_commission_sections(upload: bytes, filename: str) -> pd.DataFrame:
    raw = read_raw(upload, filename)
    def row_has_comm_header(vals):
        vals_norm = [str(x).strip().lower() for x in vals]
        return ("date" in vals_norm) and any("item name" in v for v in vals_norm)
    header_idxs = [i for i in range(len(raw)) if row_has_comm_header(raw.iloc[i].tolist())]
    if not header_idxs:
        return read_raw_then_header(upload, filename, header_row_index=1)
    sections = []
    for idx_i, hdr_idx in enumerate(header_idxs):
        next_hdr = header_idxs[idx_i + 1] if idx_i + 1 < len(header_idxs) else len(raw)
        header_row = [str(x).strip() for x in raw.iloc[hdr_idx].tolist()]
        staff_name = ""
        if hdr_idx - 1 >= 0:
            prev_vals = [str(x).strip() for x in raw.iloc[hdr_idx - 1].tolist()]
            for cell in prev_vals:
                if cell:
                    staff_name = cell
                    break
        data = raw.iloc[hdr_idx + 1: next_hdr].copy()
        if data.empty:
            continue
        data.columns = header_row
        data = data.loc[:, ~(data.columns.astype(str).str.strip() == "")]
        data = data[(data.astype(str).apply(lambda r: "".join(r.values), axis=1).str.strip() != "")]
        if data.empty:
            continue
        data["__employee_name_hint"] = staff_name
        sections.append(data)
    return pd.concat(sections, ignore_index=True).reset_index(drop=True)

def pick(colmap: dict, *names):
    for n in names:
        key = n.lower().strip()
        if key in colmap:
            return colmap[key]
        for k in colmap:
            if key in k:
                return colmap[k]
    return None

def pick_name(colmap: dict):
    candidates = [
        "sold by","salesperson","staff name","employee name","employee","staff","provider","service provider"
    ]
    for n in candidates:
        key = n.lower().strip()
        if key in colmap:
            return colmap[key]
        for k in colmap:
            if key in k:
                return colmap[k]
    return None

def normalize_commission(df: pd.DataFrame, srcname: str) -> pd.DataFrame:
    cols = {str(c).lower().strip(): c for c in df.columns}
    out = pd.DataFrame()
    out["employee_id"] = df.get(pick(cols, "ID"))
    if "__employee_name_hint" in df.columns:
        out["employee_name"] = df["__employee_name_hint"]
    else:
        name_col = pick_name(cols)
        out["employee_name"] = df.get(name_col) if name_col else ""
    out["date"] = df.get(pick(cols, "Date"))
    out["location"] = ""
    out["source"] = "commission"
    out["pay_component"] = "commission"

    price_col = pick(cols, "Item price","Price","Sale amount","Total sale","Gross")
    out["gross_amount"] = pd.to_numeric(df.get(price_col), errors="coerce").fillna(0) if price_col else 0

    pct_col = pick(cols, "Staff Standard %","Staff Promo %","Commission %")
    if pct_col:
        pct_series = df[pct_col].astype(str).str.replace("%","",regex=False)
        pct = pd.to_numeric(pct_series, errors="coerce").fillna(0.0)
        out["commission_pct"] = pct.apply(lambda x: x/100 if x>1 else x)
    else:
        out["commission_pct"] = 0.0

    amt_col = pick(cols, "Total","Commission amount","Comm amount")
    out["commission_amount"] = pd.to_numeric(df.get(amt_col), errors="coerce").fillna(0) if amt_col else 0

    out["tips"] = 0
    out["bonus"] = 0
    notes_col = pick(cols, "Item name","Description","Memo","Item")
    out["notes"] = df.get(notes_col) if notes_col else ""
    out["original_row_id"] = df.reset_index().index
    out["original_source_file"] = srcname
    out["net_amount"] = 0
    return out

def normalize_payroll(df: pd.DataFrame, srcname: str) -> pd.DataFrame:
    cols = {str(c).lower().strip(): c for c in df.columns}
    out = pd.DataFrame()
    out["employee_id"] = ""
    if "__employee_name_hint" in df.columns:
        out["employee_name"] = df["__employee_name_hint"]
    else:
        name_col = pick_name(cols)
        out["employee_name"] = df.get(name_col) if name_col else ""
    out["date"] = df.get(pick(cols, "Appointment Date","Date"))
    out["location"] = ""
    out["source"] = "payroll"
    out["pay_component"] = "appointment"

    base_col = pick(cols, "Base Pay")
    earn_col = pick(cols, "Earnings")
    out["gross_amount"] = pd.to_numeric(df.get(base_col), errors="coerce").fillna(0) if base_col else 0
    out["net_amount"] = pd.to_numeric(df.get(earn_col), errors="coerce").fillna(0) if earn_col else 0
    out["commission_pct"] = 0.0
    out["commission_amount"] = 0.0
    out["tips"] = 0.0
    out["bonus"] = 0.0
    notes_col = pick(cols, "Client Name(s)","Client Name","Client")
    out["notes"] = df.get(notes_col) if notes_col else ""
    out["original_row_id"] = df.reset_index().index
    out["original_source_file"] = srcname
    return out

def build_payload(p_norm: pd.DataFrame, c_norm: pd.DataFrame, location: str | None):
    canonical = [
        "employee_id","employee_name","date","location","source","pay_component",
        "gross_amount","net_amount","commission_pct","commission_amount",
        "tips","bonus","notes","original_row_id","original_source_file"
    ]
    merged = pd.concat([p_norm, c_norm], ignore_index=True)[canonical]
    if location:
        merged["location"] = location

    def payroll_amount_series(df: pd.DataFrame) -> pd.Series:
        na = pd.to_numeric(df.get("net_amount"), errors="coerce").fillna(0)
        ga = pd.to_numeric(df.get("gross_amount"), errors="coerce").fillna(0)
        return na.where(na > 0, ga)

    merged["payroll_component_amount"] = payroll_amount_series(merged.where(merged["source"] == "payroll"))
    merged["commission_component_amount"] = pd.to_numeric(
        merged.where(merged["source"] == "commission")["commission_amount"], errors="coerce"
    ).fillna(0)

    merged["__emp_label"] = merged.apply(lambda r: (str(r.get("employee_name") or "").strip()) or (str(r.get("employee_id") or "").strip()), axis=1)
    group_cols = ["__emp_label","employee_id","employee_name"]
    emp = merged.groupby(group_cols, dropna=False).agg(
        payroll_total=("payroll_component_amount", "sum"),
        commission_total=("commission_component_amount", "sum")
    ).reset_index()
    emp["combined_total"] = emp["payroll_total"] + emp["commission_total"]
    emp = emp.sort_values("combined_total", ascending=False).fillna("")

    grand_payroll = float(emp["payroll_total"].sum())
    grand_commission = float(emp["commission_total"].sum())
    grand_combined = float(emp["combined_total"].sum())

    def _row_amount(row):
        try:
            if str(row.get("source")) == "commission":
                return float(row.get("commission_amount") or 0)
            else:
                na = float(row.get("net_amount") or 0)
                ga = float(row.get("gross_amount") or 0)
                return na if na > 0 else ga
        except Exception:
            return 0.0

    breakdowns = []
    for _, r in emp.iterrows():
        label = r["__emp_label"]
        sub = merged.loc[merged["__emp_label"] == label, ["employee_id","employee_name","source","date","pay_component","notes","gross_amount","net_amount","commission_amount"]].copy()
        sub["line_total"] = sub.apply(_row_amount, axis=1)
        rows_payload = sub.head(50).fillna("").to_dict(orient="records")
        eid = str(sub["employee_id"].iloc[0]) if not sub.empty else ""
        enm = str(sub["employee_name"].iloc[0]) if not sub.empty else label
        breakdowns.append({
            "employee_id": eid,
            "employee_name": enm,
            "rows_count": int(len(sub)),
            "rows": rows_payload
        })

    return {
        "grand_totals": {
            "payroll_total": float(grand_payroll),
            "commission_total": float(grand_commission),
            "combined_total": float(grand_combined)
        },
        "employee_totals": emp.to_dict(orient="records"),
        "employee_breakdowns": breakdowns
    }
