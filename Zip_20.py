import os
import json
import re
import time
import random
import string
import requests
import pandas as pd
from datetime import datetime, timedelta
from functools import wraps

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_formatting import format_cell_range, CellFormat, NumberFormat
from gspread.exceptions import APIError

from dotenv import load_dotenv
load_dotenv()

# ===== Environment Variables =====
ODOO_URL = os.getenv("ODOO_URL")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")
DB = os.getenv("ODOO_DB")

MODEL = "attendance.pdf.report"
REPORT_BUTTON_METHOD = "action_generate_xlsx_report"

REPORT_TYPE = "ot_analysis"        # e.g. "ot_analysis", "job_card"
DATE_FROM = "2025-08-01"
DATE_TO = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

company_id = 1   # 1 = Zipper, 3 = Metal Trims

# ===== Google Sheets =====
GOOGLE_SHEET_URL     = "https://docs.google.com/spreadsheets/d/1W9qXHRPrSffHfcQvBxrAK2fTAqne5ohqf0tIn1oMujM/edit?gid=1647682121#gid=1647682121"
SHEET_NAME           = "Sheet1" #Staff OT Analysis
SERVICE_ACCOUNT_JSON = "credentials.json"

# Local output
DOWNLOADED_XLSX = f"{REPORT_TYPE}_{DATE_FROM}_to_{DATE_TO}_cat20.xlsx"

# ========= START SESSION ==========
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})


# ===== Retry Decorator =====
def retry(max_attempts=5, base_delay=2, backoff=2,
          allowed_exceptions=(requests.RequestException, RuntimeError, APIError)):
    """
    Retry decorator with exponential backoff + jitter.
    Retries on network errors and custom exceptions.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 1
            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except allowed_exceptions as e:
                    wait_time = base_delay * (backoff ** (attempt - 1))
                    wait_time += random.uniform(0, 1)  # jitter
                    print(f"⚠️ {func.__name__} failed (attempt {attempt}/{max_attempts}): {e}")
                    if attempt == max_attempts:
                        raise
                    time.sleep(wait_time)
                    attempt += 1
        return wrapper
    return decorator


# ===== Odoo Functions =====
@retry()
def login():
    url = f"{ODOO_URL}/web/session/authenticate"
    payload = {"jsonrpc": "2.0", "params": {"db": DB, "login": USERNAME, "password": PASSWORD}}
    r = session.post(url, json=payload, timeout=60)
    r.raise_for_status()
    res = r.json()
    uid = res.get("result", {}).get("uid")
    if not uid:
        raise RuntimeError(f"Login failed: {res}")
    print("✅ Logged in, UID =", uid)
    return uid


@retry()
def get_csrf():
    r = session.get(f"{ODOO_URL}/web", timeout=60)
    m = re.search(r'csrf_token\s*:\s*"([^"]+)"', r.text)
    if not m:
        raise RuntimeError("Could not extract CSRF token from /web")
    csrf = m.group(1)
    print("✅ CSRF token =", csrf)
    return csrf


@retry()
def onchange(uid):
    url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/onchange"
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL,
            "method": "onchange",
            "args": [[], {}, [], {
                "report_type": {}, "date_from": {}, "date_to": {},
                "is_company": {}, "atten_type": {}, "types": {}, "mode_type": {},
                "employee_id": {"fields": {"display_name": {}}},
                "mode_company_id": {"fields": {"display_name": {}}},
                "category_id": {"fields": {"display_name": {}}},
                "department_id": {"fields": {"display_name": {}}},
                "company_all": {}
            }],
            "kwargs": {
                "context": {
                    "lang": "en_US", "tz": "Asia/Dhaka", "uid": uid,
                    "allowed_company_ids": [company_id], "default_is_company": False
                }
            }
        }
    }
    r = session.post(url, json=payload, timeout=60)
    r.raise_for_status()
    val = r.json().get("result", {}).get("value", {})
    print("✅ Onchange defaults:", val)
    return val


@retry()
def web_save(uid):
    url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/web_save"
    payload = {
        "id": 3,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL,
            "method": "web_save",
            "args": [[], {
                "report_type": REPORT_TYPE,
                "date_from": DATE_FROM,
                "date_to": DATE_TO,
                "is_company": False,
                "atten_type": False,
                "types": False,
                "mode_type": "category",
                "employee_id": False,
                "mode_company_id": False,
                "category_id": 30,
                "department_id": False,
                "company_all": "allcompany"
            }],
            "kwargs": {
                "context": {
                    "lang": "en_US", "tz": "Asia/Dhaka", "uid": uid,
                    "allowed_company_ids": [company_id], "default_is_company": False
                },
                "specification": {
                    "report_type": {}, "date_from": {}, "date_to": {}, "is_company": {},
                    "atten_type": {}, "types": {}, "mode_type": {},
                    "employee_id": {"fields": {"display_name": {}}},
                    "mode_company_id": {"fields": {"display_name": {}}},
                    "category_id": {"fields": {"display_name": {}}},
                    "department_id": {"fields": {"display_name": {}}},
                    "company_all": {}
                }
            }
        }
    }
    r = session.post(url, json=payload, timeout=60)
    r.raise_for_status()
    res = r.json()
    wizard_id = (res.get("result") or [{}])[0].get("id")
    if not wizard_id:
        raise RuntimeError(f"Wizard save failed: {res}")
    print("✅ Wizard saved, ID =", wizard_id)
    return wizard_id


@retry()
def call_button(uid, wizard_id):
    url = f"{ODOO_URL}/web/dataset/call_button"
    payload = {
        "id": 4,
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": MODEL,
            "method": REPORT_BUTTON_METHOD,
            "args": [[wizard_id]],
            "kwargs": {"context": {
                "lang": "en_US", "tz": "Asia/Dhaka",
                "uid": uid, "allowed_company_ids": [company_id],
                "default_is_company": False
            }}
        }
    }
    r = session.post(url, json=payload, timeout=120)
    r.raise_for_status()
    res = r.json()
    report_name = res.get("result", {}).get("report_name")
    if not report_name:
        raise RuntimeError(f"Report button did not return report_name: {res}")
    print("✅ Report generated:", report_name)
    return report_name


@retry()
def download_xlsx(uid, csrf_token, wizard_id, report_name):
    download_url = f"{ODOO_URL}/report/download"
    options = {
        "date_from": DATE_FROM,
        "date_to": DATE_TO,
        "mode_type": "category",
        "mode_company_id": False,
        "department_id": False,
        "category_id": 31,
        "employee_id": False,
        "report_type": REPORT_TYPE,
        "atten_type": False,
        "types": False,
        "is_company": False,
        "company_all": "allcompany"
    }
    context = {
        "lang": "en_US",
        "tz": "Asia/Dhaka",
        "uid": uid,
        "allowed_company_ids": [company_id],
        "active_model": MODEL,
        "active_id": wizard_id,
        "active_ids": [wizard_id],
        "default_is_company": False
    }
    report_path = f"/report/xlsx/{report_name}?options={json.dumps(options)}&context={json.dumps(context)}"
    payload = {
        "data": json.dumps([report_path, "xlsx"]),
        "context": json.dumps(context),
        "token": "dummy-because-api-expects-one",
        "csrf_token": csrf_token
    }
    headers = {"X-CSRF-Token": csrf_token, "Referer": f"{ODOO_URL}/web"}

    r = session.post(download_url, data=payload, headers=headers, timeout=180)
    r.raise_for_status()
    ctype = r.headers.get("content-type", "").lower()
    if ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" not in ctype
            and "application/octet-stream" not in ctype):
        raise RuntimeError(f"Download failed: {r.status_code} {ctype} {r.text[:400]}")
    with open(DOWNLOADED_XLSX, "wb") as f:
        f.write(r.content)
    print(f"✅ Report downloaded as {DOWNLOADED_XLSX}")
    return DOWNLOADED_XLSX


@retry()
def read_second_tab(xlsx_path: str) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=1)  # 0-based index → second tab
    print(f"✅ Loaded 2nd tab into DataFrame: {df.shape}")
    return df


@retry()
def paste_to_google_sheet(df: pd.DataFrame):
    # Limit to first 80 rows
    df = df.head(80)

    # --- Convert row 4 (index 3) to safe date strings ---
    df_row4 = pd.to_datetime(df.iloc[3], errors='coerce', format="%Y-%m-%d")
    df_row4 = df_row4.dt.strftime('%d-%b-%y')
    df_row4 = df_row4.fillna("")
    df.iloc[3] = df_row4

    # --- Clean inf / NaN ---
    df = df.replace([float('inf'), float('-inf')], "").where(pd.notnull(df), "")

    # --- Authorize Sheets ---
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_JSON, scope)
    gc = gspread.authorize(creds)
    ws = gc.open_by_url(GOOGLE_SHEET_URL).worksheet(SHEET_NAME)

    # --- Clear sheet ---
    ws.clear()

    # --- Prepare values ---
    start_col_idx = 3
    num_cols = df.shape[1]

    def col_letter(idx):
        result = ""
        idx += 1
        while idx > 0:
            idx, rem = divmod(idx - 1, 26)
            result = chr(65 + rem) + result
        return result

    # Formulas
    formulas_row_84 = [
        f"=SUMPRODUCT((MOD(ROW({col_letter(c)}7:{col_letter(c)}80),2)=1)*{col_letter(c)}7:{col_letter(c)}80)"
        for c in range(start_col_idx, num_cols)
    ]
    formulas_row_85 = [
        f"=SUMPRODUCT((MOD(ROW({col_letter(c)}8:{col_letter(c)}81),2)=0)*{col_letter(c)}8:{col_letter(c)}81)"
        for c in range(start_col_idx, num_cols)
    ]

    values = [list(df.columns)] + df.values.tolist()
    row_84_full = [""] * start_col_idx + formulas_row_84
    row_85_full = [""] * start_col_idx + formulas_row_85
    values += [[""] * num_cols] * (84 - len(values))
    values.append(row_84_full)
    values.append(row_85_full)

    # --- Helper: Safe batched update with retries ---
    def safe_update(range_name, chunk, max_attempts=5):
        for attempt in range(1, max_attempts + 1):
            try:
                ws.update(values=chunk, range_name=range_name, value_input_option="USER_ENTERED")
                return
            except APIError as e:
                if "Quota exceeded" in str(e):
                    wait = (2 ** (attempt - 1)) + random.uniform(0, 1)
                    print(f"⚠️ Quota hit, retry {attempt}/{max_attempts} after {wait:.1f}s...")
                    time.sleep(wait)
                else:
                    raise
        raise RuntimeError("Failed to update Google Sheets after retries.")

    # --- Send in chunks of 200 rows ---
    chunk_size = 200
    for i in range(0, len(values), chunk_size):
        chunk = values[i:i + chunk_size]
        start_row = i + 1
        end_row = i + len(chunk)
        safe_update(f"A{start_row}:{col_letter(num_cols-1)}{end_row}", chunk)

    # --- Date format for row 4 ---
    fmt = CellFormat(numberFormat=NumberFormat(type="DATE", pattern="dd-mm-yyyy"))
    format_cell_range(ws, f"D4:{col_letter(num_cols-1)}4", fmt)

    print(f"✅ Pasted {len(df)} rows + formulas + formatting to Google Sheet → {SHEET_NAME}")


# ===== Main =====
def main():
    uid = login()
    csrf = get_csrf()
    onchange(uid)
    wiz_id = web_save(uid)
    report_name = call_button(uid, wiz_id)
    xlsx_path = download_xlsx(uid, csrf, wiz_id, report_name)

    df_tab2 = read_second_tab(xlsx_path)
    paste_to_google_sheet(df_tab2)

    print("🎉 Done.")


if __name__ == "__main__":
    main()
