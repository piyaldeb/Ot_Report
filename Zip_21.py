import os
import json
import re
import time
import random
from datetime import datetime, timedelta

import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from gspread_formatting import format_cell_range, CellFormat, NumberFormat

# ========= CONFIG ==========
load_dotenv()

ODOO_URL = os.getenv("ODOO_URL")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")
DB = os.getenv("ODOO_DB")

MODEL = "attendance.pdf.report"
REPORT_BUTTON_METHOD = "action_generate_xlsx_report"

REPORT_TYPE = "ot_analysis"
DATE_FROM = "2025-08-01"
DATE_TO = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
company_id = 1   # 1 = Zipper, 3 = Metal Trims

# ===== Google Sheets =====
GOOGLE_SHEET_URL     = "https://docs.google.com/spreadsheets/d/1W9qXHRPrSffHfcQvBxrAK2fTAqne5ohqf0tIn1oMujM/edit?gid=1647682121#gid=1647682121"
SHEET_NAME           = "Sheet2"
SERVICE_ACCOUNT_JSON = "credentials.json"

DOWNLOADED_XLSX = f"{REPORT_TYPE}_{DATE_FROM}_to_{DATE_TO}_cat20.xlsx"

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

# ========= RETRY HELPER ==========
def with_retry(func, *args, retries=5, base_sleep=5, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt < retries - 1:
                wait_time = base_sleep * (2 ** attempt) + random.uniform(0, 2)
                print(f"⚠️ {func.__name__} failed ({e}), retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                print(f"❌ {func.__name__} failed after {retries} attempts")
                raise

# ========= ODOO FUNCTIONS ==========
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

def get_csrf():
    r = session.get(f"{ODOO_URL}/web", timeout=60)
    m = re.search(r'csrf_token\s*:\s*"([^"]+)"', r.text)
    if not m:
        raise RuntimeError("Could not extract CSRF token")
    csrf = m.group(1)
    print("✅ CSRF token =", csrf)
    return csrf

def onchange(uid):
    url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/onchange"
    payload = {
        "id": 1, "jsonrpc": "2.0", "method": "call",
        "params": {
            "model": MODEL, "method": "onchange",
            "args": [[], {}, [], {"report_type": {}, "date_from": {}, "date_to": {}}],
            "kwargs": {"context": {"lang": "en_US", "tz": "Asia/Dhaka", "uid": uid,
                                   "allowed_company_ids": [company_id], "default_is_company": False}}
        }
    }
    r = session.post(url, json=payload, timeout=60)
    r.raise_for_status()
    val = r.json().get("result", {}).get("value", {})
    print("✅ Onchange defaults:", val)
    return val

def web_save(uid):
    url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/web_save"
    payload = {
        "id": 3, "jsonrpc": "2.0", "method": "call",
        "params": {
            "model": MODEL, "method": "web_save",
            "args": [[], {"report_type": REPORT_TYPE, "date_from": DATE_FROM, "date_to": DATE_TO,
                          "is_company": False, "mode_type": "category", "category_id": 30,
                          "company_all": "allcompany"}],
            "kwargs": {"context": {"lang": "en_US", "tz": "Asia/Dhaka", "uid": uid,
                                   "allowed_company_ids": [company_id], "default_is_company": False}}
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

def call_button(uid, wizard_id):
    url = f"{ODOO_URL}/web/dataset/call_button"
    payload = {
        "id": 4, "jsonrpc": "2.0", "method": "call",
        "params": {"model": MODEL, "method": REPORT_BUTTON_METHOD,
                   "args": [[wizard_id]],
                   "kwargs": {"context": {"lang": "en_US", "tz": "Asia/Dhaka", "uid": uid,
                                          "allowed_company_ids": [company_id], "default_is_company": False}}}
    }
    r = session.post(url, json=payload, timeout=120)
    r.raise_for_status()
    res = r.json()
    report_name = res.get("result", {}).get("report_name")
    if not report_name:
        raise RuntimeError(f"Report button failed: {res}")
    print("✅ Report generated:", report_name)
    return report_name

def download_xlsx(uid, csrf_token, wizard_id, report_name):
    download_url = f"{ODOO_URL}/report/download"
    options = {"date_from": DATE_FROM, "date_to": DATE_TO, "mode_type": "category", "category_id": 30,
               "report_type": REPORT_TYPE, "company_all": "allcompany"}
    context = {"lang": "en_US", "tz": "Asia/Dhaka", "uid": uid, "allowed_company_ids": [company_id],
               "active_model": MODEL, "active_id": wizard_id, "active_ids": [wizard_id]}
    report_path = f"/report/xlsx/{report_name}?options={json.dumps(options)}&context={json.dumps(context)}"
    payload = {"data": json.dumps([report_path, "xlsx"]), "context": json.dumps(context),
               "token": "dummy", "csrf_token": csrf_token}
    headers = {"X-CSRF-Token": csrf_token, "Referer": f"{ODOO_URL}/web"}
    r = session.post(download_url, data=payload, headers=headers, timeout=180)
    r.raise_for_status()
    with open(DOWNLOADED_XLSX, "wb") as f:
        f.write(r.content)
    print(f"✅ Report downloaded as {DOWNLOADED_XLSX}")
    return DOWNLOADED_XLSX

# ========= GOOGLE SHEETS ==========
def format_row4_as_date(ws, num_cols):
    start = "D"
    end = chr(64 + num_cols)  # simple for <26 columns
    cell_range = f"{start}4:{end}4"
    fmt = CellFormat(numberFormat=NumberFormat(type='DATE', pattern='dd-mm-yyyy'))
    format_cell_range(ws, cell_range, fmt)
    print(f"✅ Formatted row 4 {cell_range} as date")

def paste_to_google_sheet(df: pd.DataFrame):
    df = df.head(80)
    df_row4 = pd.to_datetime(df.iloc[3], errors="coerce", format="%Y-%m-%d")
    df.iloc[3] = df_row4.dt.strftime("%d-%b-%y").fillna("")
    df = df.replace([float("inf"), float("-inf")], "").where(pd.notnull(df), "")

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_JSON, scope)
    gc = gspread.authorize(creds)
    ws = gc.open_by_url(GOOGLE_SHEET_URL).worksheet(SHEET_NAME)

    with_retry(ws.clear)

    values = [list(df.columns)] + df.values.tolist()
    ws.update(values, "A1", value_input_option="USER_ENTERED")
    print("✅ Data updated")

    format_row4_as_date(ws, df.shape[1])

# ========= MAIN ==========
def main():
    uid = with_retry(login)
    csrf = with_retry(get_csrf)
    with_retry(onchange, uid)
    wiz_id = with_retry(web_save, uid)
    report_name = with_retry(call_button, uid, wiz_id)
    xlsx_path = with_retry(download_xlsx, uid, csrf, wiz_id, report_name)

    df_tab2 = pd.read_excel(xlsx_path, sheet_name=1)
    print("✅ Loaded tab2", df_tab2.shape)
    with_retry(paste_to_google_sheet, df_tab2)

    print("🎉 Done.")

if __name__ == "__main__":
    main()
