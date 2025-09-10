import os
import json
import re
import time
from datetime import datetime, timedelta

import requests
import pandas as pd

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError
from dotenv import load_dotenv

# gspread_formatting for row formatting
from gspread_formatting import format_cell_range, CellFormat, NumberFormat

load_dotenv()

# ========== CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")
DB = os.getenv("ODOO_DB")

MODEL = "attendance.pdf.report"
REPORT_BUTTON_METHOD = "action_generate_xlsx_report"

REPORT_TYPE = "ot_analysis"
DATE_FROM = "2025-08-01"
DATE_TO = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

company_id = 4

GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1W9qXHRPrSffHfcQvBxrAK2fTAqne5ohqf0tIn1oMujM/edit?gid=1647682121#gid=1647682121"
SHEET_NAME = "Sheet3"
SERVICE_ACCOUNT_JSON = "credentials.json"

DOWNLOADED_XLSX = f"{REPORT_TYPE}_{DATE_FROM}_to_{DATE_TO}_cat20.xlsx"

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})


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
        raise RuntimeError("Could not extract CSRF token from /web")
    csrf = m.group(1)
    print("✅ CSRF token =", csrf)
    return csrf


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


def web_save(uid):
    url = f"{ODOO_URL}/web/dataset/call_kw/{MODEL}/web_save"
    payload = {
        "id": 35,
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
                "category_id": 42,
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


def download_xlsx(uid, csrf_token, wizard_id, report_name):
    download_url = f"{ODOO_URL}/report/download"
    options = {
        "date_from": DATE_FROM,
        "date_to": DATE_TO,
        "mode_type": "category",
        "mode_company_id": False,
        "department_id": False,
        "category_id": 42,
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


def read_second_tab(xlsx_path: str) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=1)
    print(f"✅ Loaded 2nd tab into DataFrame: {df.shape}")
    return df


def format_row4_as_date(ws, num_cols, max_retries=5):
    start_col_idx = 3  # column D
    def col_letter(idx):
        result = ""
        idx += 1
        while idx > 0:
            idx, remainder = divmod(idx - 1, 26)
            result = chr(65 + remainder) + result
        return result

    start = col_letter(start_col_idx)
    end = col_letter(num_cols - 1)
    cell_range = f"{start}4:{end}4"

    fmt = CellFormat(numberFormat=NumberFormat(type='DATE', pattern='dd-mm-yyyy'))

    # Retry wrapper for transient 429s
    for attempt in range(1, max_retries + 1):
        try:
            format_cell_range(ws, cell_range, fmt)
            print(f"✅ Formatted row 4 range {cell_range} as dd-mm-yyyy")
            return
        except APIError as e:
            # If rate limit, backoff and retry
            err_text = str(e)
            if "429" in err_text or "quota" in err_text.lower():
                wait = 2 ** attempt
                print(f"⚠️ format_cell_range rate-limited (attempt {attempt}). Sleeping {wait}s and retrying...")
                time.sleep(wait)
                continue
            raise


def paste_to_google_sheet(df: pd.DataFrame):
    # Limit to first 80 rows
    df = df.head(80)

    # --- Convert row 4 (index 3) to string safely ---
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        df_row4 = pd.to_datetime(df.iloc[3], errors="coerce")

    df_row4 = df_row4.dt.strftime("%d-%b-%y").fillna("")
    df.iloc[3] = df_row4

    # --- Replace inf/-inf and remaining NaN ---
    df = df.replace([float("inf"), float("-inf")], "").where(pd.notnull(df), "")

    # --- Authorize Google Sheets ---
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_JSON, scope)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(GOOGLE_SHEET_URL)
    ws = sh.worksheet(SHEET_NAME)

    # --- Prepare values ---
    values = [list(df.columns)] + df.values.tolist()

    # Overwrite all data in one go
    ws.update(values=values, range_name="A1", value_input_option="USER_ENTERED")
    print(f"✅ Pasted {len(df)} rows to Google Sheet → {SHEET_NAME}")

    # --- Prepare formulas ---
    start_col_idx = 3
    num_cols = df.shape[1]

    def col_letter(idx):
        result = ""
        idx += 1
        while idx > 0:
            idx, rem = divmod(idx - 1, 26)
            result = chr(65 + rem) + result
        return result

    formulas_row_84 = [
        f"=SUMPRODUCT((MOD(ROW({col_letter(c)}7:{col_letter(c)}80),2)=1)*{col_letter(c)}7:{col_letter(c)}80)"
        for c in range(start_col_idx, num_cols)
    ]
    formulas_row_85 = [
        f"=SUMPRODUCT((MOD(ROW({col_letter(c)}8:{col_letter(c)}81),2)=0)*{col_letter(c)}8:{col_letter(c)}81)"
        for c in range(start_col_idx, num_cols)
    ]

    if formulas_row_84:
        ws.update(
            values=[formulas_row_84, formulas_row_85],
            range_name=f"D84:{col_letter(start_col_idx + len(formulas_row_84)-1)}85",
            value_input_option="USER_ENTERED"
        )
        print("✅ Applied SUMPRODUCT formulas in rows 84 and 85")

    # --- Format row 4 (D4:last) as date (single batch_update) ---
    end_col = col_letter(num_cols - 1)
    requests = [{
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": 3,  # row 4 (0-based)
                "endRowIndex": 4,
                "startColumnIndex": 3,  # col D
                "endColumnIndex": num_cols
            },
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {"type": "DATE", "pattern": "dd-mm-yyyy"}
                }
            },
            "fields": "userEnteredFormat.numberFormat"
        }
    }]
    sh.batch_update({"requests": requests})
    print(f"✅ Formatted row 4 D4:{end_col}4 as dd-mm-yyyy")


    # Format row 4 as date (single formatting call, retried inside)
    format_row4_as_date(ws, num_cols)


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
