import os
import json
import re
from datetime import datetime
import string
import requests
import pandas as pd

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ========= CONFIG ==========
ODOO_URL = "https://taps.odoo.com"
USERNAME = "supply.chain3@texzipperbd.com"
PASSWORD = "@Shanto@86"
DB = "masbha-tex-taps-master-2093561"

MODEL = "attendance.pdf.report"
REPORT_BUTTON_METHOD = "action_generate_xlsx_report"

REPORT_TYPE = "ot_analysis"        # e.g. "ot_analysis", "job_card"
DATE_FROM = "2025-08-01"
DATE_TO   = datetime.now().strftime("%Y-%m-%d")

# Company context (kept even though we’re using category mode)
company_id = 3   # 1 = Zipper, 3 = Metal Trims

# ===== Google Sheets =====
GOOGLE_SHEET_URL     = "https://docs.google.com/spreadsheets/d/1clIzaVWDNcwGIrTNCNIDXmeUf0wEnH3NrWfVZYeoa4Q/edit?gid=46242566"
SHEET_NAME           = "Sheet2"
SERVICE_ACCOUNT_JSON = "credentials.json"  # this file will exist in Actions

# Local output
DOWNLOADED_XLSX = f"{REPORT_TYPE}_{DATE_FROM}_to_{DATE_TO}_cat20.xlsx"

# ========= START SESSION ==========
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
                "mode_type": "category",     # category mode per your JSON
                "employee_id": False,
                "mode_company_id": False,    # cleared in category mode
                "category_id": 20,           # B-Worker
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
        "category_id": 20,
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
    """
    Reads ONLY the 2nd worksheet (index=1) from the downloaded Excel file.
    """
    df = pd.read_excel(xlsx_path, sheet_name=1)  # 0-based index → second tab
    print(f"✅ Loaded 2nd tab into DataFrame: {df.shape}")
    return df


from gspread_formatting import set_number_format, NumberFormat

def format_row4_as_date(ws, num_cols):
    """
    Format row 4 from column D to last column with data as date dd-mm-yyyy.
    """
    start_col_idx = 3  # D=0-based index 3

    for col_idx in range(start_col_idx, num_cols):
        col_letter = ""
        idx = col_idx
        while idx >= 0:
            col_letter = chr(idx % 26 + ord('A')) + col_letter
            idx = idx // 26 - 1

        cell_range = f"{col_letter}4"
        set_number_format(ws, cell_range, NumberFormat(type='DATE', pattern='dd-mm-yyyy'))

    print(f"✅ Formatted row 4 from D to last column ({col_letter}) as dd-mm-yyyy")


def paste_to_google_sheet(df: pd.DataFrame):
    # Limit to first 47 rows
    df = df.head(47)
    df = df.astype(object).where(pd.notnull(df), "")

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_JSON, scope)
    gc = gspread.authorize(creds)

    sh = gc.open_by_url(GOOGLE_SHEET_URL)
    ws = sh.worksheet(SHEET_NAME)

    # Clear sheet
    ws.clear()

    # Paste DataFrame
    values = [list(df.columns)] + df.values.tolist()
    ws.update(values=values, range_name="A1", value_input_option="RAW")
    print(f"✅ Pasted up to 47 rows to Google Sheet → {SHEET_NAME}")

    # Apply formulas in row 51 and 52 starting from D
    start_col_idx = 3  # D=3 (0-based)
    num_cols = df.shape[1]

    def col_letter(idx):
        """Convert 0-based index to Excel-style letter, supports >Z."""
        result = ""
        while idx >= 0:
            result = chr(idx % 26 + ord('A')) + result
            idx = idx // 26 - 1
        return result

    formulas_row_51 = []
    formulas_row_52 = []
    for col_idx in range(start_col_idx, num_cols):
        letter = col_letter(col_idx)
        formulas_row_51.append(f"=SUMPRODUCT((MOD(ROW({letter}7:{letter}47),2)=1)*{letter}7:{letter}47)")
        formulas_row_52.append(f"=SUMPRODUCT((MOD(ROW({letter}8:{letter}48),2)=0)*{letter}8:{letter}48)")

    if formulas_row_51:
        ws.update(values=[formulas_row_51], range_name=f"D51:{col_letter(start_col_idx + len(formulas_row_51)-1)}51", value_input_option="USER_ENTERED")
        ws.update(values=[formulas_row_52], range_name=f"D52:{col_letter(start_col_idx + len(formulas_row_52)-1)}52", value_input_option="USER_ENTERED")
        print("✅ Applied SUMPRODUCT formulas in rows 51 (odd) and 52 (even)")

    # Format row 4 as date
    format_row4_as_date(ws, df.shape[1])



def main():
    uid = login()
    csrf = get_csrf()
    onchange(uid)         # not strictly required, but keeps parity with UI
    wiz_id = web_save(uid)
    report_name = call_button(uid, wiz_id)
    xlsx_path = download_xlsx(uid, csrf, wiz_id, report_name)

    # Read 2nd tab and paste to Google Sheets
    df_tab2 = read_second_tab(xlsx_path)
    paste_to_google_sheet(df_tab2)

    print("🎉 Done.")


if __name__ == "__main__":
    main()
