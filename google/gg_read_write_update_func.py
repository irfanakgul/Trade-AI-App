from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
import math

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import pandas as pd
import numpy as np


DEFAULT_SPREADSHEET_ID = "1jdJkgQo1Y5tLFOUjI8Y_Ole8P4VArDtP0H-n5FJ-Zg4"
DEFAULT_CREDENTIAL_PATH = "google/tokens/trade-ai-app-google_service_acc.json"


def _build_google_client(path: str):
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_file(path, scopes=scopes)
    client = gspread.authorize(creds)
    return client


def _json_default_serializer(obj):
    if isinstance(obj, pd.Timestamp):
        if pd.isna(obj):
            return ""
        return obj.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(obj, date):
        return obj.strftime("%Y-%m-%d")

    if isinstance(obj, Decimal):
        return float(obj)

    if isinstance(obj, np.integer):
        return int(obj)

    if isinstance(obj, np.floating):
        value = float(obj)
        if math.isnan(value) or math.isinf(value):
            return ""
        return value

    if isinstance(obj, np.bool_):
        return bool(obj)

    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8")
        except Exception:
            return str(obj)

    return str(obj)


def _to_google_cell(value):
    import pandas as pd
    import numpy as np
    import math
    import json
    from datetime import datetime, date
    from decimal import Decimal

    # 🔥 CRITICAL: handle ALL null types first (NaT, NaN, None)
    if pd.isna(value):
        return ""

    # -------------------------
    # DATETIME TYPES
    # -------------------------
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    # -------------------------
    # NUMERIC TYPES
    # -------------------------
    if isinstance(value, (int, np.integer)):
        return int(value)

    if isinstance(value, (float, np.floating)):
        value = float(value)
        if math.isnan(value) or math.isinf(value):
            return ""
        return value

    if isinstance(value, Decimal):
        return float(value)

    # -------------------------
    # BOOLEAN
    # -------------------------
    if isinstance(value, (bool, np.bool_)):
        return bool(value)

    # -------------------------
    # BYTES
    # -------------------------
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return str(value)

    # -------------------------
    # COMPLEX STRUCTURES (JSON)
    # -------------------------
    if isinstance(value, (dict, list, tuple, set)):
        try:
            return json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            return str(value)

    # -------------------------
    # STRING
    # -------------------------
    if isinstance(value, str):
        return value

    # -------------------------
    # FALLBACK (object dtype safety)
    # -------------------------
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)


def _normalize_dataframe_for_google(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()

    if df.empty:
        return df.copy()

    df_clean = df.copy()

    df_clean = df_clean.replace([np.inf, -np.inf], None)

    for col in df_clean.columns:
        df_clean[col] = df_clean[col].map(_to_google_cell)

    return df_clean


def _worksheet_to_dataframe(worksheet) -> pd.DataFrame:
    data = worksheet.get_all_values()

    if not data:
        return pd.DataFrame()

    return pd.DataFrame(data[1:], columns=data[0])


def fn_write_to_google(
    df: pd.DataFrame,
    sheet_name: str,
    replace_or_append: str = "append",
    spreadsheet_id: str = DEFAULT_SPREADSHEET_ID,
    path: str = DEFAULT_CREDENTIAL_PATH,
):
    client = _build_google_client(path)
    spreadsheet = client.open_by_key(spreadsheet_id)

    sheets = spreadsheet.worksheets()
    sheet_names = [s.title for s in sheets]

    df_clean = _normalize_dataframe_for_google(df)

    # -------------------------
    # REPLACE MODE
    # -------------------------
    if replace_or_append == "replace":
        if sheet_name in sheet_names:
            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.clear()
        else:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows="200",
                cols="50",
            )

        set_with_dataframe(worksheet, df_clean, include_index=False)

        print(
            f"<--- Google Sheets WRITE (replace) | sheet={sheet_name} | rows={len(df_clean)} --->",
            flush=True,
        )

    # -------------------------
    # APPEND MODE
    # -------------------------
    elif replace_or_append == "append":
        if sheet_name not in sheet_names:
            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows="200",
                cols="50",
            )

            set_with_dataframe(worksheet, df_clean, include_index=False)

            print(
                f"<--- Google Sheets WRITE (new sheet created) | sheet={sheet_name} | rows={len(df_clean)} --->",
                flush=True,
            )

        else:
            worksheet = spreadsheet.worksheet(sheet_name)
            existing_data = worksheet.get_all_values()

            if not existing_data:
                set_with_dataframe(worksheet, df_clean, include_index=False)
            else:
                data = df_clean.values.tolist()
                next_row = len(existing_data) + 1

                if data:
                    worksheet.insert_rows(data, row=next_row)

            print(
                f"<--- Google Sheets WRITE (append) | sheet={sheet_name} | rows={len(df_clean)} --->",
                flush=True,
            )

    else:
        raise ValueError("replace_or_append must be 'replace' or 'append'")


def fn_read_from_google(
    sheet_name: str,
    spreadsheet_id: str = DEFAULT_SPREADSHEET_ID,
    path: str = DEFAULT_CREDENTIAL_PATH,
) -> pd.DataFrame:
    client = _build_google_client(path)
    spreadsheet = client.open_by_key(spreadsheet_id)

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        df = _worksheet_to_dataframe(worksheet)

        print(
            f"<--- Google Sheets READ | sheet={sheet_name} | rows={len(df)} --->",
            flush=True,
        )

        return df

    except gspread.exceptions.WorksheetNotFound:
        print(f"<--- Sheet '{sheet_name}' not found --->", flush=True)
        return pd.DataFrame()


def fn_update_cell_googlesheet(
    sheet_name,
    filter_col,
    filter_value,
    target_col,
    new_value,
    spreadsheet_id: str = DEFAULT_SPREADSHEET_ID,
    path: str = DEFAULT_CREDENTIAL_PATH,
):
    client = _build_google_client(path)
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet(sheet_name)

    all_values = worksheet.get_all_values()

    if not all_values:
        print(f"<--- Sheet '{sheet_name}' is empty --->", flush=True)
        return

    header = all_values[0]
    rows = all_values[1:]

    try:
        filter_col_idx = header.index(filter_col)
        target_col_idx = header.index(target_col)
    except ValueError as e:
        raise Exception(f"Column not found: {e}")

    filter_value_normalized = str(_to_google_cell(filter_value))
    new_value_normalized = _to_google_cell(new_value)

    updated = False

    for row_idx, row in enumerate(rows, start=2):
        cell_value = row[filter_col_idx] if filter_col_idx < len(row) else ""

        if cell_value == filter_value_normalized:
            worksheet.update_cell(row_idx, target_col_idx + 1, new_value_normalized)
            updated = True

            print(
                f"<--- Cell updated | row={row_idx} | {target_col}={new_value_normalized} --->",
                flush=True,
            )
            break

    if not updated:
        print(
            f"<--- No match found for {filter_col}={filter_value_normalized} --->",
            flush=True,
        )