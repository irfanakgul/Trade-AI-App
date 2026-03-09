import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import pandas as pd
import numpy as np

DEFAULT_SPREADSHEET_ID = "1jdJkgQo1Y5tLFOUjI8Y_Ole8P4VArDtP0H-n5FJ-Zg4"
DEFAULT_CREDENTIAL_PATH = "google/tokens/trade-ai-app-google_service_acc.json"


def fn_write_to_google(
    df: pd.DataFrame,
    sheet_name: str,
    replace_or_append: str = "append",
    spreadsheet_id: str = DEFAULT_SPREADSHEET_ID,
    path: str = DEFAULT_CREDENTIAL_PATH
):

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(path, scopes=scopes)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(spreadsheet_id)

    sheets = spreadsheet.worksheets()
    sheet_names = [s.title for s in sheets]

    df_clean = df.copy()
    df_clean = df_clean.replace([np.nan, np.inf, -np.inf], None)

    for col in df_clean.columns:
        if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
            df_clean[col] = df_clean[col].astype(str)

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
                cols="50"
            )

        set_with_dataframe(worksheet, df_clean, include_index=False)

        print(f"<--- Google Sheets WRITE (replace) | sheet={sheet_name} | rows={len(df_clean)} --->", flush=True)

    # -------------------------
    # APPEND MODE
    # -------------------------
    elif replace_or_append == "append":

        if sheet_name not in sheet_names:

            worksheet = spreadsheet.add_worksheet(
                title=sheet_name,
                rows="200",
                cols="50"
            )

            set_with_dataframe(worksheet, df_clean, include_index=False)

            print(f"<--- Google Sheets WRITE (new sheet created) | sheet={sheet_name} | rows={len(df_clean)} --->", flush=True)

        else:

            worksheet = spreadsheet.worksheet(sheet_name)

            existing_data = worksheet.get_all_values()

            if not existing_data:
                set_with_dataframe(worksheet, df_clean, include_index=False)

            else:
                data = df_clean.values.tolist()
                next_row = len(existing_data) + 1
                worksheet.insert_rows(data, row=next_row)

            print(f"<--- Google Sheets WRITE (append) | sheet={sheet_name} | rows={len(df_clean)} --->", flush=True)

    else:
        raise ValueError("replace_or_append must be 'replace' or 'append'")
    

def fn_read_from_google(
    sheet_name: str,
    spreadsheet_id: str = DEFAULT_SPREADSHEET_ID,
    path: str = DEFAULT_CREDENTIAL_PATH
) -> pd.DataFrame:

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(path, scopes=scopes)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(spreadsheet_id)

    try:

        worksheet = spreadsheet.worksheet(sheet_name)

        data = worksheet.get_all_values()

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data[1:], columns=data[0])

        print(f"<--- Google Sheets READ | sheet={sheet_name} | rows={len(df)} --->", flush=True)

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
    path: str = DEFAULT_CREDENTIAL_PATH
):

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(path, scopes=scopes)
    client = gspread.authorize(creds)

    spreadsheet = client.open_by_key(spreadsheet_id)

    worksheet = spreadsheet.worksheet(sheet_name)

    all_values = worksheet.get_all_values()

    header = all_values[0]
    rows = all_values[1:]

    try:
        filter_col_idx = header.index(filter_col)
        target_col_idx = header.index(target_col)

    except ValueError as e:
        raise Exception(f"Column not found: {e}")

    updated = False

    for row_idx, row in enumerate(rows, start=2):

        if row[filter_col_idx] == str(filter_value):

            worksheet.update_cell(row_idx, target_col_idx + 1, new_value)

            updated = True

            print(
                f"<--- Cell updated | row={row_idx} | {target_col}={new_value} --->",
                flush=True
            )

            break

    if not updated:
        print(f"<--- No match found for {filter_col}={filter_value} --->", flush=True)