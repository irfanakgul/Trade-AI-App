import gspread
from google.oauth2.service_account import Credentials

def test_google_connection(spreadsheet_id, path="/Users/yasinyilmaz/Desktop/Trade-AI-App/google/tokens/trade-ai-app-google_service_acc.json"):
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(path, scopes=scopes)
    client = gspread.authorize(creds)

    sh = client.open_by_key(spreadsheet_id)
    print("Connected to:", sh.title, flush=True)