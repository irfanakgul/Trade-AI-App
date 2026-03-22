import requests
import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
GRUP_ID = os.getenv("GRUP_ID")

def telegram_message_text(text):
    """Telegram grubuna mesaj gönder"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    
    veri = {
        "chat_id": GRUP_ID,
        "text": text
    }
    
    response = requests.post(url, data=veri)
    
    if response.status_code == 200:
        print("✅ MESSAGE SENT!")
        print(response.json())
    else:
        print(f"❌ HATA: {response.text}")