import requests
import os
import tempfile
from datetime import datetime
from dotenv import load_dotenv
from PIL import Image

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
GRUP_ID = os.getenv("GRUP_ID")

LOGO_PATH = "app/api/project_media/logo.png"


def telegram_send_message(title: str, text: str):
    """
    Styled Telegram message with:
    - Smaller logo (resized)
    - Header
    - Body
    - Footer (timestamp)
    """

    now_str = datetime.now().strftime("%d-%m-%Y %H:%M")

    message = f"""
<b>📊 Trade-AI Notification</b>

<b>🔹 {title}</b>

{text}

────────────────────
🕒 <i>Sent at: {now_str}</i>
"""

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"

    try:
        # 🔽 LOGO RESIZE
        with Image.open(LOGO_PATH) as img:
            max_width = 200  # 🔥 burayı değiştirerek boyutu ayarlayabilirsin

            ratio = max_width / img.width
            new_size = (max_width, int(img.height * ratio))

            img_resized = img.resize(new_size, Image.LANCZOS)

            # temp file oluştur
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                temp_path = tmp.name
                img_resized.save(temp_path, format="PNG")

        with open(temp_path, "rb") as photo:
            data = {
                "chat_id": GRUP_ID,
                "caption": message,
                "parse_mode": "HTML"
            }

            files = {
                "photo": photo
            }

            response = requests.post(url, data=data, files=files)

        # temp dosyayı sil
        os.remove(temp_path)

        if response.status_code == 200:
            print("✅ 💬 TELEGRAM MESSAGE SENT!")
        else:
            print(f"❌ ERROR: {response.text}")

    except FileNotFoundError:
        print("⚠️ Logo not found, sending without image...")

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

        data = {
            "chat_id": GRUP_ID,
            "text": message,
            "parse_mode": "HTML"
        }

        response = requests.post(url, data=data)

        if response.status_code == 200:
            print("✅ TELEGRAM MESSAGE SENT (no logo)")
        else:
            print(f"❌ ERROR: {response.text}")