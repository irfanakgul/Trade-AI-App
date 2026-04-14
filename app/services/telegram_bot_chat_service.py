import os
import tempfile
from datetime import datetime

import requests
from dotenv import load_dotenv
from PIL import Image

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

# Main pipeline / system notifications group
TELEGRAM_CHAT_ID_MAIN = os.getenv("TELEGRAM_CHAT_ID_MAIN")

# Dedicated trade execution group
TELEGRAM_CHAT_ID_TRADES = os.getenv("TELEGRAM_CHAT_ID_TRADES")

LOGO_PATH = "app/api/project_media/logo.png"


def _get_chat_id(channel: str) -> str:
    """
    Resolve Telegram target chat ID by channel name.
    """
    channels = {
        "main": TELEGRAM_CHAT_ID_MAIN,
        "pipeline": TELEGRAM_CHAT_ID_MAIN,
        "trades": TELEGRAM_CHAT_ID_TRADES,
        "orders": TELEGRAM_CHAT_ID_TRADES,
    }

    chat_id = channels.get(channel)

    if not chat_id:
        raise ValueError(f"Unknown or undefined Telegram channel: {channel}")

    return chat_id


def _build_message(title: str, text: str) -> str:
    """
    Build styled Telegram HTML message.
    """
    now_str = datetime.now().strftime("%d-%m-%Y %H:%M")

    return f"""
<b>📊 Trade-AI Notification</b>

<b>🔹 {title}</b>

{text}

────────────────────
🕒 <i>Sent at: {now_str}</i>
"""


def _send_with_photo(chat_id: str, message: str) -> requests.Response:
    """
    Send Telegram message with resized logo photo.
    """
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"

    with Image.open(LOGO_PATH) as img:
        max_width = 200
        ratio = max_width / img.width
        new_size = (max_width, int(img.height * ratio))

        img_resized = img.resize(new_size, Image.LANCZOS)

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            temp_path = tmp.name
            img_resized.save(temp_path, format="PNG")

    try:
        with open(temp_path, "rb") as photo:
            data = {
                "chat_id": chat_id,
                "caption": message,
                "parse_mode": "HTML",
            }
            files = {"photo": photo}
            response = requests.post(url, data=data, files=files, timeout=15)
        return response
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def _send_without_photo(chat_id: str, message: str) -> requests.Response:
    """
    Send Telegram message without image.
    """
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    data = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
    }

    return requests.post(url, data=data, timeout=15)


def telegram_send_message(title: str, text: str, channel: str = "main") -> None:
    """
    Generic Telegram sender.

    channel options:
    - main / pipeline
    - trades / orders
    """
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is not set in environment variables.")

    chat_id = _get_chat_id(channel)
    message = _build_message(title, text)

    try:
        response = _send_with_photo(chat_id, message)

        if response.status_code == 200:
            print(f"✅ TELEGRAM MESSAGE SENT! channel={channel}")
        else:
            print(f"❌ TELEGRAM ERROR ({channel}): {response.text}")

    except FileNotFoundError:
        print("⚠️ Logo not found, sending without image...")
        response = _send_without_photo(chat_id, message)

        if response.status_code == 200:
            print(f"✅ TELEGRAM MESSAGE SENT (no logo)! channel={channel}")
        else:
            print(f"❌ TELEGRAM ERROR ({channel}): {response.text}")