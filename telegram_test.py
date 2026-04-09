from app.services.telegram_bot_chat_service import telegram_send_message  # type: ignore


telegram_send_message(
    title="BUY EXECUTED",
    text="Symbol: THYAO\nPrice: 342.75\nQty: 10")