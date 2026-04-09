import asyncio
import os

from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.watch.watch_pipeline import (
    run_watch_pipeline,
    WatchPipelineFlags,
)
from app.services.telegram_bot_chat_service import telegram_send_message  # type: ignore


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = WatchPipelineFlags(
        run_watch_ingestion=True,
        run_watch_signal_check=True,
        send_telegram_buy_signal = True,
        run_watch_calc_2=False,
        run_watch_calc_3=False,
    )

    await run_watch_pipeline(
        repo=repo,
        flags=flags,
        exchange="EURONEXT",
        exc_name="ams",
        calc_1_open_hour=9,
        calc_1_open_minute=0,
        calc_1_close_hour=10,
        calc_1_close_minute=45,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
        if os.getenv("ENV_TELEGRAM_NOTIF") == "True":
            telegram_send_message(
                title="WATCH PIPELINE run",
                text="✅ AMS watch pipeline has been completed succesfuly",
            )
    except Exception as e:
        print(e)
        if os.getenv("ENV_TELEGRAM_NOTIF") == "True":
            telegram_send_message(
                title="WATCH PIPELINE ERROR!",
                text=f"❌ AMS watch pipeline stopt with error!\nERROR: {e}",
            )