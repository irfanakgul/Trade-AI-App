import asyncio
import os
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.nasdaq_hourly_data_pipeline import (
    run_nasdaq_hourly_data_pipeline,
    NasdaqHourlyDataPipelineFlags,
)
from app.services.telegram_bot_chat_service import telegram_send_message # type: ignore

exchange = "NASDAQ"

async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = NasdaqHourlyDataPipelineFlags(
        
        #=================================================================#
        # DATA INGESTION
        #=================================================================#
        ingest=True,
        sync_archive_to_working = True,
        trim_history = True,
        build_focus_dataset= True,
        run_dq = True,

        #=================================================================#
        # INDICATOR FLAGS
        #=================================================================#

        bar_status=True,
        run_frvp=True,
        run_convert_daily = True,
        run_ema_ind = True,
        run_vwap_ind = True,
        run_rsi_ind = True,
        run_mfi_ind = True,
        run_pivot_ind = True,
        run_source_end_dates_ind = True,
        run_combined_indicators = True
        
    )

    await run_nasdaq_hourly_data_pipeline(repo, flags,exchange=exchange)

if __name__ == "__main__":
    try:
        asyncio.run(main())
        print(os.getenv("ENV_TELEGRAM_NOTIF"))
        if os.getenv("ENV_TELEGRAM_NOTIF")=="True":
            telegram_send_message(
                title="PIPELINE run",
                text=f"✅ {exchange} pipeline has been completed succesfuly")
    except Exception as e:
        if os.getenv("ENV_TELEGRAM_NOTIF")=="True":
            telegram_send_message(
                title="PIPELINE ERROR!",
                text=f"❌ {exchange} pipeline stopt with error!\nERROR: {e}")
        