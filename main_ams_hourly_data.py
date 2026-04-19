import asyncio
from dotenv import load_dotenv
from app.services.telegram_bot_chat_service import telegram_send_message # type: ignore
import os
from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.ams_hourly_data_pipeline import ( # type: ignore
    run_euronext_hourly_data_pipeline,
    EuronextHourlyDataPipelineFlags,
)


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = EuronextHourlyDataPipelineFlags(

        #=================================================================#
        # DATA INGESTION
        #=================================================================#
        ingest=False,
        sync_archive_to_working = False,
        trim_history = False,
        build_focus_dataset= False,
        run_dq = False,

        #=================================================================#
        # INDICATOR FLAGS
        #=================================================================#

        bar_status=False,
        run_frvp=False,
        run_convert_daily = False,
        run_ema_ind = True,
        run_vwap_ind = True,
        run_rsi_ind = True,
        run_mfi_ind = True,
        run_pivot_ind = True,
        run_source_end_dates_ind = True,
        run_combined_indicators = True,
        run_master_score = True,
        run_master_final_combined = True,
        run_watch_realised_close = True


    )

    await run_euronext_hourly_data_pipeline(repo, flags,exchange='EURONEXT')

# asyncio.run(main())

if __name__ == "__main__":
    try:
        asyncio.run(main())
        if os.getenv("ENV_TELEGRAM_NOTIF")=="True":
            telegram_send_message(
                title="PIPELINE run",
                text="✅ AMS pipeline has been completed succesfuly")
    except Exception as e:
        if os.getenv("ENV_TELEGRAM_NOTIF")=="True":
            telegram_send_message(
                title="PIPELINE ERROR!",
                text=f"❌ AMS pipeline stopt with error!\nERROR: {e}")