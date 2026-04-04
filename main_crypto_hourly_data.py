import asyncio,os
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.crypto_hourly_data_pipeline import (
    run_binance_hourly_data_pipeline,
    BinanceHourlyDataPipelineFlags,
)
from app.services.telegram_bot_chat_service import telegram_send_message # type: ignore


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = BinanceHourlyDataPipelineFlags(

        #=================================================================#
        # DATA INGESTION
        #=================================================================#

        #data pull
        ingest=True,
        main_provider="binance_api",
        alternative_provider="tvdatafeed",
        start_date="2024-01-01",
        safe_days_back=1,
        main_provider_retries=2,
        max_concurrent_symbols=3,

        # prep
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
        run_combined_indicators = True

    )

    await run_binance_hourly_data_pipeline(repo, flags,'BINANCE')

if __name__ == "__main__":
    try:
        asyncio.run(main())
        if os.getenv("ENV_TELEGRAM_NOTIF")=="True":
            telegram_send_message(
                title="PIPELINE run",
                text="✅ CRYPTO pipeline has been completed succesfuly")
    except Exception as e:
        if os.getenv("ENV_TELEGRAM_NOTIF")=="True":
            telegram_send_message(
                title="PIPELINE ERROR!",
                text=f"❌ CRYPTO pipeline stopt with error!\nERROR: {e}")