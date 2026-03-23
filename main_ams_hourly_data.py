import asyncio
from dotenv import load_dotenv
from app.services.telegram_bot_chat_service import telegram_send_message

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

        # flags INGESTION
        ingest=True,
        main_provider="tvdatafeed",
        alternative_provider="yahooquery",
        start_date="2024-01-01",
        safe_days_back=1,
        main_provider_retries=2,
        max_concurrent_symbols=4,

        # flags SYNC
        sync_archive_to_working = True,
        
        # flags TRIM365
        trim_history = True,

        # flags ind build dataset
        build_focus_dataset = True,
        
        # flags ind build dataset   
        run_dq=True,
        dq_elemination = True,

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
        run_combined_indicators = True,
    )

    await run_euronext_hourly_data_pipeline(repo, flags,exchange='EURONEXT')


if __name__ == "__main__":
    asyncio.run(main())
    telegram_send_message(
        title="PIPELINE run",
        text="EURONEXT pipeline has been done!")