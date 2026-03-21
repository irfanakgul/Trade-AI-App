import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.nasdaq_hourly_data_pipeline import (
    run_nasdaq_hourly_data_pipeline,
    NasdaqHourlyDataPipelineFlags,
)


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = NasdaqHourlyDataPipelineFlags(
        ingest=True,
        main_provider="tvdatafeed",
        alternative_provider="not_implemented",
        enable_fallback=True,
        use_db_last_timestamp=True,
        start_date="2024-01-01",
        safe_days_back=1,
        main_provider_retries=3,
        max_concurrent_symbols=3,

        # flags SYNC
        sync_archive_to_working = True,
        
        # flags TRIM365
        trim_history = True,

        #flags indicator focus dataset prep
        build_focus_dataset=True,
        # dq
        run_dq = True,

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
        run_combined_indicators = True,
    )

    await run_nasdaq_hourly_data_pipeline(repo, flags,'NASDAQ')

if __name__ == "__main__":
    asyncio.run(main())