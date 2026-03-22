import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.bist_hourly_data_pipeline import (
    run_bist_hourly_data_pipeline,
    BistHourlyDataPipelineFlags,
)


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = BistHourlyDataPipelineFlags(
        ingest=False,
        main_provider="tvdatafeed",
        alternative_provider="yahooquery",
        start_date="2024-01-01",
        safe_days_back=2,
        main_provider_retries=2,
        max_concurrent_symbols=3,

         # flags SYNC
        sync_archive_to_working = False,
        trim_history = False,
        build_focus_dataset=False,
        run_dq = True,
        dq_elemination = True,
        
        
        #=================================================================#
        # INDICATOR FLAGS
        #=================================================================#

        bar_status=False,
        run_frvp=False,
        run_convert_daily = False,
        run_ema_ind = False,
        run_vwap_ind = False,
        run_rsi_ind = False,
        run_mfi_ind = False,
        run_combined_indicators = False,

    )

    await run_bist_hourly_data_pipeline(repo, flags,'BIST')


if __name__ == "__main__":
    asyncio.run(main())