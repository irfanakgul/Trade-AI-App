import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.assets_hourly_data_pipeline import (
    run_oanda_hourly_data_pipeline,
    OandaHourlyDataPipelineFlags,
)


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = OandaHourlyDataPipelineFlags(

        # flags ingest
        ingest=False,
        main_provider="tvdatafeed",
        alternative_provider="not_implemented",
        start_date="2024-01-01",
        safe_days_back=1,
        main_provider_retries=2,
        max_concurrent_symbols=2,
        

        # flags SYNC
        sync_archive_to_working = False,
        
        # flags TRIM365
        trim_history = False,

        # flags build indicator focus dataset
        build_focus_dataset = False,

        # dq
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

    await run_oanda_hourly_data_pipeline(repo, flags,'OANDA')


if __name__ == "__main__":
    asyncio.run(main())