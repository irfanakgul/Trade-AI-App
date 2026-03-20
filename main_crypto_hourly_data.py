import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.crypto_hourly_data_pipeline import (
    run_binance_hourly_data_pipeline,
    BinanceHourlyDataPipelineFlags,
)


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
        ingest=False,
        main_provider="binance_api",
        alternative_provider="tvdatafeed",
        enable_fallback=True,
        use_db_last_timestamp=True,
        start_date="2024-01-01",
        safe_days_back=1,
        main_provider_retries=2,
        max_concurrent_symbols=3,

        # prep
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

    )

    await run_binance_hourly_data_pipeline(repo, flags,'BINANCE')


if __name__ == "__main__":
    asyncio.run(main())