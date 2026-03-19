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
        ingest=True,
        main_provider="tvdatafeed",
        alternative_provider="yahooquery",
        enable_fallback=True,
        use_db_last_timestamp=True,
        start_date="2024-01-01",
        safe_days_back=2,
        main_provider_retries=2,
        max_concurrent_symbols=3,

         # flags SYNC
        sync_archive_to_working = True,
        
        # flags TRIM365
        trim_history = True,

        # flags indicator focus dataset
        build_focus_dataset=True,
        
        # dq
        run_dq = True
        
        )

    await run_bist_hourly_data_pipeline(repo, flags,'BIST')


if __name__ == "__main__":
    asyncio.run(main())