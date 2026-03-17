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
        safe_days_back=1,
        main_provider_retries=2,
        max_concurrent_symbols=4,
    )

    await run_bist_hourly_data_pipeline(repo, flags)


if __name__ == "__main__":
    asyncio.run(main())