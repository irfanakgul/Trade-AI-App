import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.nyse_hourly_data_pipeline import (
    run_nyse_hourly_data_pipeline,
    NyseHourlyDataPipelineFlags,
)


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = NyseHourlyDataPipelineFlags(
        ingest=True,
        main_provider="tvdatafeed",
        alternative_provider="not_implemented",
        enable_fallback=True,
        use_db_last_timestamp=True,
        start_date="2024-01-01",
        safe_days_back=1,
        main_provider_retries=3,
        max_concurrent_symbols=3,
    )

    await run_nyse_hourly_data_pipeline(repo, flags)


if __name__ == "__main__":
    asyncio.run(main())