import asyncio
from dotenv import load_dotenv

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
        ingest=False,
        main_provider="tvdatafeed",
        alternative_provider="yahooquery",
        enable_fallback=True,
        use_db_last_timestamp=True,
        start_date="2024-01-01",
        safe_days_back=1,
        main_provider_retries=2,
        max_concurrent_symbols=4,

        # flags sync
        sync_archive_to_working = True,
    )

    await run_euronext_hourly_data_pipeline(repo, flags)


if __name__ == "__main__":
    asyncio.run(main())