import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.bist_daily_data_pipeline import run_bist_daily_data_pipeline, BistDailyDataPipelineFlags


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = BistDailyDataPipelineFlags(
        ingest=True,
        fallback=True,
        sync_archive_to_working=True,
        trim365=True,
        build_focus_dataset=True,
        use_db_last_timestamp=True,
        start_date="2020-01-01",
        end_date=None,
    )

    await run_bist_daily_data_pipeline(repo, flags)


if __name__ == "__main__":
    asyncio.run(main())