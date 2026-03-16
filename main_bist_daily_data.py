import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.bist_daily_data_pipeline import run_bist_daily_data_pipeline, BistDailyDataPipelineFlags



async def main():
    load_dotenv()

    db = Database()
    db.print_connection_info()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = BistDailyDataPipelineFlags(
        ingest=False,
        fallback=False,
        sync_archive_to_working=False,
        trim365=False,
        build_focus_dataset=False,
        use_db_last_timestamp=False,
        auto_sample_run = False,
        dq = True,
    )

    await run_bist_daily_data_pipeline(repo, flags)


if __name__ == "__main__":
    asyncio.run(main())