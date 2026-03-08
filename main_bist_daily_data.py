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
        ingest=True,
        fallback=True,
        sync_archive_to_working=True,
        interval="daily",
        sync_start_date="2024-03-05", # if daily, then truncate and take data from start_date
        trim365=True,
        build_focus_dataset=True,
        use_db_last_timestamp=True,
        start_date="2026-02-01", #if use_db_last_timestamp = False
        end_date=None,
        dq = True,
        apply_dq_out_scope = True # dq failed symbols will be out of scope for ema and further.False include, True exclude
    )

    await run_bist_daily_data_pipeline(repo, flags)


if __name__ == "__main__":
    asyncio.run(main())