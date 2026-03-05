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
        ingest=False,
        fallback=False,
        sync_archive_to_working=False,
        trim365=False,
        build_focus_dataset=False,
        use_db_last_timestamp=False,
        start_date="2020-01-01",
        end_date=None,
        dq = True,
        apply_dq_out_scope = True # dq failed symbols will be out of scope for ema and further.False include, True exclude
    )

    await run_bist_daily_data_pipeline(repo, flags)


if __name__ == "__main__":
    asyncio.run(main())