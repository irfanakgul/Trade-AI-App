import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.bist_data_pipeline import run_bist_data_pipeline, BistDataPipelineFlags


async def main():
    load_dotenv()

    db = Database()
    db.print_connection_info()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = BistDataPipelineFlags(
        ingest=True,
        fallback=True,
        use_db_last_timestamp=True,
        start_date = "2026-02-01", #if use_db_last_timestamp = False
        end_date = None,
        sync_archive_to_working=False,
        trim365=False,
        build_focus_dataset=False,
        dq = False,
        apply_dq_out_scope = False # dq failed symbols will be out of scope for ema and further.False include, True exclude
    )

    await run_bist_data_pipeline(repo, flags)


if __name__ == "__main__":
    asyncio.run(main())