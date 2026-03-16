import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.usa_data_pipeline import run_usa_data_pipeline, UsaDataPipelineFlags


async def main():
    load_dotenv()

    db = Database()
    db.print_connection_info()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = UsaDataPipelineFlags(
        ingest=False,
        fallback_twelvedata=False,
        fallback_yahoo=False,
        sync_archive_to_working=False,
        trim365=False,
        build_focus_dataset=False, # prep focus symbols and datasets for all indicators
        auto_sample_run = False,
        dq = True,
    )

    await run_usa_data_pipeline(repo, flags)


if __name__ == "__main__":
    asyncio.run(main())