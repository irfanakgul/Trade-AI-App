import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.usa_data_pipeline import run_usa_data_pipeline, UsaDataPipelineFlags


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = UsaDataPipelineFlags(
        ingest=True,
        fallback_twelvedata=True,
        fallback_yahoo=True,
        sync_archive_to_working=True,
        trim365=True,
        build_focus_dataset=True, # prep focus symbols and datasets for all indicators
        dq = True,
        apply_dq_out_scope = True # dq failed symbols will be out of scope for ema and further.False include, True exclude
    )

    await run_usa_data_pipeline(repo, flags)


if __name__ == "__main__":
    asyncio.run(main())