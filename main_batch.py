# main_batch.py

import os
import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.infrastructure.api_clients.polygon_provider import PolygonProvider
from app.services.usa_historical_ingestion_service import UsaHistoricalIngestionService


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    provider = PolygonProvider(api_key=os.environ["POLYGON_API_KEY"])
    svc = UsaHistoricalIngestionService(repo=repo, provider=provider)

    await svc.run(
        use_db_last_timestamp=True,   # True => read last timestamp from bronze.usa_1min_high_filtered
        start_date="2026-01-01",      # Used only if flag is False or last_ts is NULL
        end_date=None,               # None => today
    )


if __name__ == "__main__":
    asyncio.run(main())