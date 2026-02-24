# main_batch.py

import os
import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.infrastructure.api_clients.polygon_provider import PolygonProvider
from app.services.usa_historical_ingestion_service import UsaHistoricalIngestionService


async def main():
    #Step-1: load parameters from env
    load_dotenv()

    # Step-2: load ddb connection
    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    # Step-3: update usa data
    provider = PolygonProvider(api_key=os.environ["POLYGON_API_KEY"])
    svc = UsaHistoricalIngestionService(repo=repo, provider=provider)

    await svc.run(
        use_db_last_timestamp=True,   # True => read last timestamp from bronze.usa_1min_high_filtered
        start_date="2026-01-01",      # Used only if flag is False or last_ts is NULL
        end_date=None,               # None => today
    )

    # Step-4: delete usa data if it is older than 1 year (takes from highest dataset if older)
    deleted = repo.trim_history_by_peak_or_lookback(
        schema="bronze",
        table="usa_1min_high_filtered",
        symbol_col="SYMBOL",
        ts_col="TIMESTAMP",
        high_col="HIGH",
        lookback_days=365,
        reference_days_ago=1,  # yesterday-based
    )
    print(f"[USA] trim completed. deleted_rows={deleted}")



if __name__ == "__main__":
    asyncio.run(main())