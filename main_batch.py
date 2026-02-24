import asyncio
import os
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.infrastructure.api_clients.polygon_provider import PolygonProvider
from app.services.usa_historical_ingestion_service import UsaHistoricalIngestionService

from app.infrastructure.api_clients.twelvedata_provider import TwelveDataProvider, TwelveDataConfig
from app.services.usa_historical_fallback_service import UsaHistoricalFallbackService


async def main():
    # Step-1: Load environment variables
    load_dotenv()

    # Step-2: Initialize DB connection
    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    # Step-3: USA Historical Data Ingestion
    provider = PolygonProvider(api_key=os.environ["POLYGON_API_KEY"])
    svc = UsaHistoricalIngestionService(repo=repo, provider=provider)

    await svc.run(
        use_db_last_timestamp=True,   # True => read last timestamp per symbol from bronze.usa_1min_high_filtered
        start_date="2026-01-01",      # Used only if flag is False or last_ts is NULL
        end_date=None,                # None => today
    )
    failed = getattr(svc, "permanently_failed_symbols", [])
    if failed:
        print(f"\n[USA-FB] Starting fallback for permanently failed symbols: {len(failed)}\n")

        td_provider = TwelveDataProvider(TwelveDataConfig(api_key=os.environ["TWELVEDATA_API_KEY"]))
        fb_svc = UsaHistoricalFallbackService(repo=repo, provider=td_provider)

        await fb_svc.run_last_week(failed)

        print("\n[USA-FB] Fallback completed.\n")

    # Step-4: Trim USA data (fast TS-index version)
    before = repo.count_rows(schema="bronze", table="usa_1min_high_filtered")
    print(f"[USA] rows before trim: {before}")

    deleted = repo.trim_history_by_peak_or_lookback_ts(
        schema="bronze",
        table="usa_1min_high_filtered",
        symbol_col="SYMBOL",
        ts_typed_col="TS",
        high_col="HIGH",
        lookback_days=365,
        reference_days_ago=1,  # yesterday-based
    )
    print(f"[USA] trim completed. deleted_rows={deleted}")

    after = repo.count_rows(schema="bronze", table="usa_1min_high_filtered")
    print(f"[USA] rows after trim: {after}")


if __name__ == "__main__":
    asyncio.run(main())