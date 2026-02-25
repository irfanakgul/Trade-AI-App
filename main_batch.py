import asyncio
import os
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository

# --- BIST (primary + fallback) ---
from app.infrastructure.api_clients.yahooquery_bist_provider import YahooQueryBistProvider
from app.infrastructure.api_clients.tvdatafeed_bist_provider import TvDatafeedBistProvider, TvDatafeedBistConfig
from app.services.bist_historical_ingestion_service import BistHistoricalIngestionService
from app.services.bist_historical_fallback_service import BistHistoricalFallbackService

# --- USA (primary + fallback) ---
from app.infrastructure.api_clients.polygon_provider import PolygonProvider
from app.services.usa_historical_ingestion_service import UsaHistoricalIngestionService
from app.infrastructure.api_clients.twelvedata_provider import TwelveDataProvider, TwelveDataConfig
from app.services.usa_historical_fallback_service import UsaHistoricalFallbackService

from app.infrastructure.api_clients.yahooquery_usa_provider import YahooQueryUsaProvider
from app.services.usa_historical_yahoo_fallback_service import UsaHistoricalYahooFallbackService

async def main():
    # Step-1: Load environment variables
    load_dotenv()

    # Step-2: Initialize DB connection
    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    # ==========================================================
    # Step-3: BIST Historical Data Ingestion (PRIMARY: yahooquery)
    # ==========================================================
    print("\n[BIST] Historical ingestion started...\n")

    bist_primary_provider = YahooQueryBistProvider()
    bist_svc = BistHistoricalIngestionService(repo=repo, provider=bist_primary_provider)

    await bist_svc.run(
        use_db_last_timestamp=True,   # True => read last timestamp per symbol from bronze.bist_1min_tv_past (TS)
        start_date="2026-01-01",      # Used only if flag is False or last_ts is NULL
        end_date=None,                # None => today
    )

    failed_bist = getattr(bist_svc, "permanently_failed_symbols", [])
    if failed_bist:
        print(f"\n[BIST-FB] Starting tvDatafeed fallback for permanently failed symbols: {len(failed_bist)}\n")

        tv_provider = TvDatafeedBistProvider(
            TvDatafeedBistConfig(
                username=os.environ["TV_USERNAME"],
                password=os.environ["TV_PASSWORD"],
            )
        )
        bist_fb_svc = BistHistoricalFallbackService(repo=repo, provider=tv_provider)

        await bist_fb_svc.run(
            symbols=failed_bist,
            use_db_last_timestamp=True,
            start_date="2026-01-01",
            end_date=None,
        )

        print("\n[BIST-FB] Fallback completed.\n")

    # Step-3.1: Trim BIST data (fast TS-index version)
    before_bist = repo.count_rows(schema="bronze", table="bist_1min_tv_past")
    print(f"[BIST] rows before trim: {before_bist}")

    deleted_bist = repo.trim_history_by_peak_or_lookback_ts(
        schema="bronze",
        table="bist_1min_tv_past",
        symbol_col="SYMBOL",
        ts_typed_col="TS",
        high_col="HIGH",
        lookback_days=365,
        reference_days_ago=1,  # yesterday-based
    )
    print(f"[BIST] trim completed. deleted_rows={deleted_bist}")

    after_bist = repo.count_rows(schema="bronze", table="bist_1min_tv_past")
    print(f"[BIST] rows after trim: {after_bist}")

    # ==========================================================
    # Step-4: USA Historical Data Ingestion (PRIMARY: Polygon)
    # ==========================================================
    print("\n[USA] Historical ingestion started...\n")

    usa_provider = PolygonProvider(api_key=os.environ["POLYGON_API_KEY"])
    usa_svc = UsaHistoricalIngestionService(repo=repo, provider=usa_provider)

    await usa_svc.run(
        use_db_last_timestamp=True,   # True => read last timestamp per symbol from bronze.usa_1min_high_filtered
        start_date="2026-01-01",      # Used only if flag is False or last_ts is NULL
        end_date=None,                # None => today
    )

    polygon_failed = getattr(usa_svc, "permanently_failed_symbols", [])

    # ==========================================================
    # Step-4.1: USA Fallback-1 (TwelveData)
    # ==========================================================
    td_failed: list[str] = []

    if polygon_failed:
        print(f"\n[USA-TD] Starting TwelveData fallback for permanently failed symbols: {len(polygon_failed)}\n")

        td_provider = TwelveDataProvider(
            TwelveDataConfig(api_key=os.environ["TWELVEDATA_API_KEY"])
        )
        usa_td_svc = UsaHistoricalFallbackService(repo=repo, provider=td_provider)

        await usa_td_svc.run_last_week(polygon_failed)

        td_failed = getattr(usa_td_svc, "permanently_failed_symbols", [])

        print(f"\n[USA-TD] TwelveData fallback completed. still_failed={len(td_failed)}\n")

    # ==========================================================
    # Step-4.2: USA Fallback-2 (YahooQuery) — only for still-failed
    # ==========================================================
    if td_failed:
        print(f"\n[USA-YH] Starting YahooQuery fallback for still-failed symbols: {len(td_failed)}\n")

        yh_provider = YahooQueryUsaProvider()
        usa_yh_svc = UsaHistoricalYahooFallbackService(repo=repo, provider=yh_provider)

        await usa_yh_svc.run(
            symbols=td_failed,
            use_db_last_timestamp=True,
            start_date="2026-01-01",
            end_date=None,)

        final_failed = getattr(usa_yh_svc, "permanently_failed_symbols", [])
        print(f"\n[USA-YH] YahooQuery fallback completed. still_failed={len(final_failed)}\n")
    
    
    # Step-4.1: Trim USA data (fast TS-index version)
    before_usa = repo.count_rows(schema="bronze", table="usa_1min_high_filtered")
    print(f"[USA] rows before trim: {before_usa}")

    deleted_usa = repo.trim_history_by_peak_or_lookback_ts(
        schema="bronze",
        table="usa_1min_high_filtered",
        symbol_col="SYMBOL",
        ts_typed_col="TS",
        high_col="HIGH",
        lookback_days=365,
        reference_days_ago=1,  # yesterday-based
    )
    print(f"[USA] trim completed. deleted_rows={deleted_usa}")

    after_usa = repo.count_rows(schema="bronze", table="usa_1min_high_filtered")
    print(f"[USA] rows after trim: {after_usa}")


if __name__ == "__main__":
    asyncio.run(main())