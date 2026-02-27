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

# --- USA (primary + fallback chain) ---
from app.infrastructure.api_clients.polygon_provider import PolygonProvider
from app.services.usa_historical_ingestion_service import UsaHistoricalIngestionService

from app.infrastructure.api_clients.twelvedata_provider import TwelveDataProvider, TwelveDataConfig
from app.services.usa_historical_fallback_service import UsaHistoricalFallbackService

from app.infrastructure.api_clients.yahooquery_usa_provider import YahooQueryUsaProvider
from app.services.usa_historical_yahoo_fallback_service import UsaHistoricalYahooFallbackService


USA_ERR_SCHEMA = "logs"
USA_ERR_TABLE = "ingestion_errors"
USA_JOB_NAME = "usa_historical_ingestion"


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    # ==========================================================
    # BIST: primary (yahooquery) -> fallback (tvDatafeed) -> trim
    # ==========================================================
    print("\n[BIST] Historical ingestion started...\n")

    bist_primary_provider = YahooQueryBistProvider()
    bist_svc = BistHistoricalIngestionService(repo=repo, provider=bist_primary_provider)

    await bist_svc.run(
        use_db_last_timestamp=True,
        start_date="2026-01-01",
        end_date=None,
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

    before_bist = repo.count_rows(schema="bronze", table="bist_1min_tv_past")
    print(f"[BIST] rows before trim: {before_bist}")

    deleted_bist = repo.trim_history_by_peak_or_lookback_ts(
        schema="bronze",
        table="bist_1min_tv_past",
        symbol_col="SYMBOL",
        ts_typed_col="TS",
        high_col="HIGH",
        lookback_days=365,
        reference_days_ago=1,
    )
    print(f"[BIST] trim completed. deleted_rows={deleted_bist}")

    after_bist = repo.count_rows(schema="bronze", table="bist_1min_tv_past")
    print(f"[BIST] rows after trim: {after_bist}")

    # ==========================================================
    # BIST FRVP FOCUS TRIM NEW DATASET PREP
    # ==========================================================

    stats_bist = repo.build_frvp_focus_dataset(
        source_schema="bronze",
        source_table="bist_1min_tv_past",
        target_schema="silver",
        target_table="FRVP_BIST_FOCUS_DATASET",
        ts_col="TS",
        high_col="HIGH",
        exchange="BIST",
        min_trading_days=15,
    )
    print(
        f'[BIST] Focus dataset built. '
        f'symbols: {stats_bist["before_symbols"]} -> {stats_bist["after_symbols"]}, '
        f'rows: {stats_bist["before_rows"]} -> {stats_bist["after_rows"]}'
    )

    # ==========================================================
    # USA: primary (Polygon) -> fallback-1 (TwelveData) -> fallback-2 (Yahoo) -> trim
    # Provider chain is driven by logs.ingestion_errors state.
    # ==========================================================
    print("\n[USA] Historical ingestion started...\n")

    usa_provider = PolygonProvider(api_key=os.environ["POLYGON_API_KEY"])
    usa_svc = UsaHistoricalIngestionService(repo=repo, provider=usa_provider)

    await usa_svc.run(
        use_db_last_timestamp=True,
        start_date="2026-01-01",
        end_date=None,
    )

    # After Polygon, decide next step from error table (source of truth)
    still_failed = repo.get_active_error_symbols(
        schema=USA_ERR_SCHEMA,
        table=USA_ERR_TABLE,
        job_name=USA_JOB_NAME,
        exchange="USA",
    )
    print(f"\n[USA] After Polygon: still_failed={len(still_failed)}\n")

    # ----------------------------------------------------------
    # USA fallback-1: TwelveData
    # ----------------------------------------------------------
    if still_failed:
        print(f"\n[USA-TD] Starting TwelveData fallback for still-failed symbols: {len(still_failed)}\n")

        td_provider = TwelveDataProvider(TwelveDataConfig(api_key=os.environ["TWELVEDATA_API_KEY"]))
        usa_td_svc = UsaHistoricalFallbackService(repo=repo, provider=td_provider)

        await usa_td_svc.run_last_week(still_failed)

        still_failed = repo.get_active_error_symbols(
            schema=USA_ERR_SCHEMA,
            table=USA_ERR_TABLE,
            job_name=USA_JOB_NAME,
            exchange="USA",
        )
        print(f"\n[USA] After TwelveData: still_failed={len(still_failed)}\n")

    # ----------------------------------------------------------
    # USA fallback-2: YahooQuery
    # ----------------------------------------------------------
    if still_failed:
        print(f"\n[USA-YH] Starting YahooQuery fallback for still-failed symbols: {len(still_failed)}\n")

        yh_provider = YahooQueryUsaProvider()
        usa_yh_svc = UsaHistoricalYahooFallbackService(repo=repo, provider=yh_provider)

        # NOTE: call the correct method name in your service (run vs run_last_week).
        await usa_yh_svc.run(
            symbols=still_failed,
            use_db_last_timestamp=True,
            start_date="2026-01-01",
            end_date=None,
        )

        still_failed = repo.get_active_error_symbols(
            schema=USA_ERR_SCHEMA,
            table=USA_ERR_TABLE,
            job_name=USA_JOB_NAME,
            exchange="USA",
        )
        print(f"\n[USA] After YahooQuery: still_failed={len(still_failed)}\n")

    # ==========================================================
    # USA trim
    # ==========================================================
    before_usa = repo.count_rows(schema="bronze", table="usa_1min_high_filtered")
    print(f"[USA] rows before trim: {before_usa}")

    deleted_usa = repo.trim_history_by_peak_or_lookback_ts(
        schema="bronze",
        table="usa_1min_high_filtered",
        symbol_col="SYMBOL",
        ts_typed_col="TS",
        high_col="HIGH",
        lookback_days=365,
        reference_days_ago=1,
    )
    print(f"[USA] trim completed. deleted_rows={deleted_usa}")

    after_usa = repo.count_rows(schema="bronze", table="usa_1min_high_filtered")
    print(f"[USA] rows after trim: {after_usa}")

    # Final: remaining errors in logs table are truly unfillable by the provider chain
    final_failed = repo.get_active_error_symbols(
        schema=USA_ERR_SCHEMA,
        table=USA_ERR_TABLE,
        job_name=USA_JOB_NAME,
        exchange="USA",
    )
    print(f"\n[USA] Final remaining failed symbols in logs: {len(final_failed)}\n")

    # ==========================================================
    # USA FRVP FOCUS TRIM NEW DATASET PREP
    # ==========================================================

    stats_usa = repo.build_frvp_focus_dataset(
    source_schema="bronze",
    source_table="usa_1min_high_filtered",
    target_schema="silver",
    target_table="FRVP_USA_FOCUS_DATASET",
    ts_col="TS",
    high_col="HIGH",
    exchange="USA",
    min_trading_days=15,)

    print(
        f'[USA] Focus FRVP dataset built. '
        f'symbols: {stats_usa["before_symbols"]} -> {stats_usa["after_symbols"]}, '
        f'rows: {stats_usa["before_rows"]} -> {stats_usa["after_rows"]}'
    )

    sym_stats = repo.rebuild_frvp_focus_symbol_list()
    print(f'[FRVP] Focus symbol list rebuilt. rows={sym_stats["rows"]}')

if __name__ == "__main__":
    asyncio.run(main())