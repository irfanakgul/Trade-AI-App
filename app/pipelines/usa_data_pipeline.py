import os
from dataclasses import dataclass
from typing import Optional

from app.infrastructure.api_clients.polygon_provider import PolygonProvider
from app.services.usa_historical_ingestion_service import UsaHistoricalIngestionService

from app.infrastructure.api_clients.twelvedata_provider import TwelveDataProvider, TwelveDataConfig
from app.services.usa_historical_fallback_service import UsaHistoricalFallbackService

from app.infrastructure.api_clients.yahooquery_usa_provider import YahooQueryUsaProvider
from app.services.usa_historical_yahoo_fallback_service import UsaHistoricalYahooFallbackService

from app.infrastructure.database.repository import PostgresRepository


@dataclass(frozen=True)
class UsaDataPipelineFlags:
    ingest: bool = True
    fallback_twelvedata: bool = True
    fallback_yahoo: bool = True
    sync_archive_to_working: bool = True
    trim365: bool = True
    build_focus_dataset: bool = True

    use_db_last_timestamp: bool = True
    start_date: str = "2026-01-01"
    end_date: Optional[str] = None

    safety_days: int = 1
    lookback_days: int = 365
    reference_days_ago: int = 1
    min_trading_days: int = 15

    err_schema: str = "logs"
    err_table: str = "ingestion_errors"
    job_name: str = "usa_historical_ingestion"


async def run_usa_data_pipeline(repo: PostgresRepository, flags: UsaDataPipelineFlags) -> None:
    print("\n[USA] Data pipeline started...\n")

    # 1) Ingestion (Polygon)
    if flags.ingest:
        usa_provider = PolygonProvider(api_key=os.environ["POLYGON_API_KEY"])
        usa_svc = UsaHistoricalIngestionService(repo=repo, provider=usa_provider)

        await usa_svc.run(
            use_db_last_timestamp=flags.use_db_last_timestamp,
            start_date=flags.start_date,
            end_date=flags.end_date,
        )
    else:
        print("[USA] ingest skipped")

    # After Polygon (or if ingest skipped), use error table as source of truth
    still_failed = repo.get_active_error_symbols(
        schema=flags.err_schema,
        table=flags.err_table,
        job_name=flags.job_name,
        exchange="USA",
    )
    print(f"\n[USA] After Polygon: still_failed={len(still_failed)}\n")

    # 2) Fallback-1 (TwelveData)
    if flags.fallback_twelvedata and still_failed:
        print(f"\n[USA-TD] TwelveData fallback started. symbols={len(still_failed)}\n")
        td_provider = TwelveDataProvider(TwelveDataConfig(api_key=os.environ["TWELVEDATA_API_KEY"]))
        usa_td_svc = UsaHistoricalFallbackService(repo=repo, provider=td_provider)

        await usa_td_svc.run_last_week(still_failed)

        still_failed = repo.get_active_error_symbols(
            schema=flags.err_schema,
            table=flags.err_table,
            job_name=flags.job_name,
            exchange="USA",
        )
        print(f"\n[USA] After TwelveData: still_failed={len(still_failed)}\n")
    elif flags.fallback_twelvedata:
        print("[USA-TD] fallback skipped (no failed symbols)")
    else:
        print("[USA-TD] fallback disabled")

    # 3) Fallback-2 (YahooQuery)
    if flags.fallback_yahoo and still_failed:
        print(f"\n[USA-YH] YahooQuery fallback started. symbols={len(still_failed)}\n")
        yh_provider = YahooQueryUsaProvider()
        usa_yh_svc = UsaHistoricalYahooFallbackService(repo=repo, provider=yh_provider)

        await usa_yh_svc.run(
            symbols=still_failed,
            use_db_last_timestamp=flags.use_db_last_timestamp,
            start_date=flags.start_date,
            end_date=flags.end_date,
        )

        still_failed = repo.get_active_error_symbols(
            schema=flags.err_schema,
            table=flags.err_table,
            job_name=flags.job_name,
            exchange="USA",
        )
        print(f"\n[USA] After YahooQuery: still_failed={len(still_failed)}\n")
    elif flags.fallback_yahoo:
        print("[USA-YH] fallback skipped (no failed symbols)")
    else:
        print("[USA-YH] fallback disabled")

    # 4) Sync raw -> bronze working
    if flags.sync_archive_to_working:
        print("\n[USA] Sync archive -> working started...\n")
        ins_usa = repo.sync_archive_to_working(
            archive_schema="raw",
            archive_table="usa_1min_archive",
            working_schema="bronze",
            working_table="usa_1min_high_filtered",
            ts_col="TS",
            safety_days=flags.safety_days,
        )
        print(f"[USA] Sync completed. inserted_rows={ins_usa}\n")
    else:
        print("[USA] sync skipped")

    # 5) Trim
    if flags.trim365:
        before_usa = repo.count_rows(schema="bronze", table="usa_1min_high_filtered")
        print(f"[USA] rows before trim: {before_usa}")

        deleted_usa = repo.trim_history_by_peak_or_lookback_ts(
            schema="bronze",
            table="usa_1min_high_filtered",
            symbol_col="SYMBOL",
            ts_typed_col="TS",
            high_col="HIGH",
            lookback_days=flags.lookback_days,
            reference_days_ago=flags.reference_days_ago,
        )
        print(f"[USA] trim completed. deleted_rows={deleted_usa}")

        after_usa = repo.count_rows(schema="bronze", table="usa_1min_high_filtered")
        print(f"[USA] rows after trim: {after_usa}")
    else:
        print("[USA] trim skipped")

    # 6) Focus dataset + focus symbol list rebuild
    if flags.build_focus_dataset:
        stats_usa = repo.build_frvp_focus_dataset(
            source_schema="bronze",
            source_table="usa_1min_high_filtered",
            target_schema="silver",
            target_table="FRVP_USA_FOCUS_DATASET",
            ts_col="TS",
            high_col="HIGH",
            exchange="USA",
            min_trading_days=flags.min_trading_days,
        )
        print(
            f'[USA] Focus dataset built. '
            f'symbols: {stats_usa["before_symbols"]} -> {stats_usa["after_symbols"]}, '
            f'rows: {stats_usa["before_rows"]} -> {stats_usa["after_rows"]}'
        )
    else:
        print("[USA] focus dataset build skipped")

    # Optional: final remaining errors count
    final_failed = repo.get_active_error_symbols(
        schema=flags.err_schema,
        table=flags.err_table,
        job_name=flags.job_name,
        exchange="USA",
    )
    print(f"\n[USA] Final remaining failed symbols in logs: {len(final_failed)}\n")

    print("\n[USA] Data pipeline finished.\n")