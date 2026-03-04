import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    print("\n[TEST] Focus dataset build started (no ingestion)...\n")

    # -----------------------------
    # (Optional) show current sizes
    # -----------------------------
    bist_rows = repo.count_rows(schema="bronze", table="bist_1min_tv_past")
    usa_rows = repo.count_rows(schema="bronze", table="usa_1min_high_filtered")
    print(f"[TEST] bronze.bist_1min_tv_past rows={bist_rows}")
    print(f"[TEST] bronze.usa_1min_high_filtered rows={usa_rows}")

    # ---------------------------------------------------------
    # Build focus dataset: BIST
    # ---------------------------------------------------------
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

    # ---------------------------------------------------------
    # Build focus dataset: USA
    # ---------------------------------------------------------
    stats_usa = repo.build_frvp_focus_dataset(
        source_schema="bronze",
        source_table="usa_1min_high_filtered",
        target_schema="silver",
        target_table="FRVP_USA_FOCUS_DATASET",
        ts_col="TS",
        high_col="HIGH",
        exchange="USA",
        min_trading_days=15,
    )
    print(
        f'[USA] Focus dataset built. '
        f'symbols: {stats_usa["before_symbols"]} -> {stats_usa["after_symbols"]}, '
        f'rows: {stats_usa["before_rows"]} -> {stats_usa["after_rows"]}'
    )

    # ---------------------------------------------------------
    # Rebuild unified focus symbol list
    # ---------------------------------------------------------
    sym_stats = repo.rebuild_frvp_focus_symbol_list()
    print(f'[FRVP] Focus symbol list rebuilt. rows={sym_stats["rows"]}')

    print("\n[TEST] Focus dataset build completed.\n")


if __name__ == "__main__":
    asyncio.run(main())