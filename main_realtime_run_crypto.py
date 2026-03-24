import asyncio
from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository_realtime import PostgresRepository
from app.pipelines.real_time_watching_pipeline_crypto import (
    run_real_time_watching_pipeline_crypto,
    RealtimeWatchingPipelineCryptoFlags,
)


async def main():
    load_dotenv()

    db = Database()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = RealtimeWatchingPipelineCryptoFlags(
        run_realtime_watching=True,

        watchlist_schema="prod",
        watchlist_table="watch_list_crypto",
        watchlist_in_scope=True,

        reconnect_seconds=5,
        ping_interval=20,
        ping_timeout=20,

        target_buy_operator="lte",
        stop_sell_operator="lte",
    )

    await run_real_time_watching_pipeline_crypto(repo, flags,'BINANCE')


if __name__ == "__main__":
    asyncio.run(main())