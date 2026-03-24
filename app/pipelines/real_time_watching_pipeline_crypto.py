from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from app.services.realtime_watching_service_crypto_binance import (
    BinanceRealtimeWatchingConfig,
    BinanceRealtimeWatchingService,
)


@dataclass(frozen=True)
class RealtimeWatchingPipelineCryptoFlags:
    run_realtime_watching: bool = True

    watchlist_schema: str = "prod"
    watchlist_table: str = "watch_list_crypto"
    watchlist_exchange: str = "OANDA"
    watchlist_in_scope: bool = True

    reconnect_seconds: int = 5
    ping_interval: int = 20
    ping_timeout: int = 20

    target_buy_operator: str = "lte"
    stop_sell_operator: str = "lte"


async def custom_trigger(payload: dict) -> None:
    print(
        f"[CUSTOM-TRIGGER] "
        f"event={payload['event_type']} | "
        f"symbol={payload['symbol']} | "
        f"price={payload['price']:.8f} | "
        f"level={payload['level']:.8f}"
    )

    # later:
    # await your_buy_system(payload)
    # await your_sell_system(payload)
    # await your_notification_system(payload)
    # await your_log_system(payload)


async def run_real_time_watching_pipeline_crypto(
    repo,
    flags: RealtimeWatchingPipelineCryptoFlags,
    exchange,
) -> None:
    print(
        "\n►►►►►►►►►►►►[PIPELINE CRYPTO REALTIME WATCHING] started ◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎"
        + datetime.now().strftime("%d-%m-%Y %H:%M")
        + "\n"
    )

    if flags.run_realtime_watching:
        print("[REALTIME-WATCHING] CRYPTO started...")

        svc = BinanceRealtimeWatchingService(
            repo=repo,
            cfg=BinanceRealtimeWatchingConfig(
                watchlist_schema=flags.watchlist_schema,
                watchlist_table=flags.watchlist_table,
                watchlist_exchange=exchange,
                watchlist_in_scope=flags.watchlist_in_scope,
                reconnect_seconds=flags.reconnect_seconds,
                ping_interval=flags.ping_interval,
                ping_timeout=flags.ping_timeout,
                target_buy_operator=flags.target_buy_operator,
                stop_sell_operator=flags.stop_sell_operator,
            ),
        )

        svc.register_trigger(custom_trigger)
        await svc.run()

    else:
        print("❌ [REALTIME-WATCHING] CRYPTO skipped!")