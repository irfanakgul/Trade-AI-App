from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

import websockets

from app.infrastructure.database.repository_realtime import (
    PostgresRepository,
    RealtimeWatchItem,
)


TriggerFunc = Callable[[dict], Awaitable[None]]


@dataclass(frozen=True)
class BinanceRealtimeWatchingConfig:
    watchlist_schema: str
    watchlist_table: str
    watchlist_exchange: str
    watchlist_in_scope: bool = True

    reconnect_seconds: int = 5
    ping_interval: int = 20
    ping_timeout: int = 20

    target_buy_operator: str = "lte"
    stop_sell_operator: str = "lte"


class BinanceRealtimeWatchingService:
    def __init__(
        self,
        repo: PostgresRepository,
        cfg: BinanceRealtimeWatchingConfig,
    ):
        self.repo = repo
        self.cfg = cfg

        self.watch_items: dict[str, RealtimeWatchItem] = {}
        self.target_buy_triggered: dict[str, bool] = {}
        self.stop_sell_triggered: dict[str, bool] = {}
        self._custom_trigger: Optional[TriggerFunc] = None

    def register_trigger(self, trigger_func: TriggerFunc) -> None:
        self._custom_trigger = trigger_func

    def load_watchlist(self) -> list[RealtimeWatchItem]:
        items = self.repo.get_realtime_watchlist(
            schema=self.cfg.watchlist_schema,
            table=self.cfg.watchlist_table,
            exchange=self.cfg.watchlist_exchange,
            in_scope=self.cfg.watchlist_in_scope,
        )

        self.watch_items = {item.symbol: item for item in items}
        self.target_buy_triggered = {item.symbol: False for item in items}
        self.stop_sell_triggered = {item.symbol: False for item in items}

        return items

    def build_stream_url(self, symbols: list[str]) -> str:
        streams = "/".join(f"{symbol.lower()}@miniTicker" for symbol in symbols)
        return f"wss://stream.binance.com:9443/stream?streams={streams}"

    def _compare(self, price: float, level: float, operator: str) -> bool:
        op = operator.lower().strip()

        if op == "gte":
            return price >= level
        if op == "lte":
            return price <= level
        if op == "gt":
            return price > level
        if op == "lt":
            return price < level

        raise ValueError(f"Unsupported operator: {operator}")

    async def _default_trigger(self, payload: dict) -> None:
        print(
            f"[TRIGGER] "
            f"event={payload['event_type']} | "
            f"symbol={payload['symbol']} | "
            f"price={payload['price']:.8f} | "
            f"level={payload['level']:.8f}"
        )

    async def _dispatch_trigger(self, payload: dict) -> None:
        if self._custom_trigger is not None:
            await self._custom_trigger(payload)
            return

        await self._default_trigger(payload)

    async def _handle_target_buy(
        self,
        symbol: str,
        price: float,
        level: float,
    ) -> None:
        matched = self._compare(
            price=price,
            level=level,
            operator=self.cfg.target_buy_operator,
        )
        triggered = self.target_buy_triggered.get(symbol, False)

        if matched and not triggered:
            await self._dispatch_trigger(
                {
                    "event_type": "TARGET_BUY_REACHED",
                    "symbol": symbol,
                    "price": price,
                    "level": level,
                }
            )
            self.target_buy_triggered[symbol] = True

        elif not matched and triggered:
            self.target_buy_triggered[symbol] = False

    async def _handle_stop_sell(
        self,
        symbol: str,
        price: float,
        level: float,
    ) -> None:
        matched = self._compare(
            price=price,
            level=level,
            operator=self.cfg.stop_sell_operator,
        )
        triggered = self.stop_sell_triggered.get(symbol, False)

        if matched and not triggered:
            await self._dispatch_trigger(
                {
                    "event_type": "STOP_SELL_REACHED",
                    "symbol": symbol,
                    "price": price,
                    "level": level,
                }
            )
            self.stop_sell_triggered[symbol] = True

        elif not matched and triggered:
            self.stop_sell_triggered[symbol] = False

    async def process_price_event(
        self,
        symbol: str,
        price: float,
    ) -> None:
        item = self.watch_items.get(symbol)
        if item is None:
            return

        if item.target_buy_price is not None:
            await self._handle_target_buy(
                symbol=symbol,
                price=price,
                level=item.target_buy_price,
            )

        if item.stop_sell_price is not None:
            await self._handle_stop_sell(
                symbol=symbol,
                price=price,
                level=item.stop_sell_price,
            )

    async def run(self) -> None:
        items = self.load_watchlist()
        if not items:
            print("[REALTIME-WATCHING] no watchlist item found")
            return

        symbols = [item.symbol for item in items]
        ws_url = self.build_stream_url(symbols)

        print(f"[REALTIME-WATCHING] watchlist_count={len(symbols)}")
        print(f"[REALTIME-WATCHING] symbols={symbols}")
        print("[REALTIME-WATCHING] websocket connecting...")

        while True:
            try:
                async with websockets.connect(
                    ws_url,
                    ping_interval=self.cfg.ping_interval,
                    ping_timeout=self.cfg.ping_timeout,
                ) as ws:
                    print("[REALTIME-WATCHING] websocket connected")

                    while True:
                        raw_message = await ws.recv()
                        payload = json.loads(raw_message)
                        data = payload.get("data", {})

                        symbol = str(data.get("s", "")).strip().upper()
                        close_price = data.get("c")

                        if not symbol or close_price is None:
                            continue

                        await self.process_price_event(
                            symbol=symbol,
                            price=float(close_price),
                        )
                        print(f"[PRICE] symbol={symbol} price={float(close_price):.8f}")

            except Exception as exc:
                print(f"[REALTIME-WATCHING] websocket error={exc}")
                print(
                    f"[REALTIME-WATCHING] reconnecting in "
                    f"{self.cfg.reconnect_seconds} seconds..."
                )
                await asyncio.sleep(self.cfg.reconnect_seconds)