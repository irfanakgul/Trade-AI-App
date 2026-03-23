from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text


@dataclass(frozen=True)
class RealtimeWatchItem:
    exchange: str
    symbol: str
    target_buy_price: Optional[float]
    stop_sell_price: Optional[float]


class PostgresRepository:
    def __init__(self, engine):
        self.engine = engine

    # ... existing methods ...

    def get_realtime_watchlist(
        self,
        schema: str,
        table: str,
        exchange: str,
        in_scope: bool = True,
    ) -> list[RealtimeWatchItem]:
        q = text(f'''
            SELECT
                "EXCHANGE",
                "SYMBOL",
                "TARGET_BUY_PRICE",
                "STOP_SELL_PRICE"
            FROM {schema}."{table}"
            WHERE "EXCHANGE" = :exchange
              AND "IN_SCOPE" = :in_scope
              AND "SYMBOL" IS NOT NULL
            ORDER BY "SYMBOL"
        ''')

        with self.engine.begin() as conn:
            rows = conn.execute(
                q,
                {
                    "exchange": exchange,
                    "in_scope": in_scope,
                },
            ).mappings().all()

        items: list[RealtimeWatchItem] = []
        for row in rows:
            items.append(
                RealtimeWatchItem(
                    exchange=str(row["EXCHANGE"]).strip().upper(),
                    symbol=str(row["SYMBOL"]).strip().upper(),
                    target_buy_price=(
                        float(row["TARGET_BUY_PRICE"])
                        if row["TARGET_BUY_PRICE"] is not None
                        else None
                    ),
                    stop_sell_price=(
                        float(row["STOP_SELL_PRICE"])
                        if row["STOP_SELL_PRICE"] is not None
                        else None
                    ),
                )
            )

        return items