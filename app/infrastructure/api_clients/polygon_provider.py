# app/infrastructure/api_clients/polygon_provider.py

from __future__ import annotations

import asyncio
from datetime import date
from typing import AsyncIterator, List, Optional

import httpx

from app.infrastructure.api_clients.market_data_provider import MarketDataProvider, AggBar


class PolygonProvider(MarketDataProvider):
    def __init__(
        self,
        api_key: str,
        timeout_connect_s: float = 15.0,
        timeout_read_s: float = 60.0,
        max_retries: int = 6,
        base_backoff_s: float = 1.5,
        max_connections: int = 20,
        max_keepalive_connections: int = 10,
    ):
        self.api_key = api_key
        self.timeout = httpx.Timeout(connect=timeout_connect_s, read=timeout_read_s, write=timeout_read_s, pool=timeout_read_s)
        self.max_retries = max_retries
        self.base_backoff_s = base_backoff_s
        self.limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
        )

    async def fetch_1min_aggs(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> AsyncIterator[List[AggBar]]:
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/"
            f"{start_date.isoformat()}/{end_date.isoformat()}"
            f"?adjusted=true&sort=asc&limit=50000&apiKey={self.api_key}"
        )

        async with httpx.AsyncClient(timeout=self.timeout, limits=self.limits) as client:
            next_url: Optional[str] = url

            while next_url:
                data = await self._get_with_retries(client, next_url)

                results = data.get("results") or []
                if results:
                    batch: List[AggBar] = []
                    for r in results:
                        batch.append(
                            AggBar(
                                ts_ms=int(r["t"]),
                                open=float(r["o"]),
                                high=float(r["h"]),
                                low=float(r["l"]),
                                close=float(r["c"]),
                                volume=float(r["v"]),
                            )
                        )
                    yield batch

                nu = data.get("next_url")
                if nu:
                    next_url = nu + f"&apiKey={self.api_key}"
                    # Small pacing to avoid bursts
                    await asyncio.sleep(0.05)
                else:
                    next_url = None

    async def _get_with_retries(self, client: httpx.AsyncClient, url: str) -> dict:
        attempt = 0

        while True:
            attempt += 1
            try:
                resp = await client.get(url)

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code == 429:
                    # Polygon rate limit; wait longer
                    await asyncio.sleep(60.0)
                    continue

                if 500 <= resp.status_code < 600:
                    # Retry on server errors
                    if attempt >= self.max_retries:
                        resp.raise_for_status()
                    await asyncio.sleep(self._backoff(attempt))
                    continue

                # Other non-200 errors: do not loop forever
                resp.raise_for_status()

            except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError) as e:
                if attempt >= self.max_retries:
                    raise e
                await asyncio.sleep(self._backoff(attempt))

    def _backoff(self, attempt: int) -> float:
        # Exponential backoff with a soft cap
        return min(30.0, self.base_backoff_s * (2 ** (attempt - 1)))