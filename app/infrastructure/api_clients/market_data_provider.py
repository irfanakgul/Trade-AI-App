# app/infrastructure/api_clients/market_data_provider.py

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import AsyncIterator, Dict, List, Any


@dataclass(frozen=True)
class AggBar:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class MarketDataProvider(ABC):
    @abstractmethod
    async def fetch_1min_aggs(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> AsyncIterator[List[AggBar]]:
        """
        Yields batches of 1-min aggregate bars for a symbol.
        """
        raise NotImplementedError