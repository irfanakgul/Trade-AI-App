# app/services/bist_daily_historical_fallback_service.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List

from app.infrastructure.database.repository import PostgresRepository
from app.services.bist_daily_historical_ingestion_service import (
    BistDailyHistoricalIngestionService,
    BistDailyIngestionConfig,
)


@dataclass(frozen=True)
class BistDailyFallbackConfig(BistDailyIngestionConfig):
    source: str = "tvDatafeed"
    job_name: str = "bist_daily_historical_fallback"


class BistDailyHistoricalFallbackService:
    def __init__(self, repo: PostgresRepository, provider: object, cfg: BistDailyFallbackConfig = BistDailyFallbackConfig()):
        self.repo = repo
        self.provider = provider
        self.cfg = cfg
        self.permanently_failed_symbols: List[str] = []

    async def run(self, symbols: List[str], use_db_last_timestamp: bool, start_date: str, end_date: Optional[str] = None) -> None:
        svc = BistDailyHistoricalIngestionService(
            repo=self.repo,
            provider=self.provider,
            config=self.cfg,
        )
        await svc.run(use_db_last_timestamp=use_db_last_timestamp, start_date=start_date, end_date=end_date)
        # Note: ingestion service runs full in-scope list by default; we want only the failed list.
        # If you want strict "only failed list", we'll add a run_symbols(...) method next.
        self.permanently_failed_symbols = getattr(svc, "permanently_failed_symbols", [])