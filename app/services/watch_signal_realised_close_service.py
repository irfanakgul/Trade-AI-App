from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict

import pandas as pd

from app.infrastructure.database.repository import PostgresRepository


@dataclass(frozen=True)
class WatchSignalRealisedCloseConfig:
    job_name: str = "watch_signal_realised_close"

    source_schema: str = "raw"
    source_table: str = "bist_hourly_archive"

    target_schema: str = "prod"
    target_table: str = "watch_signal_check_bist"

    log_schema: str = "logs"
    log_table: str = "watch_signal_check_all"


class WatchSignalRealisedCloseService:
    def __init__(
        self,
        repo: PostgresRepository,
        cfg: WatchSignalRealisedCloseConfig = WatchSignalRealisedCloseConfig(),
    ):
        self.repo = repo
        self.cfg = cfg

    def run(
        self,
        exchange: Optional[str] = None,
        source_schema: Optional[str] = None,
        source_table: Optional[str] = None,
        target_schema: Optional[str] = None,
        target_table: Optional[str] = None,
        log_schema: Optional[str] = None,
        log_table: Optional[str] = None,
        overwrite: bool = True,
    ) -> Dict[str, int]:
        exchange = exchange.upper().strip() if exchange else None
        source_schema = source_schema or self.cfg.source_schema
        source_table = source_table or self.cfg.source_table
        target_schema = target_schema or self.cfg.target_schema
        target_table = target_table or self.cfg.target_table
        log_schema = log_schema or self.cfg.log_schema
        log_table = log_table or self.cfg.log_table

        updated_target = self.repo.update_realised_close_fields_from_hourly_source(
            source_schema=source_schema,
            source_table=source_table,
            target_schema=target_schema,
            target_table=target_table,
            exchange=exchange,
            overwrite=overwrite,
        )

        updated_log = self.repo.update_realised_close_fields_from_hourly_source(
            source_schema=source_schema,
            source_table=source_table,
            target_schema=log_schema,
            target_table=log_table,
            exchange=exchange,
            overwrite=overwrite,
        )

        print(
            f"[WATCH-REALISED] completed | "
            f"exchange={exchange or 'ALL'} | "
            f"target_updated={updated_target} | "
            f"log_updated={updated_log}"
        )

        return {
            "target_updated": updated_target,
            "log_updated": updated_log,
        }