from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from app.infrastructure.database.repository import PostgresRepository


@dataclass(frozen=True)
class MasterCombinedIndicatorsConfig:
    job_name: str = "master_combined_indicators"


class IndMasterCombinedIndicatorsService:
    def __init__(
        self,
        repo: PostgresRepository,
        cfg: MasterCombinedIndicatorsConfig = MasterCombinedIndicatorsConfig(),
    ):
        self.repo = repo
        self.cfg = cfg

    def run(
        self,
        exchange: str,
        target_schema: str,
        target_table: str,
        log_schema: str,
        log_table: str,
        frvp_table: str,
        bs_table: str,
        end_dates_table: str,
        ema_table: str,
        rsi_table: str,
        mfi_table: str,
        vwap_table: str,
        pivot_table: str,
    ) -> Dict[str, int]:
        exchange = exchange.upper().strip()

        result = self.repo.build_master_combined_indicators(
            exchange=exchange,
            target_schema=target_schema,
            target_table=target_table,
            log_schema=log_schema,
            log_table=log_table,
            frvp_table=frvp_table,
            bs_table=bs_table,
            end_dates_table=end_dates_table,
            ema_table=ema_table,
            rsi_table=rsi_table,
            mfi_table=mfi_table,
            vwap_table=vwap_table,
            pivot_table=pivot_table,
        )

        print(
            f"[MASTER] exchange={exchange} "
            f"target={target_schema}.{target_table} "
            f"log={log_schema}.{log_table} "
            f"inserted={result['master_inserted_rows']}"
        )

        return result