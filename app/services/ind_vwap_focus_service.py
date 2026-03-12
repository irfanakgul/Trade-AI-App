from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from app.infrastructure.database.repository import PostgresRepository
from app.core.indicators.vwap.vwap_math import build_anchored_vwap_summary


@dataclass(frozen=True)
class VwapFocusConfig:
    job_name: str = "ind_vwap_focus"
    calc_group: str = "VWAP"
    calc_name: str = "ANCHORED_VWAP"


class IndVwapFocusService:
    def __init__(self, repo: PostgresRepository, cfg: VwapFocusConfig = VwapFocusConfig()):
        self.repo = repo
        self.cfg = cfg

    def run(
        self,
        exchange: str,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        lookback_month: int = 4,
        is_truncate_scope: bool = True,
    ) -> None:
        exchange = exchange.upper().strip()

        if is_truncate_scope:
            deleted = self.repo.delete_ind_vwap_scope(
                schema=target_schema,
                table=target_table,
                exchange=exchange,
            )
            print(f"[VWAP] Cleared output scope: exchange={exchange} deleted_rows={deleted}")

        rows = self.repo.fetch_vwap_focus_source_data(
            source_schema=source_schema,
            source_table=source_table,
            exchange=exchange,
            lookback_month=lookback_month,
        )

        if not rows:
            print(f"[VWAP] No source data returned. exchange={exchange}")
            return

        try:
            df = pd.DataFrame(rows)
            summary_df = build_anchored_vwap_summary(df)

            if summary_df.empty:
                print(f"[VWAP] Summary empty. exchange={exchange}")
                return

            created_at = datetime.now()
            out_rows = []

            for _, row in summary_df.iterrows():
                out_rows.append(
                    {
                        "EXCHANGE": row["EXCHANGE"],
                        "SYMBOL": row["SYMBOL"],
                        "START_TIME": row["START_TIME"],
                        "END_TIME": row["END_TIME"],
                        "HIGHEST_VALUE": row["HIGHEST_VALUE"],
                        "HIGHEST_TIMESTAMP": row["HIGHEST_TIMESTAMP"],
                        "VWAP": row["VWAP"],

                        "AVG_VOLUME_10D": row["AVG_VOLUME_10D"],
                        "AVG_VOLUME_20D": row["AVG_VOLUME_20D"],
                        "AVG_VOLUME_30D": row["AVG_VOLUME_30D"],

                        "CREATED_AT": created_at,
                    }
                )

            inserted = self.repo.insert_ind_vwap_rows(
                target_schema=target_schema,
                target_table=target_table,
                rows=out_rows,
            )

            print(
                f"[VWAP] exchange={exchange} lookback_month={lookback_month} "
                f"symbols={len(summary_df)} inserted={inserted}"
            )

        except Exception as e:
            self.repo.log_indicator_error(
                job_name=self.cfg.job_name,
                calc_group=self.cfg.calc_group,
                calc_name=self.cfg.calc_name,
                exchange=exchange,
                symbol="__ALL__",
                interval="daily",
                frvp_period_type=None,
                error_type=type(e).__name__,
                error_message=str(e),
                error_stack=traceback.format_exc(),
            )
            raise