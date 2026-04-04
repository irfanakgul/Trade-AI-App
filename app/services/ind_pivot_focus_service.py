from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd

from app.infrastructure.database.repository import PostgresRepository
from app.core.indicators.pivot.pivot_math import calculate_camarilla_pivots # type: ignore


@dataclass(frozen=True)
class PivotServiceConfig:
    job_name: str = "ind_pivot_focus"
    calc_group: str = "PIVOT"
    calc_name: str = "CAMARILLA_YEARLY"


class IndPivotFocusService:
    def __init__(self, repo: PostgresRepository, cfg: PivotServiceConfig = PivotServiceConfig()):
        self.repo = repo
        self.cfg = cfg

    def run(
        self,
        exchange: str,
        input_schema: str,
        input_table: str,
        output_schema: str,
        output_table: str,
        lookback_days: int = 365,
        is_truncate_scope: bool = True,
    ) -> None:
        exchange = exchange.upper().strip()

        if isinstance(lookback_days, tuple):
            lookback_days = int(lookback_days[0])
        lookback_days = int(lookback_days)

        if is_truncate_scope:
            deleted = self.repo.delete_indicator_scope_by_exchange(
                schema=output_schema,
                table=output_table,
                exchange=exchange,
            )
            print(f"[PIVOT] Cleared output scope: exchange={exchange} deleted_rows={deleted}")

        rows = self.repo.fetch_last_n_days_ohlc_for_exchange(
            schema=input_schema,
            table=input_table,
            exchange=exchange,
            n_days=lookback_days,
            ts_col="TIMESTAMP",
            high_col="HIGH",
            low_col="LOW",
            close_col="CLOSE",
        )

        if not rows:
            print(f"[PIVOT] No input rows returned. exchange={exchange}")
            return

        df = pd.DataFrame(rows)
        if df.empty:
            print(f"[PIVOT] Input dataframe empty. exchange={exchange}")
            return

        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
        df["HIGH"] = pd.to_numeric(df["HIGH"], errors="coerce")
        df["LOW"] = pd.to_numeric(df["LOW"], errors="coerce")
        df["CLOSE"] = pd.to_numeric(df["CLOSE"], errors="coerce")

        df = df.dropna(subset=["EXCHANGE", "SYMBOL", "TIMESTAMP", "HIGH", "LOW", "CLOSE"])
        if df.empty:
            print(f"[PIVOT] Input dataframe became empty after cleaning. exchange={exchange}")
            return

        df = df.sort_values(["SYMBOL", "TIMESTAMP"]).reset_index(drop=True)

        try:
            df_result = calculate_camarilla_pivots(
                df=df,
                high_col="HIGH",
                low_col="LOW",
                close_col="CLOSE",
            )
        except Exception as e:
            self.repo.log_indicator_error(
                job_name=self.cfg.job_name,
                calc_group=self.cfg.calc_group,
                calc_name=self.cfg.calc_name,
                exchange=exchange,
                symbol=None,
                interval="daily",
                frvp_period_type=None,
                error_type=type(e).__name__,
                error_message=str(e),
                error_stack=traceback.format_exc(),
            )
            raise

        if df_result.empty:
            print(f"[PIVOT] No pivot result rows after calculation. exchange={exchange}")
            return

        # keep only latest row per symbol
        df_result = df_result.sort_values(["SYMBOL", "TIMESTAMP"]).reset_index(drop=True)
        latest_rows = (
            df_result.groupby("SYMBOL", as_index=False, group_keys=False)
            .tail(1)
            .reset_index(drop=True)
        )

        # start/end dates from the filtered input dataframe
        date_ranges = (
            df.groupby("SYMBOL", as_index=False)
            .agg(
                PVT_START_DATE=("TIMESTAMP", "min"),
                PVT_END_DATE=("TIMESTAMP", "max"),
            )
        )

        latest_rows = latest_rows.merge(date_ranges, on="SYMBOL", how="left")

        out_rows: List[Dict[str, Any]] = []
        created_at = datetime.now()

        for _, row in latest_rows.iterrows():
            try:
                out_rows.append({
                    "EXCHANGE": exchange,
                    "SYMBOL": str(row["SYMBOL"]),

                    "PVT_START_DATE": pd.to_datetime(row["PVT_START_DATE"]).to_pydatetime(),
                    "PVT_END_DATE": pd.to_datetime(row["PVT_END_DATE"]).to_pydatetime(),
                    "PVT_YEAR": int(row["PVT_YEAR"]) if pd.notna(row["PVT_YEAR"]) else None,

                    "PIVOT": float(row["PIVOT"]) if pd.notna(row["PIVOT"]) else None,

                    "PVT_R1": float(row["PVT_R1"]) if pd.notna(row["PVT_R1"]) else None,
                    "PVT_R2": float(row["PVT_R2"]) if pd.notna(row["PVT_R2"]) else None,
                    "PVT_R3": float(row["PVT_R3"]) if pd.notna(row["PVT_R3"]) else None,
                    "PVT_R4": float(row["PVT_R4"]) if pd.notna(row["PVT_R4"]) else None,
                    "PVT_R5": float(row["PVT_R5"]) if pd.notna(row["PVT_R5"]) else None,

                    "PVT_S1": float(row["PVT_S1"]) if pd.notna(row["PVT_S1"]) else None,
                    "PVT_S2": float(row["PVT_S2"]) if pd.notna(row["PVT_S2"]) else None,
                    "PVT_S3": float(row["PVT_S3"]) if pd.notna(row["PVT_S3"]) else None,
                    "PVT_S4": float(row["PVT_S4"]) if pd.notna(row["PVT_S4"]) else None,
                    "PVT_S5": float(row["PVT_S5"]) if pd.notna(row["PVT_S5"]) else None,

                    "CREATED_AT": created_at,
                })
            except Exception as e:
                self.repo.log_indicator_error(
                    job_name=self.cfg.job_name,
                    calc_group=self.cfg.calc_group,
                    calc_name=self.cfg.calc_name,
                    exchange=exchange,
                    symbol=str(row["SYMBOL"]) if "SYMBOL" in row else None,
                    interval="daily",
                    frvp_period_type=None,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    error_stack=traceback.format_exc(),
                )

        inserted = self.repo.insert_ind_pivot_focus_rows(
            schema=output_schema,
            table=output_table,
            rows=out_rows,
        )

        print(
            f"[PIVOT] exchange={exchange} "
            f"lookback_days={lookback_days} "
            f"computed_symbols={len(out_rows)} "
            f"inserted={inserted}"
        )