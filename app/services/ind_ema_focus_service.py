from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd

from app.infrastructure.database.repository import PostgresRepository
from app.core.indicators.ema.ema_math import calculate_ema_cross


@dataclass(frozen=True)
class EmaServiceConfig:
    job_name: str = "ind_ema_focus"
    calc_group: str = "EMA"
    calc_name: str = "EMA_MULTI_CROSS"
    price_col: str = "CLOSE"


class IndEmaFocusService:
    def __init__(self, repo: PostgresRepository, cfg: EmaServiceConfig = EmaServiceConfig()):
        self.repo = repo
        self.cfg = cfg

    def run(
        self,
        exchange: str,
        input_schema: str,
        input_table: str,
        ema_calc_history_days: int = 120,
        ema_signal_lookback_days: int = 20,
        is_truncate_scope: bool = True,
    ) -> None:
        exchange = exchange.upper().strip()

        if isinstance(ema_calc_history_days, tuple):
            ema_calc_history_days = int(ema_calc_history_days[0])
        if isinstance(ema_signal_lookback_days, tuple):
            ema_signal_lookback_days = int(ema_signal_lookback_days[0])

        ema_calc_history_days = int(ema_calc_history_days)
        ema_signal_lookback_days = int(ema_signal_lookback_days)

        symbols = self.repo.get_cloned_focus_symbols(exchange=exchange)
        if not symbols:
            print(f"[EMA] No in-scope symbols found. exchange={exchange}")
            return

        if is_truncate_scope:
            deleted = self.repo.delete_ind_ema_scope(exchange=exchange)
            print(f"[EMA] Cleared output scope: exchange={exchange} deleted_rows={deleted}")

        rows = self.repo.fetch_last_n_days_close_for_symbols(
            schema=input_schema,
            table=input_table,
            exchange=exchange,
            symbols=symbols,
            n_days=ema_calc_history_days,
            ts_col="TIMESTAMP",
            close_col=self.cfg.price_col,
        )

        if not rows:
            print(f"[EMA] No input rows returned. exchange={exchange}")
            return

        df = pd.DataFrame(rows)
        if df.empty:
            print(f"[EMA] Input dataframe empty. exchange={exchange}")
            return

        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
        df[self.cfg.price_col] = pd.to_numeric(df[self.cfg.price_col], errors="coerce")
        df = df.dropna(subset=["SYMBOL", "TIMESTAMP", self.cfg.price_col])

        if df.empty:
            print(f"[EMA] Input dataframe became empty after cleaning. exchange={exchange}")
            return

        df = df.sort_values(["SYMBOL", "TIMESTAMP"]).reset_index(drop=True)

        try:
            df_result = calculate_ema_cross(
                df=df,
                price_col=self.cfg.price_col,
                signal_lookback_days=ema_signal_lookback_days,
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

        if "SYMBOL" not in df_result.columns:
            raise ValueError(f"EMA result missing SYMBOL column. Columns={list(df_result.columns)}")

        if "TIMESTAMP" not in df_result.columns:
            raise ValueError(f"EMA result missing TIMESTAMP column. Columns={list(df_result.columns)}")

        if df_result.empty:
            print(f"[EMA] No EMA result rows after calculation. exchange={exchange}")
            return

        # Latest row per symbol
        df_result = df_result.sort_values(["SYMBOL", "TIMESTAMP"]).reset_index(drop=True)
        latest_rows = (
            df_result.groupby("SYMBOL", as_index=False, group_keys=False)
            .tail(1)
            .reset_index(drop=True)
        )

        # END_DATE = last timestamp in full fetched input dataset per symbol
        end_dates = (
            df.groupby("SYMBOL", as_index=False)["TIMESTAMP"]
            .max()
            .rename(columns={"TIMESTAMP": "END_DATE"})
        )

        latest_rows = latest_rows.merge(end_dates, on="SYMBOL", how="left")

        out_rows: List[Dict[str, Any]] = []
        created_at = datetime.now()

        total_scope = len(symbols)
        computed_symbols = 0

        for _, row in latest_rows.iterrows():
            try:
                out_rows.append({
                    "EXCHANGE": exchange,
                    "SYMBOL": str(row["SYMBOL"]),
                    "TIMESTAMP": pd.to_datetime(row["TIMESTAMP"]).to_pydatetime(),
                    "END_DATE": pd.to_datetime(row["END_DATE"]).to_pydatetime(),

                    "EMA3": float(row["EMA3"]) if pd.notna(row["EMA3"]) else None,
                    "EMA5": float(row["EMA5"]) if pd.notna(row["EMA5"]) else None,
                    "EMA14": float(row["EMA14"]) if pd.notna(row["EMA14"]) else None,
                    "EMA20": float(row["EMA20"]) if pd.notna(row["EMA20"]) else None,

                    "EMA_STATUS_5_20": int(row["EMA_Status_5_20"]) if pd.notna(row["EMA_Status_5_20"]) else None,
                    "EMA_CROSS_5_20": int(row["EMA_Cross_5_20"]) if pd.notna(row["EMA_Cross_5_20"]) else None,

                    "EMA_STATUS_3_20": int(row["EMA_Status_3_20"]) if pd.notna(row["EMA_Status_3_20"]) else None,
                    "EMA_CROSS_3_20": int(row["EMA_Cross_3_20"]) if pd.notna(row["EMA_Cross_3_20"]) else None,

                    "EMA_STATUS_3_14": int(row["EMA_Status_3_14"]) if pd.notna(row["EMA_Status_3_14"]) else None,
                    "EMA_CROSS_3_14": int(row["EMA_Cross_3_14"]) if pd.notna(row["EMA_Cross_3_14"]) else None,

                    "DAYS_SINCE_CROSS_5_20": int(row["DAYS_SINCE_CROSS_5_20"]) if pd.notna(row["DAYS_SINCE_CROSS_5_20"]) else None,
                    "DAYS_SINCE_CROSS_3_20": int(row["DAYS_SINCE_CROSS_3_20"]) if pd.notna(row["DAYS_SINCE_CROSS_3_20"]) else None,
                    "DAYS_SINCE_CROSS_3_14": int(row["DAYS_SINCE_CROSS_3_14"]) if pd.notna(row["DAYS_SINCE_CROSS_3_14"]) else None,

                    "CREATED_AT": created_at,
                })
                computed_symbols += 1
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

        inserted = self.repo.insert_ind_ema_focus_rows(out_rows)

        print(
            f"[EMA] exchange={exchange} "
            f"symbols_in_scope={total_scope} "
            f"history_days={ema_calc_history_days} "
            f"signal_lookback_days={ema_signal_lookback_days} "
            f"computed_symbols={computed_symbols} "
            f"inserted={inserted}"
        )