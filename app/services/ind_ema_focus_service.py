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
    calc_name: str = "EMA5_EMA20_CROSS"
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
        """
        Flow:
        1) Read EMA scope symbols from silver.IND_FRV_POC_PROFILE where IN_SCOPE_FOR_EMA_RSI = True
        2) Fetch last ema_calc_history_days daily rows for all those symbols
        3) Apply legacy EMA math exactly
        4) Restrict final outputs to latest row per symbol
        5) Write to silver.IND_EMA_FOCUS after exchange-based delete
        """
        exchange = exchange.upper().strip()

        symbols = self.repo.get_ema_focus_symbols(exchange=exchange)
        if not symbols:
            print(f"[EMA] No in-scope symbols found. exchange={exchange}")
            return

        if is_truncate_scope:
            deleted = self.repo.delete_ind_ema_scope(exchange=exchange)
            print(f"[EMA] Cleared output scope: exchange={exchange} deleted_rows={deleted}")

        # Fetch enough history so EMA20 matches legacy pandas better
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

        # Keep only latest row per symbol
        df_result = df_result.sort_values(["SYMBOL", "TIMESTAMP"]).reset_index(drop=True)
        latest_rows = (
            df_result.groupby("SYMBOL", as_index=False, group_keys=False)
            .tail(1)
            .reset_index(drop=True)
        )

        out_rows: List[Dict[str, Any]] = []
        created_at = datetime.now()

        total_scope = len(symbols)
        computed_symbols = 0

        for _, row in latest_rows.iterrows():
            try:
                out_rows.append({
                    "EXCHANGE": exchange,
                    "SYMBOL": str(row["SYMBOL"]),
                    "END_DATE": pd.to_datetime(row["TIMESTAMP"]).to_pydatetime(),
                    "EMA5": float(row["EMA5"]) if pd.notna(row["EMA5"]) else None,
                    "EMA20": float(row["EMA20"]) if pd.notna(row["EMA20"]) else None,
                    "EMA_STATUS": int(row["EMA_Status"]) if pd.notna(row["EMA_Status"]) else None,
                    "EMA_CROSS": int(row["EMA_Cross"]) if pd.notna(row["EMA_Cross"]) else None,
                    "DAYS_SINCE_CROSS": int(row["DAYS_SINCE_CROSS"]) if pd.notna(row["DAYS_SINCE_CROSS"]) else None,
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