from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd

from app.infrastructure.database.repository import PostgresRepository
from app.core.indicators.rsi.rsi_math import calculate_rsi_features


@dataclass(frozen=True)
class RsiServiceConfig:
    job_name: str = "ind_rsi_focus"
    calc_group: str = "RSI"
    calc_name: str = "RSI14_RSI_MA14_CROSS"
    price_col: str = "CLOSE"
    rsi_length: int = 14
    ma_length: int = 14


class IndRsiFocusService:
    def __init__(self, repo: PostgresRepository, cfg: RsiServiceConfig = RsiServiceConfig()):
        self.repo = repo
        self.cfg = cfg

    def run(
        self,
        exchange: str,
        input_schema: str,
        input_table: str,
        rsi_calc_history_days: int = 120,
        rsi_signal_lookback_days: int = 20,
        is_truncate_scope: bool = True,
    ) -> None:
        exchange = exchange.upper().strip()

        # Defensive normalization
        if isinstance(rsi_calc_history_days, tuple):
            rsi_calc_history_days = int(rsi_calc_history_days[0])
        if isinstance(rsi_signal_lookback_days, tuple):
            rsi_signal_lookback_days = int(rsi_signal_lookback_days[0])

        rsi_calc_history_days = int(rsi_calc_history_days)
        rsi_signal_lookback_days = int(rsi_signal_lookback_days)

        symbols = self.repo.get_cloned_focus_symbols(exchange=exchange)
        if not symbols:
            print(f"[RSI] No in-scope symbols found. exchange={exchange}")
            return

        if is_truncate_scope:
            deleted = self.repo.delete_ind_rsi_scope(exchange=exchange)
            print(f"[RSI] Cleared output scope: exchange={exchange} deleted_rows={deleted}")

        rows = self.repo.fetch_last_n_days_close_for_symbols(
            schema=input_schema,
            table=input_table,
            exchange=exchange,
            symbols=symbols,
            n_days=rsi_calc_history_days,
            ts_col="TIMESTAMP",
            close_col=self.cfg.price_col,
        )

        if not rows:
            print(f"[RSI] No input rows returned. exchange={exchange}")
            return

        df = pd.DataFrame(rows)
        if df.empty:
            print(f"[RSI] Input dataframe empty. exchange={exchange}")
            return

        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
        df[self.cfg.price_col] = pd.to_numeric(df[self.cfg.price_col], errors="coerce")
        df = df.dropna(subset=["SYMBOL", "TIMESTAMP", self.cfg.price_col])

        if df.empty:
            print(f"[RSI] Input dataframe became empty after cleaning. exchange={exchange}")
            return

        df = df.sort_values(["SYMBOL", "TIMESTAMP"]).reset_index(drop=True)

        try:
            df_result = calculate_rsi_features(
                df=df,
                rsi_length=self.cfg.rsi_length,
                ma_length=self.cfg.ma_length,
                price_col=self.cfg.price_col,
                signal_lookback_days=rsi_signal_lookback_days,
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
            raise ValueError(f"RSI result missing SYMBOL column. Columns={list(df_result.columns)}")

        if "TIMESTAMP" not in df_result.columns:
            raise ValueError(f"RSI result missing TIMESTAMP column. Columns={list(df_result.columns)}")

        if df_result.empty:
            print(f"[RSI] No RSI result rows after calculation. exchange={exchange}")
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

                    "RSI": float(row["RSI"]) if pd.notna(row["RSI"]) else None,
                    "RSI_MA": float(row["RSI_MA"]) if pd.notna(row["RSI_MA"]) else None,
                    "RSI_STATUS": int(row["RSI_Status"]) if pd.notna(row["RSI_Status"]) else None,
                    "RSI_CROSS": int(row["RSI_Cross"]) if pd.notna(row["RSI_Cross"]) else None,
                    "RSI_CROSS_DAYS_AGO": int(row["RSI_Cross_Days_Ago"]) if pd.notna(row["RSI_Cross_Days_Ago"]) else None,

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

        inserted = self.repo.insert_ind_rsi_focus_rows(out_rows)

        print(
            f"[RSI] exchange={exchange} "
            f"symbols_in_scope={total_scope} "
            f"history_days={rsi_calc_history_days} "
            f"signal_lookback_days={rsi_signal_lookback_days} "
            f"computed_symbols={computed_symbols} "
            f"inserted={inserted}"
        )