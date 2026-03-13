from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd

from app.infrastructure.database.repository import PostgresRepository
from app.core.indicators.mfi.mfi_math import calculate_mfi


@dataclass(frozen=True)
class MfiServiceConfig:
    job_name: str = "ind_mfi_focus"
    calc_group: str = "MFI"
    calc_name: str = "MFI14"
    price_col: str = "CLOSE"
    volume_col: str = "VOLUME"
    high_col: str = "HIGH"
    low_col: str = "LOW"
    mfi_length: int = 14


class IndMfiFocusService:
    def __init__(self, repo: PostgresRepository, cfg: MfiServiceConfig = MfiServiceConfig()):
        self.repo = repo
        self.cfg = cfg

    def run(
        self,
        exchange: str,
        input_schema: str,
        input_table: str,
        mfi_calc_history_days: int = 120,
        is_truncate_scope: bool = True,
    ) -> None:
        exchange = exchange.upper().strip()

        if isinstance(mfi_calc_history_days, tuple):
            mfi_calc_history_days = int(mfi_calc_history_days[0])
        mfi_calc_history_days = int(mfi_calc_history_days)

        if not input_schema or not str(input_schema).strip():
            raise ValueError(f"MFI input_schema is empty. exchange={exchange}, input_table={input_table}")

        if not input_table or not str(input_table).strip():
            raise ValueError(f"MFI input_table is empty. exchange={exchange}")

        # Same symbol scope as EMA / RSI
        symbols = self.repo.get_ema_focus_symbols(exchange=exchange)
        if not symbols:
            print(f"[MFI] No in-scope symbols found. exchange={exchange}")
            return

        if is_truncate_scope:
            deleted = self.repo.delete_ind_mfi_scope(exchange=exchange)
            print(f"[MFI] Cleared output scope: exchange={exchange} deleted_rows={deleted}")

        rows = self.repo.fetch_last_n_days_ohlcv_for_symbols(
            schema=input_schema,
            table=input_table,
            exchange=exchange,
            symbols=symbols,
            n_days=mfi_calc_history_days,
            ts_col="TIMESTAMP",
            close_col=self.cfg.price_col,
            volume_col=self.cfg.volume_col,
            high_col=self.cfg.high_col,
            low_col=self.cfg.low_col,
        )

        if not rows:
            print(f"[MFI] No input rows returned. exchange={exchange}")
            return

        df = pd.DataFrame(rows)
        if df.empty:
            print(f"[MFI] Input dataframe empty. exchange={exchange}")
            return

        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
        for c in ["CLOSE", "VOLUME", "HIGH", "LOW"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # CLOSE / VOLUME must exist
        df = df.dropna(subset=["SYMBOL", "TIMESTAMP", "CLOSE", "VOLUME"])

        if df.empty:
            print(f"[MFI] Input dataframe became empty after cleaning. exchange={exchange}")
            return

        df = df.sort_values(["SYMBOL", "TIMESTAMP"]).reset_index(drop=True)

        try:
            df_result = calculate_mfi(
                df=df,
                length=self.cfg.mfi_length,
                price_col=self.cfg.price_col,
                volume_col=self.cfg.volume_col,
                high_col=self.cfg.high_col,
                low_col=self.cfg.low_col,
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
            raise ValueError(f"MFI result missing SYMBOL column. Columns={list(df_result.columns)}")

        if "TIMESTAMP" not in df_result.columns:
            raise ValueError(f"MFI result missing TIMESTAMP column. Columns={list(df_result.columns)}")

        if df_result.empty:
            print(f"[MFI] No MFI result rows after calculation. exchange={exchange}")
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

                    "MFI": float(row["MFI"]) if pd.notna(row["MFI"]) else None,
                    "MF_TODAY": float(row["MF_TODAY"]) if pd.notna(row["MF_TODAY"]) else None,
                    "MF_YESTERDAY": float(row["MF_YESTERDAY"]) if pd.notna(row["MF_YESTERDAY"]) else None,
                    "MF_12DAY_AVG": float(row["MF_12DAY_AVG"]) if pd.notna(row["MF_12DAY_AVG"]) else None,
                    "MF_DIRECTION": str(row["MF_DIRECTION"]) if pd.notna(row["MF_DIRECTION"]) else None,

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

        inserted = self.repo.insert_ind_mfi_focus_rows(out_rows)

        print(
            f"[MFI] exchange={exchange} "
            f"symbols_in_scope={total_scope} "
            f"history_days={mfi_calc_history_days} "
            f"computed_symbols={computed_symbols} "
            f"inserted={inserted}"
        )