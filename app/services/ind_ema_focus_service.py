from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd

from app.infrastructure.database.repository import PostgresRepository


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
        lookback_days: int = 20,
        is_truncate_scope: bool = True,
    ) -> None:
        exchange = exchange.upper().strip()

        symbols = self.repo.get_ema_focus_symbols(exchange=exchange)
        if not symbols:
            print(f"[EMA] No in-scope symbols found. exchange={exchange}")
            return

        if is_truncate_scope:
            deleted = self.repo.delete_ind_ema_scope(exchange=exchange)
            print(f"[EMA] Cleared output scope: exchange={exchange} deleted_rows={deleted}")

        # 1) Fetch all symbols last N rows in ONE query
        rows = self.repo.fetch_last_n_days_close_for_symbols(
            schema=input_schema,
            table=input_table,
            exchange=exchange,
            symbols=symbols,
            n_days=lookback_days,
            ts_col="TIMESTAMP",
            close_col="CLOSE",
        )

        if not rows:
            print(f"[EMA] No input data returned. exchange={exchange}")
            return

        df = pd.DataFrame(rows)
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"])
        df = df.sort_values(["SYMBOL", "TIMESTAMP"]).reset_index(drop=True)

        out_rows = []
        created_at = datetime.now()

        total = len(symbols)
        processed = 0

        for symbol, g in df.groupby("SYMBOL", sort=False):
            try:
                # g already sorted
                if g.empty:
                    continue

                # --- SAME MATH LOGIC AS OLD CODE ---
                g = g.copy()

                g["EMA5"] = g[self.cfg.price_col].ewm(span=5, adjust=False).mean()
                g["EMA20"] = g[self.cfg.price_col].ewm(span=20, adjust=False).mean()
                g["EMA_STATUS"] = (g["EMA5"] > g["EMA20"]).astype(int)
                g["EMA_CROSS"] = g["EMA_STATUS"].diff().fillna(0).astype(int)

                last_cross_pos = None
                days_since = []

                # NOTE: pandas iterrows ok because g is small (<= lookback_days)
                for pos, row in g.reset_index(drop=True).iterrows():
                    if row["EMA_CROSS"] == 1:
                        last_cross_pos = pos
                    if row["EMA_STATUS"] == 0 or last_cross_pos is None:
                        days_since.append(0)
                    else:
                        days_since.append(pos - last_cross_pos)

                g["DAYS_SINCE_CROSS"] = days_since

                # only last row output
                last = g.iloc[-1]

                # Filter rule same as old output behavior:
                # old code returns result[result["DAYS_SINCE_CROSS"].between(0, 20)]
                # burada zaten last row alıyoruz, ama yine de keep condition uygulayalım.
                if not (0 <= int(last["DAYS_SINCE_CROSS"]) <= lookback_days):
                    continue

                out_rows.append({
                    "EXCHANGE": exchange,
                    "SYMBOL": symbol,
                    "END_DATE": pd.to_datetime(last["TIMESTAMP"]).to_pydatetime(),
                    "EMA5": float(last["EMA5"]) if pd.notna(last["EMA5"]) else None,
                    "EMA20": float(last["EMA20"]) if pd.notna(last["EMA20"]) else None,
                    "EMA_STATUS": int(last["EMA_STATUS"]) if pd.notna(last["EMA_STATUS"]) else None,
                    "EMA_CROSS": int(last["EMA_CROSS"]) if pd.notna(last["EMA_CROSS"]) else None,
                    "DAYS_SINCE_CROSS": int(last["DAYS_SINCE_CROSS"]) if pd.notna(last["DAYS_SINCE_CROSS"]) else None,
                    "CREATED_AT": created_at,
                })

                processed += 1

            except Exception as e:
                # reuse logs.INDICATOR_ERRORS
                self.repo.log_indicator_error(
                    job_name=self.cfg.job_name,
                    calc_group=self.cfg.calc_group,
                    calc_name=self.cfg.calc_name,
                    exchange=exchange,
                    symbol=symbol,
                    interval="daily",
                    frvp_period_type=None,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    error_stack=traceback.format_exc(),
                )

        inserted = self.repo.insert_ind_ema_focus_rows(out_rows)
        print(f"[EMA] exchange={exchange} symbols_in_scope={total} computed={processed} inserted={inserted}")