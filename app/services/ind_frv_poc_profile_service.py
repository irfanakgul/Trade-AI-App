from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any

import pandas as pd
from dateutil.relativedelta import relativedelta

from app.infrastructure.database.repository import PostgresRepository
from app.core.indicators.frvp.frvp_math_fast import calculate_tv_frvp_v2_fast as calculate_tv_frvp_v2

@dataclass(frozen=True)
class FrvpServiceConfig:
    interval: str = "1min"
    job_name: str = "ind_frv_poc_profile"
    calc_group: str = "FRVP"
    calc_name: str = "POC_VAL_VAH"
    max_concurrent_symbols: int = 1  # IMPORTANT: keep low; CPU-heavy math


class IndFrvPocProfileService:
    def __init__(self, repo: PostgresRepository, cfg: FrvpServiceConfig = FrvpServiceConfig()):
        self.repo = repo
        self.cfg = cfg

    def run(
        self,
        exchange: str,
        periods: List[str],
        cutt_off_date: Optional[str],
        is_truncate_scope: bool = True,
    ) -> None:
        symbols = self.repo.get_frvp_focus_symbols(exchange=exchange)
        if not symbols:
            print(f"[FRVP] No symbols found for exchange={exchange}")
            return

        # Clean output for the requested scope (exchange + interval + periods)
        if is_truncate_scope:
            deleted = self.repo.delete_ind_frvp_scope(
                exchange=exchange,
                interval=self.cfg.interval,
                periods=periods,
            )
            print(f"[FRVP] Scope cleaned: exchange={exchange} interval={self.cfg.interval} deleted_rows={deleted}")

        source_table = "FRVP_USA_FOCUS_DATASET" if exchange == "USA" else "FRVP_BIST_FOCUS_DATASET"

        # Precompute max lookback for one DB fetch per symbol
        max_delta = self._max_period_delta(periods)

        total = len(symbols)
        for idx, symbol in enumerate(symbols, start=1):
            try:
                self._process_symbol(
                    idx=idx,
                    total=total,
                    exchange=exchange,
                    symbol=symbol,
                    source_table=source_table,
                    periods=periods,
                    cutt_off_date=cutt_off_date,
                )
            except Exception as e:
                self._log_error(
                    exchange=exchange,
                    symbol=symbol,
                    interval=self.cfg.interval,
                    frvp_period_type=None,
                    e=e,
                )

    def _process_symbol(
        self,
        idx: int,
        total: int,
        exchange: str,
        symbol: str,
        source_table: str,
        periods: List[str],
        cutt_off_date: Optional[str],
    ) -> None:
        max_ts = self.repo.get_symbol_max_ts(table=source_table, symbol=symbol, ts_col="TS")
        if max_ts is None:
            print(f"[FRVP {exchange} {idx}/{total}] {symbol}: no data")
            return

        if cutt_off_date:
            cutoff_dt = pd.to_datetime(cutt_off_date)
            end_ts = min(max_ts, cutoff_dt.to_pydatetime())
        else:
            end_ts = max_ts

        out_rows: List[Dict[str, Any]] = []

        for p in periods:
            try:
                delta = self._period_to_delta(p)
                window_start = (pd.Timestamp(end_ts) - delta).to_pydatetime()

                # 1) Find peak timestamp in the period window via SQL (fast)
                peak_ts = self.repo.get_peak_ts_in_window(
                    table=source_table,
                    symbol=symbol,
                    start_ts=window_start,
                    end_ts=end_ts,
                    ts_col="TS",
                    high_col="HIGH",
                )
                if peak_ts is None:
                    continue

                # 2) Fetch only peak_ts -> end_ts OHLCV (much smaller)
                rows = self.repo.fetch_ohlcv_between_no_rowid(
                    table=source_table,
                    symbol=symbol,
                    start_ts=peak_ts,
                    end_ts=end_ts,
                    ts_col="TS",
                    chunk_size=100000,
                )
                if not rows:
                    continue

                df_final = pd.DataFrame(rows)
                df_final["TS"] = pd.to_datetime(df_final["TS"])
                df_final = df_final.sort_values("TS")

                min_date = df_final["TS"].min()
                max_date = df_final["TS"].max()

                row_count = int(len(df_final))
                day_count = int(df_final["TS"].dt.date.nunique())

                # Fetch ROW_ID only for the peak point (tiny query)
                highest_row_id = self.repo.get_row_id_at_ts(
                    table=source_table,
                    symbol=symbol,
                    ts_value=peak_ts,
                    ts_col="TS",
                )

                # IMPORTANT: do not change FRVP math
                math_df = df_final.rename(columns={"TS": "DATETIME"})
                result = calculate_tv_frvp_v2(
                    math_df[["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]].copy(),
                    value_area_pct=68,
                    row_size=1,
                )

                runtime_str = datetime.now().strftime("%d-%m-%Y %H:%M")

                out_rows.append({
                    "EXCHANGE": exchange,
                    "SYMBOL": symbol,
                    "INTERVAL": self.cfg.interval,
                    "FRVP_PERIOD_TYPE": p,
                    "MIN_DATE": min_date.to_pydatetime(),
                    "HIGHEST_DATE": pd.to_datetime(peak_ts).to_pydatetime(),
                    "MAX_DATE": max_date.to_pydatetime(),
                    "HIGHEST_ROW_ID": str(highest_row_id) if highest_row_id else None,
                    "ROW_COUNT_AFTER_HIGHEST": row_count,
                    "DAY_COUNT_AFTER_HIGHEST": day_count,
                    "CUTT_OFF_DATE": pd.to_datetime(cutt_off_date).to_pydatetime() if cutt_off_date else None,
                    "POC": float(result["POC"]),
                    "VAL": float(result["VAL"]),
                    "VAH": float(result["VAH"]),
                    "RUNTIME": runtime_str,
                })

            except Exception as e:
                self._log_error(
                    exchange=exchange,
                    symbol=symbol,
                    interval=self.cfg.interval,
                    frvp_period_type=p,
                    e=e,
                )

        inserted = self.repo.insert_ind_frvp_rows(out_rows)
        print(f"[FRVP {exchange} {idx}/{total}] {symbol}: inserted_rows={inserted}")

    def _log_error(
        self,
        exchange: str,
        symbol: str,
        interval: Optional[str],
        frvp_period_type: Optional[str],
        e: Exception,
    ) -> None:
        self.repo.log_indicator_error(
            job_name=self.cfg.job_name,
            calc_group=self.cfg.calc_group,
            calc_name=self.cfg.calc_name,
            exchange=exchange,
            symbol=symbol,
            interval=interval,
            frvp_period_type=frvp_period_type,
            error_type=type(e).__name__,
            error_message=str(e),
            error_stack=traceback.format_exc(),
        )

    def _period_to_delta(self, period: str) -> relativedelta:
        p = period.strip().lower()
        if p.endswith("year") or p.endswith("years"):
            n = int("".join([c for c in p if c.isdigit()]) or "1")
            return relativedelta(years=n)
        if "month" in p:
            n = int("".join([c for c in p if c.isdigit()]) or "1")
            return relativedelta(months=n)
        if "week" in p:
            n = int("".join([c for c in p if c.isdigit()]) or "1")
            return relativedelta(weeks=n)
        if "day" in p:
            n = int("".join([c for c in p if c.isdigit()]) or "1")
            return relativedelta(days=n)
        raise ValueError(f"Unsupported period: {period}")

    def _max_period_delta(self, periods: List[str]) -> relativedelta:
        # Conservative: choose the widest range by converting to an approximate ordering
        # without changing date math in actual filtering.
        # We'll map year/month/week/day to a comparable scale just to pick "largest".
        best = None
        best_score = -1

        for p in periods:
            d = self._period_to_delta(p)
            score = (d.years or 0) * 10_000 + (d.months or 0) * 500 + (d.weeks or 0) * 100 + (d.days or 0)
            if score > best_score:
                best_score = score
                best = d

        return best or relativedelta(years=2)