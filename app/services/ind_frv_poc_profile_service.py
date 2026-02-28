from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any

import pandas as pd
from dateutil.relativedelta import relativedelta

from app.infrastructure.database.repository import PostgresRepository
from app.core.indicators.frvp.frvp_math_fast import (
    calculate_tv_frvp_v2_fast as calculate_tv_frvp_v2,
)


@dataclass(frozen=True)
class FrvpServiceConfig:
    interval: str = "1min"
    job_name: str = "ind_frv_poc_profile"
    calc_group: str = "FRVP"
    calc_name: str = "POC_VAL_VAH"
    max_concurrent_symbols: int = 1  # CPU-heavy math; keep low


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
        exchange = exchange.upper().strip()

        symbols = self.repo.get_frvp_focus_symbols(exchange=exchange)
        if not symbols:
            print(f"[FRVP] No symbols found for exchange={exchange}")
            return

        periods_sorted = self._sort_periods_short_to_long(periods)
        if not periods_sorted:
            print(f"[FRVP] No valid periods provided. exchange={exchange}")
            return

        if is_truncate_scope:
            deleted = self.repo.delete_ind_frvp_scope(
                exchange=exchange,
                interval=self.cfg.interval,
                periods=periods_sorted,
            )
            print(
                f"[FRVP] Scope cleaned: exchange={exchange} interval={self.cfg.interval} "
                f"deleted_rows={deleted}"
            )

        source_table = "FRVP_USA_FOCUS_DATASET" if exchange == "USA" else "FRVP_BIST_FOCUS_DATASET"

        total = len(symbols)
        for idx, symbol in enumerate(symbols, start=1):
            try:
                self._process_symbol(
                    idx=idx,
                    total=total,
                    exchange=exchange,
                    symbol=symbol,
                    source_table=source_table,
                    periods_sorted=periods_sorted,
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

        total_sym, true_sym = self.repo.update_in_scope_for_ema_rsi(
            exchange=exchange,
            interval=self.cfg.interval,
        )
        print(
            f"[FRVP {exchange}] IN_SCOPE_FOR_EMA_RSI computed. "
            f"unique_symbols={total_sym} true_symbols={true_sym}"
        )
        
    def _process_symbol(
        self,
        idx: int,
        total: int,
        exchange: str,
        symbol: str,
        source_table: str,
        periods_sorted: List[str],
        cutt_off_date: Optional[str],
    ) -> None:
        # Determine end_ts
        max_ts = self.repo.get_symbol_max_ts(table=source_table, symbol=symbol, ts_col="TS")
        if max_ts is None:
            print(f"[FRVP {exchange} {idx}/{total}] {symbol}: no data")
            return

        if cutt_off_date:
            cutoff_dt = pd.to_datetime(cutt_off_date)
            end_ts = min(max_ts, cutoff_dt.to_pydatetime())
        else:
            end_ts = max_ts

        # Latest close once per symbol (same for all periods)
        latest_close = self.repo.get_latest_close_value(
            schema="silver",
            table=source_table,
            symbol=symbol,
            ts_col="TS",
            close_col="CLOSE",
        )

        # ---- Optimization: find global peak in the LONGEST window once ----
        longest_p = periods_sorted[-1]
        longest_delta = self._period_to_delta(longest_p)
        longest_start = (pd.Timestamp(end_ts) - longest_delta).to_pydatetime()

        global_peak_ts = self.repo.get_peak_ts_in_window(
            table=source_table,
            symbol=symbol,
            start_ts=longest_start,
            end_ts=end_ts,
            ts_col="TS",
            high_col="HIGH",
        )
        if global_peak_ts is None:
            print(f"[FRVP {exchange} {idx}/{total}] {symbol}: no peak in longest window")
            return

        global_peak_dt = pd.to_datetime(global_peak_ts).to_pydatetime()

        # Determine BASE_PERIOD: the first (shortest) period window that contains the global peak
        base_period = longest_p
        base_index = len(periods_sorted) - 1

        for i, p in enumerate(periods_sorted):
            d = self._period_to_delta(p)
            w_start = (pd.Timestamp(end_ts) - d).to_pydatetime()
            if global_peak_dt >= w_start:
                base_period = p
                base_index = i
                break

        out_rows: List[Dict[str, Any]] = []

        # ---- 1) SHORT periods (strictly shorter than base) computed normally ----
        short_periods = periods_sorted[:base_index]
        for p in short_periods:
            try:
                row = self._compute_period_row(
                    exchange=exchange,
                    symbol=symbol,
                    source_table=source_table,
                    period=p,
                    end_ts=end_ts,
                    cutt_off_date=cutt_off_date,
                    forced_peak_ts=None,              # compute peak per-window
                    based_period=p,                   # computed from itself
                    latest_close=latest_close,
                )
                if row:
                    out_rows.append(row)
            except Exception as e:
                self._log_error(
                    exchange=exchange,
                    symbol=symbol,
                    interval=self.cfg.interval,
                    frvp_period_type=p,
                    e=e,
                )

        # ---- 2) BASE period computed once using global peak ----
        base_row: Optional[Dict[str, Any]] = None
        try:
            base_row = self._compute_period_row(
                exchange=exchange,
                symbol=symbol,
                source_table=source_table,
                period=base_period,
                end_ts=end_ts,
                cutt_off_date=cutt_off_date,
                forced_peak_ts=global_peak_dt,       # IMPORTANT: global peak
                based_period=base_period,            # computed from base_period
                latest_close=latest_close,
            )
            if base_row:
                # Ensure not blank (hard safety)
                base_row["BASED_PERIOD"] = base_row.get("BASED_PERIOD") or base_period
                out_rows.append(base_row)
        except Exception as e:
            self._log_error(
                exchange=exchange,
                symbol=symbol,
                interval=self.cfg.interval,
                frvp_period_type=base_period,
                e=e,
            )

        # ---- 3) LONGER periods copied from BASE (no math) ----
        copied_cnt = 0
        if base_row:
            longer_periods = periods_sorted[base_index + 1 :]
            for p in longer_periods:
                copied = dict(base_row)
                copied["FRVP_PERIOD_TYPE"] = p
                copied["BASED_PERIOD"] = base_period  # GUARANTEE
                # If you want unique runtime per copied row, uncomment:
                # copied["RUNTIME"] = datetime.now().strftime("%d-%m-%Y %H:%M")
                out_rows.append(copied)
                copied_cnt += 1

        inserted = self.repo.insert_ind_frvp_rows(out_rows)

        print(
            f"[FRVP {exchange} {idx}/{total}] {symbol}: inserted_rows={inserted} "
            f"base_period={base_period} short={len(short_periods)} copied={copied_cnt}"
        )

    def _compute_period_row(
        self,
        exchange: str,
        symbol: str,
        source_table: str,
        period: str,
        end_ts: datetime,
        cutt_off_date: Optional[str],
        forced_peak_ts: Optional[datetime],
        based_period: Optional[str],
        latest_close: Optional[float],
    ) -> Optional[Dict[str, Any]]:
        """
        Computes one FRVP row for a single period.
        FRVP math is NOT changed.

        forced_peak_ts:
          - None => compute peak_ts via SQL in that period window
          - datetime => use given peak_ts directly (BASE optimization)
        """
        period = period.strip()
        based_period = (based_period or period).strip()  # GUARANTEE not blank

        if forced_peak_ts is None:
            delta = self._period_to_delta(period)
            window_start = (pd.Timestamp(end_ts) - delta).to_pydatetime()

            peak_ts = self.repo.get_peak_ts_in_window(
                table=source_table,
                symbol=symbol,
                start_ts=window_start,
                end_ts=end_ts,
                ts_col="TS",
                high_col="HIGH",
            )
            if peak_ts is None:
                return None
            peak_dt = pd.to_datetime(peak_ts).to_pydatetime()
        else:
            peak_dt = forced_peak_ts

        # Fetch only peak_dt -> end_ts OHLCV
        rows = self.repo.fetch_ohlcv_between_no_rowid(
            table=source_table,
            symbol=symbol,
            start_ts=peak_dt,
            end_ts=end_ts,
            ts_col="TS",
            chunk_size=100000,
        )
        if not rows:
            return None

        df_final = pd.DataFrame(rows)
        df_final["TS"] = pd.to_datetime(df_final["TS"])
        df_final = df_final.sort_values("TS")

        min_date = df_final["TS"].min()
        max_date = df_final["TS"].max()

        row_count = int(len(df_final))
        day_count = int(df_final["TS"].dt.date.nunique())

        # Fetch ROW_ID only for the peak point
        highest_row_id = self.repo.get_row_id_at_ts(
            table=source_table,
            symbol=symbol,
            ts_value=peak_dt,
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

        return {
            "EXCHANGE": exchange,
            "SYMBOL": symbol,
            "INTERVAL": self.cfg.interval,
            "FRVP_PERIOD_TYPE": period,
            "BASED_PERIOD": based_period,

            "MIN_DATE": min_date.to_pydatetime(),
            "HIGHEST_DATE": pd.to_datetime(peak_dt).to_pydatetime(),
            "MAX_DATE": max_date.to_pydatetime(),

            "HIGHEST_ROW_ID": str(highest_row_id) if highest_row_id else None,
            "ROW_COUNT_AFTER_HIGHEST": row_count,
            "DAY_COUNT_AFTER_HIGHEST": day_count,

            "CUTT_OFF_DATE": pd.to_datetime(cutt_off_date).to_pydatetime() if cutt_off_date else None,
            "POC": float(result["POC"]),
            "VAL": float(result["VAL"]),
            "VAH": float(result["VAH"]),
            "RUNTIME": runtime_str,
            "LATEST_CLOSE_VALUE": float(latest_close) if latest_close is not None else None,
        }

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
        p = period.strip().lower().replace(" ", "")

        if "year" in p:
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

    def _period_sort_score(self, period: str) -> int:
        """
        Sorting helper only (short -> long).
        Uses an approximate mapping; does NOT affect actual date math.
        """
        d = self._period_to_delta(period)
        years = d.years or 0
        months = d.months or 0
        days = d.days or 0
        # relativedelta has no .weeks reliably; weeks become days in most cases
        return years * 100000 + months * 1000 + days

    def _sort_periods_short_to_long(self, periods: List[str]) -> List[str]:
        cleaned: List[str] = []
        for p in periods:
            if p and str(p).strip():
                cleaned.append(str(p).strip())

        # Deduplicate while preserving "first seen"
        cleaned = list(dict.fromkeys(cleaned))
        return sorted(cleaned, key=self._period_sort_score)