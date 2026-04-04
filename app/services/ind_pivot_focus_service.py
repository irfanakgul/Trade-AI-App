from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd

from app.infrastructure.database.repository import PostgresRepository
from app.core.indicators.pivot.pivot_math import calculate_camarilla_pivots


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

        # Scope symbols artık cloned_focus_symbol_list'ten geliyor
        scope_symbols = self.repo.get_cloned_focus_symbols(exchange=exchange)
        if not scope_symbols:
            print(f"[PIVOT] No scope symbols found. exchange={exchange}")
            return

        if is_truncate_scope:
            deleted = self.repo.delete_indicator_scope_by_exchange(
                schema=output_schema,
                table=output_table,
                exchange=exchange,
            )
            print(f"[PIVOT] Cleared output scope: exchange={exchange} deleted_rows={deleted}")

        rows = self.repo.fetch_last_n_days_ohlc_for_symbols(
            schema=input_schema,
            table=input_table,
            exchange=exchange,
            symbols=scope_symbols,
            n_days=lookback_days,
            ts_col="TIMESTAMP",
            high_col="HIGH",
            low_col="LOW",
            close_col="CLOSE",
        )

        if not rows:
            print(f"[PIVOT] No input rows returned. exchange={exchange}")
            # yine de her scope symbol için FAILED satır yaz
            created_at = datetime.now()
            failed_rows = [{
                "EXCHANGE": exchange,
                "SYMBOL": s,
                "PVT_START_DATE": None,
                "PVT_END_DATE": None,
                "PVT_YEAR": None,
                "PIVOT": None,
                "PVT_R1": None,
                "PVT_R2": None,
                "PVT_R3": None,
                "PVT_R4": None,
                "PVT_R5": None,
                "PVT_S1": None,
                "PVT_S2": None,
                "PVT_S3": None,
                "PVT_S4": None,
                "PVT_S5": None,
                "STATUS": "FAILED",
                "CREATED_AT": created_at,
            } for s in scope_symbols]

            inserted = self.repo.insert_ind_pivot_focus_rows(
                schema=output_schema,
                table=output_table,
                rows=failed_rows,
            )
            print(f"[PIVOT] exchange={exchange} failed_symbols={len(scope_symbols)} inserted={inserted}")
            return

        df = pd.DataFrame(rows)
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
        df["HIGH"] = pd.to_numeric(df["HIGH"], errors="coerce")
        df["LOW"] = pd.to_numeric(df["LOW"], errors="coerce")
        df["CLOSE"] = pd.to_numeric(df["CLOSE"], errors="coerce")

        df = df.dropna(subset=["EXCHANGE", "SYMBOL", "TIMESTAMP", "HIGH", "LOW", "CLOSE"])
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

        # latest row per symbol from successful results
        if not df_result.empty:
            df_result = df_result.sort_values(["SYMBOL", "TIMESTAMP"]).reset_index(drop=True)
            latest_rows = (
                df_result.groupby("SYMBOL", as_index=False, group_keys=False)
                .tail(1)
                .reset_index(drop=True)
            )
        else:
            latest_rows = pd.DataFrame(columns=["SYMBOL"])

        # input filtered dataset start/end dates
        date_ranges = (
            df.groupby("SYMBOL", as_index=False)
            .agg(
                PVT_START_DATE=("TIMESTAMP", "min"),
                PVT_END_DATE=("TIMESTAMP", "max"),
            )
        )

        latest_rows = latest_rows.merge(date_ranges, on="SYMBOL", how="left")

        # success map
        success_map = {}
        for _, row in latest_rows.iterrows():
            success_map[str(row["SYMBOL"])] = {
                "EXCHANGE": exchange,
                "SYMBOL": str(row["SYMBOL"]),
                "PVT_START_DATE": pd.to_datetime(row["PVT_START_DATE"]).to_pydatetime() if pd.notna(row["PVT_START_DATE"]) else None,
                "PVT_END_DATE": pd.to_datetime(row["PVT_END_DATE"]).to_pydatetime() if pd.notna(row["PVT_END_DATE"]) else None,
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
                "STATUS": "PASSED",
            }

        # input date ranges for failed rows too
        date_range_map = {}
        for _, row in date_ranges.iterrows():
            date_range_map[str(row["SYMBOL"])] = {
                "PVT_START_DATE": pd.to_datetime(row["PVT_START_DATE"]).to_pydatetime() if pd.notna(row["PVT_START_DATE"]) else None,
                "PVT_END_DATE": pd.to_datetime(row["PVT_END_DATE"]).to_pydatetime() if pd.notna(row["PVT_END_DATE"]) else None,
            }

        created_at = datetime.now()
        out_rows: List[Dict[str, Any]] = []

        for symbol in scope_symbols:
            if symbol in success_map:
                row = dict(success_map[symbol])
                row["CREATED_AT"] = created_at
                out_rows.append(row)
            else:
                dr = date_range_map.get(symbol, {})
                out_rows.append({
                    "EXCHANGE": exchange,
                    "SYMBOL": symbol,
                    "PVT_START_DATE": dr.get("PVT_START_DATE"),
                    "PVT_END_DATE": dr.get("PVT_END_DATE"),
                    "PVT_YEAR": None,
                    "PIVOT": None,
                    "PVT_R1": None,
                    "PVT_R2": None,
                    "PVT_R3": None,
                    "PVT_R4": None,
                    "PVT_R5": None,
                    "PVT_S1": None,
                    "PVT_S2": None,
                    "PVT_S3": None,
                    "PVT_S4": None,
                    "PVT_S5": None,
                    "STATUS": "FAILED",
                    "CREATED_AT": created_at,
                })

        inserted = self.repo.insert_ind_pivot_focus_rows(
            schema=output_schema,
            table=output_table,
            rows=out_rows,
        )

        passed_cnt = sum(1 for r in out_rows if r["STATUS"] == "PASSED")
        failed_cnt = sum(1 for r in out_rows if r["STATUS"] == "FAILED")

        print(
            f"[PIVOT] exchange={exchange} "
            f"lookback_days={lookback_days} "
            f"scope_symbols={len(scope_symbols)} "
            f"passed={passed_cnt} failed={failed_cnt} inserted={inserted}"
        )