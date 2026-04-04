from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, time
from typing import Optional

from app.infrastructure.database.repository import PostgresRepository


@dataclass(frozen=True)
class EndDatesServiceConfig:
    job_name: str = "ind_end_dates"
    calc_group: str = "END_DATES"
    calc_name: str = "LAST_TIMESTAMPS"


class IndEndDatesService:
    def __init__(self, repo: PostgresRepository, cfg: EndDatesServiceConfig = EndDatesServiceConfig()):
        self.repo = repo
        self.cfg = cfg

    def _get_previous_trading_day(self, dt: datetime) -> datetime.date:
        d = dt.date() - timedelta(days=1)

        # weekend logic only
        # Saturday -> Friday
        # Sunday   -> Friday
        while d.weekday() >= 5:
            d -= timedelta(days=1)

        return d

    def _build_expected_end_date(
        self,
        now_dt: datetime,
        market_close_hour: int,
        market_close_minute: int,
    ) -> datetime:
        prev_trading_day = self._get_previous_trading_day(now_dt)
        return datetime.combine(
            prev_trading_day,
            time(hour=market_close_hour, minute=market_close_minute)
        )

    def run(
        self,
        exchange: str,
        raw_schema: str,
        raw_table: str,
        raw_ts_col: str,
        bronze_schema: str,
        bronze_table: str,
        bronze_ts_col: str,
        silver_schema: str,
        silver_table: str,
        silver_ts_col: str,
        silver_conv_schema: str,
        silver_conv_table: str,
        silver_conv_ts_col: str,
        market_close_hour: int = 0,
        market_close_minute: int = 0,
        is_truncate_scope: bool = True,
    ) -> None:
        exchange = exchange.upper().strip()

        symbols = self.repo.get_cloned_focus_symbols(exchange=exchange)
        if not symbols:
            print(f"[END-DATES] No scope symbols found. exchange={exchange}")
            return

        if is_truncate_scope:
            deleted = self.repo.delete_ind_end_dates_scope(exchange=exchange)
            print(f"[END-DATES] Cleared output scope: exchange={exchange} deleted_rows={deleted}")

        created_at = datetime.now()
        expected_end_date = self._build_expected_end_date(
            now_dt=created_at,
            market_close_hour=int(market_close_hour),
            market_close_minute=int(market_close_minute),
        )

        raw_map = self.repo.fetch_symbol_end_dates(
            schema=raw_schema,
            table=raw_table,
            symbols=symbols,
            ts_col=raw_ts_col,
        )

        bronze_map = self.repo.fetch_symbol_end_dates(
            schema=bronze_schema,
            table=bronze_table,
            symbols=symbols,
            ts_col=bronze_ts_col,
        )

        silver_map = self.repo.fetch_symbol_end_dates(
            schema=silver_schema,
            table=silver_table,
            symbols=symbols,
            ts_col=silver_ts_col,
        )

        silver_conv_map = self.repo.fetch_symbol_end_dates(
            schema=silver_conv_schema,
            table=silver_conv_table,
            symbols=symbols,
            ts_col=silver_conv_ts_col,
        )

        out_rows = []
        for symbol in symbols:
            out_rows.append({
                "EXCHANGE": exchange,
                "EXPECTED_END_DATE": expected_end_date,
                "SYMBOL": symbol,
                "RAW_END_DATE": raw_map.get(symbol),
                "BRONZE_END_DATE": bronze_map.get(symbol),
                "SILVER_END_DATE": silver_map.get(symbol),
                "SILVER_CONVERTED_END_DATE": silver_conv_map.get(symbol),
                "CREATED_AT": created_at,
            })

        inserted = self.repo.insert_ind_end_dates_rows(out_rows)

        print(
            f"[END-DATES] exchange={exchange} "
            f"symbols={len(symbols)} inserted={inserted} "
            f"expected_end_date={expected_end_date}"
        )