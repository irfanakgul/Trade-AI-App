from __future__ import annotations

import traceback
from dataclasses import dataclass
from datetime import datetime

from app.infrastructure.database.repository import PostgresRepository


@dataclass(frozen=True)
class BarStatusConfig:
    job_name: str = "ind_bar_status"
    calc_group: str = "BAR_STATUS"
    calc_name: str = "LAST_DAY_OPEN_CLOSE_COMPARE"


class IndBarStatusService:
    def __init__(self, repo: PostgresRepository, cfg: BarStatusConfig = BarStatusConfig()):
        self.repo = repo
        self.cfg = cfg

    def run(
        self,
        exchange: str,
        source_schema: str,
        source_table: str,
        bs_target_schema: str,
        bs_target_table: str,
        is_truncate_scope: bool = True,
    ) -> None:
        exchange = exchange.upper().strip()

        symbols = self.repo.get_bar_status_focus_symbols(exchange=exchange,focus_symbol_schema='silver',focus_symbol_table='cloned_focus_symbol_list')
        if not symbols:
            print(f"[BAR_STATUS] No in-scope symbols found. exchange={exchange}")
            return

        if is_truncate_scope:
            deleted = self.repo.delete_ind_bar_status_scope(
                schema=bs_target_schema,
                table=bs_target_table,
                exchange=exchange,
            )
            print(f"[BAR_STATUS] Cleared output scope: exchange={exchange} deleted_rows={deleted}")

        rows = self.repo.fetch_bar_status_source_rows(
            source_schema=source_schema,
            source_table=source_table,
            exchange=exchange,
            symbols=symbols,
        )

        if not rows:
            print(f"[BAR_STATUS] No source rows returned. exchange={exchange}")
            return

        try:
            created_at = datetime.now()
            out_rows = []

            for row in rows:
                open_price = row["OPEN_PRICE"]
                close_price = row["CLOSE_PRICE"]

                if open_price is None or close_price is None:
                    continue

                open_price = float(open_price)
                close_price = float(close_price)

                differ = round(close_price - open_price, 2)

                if open_price == 0:
                    perc = None
                else:
                    perc = round(((close_price - open_price) / open_price) * 100, 2)

                if close_price > open_price:
                    bar_status = "GREEN"
                elif close_price < open_price:
                    bar_status = "RED"
                else:
                    bar_status = "AMBER"

                out_rows.append(
                    {
                        "EXCHANGE": row["EXCHANGE"],
                        "SYMBOL": row["SYMBOL"],
                        "FIRST_MINUTE": row["FIRST_MINUTE"],
                        "LAST_MINUTE": row["LAST_MINUTE"],
                        "OPEN_PRICE": open_price,
                        "CLOSE_PRICE": close_price,
                        "DIFFER": differ,
                        "PERC": perc,
                        "BAR_STATUS": bar_status,
                        "CREATED_AT": created_at,
                    }
                )

            # 🔍 DEBUG: GREEN, RED, AMBER sayılarını yazdır
            green_count = sum(1 for row in out_rows if row["BAR_STATUS"] == "GREEN")
            red_count = sum(1 for row in out_rows if row["BAR_STATUS"] == "RED")
            amber_count = sum(1 for row in out_rows if row["BAR_STATUS"] == "AMBER")
            
            print(
                f"[BAR_STATUS DEBUG] exchange={exchange} "
                f"GREEN={green_count} RED={red_count} AMBER={amber_count}"
            )

            inserted = self.repo.insert_ind_bar_status_rows(
                target_schema=bs_target_schema,
                target_table=bs_target_table,
                rows=out_rows,
            )

            print(
                f"[BAR_STATUS] exchange={exchange} "
                f"symbols={len(symbols)} inserted={inserted}"
            )

        except Exception as e:
            self.repo.log_indicator_error(
                job_name=self.cfg.job_name,
                calc_group=self.cfg.calc_group,
                calc_name=self.cfg.calc_name,
                exchange=exchange,
                symbol="__ALL__",
                interval="1min",
                frvp_period_type=None,
                error_type=type(e).__name__,
                error_message=str(e),
                error_stack=traceback.format_exc(),
            )
            raise