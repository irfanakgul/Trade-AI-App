from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Any

from app.infrastructure.database.repository import PostgresRepository


@dataclass(frozen=True)
class WatchSignalCheckConfig:
    job_name: str = "watch_signal_check"


class WatchSignalCheckService:
    def __init__(
        self,
        repo: PostgresRepository,
        cfg: WatchSignalCheckConfig = WatchSignalCheckConfig(),
    ):
        self.repo = repo
        self.cfg = cfg

    def run(
        self,
        exchange: str,
        input_schema: str,
        input_table: str,
        output_schema: str,
        output_table: str,
        log_schema: str,
        log_table: str,
        open_hour: int,
        open_minute: int,
        close_hour: int,
        close_minute: int,
        is_truncate_output: bool = True,
    ) -> Dict[str, Any]:
        exchange = exchange.upper().strip()

        if is_truncate_output:
            deleted = self.repo.truncate_table(output_schema, output_table)
            deleted = 0 if deleted is None else int(deleted)
        else:
            deleted = 0

        rows = self.repo.build_watch_signal_check_rows(
            exchange=exchange,
            input_schema=input_schema,
            input_table=input_table,
            open_hour=int(open_hour),
            open_minute=int(open_minute),
            close_hour=int(close_hour),
            close_minute=int(close_minute),
        )

        inserted_output = 0
        inserted_log = 0
        buy_rows: List[Dict[str, Any]] = []

        if rows:
            inserted_output = self.repo.insert_watch_signal_check_rows(
                schema=output_schema,
                table=output_table,
                rows=rows,
            )
            inserted_log = self.repo.insert_watch_signal_check_rows(
                schema=log_schema,
                table=log_table,
                rows=rows,
            )
            buy_rows = [r for r in rows if r.get("STATUS") == "BUY"]

        telegram_lines: List[str] = []

        for r in buy_rows:
            price = f"{float(r['CLOSE']):.2f}" if r.get("CLOSE") is not None else "None"
            line = (
                f"{r['SYMBOL']} | "
                f"PRICE: {price} | "
                f"SIGNAL: {r['STATUS']}"
            )
            print(line)
            telegram_lines.append(line)

        telegram_text = "\n".join(telegram_lines)

        return {
            "deleted_output_rows": deleted,
            "inserted_output_rows": int(inserted_output),
            "inserted_log_rows": int(inserted_log),
            "buy_count": int(len(buy_rows)),
            "total_rows": int(len(rows)),
            "telegram_text": telegram_text
            }