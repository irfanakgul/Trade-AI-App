# app/infrastructure/database/repository.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple, Dict, Any
from sqlalchemy import text
from sqlalchemy.engine import Engine
from datetime import datetime


@dataclass(frozen=True)
class InScopeSymbol:
    symbol: str
    exchange: str


class PostgresRepository:
    def __init__(self, engine: Engine):
        self.engine = engine

    def get_in_scope_symbols(self, exchange: str, schema: str) -> List[str]:
        """
        Returns symbols where in_scope = true and EXCHANGE matches.
        """
        q = text(f"""
            SELECT "SYMBOL"
            FROM {schema}.scope_symbol_list
            WHERE in_scope = true
            AND "EXCHANGE" = :exchange
            ORDER BY "SYMBOL";
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(q, {"exchange": exchange}).fetchall()

        return [r[0] for r in rows]

    def get_last_timestamp(
        self,
        symbol: str,
        schema: str,
        table: str,
        ts_column: str = "TIMESTAMP",
    ) -> Optional[datetime]:
        """
        Returns MAX(timestamp) casted to timestamp.
        Works even if TIMESTAMP is stored as text.
        """
        q = text(f"""
            SELECT MAX(("{ts_column}")::timestamp) AS max_ts
            FROM {schema}.{table}
            WHERE "SYMBOL" = :symbol;
        """)
        with self.engine.connect() as conn:
            row = conn.execute(q, {"symbol": symbol}).fetchone()
        return row[0] if row and row[0] else None

    def bulk_insert_on_conflict_do_nothing(
        self,
        schema: str,
        table: str,
        rows: Sequence[Dict[str, Any]],
        conflict_column: str = "ROW_ID",
    ) -> int:
        """
        Inserts rows using INSERT ... ON CONFLICT DO NOTHING.
        Returns number of rows attempted (not necessarily inserted).
        """
        if not rows:
            return 0

        cols = list(rows[0].keys())
        col_list = ", ".join([f'"{c}"' for c in cols])
        placeholders = ", ".join([f":{c}" for c in cols])

        stmt = text(f"""
            INSERT INTO {schema}.{table} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT ("{conflict_column}") DO NOTHING;
        """)

        with self.engine.begin() as conn:
            conn.execute(stmt, rows)  # SQLAlchemy will executemany
        return len(rows)
    
# func for error symbols. it will be retry in the end
def log_ingestion_error(
    self,
    schema: str,
    table: str,
    job_name: str,
    symbol: str,
    exchange: str,
    error_type: str,
    error_message: str,
) -> None:
    q = text(f"""
        INSERT INTO {schema}.{table}
        (job_name, symbol, exchange, error_type, error_message)
        VALUES (:job_name, :symbol, :exchange, :error_type, :error_message);
    """)
    with self.engine.begin() as conn:
        conn.execute(
            q,
            {
                "job_name": job_name,
                "symbol": symbol,
                "exchange": exchange,
                "error_type": error_type,
                "error_message": error_message,
            },
        )