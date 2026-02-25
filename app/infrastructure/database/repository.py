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
        Returns the number of inserted rows (best-effort via rowcount).
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
            res = conn.execute(stmt, rows)  # executemany
            # rowcount should reflect inserted rows for INSERT..DO NOTHING
            return int(res.rowcount) if res.rowcount is not None and res.rowcount >= 0 else 0
    
    def count_rows(self, schema: str, table: str) -> int:
        """
        Returns total row count of a table.
        """
        # Basic identifier safety
        if not schema.replace("_", "").isalnum():
            raise ValueError("Invalid schema name")
        if not table.replace("_", "").isalnum():
            raise ValueError("Invalid table name")

        q = text(f"SELECT COUNT(*) FROM {schema}.{table};")

        with self.engine.connect() as conn:
            result = conn.execute(q).scalar()

        return int(result)
    
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

    def trim_history_by_peak_or_lookback_ts(
        self,
        schema: str,
        table: str,
        symbol_col: str = "SYMBOL",
        ts_typed_col: str = "TS",
        high_col: str = "HIGH",
        lookback_days: int = 365,
        reference_days_ago: int = 1,
    ) -> int:
        """
        Fast trim using a typed timestamp column (e.g., TS) and an index on (SYMBOL, TS).
        """
        def _is_safe_ident(x: str) -> bool:
            return x.replace("_", "").isalnum()

        for ident in (schema, table, symbol_col, ts_typed_col, high_col):
            if not _is_safe_ident(ident):
                raise ValueError(f"Unsafe identifier: {ident}")

        q = text(f"""
            WITH max_high AS (
                SELECT "{symbol_col}" AS symbol, MAX("{high_col}"::double precision) AS mh
                FROM {schema}.{table}
                GROUP BY "{symbol_col}"
            ),
            peak AS (
                SELECT t."{symbol_col}" AS symbol, MIN(t."{ts_typed_col}") AS peak_ts
                FROM {schema}.{table} t
                JOIN max_high m
                ON t."{symbol_col}" = m.symbol
                AND t."{high_col}"::double precision = m.mh
                GROUP BY t."{symbol_col}"
            ),
            keep AS (
                SELECT
                    p.symbol,
                    CASE
                        WHEN p.peak_ts < (
                            (CURRENT_DATE - (CAST(:reference_days_ago AS int) * INTERVAL '1 day'))
                            - (CAST(:lookback_days AS int) * INTERVAL '1 day')
                        )::timestamp
                        THEN p.peak_ts
                        ELSE (
                            (CURRENT_DATE - (CAST(:reference_days_ago AS int) * INTERVAL '1 day'))
                            - (CAST(:lookback_days AS int) * INTERVAL '1 day')
                        )::timestamp
                    END AS keep_from
                FROM peak p
            )
            DELETE FROM {schema}.{table} t
            USING keep k
            WHERE t."{symbol_col}" = k.symbol
            AND t."{ts_typed_col}" < k.keep_from;
        """)

        with self.engine.begin() as conn:
            res = conn.execute(q, {"lookback_days": lookback_days, "reference_days_ago": reference_days_ago})
            return int(res.rowcount) if res.rowcount is not None and res.rowcount >= 0 else 0
        
    def upsert_ingestion_error(
        self,
        schema: str,
        table: str,
        job_name: str,
        symbol: str,
        exchange: str,
        error_type: str,
        error_message: str,
    ) -> None:
        """
        Upserts an active ingestion error. Requires a UNIQUE constraint on (job_name, exchange, symbol).
        """
        q = text(f"""
            INSERT INTO {schema}.{table}
                (job_name, exchange, symbol, error_type, error_message, updated_at)
            VALUES
                (:job_name, :exchange, :symbol, :error_type, :error_message, :updated_at)
            ON CONFLICT (job_name, exchange, symbol)
            DO UPDATE SET
                error_type = EXCLUDED.error_type,
                error_message = EXCLUDED.error_message,
                updated_at = EXCLUDED.updated_at;
        """)

        with self.engine.begin() as conn:
            conn.execute(
                q,
                {
                    "job_name": job_name,
                    "exchange": exchange,
                    "symbol": symbol,
                    "error_type": error_type,
                    "error_message": error_message,
                    "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
                },
            )


    def clear_ingestion_error(
        self,
        schema: str,
        table: str,
        job_name: str,
        symbol: str,
        exchange: str,
    ) -> None:
        """
        Clears an active ingestion error when a symbol succeeds later.
        """
        q = text(f"""
            DELETE FROM {schema}.{table}
            WHERE job_name = :job_name
            AND exchange = :exchange
            AND symbol = :symbol;
        """)

        with self.engine.begin() as conn:
            conn.execute(q, {"job_name": job_name, "exchange": exchange, "symbol": symbol})

    def get_active_error_symbols(
        self,
        schema: str,
        table: str,
        job_name: str,
        exchange: str,
    ) -> List[str]:
        """
        Returns symbols that still have an active ingestion error record.
        """
        q = text(f"""
            SELECT symbol
            FROM {schema}.{table}
            WHERE job_name = :job_name
            AND exchange = :exchange
            ORDER BY symbol;
        """)

        with self.engine.begin() as conn:
            rows = conn.execute(q, {"job_name": job_name, "exchange": exchange}).fetchall()

        return [r[0] for r in rows]