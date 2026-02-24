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

def trim_history_by_peak_or_lookback(
    self,
    schema: str,
    table: str,
    symbol_col: str = "SYMBOL",
    ts_col: str = "TIMESTAMP",
    high_col: str = "HIGH",
    lookback_days: int = 365,
    reference_days_ago: int = 1,
) -> int:
    """
    Trims per-symbol history in-place:
      - Find earliest timestamp where HIGH is max for the symbol (peak_ts).
      - Define cutoff = (CURRENT_DATE - reference_days_ago) - lookback_days.
      - If peak_ts < cutoff: keep from peak_ts onward, else keep only last lookback window.
      - Delete rows older than keep_from per symbol.

    Returns number of deleted rows (best effort; may be -1 depending on driver).
    """

    # Minimal identifier validation to avoid accidental injection
    def _is_safe_ident(x: str) -> bool:
        return x.replace("_", "").isalnum()

    for ident in (schema, table, symbol_col, ts_col, high_col):
        if not _is_safe_ident(ident):
            raise ValueError(f"Unsafe identifier: {ident}")

    q = text(f"""
        WITH base AS (
            SELECT
                "{symbol_col}" AS symbol,
                ("{ts_col}")::timestamp AS ts,
                "{high_col}"::double precision AS high
            FROM {schema}.{table}
            WHERE "{symbol_col}" IS NOT NULL
              AND "{ts_col}" IS NOT NULL
              AND "{high_col}" IS NOT NULL
        ),
        max_high AS (
            SELECT symbol, MAX(high) AS mh
            FROM base
            GROUP BY symbol
        ),
        peak AS (
            -- Earliest timestamp where HIGH equals the per-symbol maximum
            SELECT b.symbol, MIN(b.ts) AS peak_ts
            FROM base b
            JOIN max_high m
              ON b.symbol = m.symbol
             AND b.high = m.mh
            GROUP BY b.symbol
        ),
        cutoff AS (
            SELECT
                p.symbol,
                p.peak_ts,
                (
                    (CURRENT_DATE - (:reference_days_ago::int) * INTERVAL '1 day')
                    - (:lookback_days::int) * INTERVAL '1 day'
                )::timestamp AS cutoff_ts
            FROM peak p
        ),
        keep AS (
            SELECT
                c.symbol,
                CASE
                    WHEN c.peak_ts < c.cutoff_ts THEN c.peak_ts
                    ELSE c.cutoff_ts
                END AS keep_from
            FROM cutoff c
        ),
        to_delete AS (
            SELECT t.ctid
            FROM {schema}.{table} t
            JOIN keep k
              ON t."{symbol_col}" = k.symbol
            WHERE (t."{ts_col}")::timestamp < k.keep_from
        )
        DELETE FROM {schema}.{table} t
        USING to_delete d
        WHERE t.ctid = d.ctid;
    """)

    with self.engine.begin() as conn:
        res = conn.execute(
            q,
            {"lookback_days": lookback_days, "reference_days_ago": reference_days_ago},
        )

    # res.rowcount may be -1 for some drivers; still OK
    return res.rowcount